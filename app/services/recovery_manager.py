"""Recovery and reconciliation.

Runs at start-up (after a crash) and periodically. It is idempotent and safe
to run on several workers at once, because every repair is a conditional
update or goes through the idempotent event processor.

Principles
----------
* **The database is the source of truth for intent.** The provider is the
  source of truth for what physically happened to a call.
* **Never release an agent just because a worker died.** If the provider might
  still have a live call, the agent stays reserved until the provider is asked
  (``get_call_status``) or a hard deadline passes.
* Reconciliation results are injected as synthetic provider events
  (``event_id = reconcile:...``) into the *same* idempotent event processor,
  so there is a single code path for state transitions.
"""
from __future__ import annotations

from collections import Counter
from typing import Optional

from app.clock import Clock
from app.config import DialerConfig
from app.database.connection import Database
from app.database.repositories import AgentRepository, CallRepository
from app.logging_setup import get_logger, log_event
from app.models.agent import AgentStatus
from app.models.call import Binding, Call, CallOutcome, CallStatus, PRE_ANSWER_STATES
from app.models.provider_event import EventResult, ProviderEvent, ProviderEventType
from app.providers.base_provider import ProviderTimeout
from app.providers.router import ProviderRouter
from app.services.call_allocator import CallAllocator
from app.services.call_lifecycle import CallLifecycle
from app.services.event_processor import EventProcessor

log = get_logger("recovery")
CS = CallStatus


class RecoveryManager:
    def __init__(self, db: Database, clock: Clock, router: ProviderRouter, processor: EventProcessor,
                 allocator: CallAllocator, lifecycle: CallLifecycle, dialer_cfg: DialerConfig,
                 worker_id: str = "recovery") -> None:
        self._db = db
        self._clock = clock
        self._router = router
        self._processor = processor
        self._allocator = allocator
        self._lifecycle = lifecycle
        self._cfg = dialer_cfg
        self._calls = CallRepository(db)
        self._agents = AgentRepository(db)
        self.worker_id = worker_id

    # ================================================================== run
    def run(self) -> dict[str, int]:
        now = self._clock.now()
        report: Counter[str] = Counter()
        report["permits_expired"] = self._db.execute(
            "UPDATE dial_permits SET status='EXPIRED' WHERE status='ACTIVE' AND expires_at <= ?", (now,)).rowcount

        # 1. Setup leases that expired: the worker crashed or the initiate timed out.
        for row in self._db.query("SELECT id FROM calls WHERE status='RESERVED' AND lease_until < ?", (now,)):
            report["setup_" + self.reconcile_call(row["id"])] += 1

        # 2. In-flight calls that went silent (lost webhooks, provider outage).
        silence = self._cfg.ring_timeout_s + self._cfg.event_silence_reconcile_s
        for row in self._db.query(
                """SELECT id FROM calls WHERE status IN ('INITIATED','RINGING','ANSWERED')
                   AND COALESCE(last_event_at, initiated_at, created_at) < ?""", (now - silence,)):
            report["silent_" + self.reconcile_call(row["id"])] += 1
        for row in self._db.query(
                "SELECT id FROM calls WHERE status='CONNECTED' AND COALESCE(last_event_at, connected_at) < ?",
                (now - self._cfg.max_call_duration_s,)):
            report["long_" + self.reconcile_call(row["id"])] += 1

        # 3. Cancellations whose hangup never got through.
        for row in self._db.query(
                """SELECT id FROM calls WHERE cancel_requested_at IS NOT NULL AND cancel_requested_at < ?
                   AND status IN ('QUEUED','RESERVED','INITIATED','RINGING')""", (now - 10.0,)):
            call = self._calls.get(row["id"])
            if call and self._allocator.hangup(call):
                report["hangup_retried"] += 1

        # 4. Consistency repair: agents attached to calls that are over.
        for row in self._db.query(
                """SELECT a.id AS agent_id, a.current_call_id, a.status, c.status AS call_status
                   FROM agents a LEFT JOIN calls c ON c.id = a.current_call_id
                   WHERE a.current_call_id IS NOT NULL
                     AND (c.id IS NULL OR c.status IN ('COMPLETED','FAILED','CANCELLED'))"""):
            target = AgentStatus.WRAP_UP if row["status"] == "CONNECTED" else AgentStatus.AVAILABLE
            wrap = now + 1.0 if target == AgentStatus.WRAP_UP else None
            if self._agents.release_from_call(row["agent_id"], row["current_call_id"], target, now, wrap):
                report["agents_repaired"] += 1

        # 5. Consistency repair: borrowers reserved by calls that are over.
        report["borrowers_repaired"] = self._db.execute(
            """UPDATE borrowers SET status='RETRY_WAIT', active_call_id=NULL, reserved_by=NULL,
                   reserved_until=NULL, next_eligible_at=?, updated_at=?
               WHERE status='RESERVED' AND NOT EXISTS (
                   SELECT 1 FROM calls c WHERE c.id = borrowers.active_call_id
                   AND c.status IN ('QUEUED','RESERVED','INITIATED','RINGING','ANSWERED','CONNECTED'))""",
            (now + self._cfg.setup_failure_retry_s, now)).rowcount

        result = {k: v for k, v in report.items() if v}
        if result:
            log_event(log, "recovery_run", at=round(now, 3), **result)
        return result

    # ============================================================ reconcile
    def reconcile_call(self, call_id: str) -> str:
        call = self._calls.get(call_id)
        if call is None or call.is_terminal:
            return "skipped"
        now = self._clock.now()
        provider = self._router.get(call.provider) if call.provider else None
        if provider is None:
            with self._db.transaction():
                fresh = self._calls.get(call_id)
                if fresh and fresh.status == CS.RESERVED:
                    self._lifecycle.finish(fresh, CS.CANCELLED, CallOutcome.CANCELLED_NOT_PLACED)
            return "not_placed"
        try:
            status = provider.get_call_status(call.id)
        except ProviderTimeout:
            age = now - call.created_at
            if age > self._cfg.max_call_duration_s:
                with self._db.transaction():
                    fresh = self._calls.get(call_id)
                    if fresh and not fresh.is_terminal:
                        self._lifecycle.finish(fresh, CS.FAILED, CallOutcome.UNRESOLVED)
                log_event(log, "call_unresolved", level=40, call_id=call_id, age_s=round(age, 1))
                return "unresolved"
            # Unknown: keep every reservation (safety over utilisation) and check
            # again after another lease period instead of hammering the provider.
            if call.status == CS.RESERVED:
                self._calls.set_fields(call_id, now, lease_until=now + self._cfg.setup_lease_s)
            else:
                self._calls.set_fields(call_id, now, last_event_at=now)
            return "unreachable"

        if status is None:
            with self._db.transaction():
                fresh = self._calls.get(call_id)
                if fresh and fresh.status == CS.RESERVED:
                    # The provider never received it: safe to release and retry later.
                    self._lifecycle.finish(fresh, CS.CANCELLED, CallOutcome.CANCELLED_NOT_PLACED)
                elif fresh and not fresh.is_terminal:
                    self._lifecycle.finish(fresh, CS.FAILED, CallOutcome.PROVIDER_FAILED)
            log_event(log, "reconciled_not_placed", call_id=call_id)
            return "not_placed"

        event = ProviderEvent(
            provider=provider.name,
            event_id=f"reconcile:{status.provider_call_id}:{status.sequence}:{status.state}",
            client_ref=call.id, provider_call_id=status.provider_call_id,
            event_type=ProviderEventType(status.state), sequence=status.sequence, occurred_at=now,
            payload={"answered": status.answered, "talk_duration_s": status.talk_duration_s or 0.0,
                     "reconciled": True})
        result = self._processor.handle(event)
        # Whatever happened, we just heard from the provider: reset the silence timer.
        self._db.execute("UPDATE calls SET last_event_at=?, lease_until=NULL, lease_owner=NULL WHERE id=?",
                         (now, call_id))
        log_event(log, "reconciled", call_id=call_id, provider_state=status.state, result=result.value)
        return "adopted" if result == EventResult.APPLIED else "confirmed"

    # ============================================================ agent lost
    def handle_agent_lost(self, agent_id: str, call_id: str) -> str:
        """The agent attached to ``call_id`` disappeared involuntarily."""
        call = self._calls.get(call_id)
        if call is None or call.is_terminal:
            return "nothing_to_do"
        now = self._clock.now()
        if call.status in PRE_ANSWER_STATES:
            target = AgentStatus.RESERVED if call.status in (CS.QUEUED, CS.RESERVED) else AgentStatus.DIALING
            with self._db.transaction():
                new_agent = self._agents.reserve_any(call_id, self.worker_id, now,
                                                     now + self._cfg.agent_reservation_lease_s)
                if new_agent is not None:
                    if target == AgentStatus.DIALING:
                        self._agents.mark_dialing(new_agent, call_id, now)
                    self._calls.set_fields(call_id, now, agent_id=new_agent)
            if new_agent is not None:
                log_event(log, "call_rebound", call_id=call_id, lost_agent=agent_id, new_agent=new_agent)
                return "rebound"
            self._allocator.request_cancel([call_id], CallOutcome.CANCELLED_AGENT_LOST)
            log_event(log, "call_cancel_agent_lost", call_id=call_id, lost_agent=agent_id)
            return "cancel_requested"
        # Mid-conversation: end the borrower's leg politely; outcome AGENT_DROPPED.
        self._db.execute("UPDATE calls SET cancel_reason=?, updated_at=? WHERE id=?",
                         (CallOutcome.AGENT_DROPPED.value, now, call_id))
        self._allocator.hangup(self._calls.get(call_id))
        return "hangup_connected"
