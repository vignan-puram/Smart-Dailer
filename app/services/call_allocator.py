"""Call Allocator: turns an approved permit into real calls.

Boundary enforcement
--------------------
``execute`` accepts only a ``DialPermit``. Before every single call it consumes
one permit slot with a conditional UPDATE (``status='ACTIVE' AND consumed <
approved AND expires_at > now``). A fabricated, expired, revoked or exhausted
permit yields zero rows, so no call is placed. The permit's *binding* is read
back from the database row, not from the Python object, so a caller cannot
turn a BOUND permit into a pooled one.

Per-call sequence (one transaction)
-----------------------------------
consume slot -> reserve borrower -> [BOUND: reserve agent] -> choose provider
-> insert call row (status RESERVED, setup lease). Any failure rolls back
*everything*, including the consumed slot.

Then, outside the transaction, the provider is contacted:
* success  -> INITIATED (+ agent DIALING), in one transaction;
* rejected -> definitive: call FAILED, agent and borrower released immediately;
* timeout  -> ambiguous: reservations are KEPT. The setup lease expires and
  the RecoveryManager asks the provider what really happened.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Optional

from app.clock import Clock
from app.config import DialerConfig, PacingConfig
from app.database.connection import Database
from app.database.repositories import (
    AgentRepository,
    BorrowerRepository,
    CallRepository,
    PacingStatsRepository,
)
from app.faults import CrashHook, maybe
from app.logging_setup import get_logger, log_event
from app.models.call import Binding, Call, CallOutcome, CallStatus, PRE_ANSWER_STATES
from app.models.safety import DialPermit
from app.providers.base_provider import CallRequest, ProviderRejected, ProviderTimeout
from app.providers.router import ProviderRouter
from app.services.call_lifecycle import CallLifecycle

log = get_logger("call_allocator")


class UnauthorizedDialError(PermissionError):
    """Raised when asked to dial without a valid Safety Controller permit."""


class _Abort(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass
class AllocationResult:
    permit_id: str
    approved: int
    placed: int = 0
    setup_failed: int = 0
    ambiguous: int = 0
    stopped_reason: Optional[str] = None
    call_ids: list[str] = field(default_factory=list)


class CallAllocator:
    def __init__(self, db: Database, clock: Clock, router: ProviderRouter, lifecycle: CallLifecycle,
                 dialer_cfg: DialerConfig, pacing_cfg: PacingConfig, stats: PacingStatsRepository,
                 worker_id: str = "worker-1", crash_hook: Optional[CrashHook] = None) -> None:
        self._db = db
        self._clock = clock
        self._router = router
        self._lifecycle = lifecycle
        self._dcfg = dialer_cfg
        self._pcfg = pacing_cfg
        self._stats = stats
        self.worker_id = worker_id
        self.crash_hook = crash_hook
        self._agents = AgentRepository(db)
        self._borrowers = BorrowerRepository(db)
        self._calls = CallRepository(db)

    # ================================================================ execute
    def execute(self, permit: DialPermit) -> AllocationResult:
        if not isinstance(permit, DialPermit):
            raise UnauthorizedDialError("the allocator only accepts DialPermit objects")
        row = self._db.query_one("SELECT * FROM dial_permits WHERE id=?", (permit.permit_id,))
        now = self._clock.now()
        if row is None:
            raise UnauthorizedDialError(f"permit {permit.permit_id} was not issued by the Safety Controller")
        if row["campaign_id"] != permit.campaign_id:
            raise UnauthorizedDialError("permit/campaign mismatch")
        if row["status"] != "ACTIVE" or row["expires_at"] <= now:
            raise UnauthorizedDialError(f"permit {permit.permit_id} is {row['status']} / expired")

        binding = Binding(row["binding"])          # authoritative: from the DB row
        result = AllocationResult(permit.permit_id, row["approved"])
        for _ in range(row["approved"] - row["consumed"]):
            call_id = f"call-{uuid.uuid4().hex[:16]}"
            try:
                info = self._reserve(permit.permit_id, permit.campaign_id, binding, call_id)
            except _Abort as abort:
                result.stopped_reason = abort.reason
                break
            outcome = self._initiate(info)
            result.call_ids.append(call_id)
            if outcome == "placed":
                result.placed += 1
            elif outcome == "ambiguous":
                result.ambiguous += 1
            else:
                result.setup_failed += 1
        # Unused slots must not keep holding capacity.
        self._db.execute("UPDATE dial_permits SET status='EXHAUSTED' WHERE id=? AND status='ACTIVE'",
                         (permit.permit_id,))
        log_event(log, "allocation", permit_id=permit.permit_id, binding=binding.value, approved=row["approved"],
                  placed=result.placed, setup_failed=result.setup_failed, ambiguous=result.ambiguous,
                  stopped=result.stopped_reason, at=round(now, 3))
        return result

    # ================================================================ reserve
    def _reserve(self, permit_id: str, campaign_id: str, binding: Binding, call_id: str) -> dict:
        now = self._clock.now()
        lease = now + self._dcfg.setup_lease_s
        with self._db.transaction():
            consumed = self._db.execute(
                """UPDATE dial_permits SET consumed=consumed+1,
                       status = CASE WHEN consumed+1 >= approved THEN 'EXHAUSTED' ELSE status END
                   WHERE id=? AND status='ACTIVE' AND consumed < approved AND expires_at > ?""",
                (permit_id, now)).rowcount
            if not consumed:
                raise _Abort("permit exhausted, expired or revoked")
            campaign = self._db.query_one("SELECT max_attempts, status FROM campaigns WHERE id=?", (campaign_id,))
            if campaign is None or campaign["status"] != "RUNNING":
                raise _Abort("campaign not running")
            borrower = self._borrowers.reserve_next(campaign_id, call_id, self.worker_id, now, lease,
                                                    campaign["max_attempts"])
            if borrower is None:
                raise _Abort("no eligible borrower")
            agent_id = None
            if binding == Binding.BOUND:
                agent_id = self._agents.reserve_any(call_id, self.worker_id, now,
                                                    now + self._dcfg.agent_reservation_lease_s)
                if agent_id is None:
                    raise _Abort("no AVAILABLE agent (agent disappeared or taken by another worker)")
            provider = self._router.select()
            if provider is None:
                raise _Abort("no provider available (circuits open)")
            self._calls.insert(call_id=call_id, campaign_id=campaign_id, borrower_id=borrower["id"],
                               agent_id=agent_id, binding=binding, permit_id=permit_id,
                               attempt_seq=borrower["call_count"], provider=provider.name, now=now,
                               lease_owner=self.worker_id, lease_until=lease)
        return {"call_id": call_id, "phone": borrower["phone"], "campaign_id": campaign_id,
                "agent_id": agent_id, "provider": provider.name}

    # =============================================================== initiate
    def _initiate(self, info: dict) -> str:
        call_id = info["call_id"]
        provider = self._router.get(info["provider"])
        breaker = self._router.breaker
        maybe(self.crash_hook, "before_initiate", call_id)
        # Re-check right before dialing: the agent may have vanished (and the
        # call been cancelled or re-bound) since the reservation committed.
        current = self._calls.get(call_id)
        if current is None or current.status != CallStatus.RESERVED or current.cancel_requested_at is not None:
            log_event(log, "initiate_skipped", call_id=call_id,
                      status=current.status.value if current else None)
            return "failed"
        try:
            result = provider.initiate_call(CallRequest(call_id, info["phone"], info["campaign_id"]))
        except ProviderRejected as exc:
            breaker.record_failure(provider.name, provider.profile.setup_latency_ms
                                   if hasattr(provider, "profile") else 0.0, timeout=False)
            self._setup_failed(call_id, str(exc))
            return "failed"
        except ProviderTimeout as exc:
            timeout_ms = provider.profile.timeout_ms if hasattr(provider, "profile") else 5000.0
            breaker.record_failure(provider.name, timeout_ms, timeout=True)
            # Ambiguous: the provider may be ringing the borrower right now. Keep
            # agent + borrower reserved. Recovery reconciles once the lease expires.
            log_event(log, "initiate_ambiguous", call_id=call_id, provider=provider.name, error=str(exc))
            return "ambiguous"
        maybe(self.crash_hook, "after_initiate", call_id)
        breaker.record_success(provider.name, result.latency_ms)
        now = self._clock.now()
        with self._db.transaction():
            self._calls.record_initiated(call_id, result.provider_call_id, now, result.latency_ms)
            if info["agent_id"]:
                self._agents.mark_dialing(info["agent_id"], call_id, now)
            after = self._calls.get(call_id)
        if after is not None and (after.is_terminal or after.cancel_requested_at is not None):
            # Cancelled while the provider was dialing (narrow race): hang up at once.
            try:
                provider.hangup(result.provider_call_id)
            except ProviderTimeout:
                # Not silent: if the leg is answered later, EventProcessor hangs up a late
                # ANSWERED on a cancelled call; cancel-requested calls are retried by recovery.
                log_event(log, "hangup_failed_after_cancel", level=30, call_id=call_id,
                          provider_call_id=result.provider_call_id)
            return "failed"
        return "placed"

    def _setup_failed(self, call_id: str, error: str) -> None:
        with self._db.transaction():
            call = self._calls.get(call_id)
            if call is not None and call.status == CallStatus.RESERVED:
                self._lifecycle.finish(call, CallStatus.FAILED, CallOutcome.SETUP_FAILED)
        log_event(log, "setup_failed", call_id=call_id, error=error)

    # ================================================================ cancel
    def request_cancel(self, call_ids: list[str], reason: CallOutcome) -> int:
        """Ask providers to hang up pre-answer calls (capacity deficit / agent lost).

        The call row is only marked cancel-requested. It becomes CANCELLED when
        the provider confirms the call ended unanswered. If the borrower answers
        first, the event processor still tries to give them an agent.
        """
        now = self._clock.now()
        done = 0
        for call_id in call_ids:
            self._db.execute("UPDATE calls SET cancel_requested_at=COALESCE(cancel_requested_at, ?), "
                             "cancel_reason=COALESCE(cancel_reason, ?), updated_at=? WHERE id=?",
                             (now, reason.value, now, call_id))
            call = self._calls.get(call_id)
            if call is None or call.status not in PRE_ANSWER_STATES:
                continue
            if self.hangup(call):
                done += 1
        return done

    def hangup(self, call: Call) -> bool:
        provider = self._router.get(call.provider) if call.provider else None
        if provider is None:
            return False
        try:
            pcid = call.provider_call_id
            if pcid is None:
                status = provider.get_call_status(call.id)
                if status is None:
                    # Provider never saw it: nothing to hang up. Finish locally.
                    with self._db.transaction():
                        fresh = self._calls.get(call.id)
                        if fresh and fresh.status in PRE_ANSWER_STATES:
                            self._lifecycle.finish(fresh, CallStatus.CANCELLED, CallOutcome.CANCELLED_NOT_PLACED)
                    return True
                pcid = status.provider_call_id
            provider.hangup(pcid)
            return True
        except ProviderTimeout:
            return False   # recovery will retry the hangup
