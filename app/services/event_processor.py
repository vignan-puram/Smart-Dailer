"""Provider event processor: idempotent, order-tolerant, crash-safe.

Every event is processed in ONE ``BEGIN IMMEDIATE`` transaction:

1. **Inbox insert**: ``INSERT INTO provider_events`` with ``UNIQUE(provider,
   event_id)``. If the row exists, the event is a DUPLICATE and nothing else
   happens (only a duplicate counter is incremented). Two workers processing
   the same webhook concurrently are serialised by the write lock, and the
   second one hits the unique key.
2. **Validation**: terminal calls are never modified (TERMINAL_PROTECTED).
   Events whose state rank is not ahead of the current state are STALE (for
   example RINGING after ANSWERED). Forward jumps are allowed, because
   providers skip states.
3. **Transition + side effects**: call, agent, borrower and statistics change
   together.

The inbox row and the state change commit atomically. A worker that crashes
before COMMIT leaves *no trace*, so the provider's redelivery is processed as
new. A worker that crashes after COMMIT already applied the event, and the
redelivery is a harmless DUPLICATE. Effect: exactly-once state transitions
over at-least-once delivery.

Ambiguous outcomes
------------------
* COMPLETED(answered=True) arriving while we still think the call is
  ringing (ANSWERED was delayed or lost) is processed as an *implied
  ANSWERED followed by COMPLETED*. The late ANSWERED is then terminal-protected.
* ANSWERED for a call whose bound agent vanished: we try to claim any free
  agent. If none exists, the call is recorded as ABANDONED (counted, logged,
  borrower scheduled for a prompt call-back) and hung up.
"""
from __future__ import annotations

from collections import Counter
from typing import Callable, Optional

from app.clock import Clock
from app.config import DialerConfig, PacingConfig
from app.database.connection import Database
from app.database.repositories import AgentRepository, CallRepository, EventRepository, PacingStatsRepository
from app.faults import CrashHook, maybe
from app.logging_setup import get_logger, log_event
from app.models.call import RANK, TERMINAL_STATES, Call, CallOutcome, CallStatus, PRE_ANSWER_STATES
from app.models.provider_event import EventResult, ProviderEvent, ProviderEventType
from app.providers.base_provider import ProviderTimeout
from app.providers.router import ProviderRouter
from app.services.call_lifecycle import CallLifecycle

log = get_logger("event_processor")
CS = CallStatus


class EventProcessor:
    def __init__(self, db: Database, clock: Clock, lifecycle: CallLifecycle, router: ProviderRouter,
                 dialer_cfg: DialerConfig, pacing_cfg: PacingConfig, stats: PacingStatsRepository,
                 crash_hook: Optional[CrashHook] = None) -> None:
        self._db = db
        self._clock = clock
        self._lifecycle = lifecycle
        self._router = router
        self._dcfg = dialer_cfg
        self._pcfg = pacing_cfg
        self._stats = stats
        self.crash_hook = crash_hook
        self._events = EventRepository(db)
        self._calls = CallRepository(db)
        self._agents = AgentRepository(db)
        self.counts: Counter[str] = Counter()
        self.abandoned_calls: list[str] = []
        self.late_answers_after_cancel = 0

    # ================================================================ handle
    def handle(self, event: ProviderEvent) -> EventResult:
        now = self._clock.now()
        post_commit: list[Callable[[], None]] = []
        detail = ""
        call_id: Optional[str] = None
        with self._db.transaction():
            fresh = self._events.insert_inbox(
                provider=event.provider, event_id=event.event_id, call_id=event.client_ref,
                provider_call_id=event.provider_call_id, event_type=event.event_type.value,
                sequence=event.sequence, occurred_at=event.occurred_at, received_at=now,
                payload=event.payload)
            if not fresh:
                result, detail = EventResult.DUPLICATE, "event_id already processed"
            else:
                call = self._find_call(event)
                if call is None:
                    result, detail = EventResult.ORPHAN, "no call matches this event"
                else:
                    call_id = call.id
                    result, detail = self._apply(call, event, now, post_commit)
                self._events.set_result(event.provider, event.event_id, result.value, detail, call_id, now)
                # Fault-injection point: a crash here rolls back the inbox row too.
                maybe(self.crash_hook, "event_before_commit", event.event_id)
        for action in post_commit:
            action()
        self.counts[result.value] += 1
        log_event(log, "provider_event", level=10, provider=event.provider, event_id=event.event_id,
                  type=event.event_type.value, call_id=call_id or event.client_ref, result=result.value,
                  detail=detail, at=round(now, 3))
        return result

    def _find_call(self, event: ProviderEvent) -> Optional[Call]:
        call = self._calls.get(event.client_ref) if event.client_ref else None
        if call is None and event.provider_call_id:
            call = self._calls.find_by_provider_ref(event.provider, event.provider_call_id)
        return call

    # ================================================================= apply
    def _apply(self, call: Call, event: ProviderEvent, now: float,
               post: list[Callable[[], None]]) -> tuple[EventResult, str]:
        target = CallStatus(event.event_type.value)
        if call.status in TERMINAL_STATES:
            if target == CS.ANSWERED and call.status == CS.CANCELLED:
                # The borrower picked up a call we had already given up on (the
                # hangup lost the race). Never leave them on a dead line.
                self.late_answers_after_cancel += 1
                log_event(log, "late_answer_after_cancel", level=30, call_id=call.id)
                pcid = call.provider_call_id or event.provider_call_id
                post.append(lambda: self._hangup_ref(call.provider, pcid))
            return EventResult.TERMINAL_PROTECTED, f"call already {call.status.value}; {target.value} ignored"
        if target not in TERMINAL_STATES and RANK[target] <= RANK[call.status]:
            return EventResult.STALE, f"{target.value} is not ahead of {call.status.value}"

        self._touch(call, event, now)
        call = self._calls.get(call.id)
        et = event.event_type
        if et in (ProviderEventType.INITIATED, ProviderEventType.RINGING):
            self._calls.transition(call.id, call.status, target, now, lease_owner=None, lease_until=None,
                                   initiated_at=call.initiated_at or event.occurred_at)
            if call.agent_id:
                self._agents.mark_dialing(call.agent_id, call.id, now)
            return EventResult.APPLIED, f"{call.status.value} -> {target.value}"
        if et == ProviderEventType.ANSWERED:
            return self._answered(call, event, now, post)
        if et == ProviderEventType.COMPLETED:
            return self._completed(call, event, now, post)
        return self._failed(call, event)

    def _touch(self, call: Call, event: ProviderEvent, now: float) -> None:
        self._db.execute(
            """UPDATE calls SET last_event_seq=MAX(last_event_seq, ?), last_event_at=?,
                   provider_call_id=COALESCE(provider_call_id, ?), updated_at=? WHERE id=?""",
            (event.sequence, now, event.provider_call_id, now, call.id))

    def _answered(self, call: Call, event: ProviderEvent, now: float,
                  post: list[Callable[[], None]]) -> tuple[EventResult, str]:
        self._calls.transition(call.id, call.status, CS.ANSWERED, now, answered_at=event.occurred_at,
                               lease_owner=None, lease_until=None)
        self._stats.record_dial_outcome(call.campaign_id, True, self._pcfg.ewma_alpha,
                                        self._pcfg.fast_ewma_alpha, now)
        self._stats.record_setup_time(call.campaign_id, max(0.0, event.occurred_at - call.created_at),
                                      self._pcfg.ewma_alpha, now)
        agent_id = None
        if call.agent_id and self._agents.connect_bound(call.agent_id, call.id, now):
            agent_id = call.agent_id
        else:
            agent_id = self._agents.claim_for_answered_call(call.id, now)
        answered = self._calls.get(call.id)
        if agent_id is not None:
            self._calls.transition(call.id, CS.ANSWERED, CS.CONNECTED, now, agent_id=agent_id, connected_at=now)
            return EventResult.APPLIED, f"ANSWERED -> CONNECTED to agent {agent_id}"
        # No agent can take this borrower: a compliance-relevant abandoned call.
        self._lifecycle.finish(answered, CS.COMPLETED, CallOutcome.ABANDONED, answered=True)
        self._stats.record_abandoned(call.campaign_id, now)
        self.abandoned_calls.append(call.id)
        log_event(log, "abandoned_call", level=30, call_id=call.id, binding=call.binding.value, at=now)
        post.append(lambda: self._hangup(answered))
        return EventResult.APPLIED, "ANSWERED but no agent free -> ABANDONED"

    def _completed(self, call: Call, event: ProviderEvent, now: float,
                   post: list[Callable[[], None]]) -> tuple[EventResult, str]:
        payload = event.payload
        talk = payload.get("talk_duration_s")
        answered = bool(payload.get("answered")) or call.status in (CS.ANSWERED, CS.CONNECTED)
        if call.status in (CS.ANSWERED, CS.CONNECTED):
            outcome = (CallOutcome.AGENT_DROPPED if call.cancel_reason == CallOutcome.AGENT_DROPPED.value
                       else CallOutcome.CONNECTED)
            self._lifecycle.finish(call, CS.COMPLETED, outcome, talk_s=talk, answered=True)
            return EventResult.APPLIED, f"{call.status.value} -> COMPLETED ({outcome.value})"
        if answered:
            # ANSWERED is late or lost: process the implied answer first.
            res, detail = self._answered(call, event, now, post)
            after = self._calls.get(call.id)
            if after.status == CS.CONNECTED:
                self._lifecycle.finish(after, CS.COMPLETED, CallOutcome.CONNECTED, talk_s=talk, answered=True)
            return EventResult.APPLIED, f"implied ANSWERED then COMPLETED ({detail})"
        if call.cancel_requested_at is not None:
            outcome = CallOutcome(call.cancel_reason or CallOutcome.CANCELLED_CAPACITY.value)
            self._lifecycle.finish(call, CS.CANCELLED, outcome)
            return EventResult.APPLIED, f"cancel confirmed -> CANCELLED ({outcome.value})"
        self._stats.record_dial_outcome(call.campaign_id, False, self._pcfg.ewma_alpha,
                                        self._pcfg.fast_ewma_alpha, now)
        self._lifecycle.finish(call, CS.COMPLETED, CallOutcome.NO_ANSWER)
        return EventResult.APPLIED, f"{call.status.value} -> COMPLETED (NO_ANSWER)"

    def _failed(self, call: Call, event: ProviderEvent) -> tuple[EventResult, str]:
        reason = event.payload.get("reason", "")
        now = self._clock.now()
        if call.status in (CS.ANSWERED, CS.CONNECTED):
            self._lifecycle.finish(call, CS.FAILED, CallOutcome.PROVIDER_FAILED, answered=True,
                                   talk_s=max(0.0, now - (call.connected_at or now)))
            return EventResult.APPLIED, "call dropped mid-conversation -> FAILED"
        if reason == "BUSY":
            self._stats.record_dial_outcome(call.campaign_id, False, self._pcfg.ewma_alpha,
                                            self._pcfg.fast_ewma_alpha, now)
            self._lifecycle.finish(call, CS.FAILED, CallOutcome.BUSY)
            return EventResult.APPLIED, "FAILED (BUSY)"
        if call.cancel_requested_at is not None:
            outcome = CallOutcome(call.cancel_reason or CallOutcome.CANCELLED_CAPACITY.value)
            self._lifecycle.finish(call, CS.CANCELLED, outcome)
            return EventResult.APPLIED, "cancel confirmed via FAILED"
        self._lifecycle.finish(call, CS.FAILED, CallOutcome.PROVIDER_FAILED)
        return EventResult.APPLIED, f"FAILED ({reason or 'provider error'})"

    def _hangup(self, call: Call) -> None:
        fresh = self._calls.get(call.id)
        self._hangup_ref(call.provider, fresh.provider_call_id if fresh else call.provider_call_id)

    def _hangup_ref(self, provider_name: Optional[str], pcid: Optional[str]) -> None:
        provider = self._router.get(provider_name) if provider_name else None
        if provider is None or pcid is None:
            return
        try:
            provider.hangup(pcid)
        except ProviderTimeout:
            pass  # the call ends at the provider's own timeout; nothing else to release
