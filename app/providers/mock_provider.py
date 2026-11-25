"""Configurable mock telecom provider driven by the virtual-time scheduler.

The mock models *the network*: once a call is placed, it has a timeline
(INITIATED -> RINGING -> ANSWERED -> COMPLETED, or NO_ANSWER, BUSY, FAILED).
Events are then *delivered* to the dialer as webhooks, with configurable
delay, duplication, re-ordering and outages. Whether a borrower answers, and
how long they talk, comes from a pluggable ``OutcomeModel`` (the
"world"). Scenarios can change answer rates over time.

All randomness comes from a seeded ``random.Random``, so runs are reproducible.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Optional, Protocol

from app.clock import Scheduler
from app.models.provider_event import ProviderEvent, ProviderEventType
from app.providers.base_provider import (
    CallRequest,
    InitiateResult,
    ProviderCallStatus,
    ProviderRejected,
    ProviderTimeout,
    TelecomProvider,
)

E = ProviderEventType


@dataclass(frozen=True)
class SampledOutcome:
    answered: bool
    busy: bool
    ring_s: float          # time from RINGING to ANSWERED (if answered)
    talk_s: float          # conversation length (if answered)


class OutcomeModel(Protocol):
    def sample(self, now: float, rng: random.Random) -> SampledOutcome: ...


@dataclass
class StaticOutcomeModel:
    answer_rate: float = 0.35
    mean_talk_s: float = 120.0
    busy_rate: float = 0.05
    min_ring_s: float = 3.0
    max_ring_s: float = 15.0

    def sample(self, now: float, rng: random.Random) -> SampledOutcome:
        answered = rng.random() < self.answer_rate
        busy = (not answered) and rng.random() < self.busy_rate
        # Talk time ~ lognormal-ish: exponential clipped to [20%, 300%] of the mean.
        talk = min(max(rng.expovariate(1.0 / self.mean_talk_s), 0.2 * self.mean_talk_s), 3.0 * self.mean_talk_s)
        return SampledOutcome(answered, busy, rng.uniform(self.min_ring_s, self.max_ring_s), talk)


@dataclass(frozen=True)
class ProviderProfile:
    name: str
    setup_latency_ms: float = 100.0
    setup_latency_jitter_ms: float = 20.0
    reject_prob: float = 0.01              # definitive failure on initiate
    timeout_prob: float = 0.0              # ambiguous failure on initiate
    timeout_ms: float = 5000.0
    timeout_call_placed_prob: float = 0.5  # on timeout, chance the call was actually placed
    network_failure_prob: float = 0.01     # FAILED after INITIATED
    event_delay_s: float = 0.2
    event_delay_jitter_s: float = 0.1
    duplicate_prob: float = 0.0
    out_of_order_prob: float = 0.0
    out_of_order_extra_delay_s: tuple[float, float] = (2.0, 10.0)
    ring_timeout_s: float = 30.0


@dataclass
class _NetworkCall:
    provider_call_id: str
    client_ref: str
    placed_at: float
    timeline: list[tuple[float, ProviderEventType, dict]] = field(default_factory=list)
    hung_up_at: Optional[float] = None
    answered_at: Optional[float] = None


class MockTelecomProvider(TelecomProvider):
    def __init__(self, profile: ProviderProfile, scheduler: Scheduler, outcome_model: OutcomeModel,
                 seed: int) -> None:
        super().__init__()
        self.profile = profile
        self.name = profile.name
        self.scheduler = scheduler
        self.outcome_model = outcome_model
        self.rng = random.Random(seed)
        self._calls: dict[str, _NetworkCall] = {}
        self._by_ref: dict[str, str] = {}
        self._outage = False
        self._webhook_outage = False
        self._buffer: list[ProviderEvent] = []
        self._counter = 0
        # Distinguishes provider-call ids across mock instances (several workers
        # in the load test each own a mock instance but share one database).
        self._instance = f"{random.Random(seed).getrandbits(24):06x}"
        self.stats ={"initiated": 0, "rejected": 0, "timeouts": 0, "duplicates_sent": 0,
                      "reordered": 0, "events_sent": 0}

    # ----------------------------------------------------------- fault injection
    @property
    def in_outage(self) -> bool:
        return self._outage

    def set_outage(self, active: bool, *, webhooks: bool = False) -> None:
        """Outage: ``initiate_call``/``hangup``/``get_call_status`` time out.

        Calls already in the network keep going (the phone call is real). With
        ``webhooks=True`` their events are also held back until the outage ends.
        """
        self._outage = active
        self._webhook_outage = active and webhooks
        if not active and self._buffer:
            pending, self._buffer = self._buffer, []
            for i, ev in enumerate(pending):
                self.scheduler.schedule_in(0.5 + 0.01 * i, lambda ev=ev: self._deliver(ev))

    # --------------------------------------------------------------- interface
    def initiate_call(self, request: CallRequest) -> InitiateResult:
        p = self.profile
        now = self.scheduler.clock.now()
        if self._outage:
            self.stats["timeouts"] += 1
            raise ProviderTimeout(f"{self.name}: timed out after {p.timeout_ms:.0f}ms (outage)")
        roll = self.rng.random()
        if roll < p.reject_prob:
            self.stats["rejected"] += 1
            raise ProviderRejected(f"{self.name}: 503 service unavailable")
        latency_ms = max(1.0, self.rng.gauss(p.setup_latency_ms, p.setup_latency_jitter_ms))
        if roll < p.reject_prob + p.timeout_prob:
            self.stats["timeouts"] += 1
            if self.rng.random() < p.timeout_call_placed_prob:
                self._place(request, now, latency_ms)   # the ambiguous case: it IS ringing
            raise ProviderTimeout(f"{self.name}: no response within {p.timeout_ms:.0f}ms")
        call = self._place(request, now, latency_ms)
        self.stats["initiated"] += 1
        return InitiateResult(call.provider_call_id, latency_ms)

    def hangup(self, provider_call_id: str) -> None:
        if self._outage:
            raise ProviderTimeout(f"{self.name}: hangup API unreachable")
        call = self._calls.get(provider_call_id)
        if call is None or call.hung_up_at is not None:
            return
        now = self.scheduler.clock.now()
        final = [t for t, typ, _ in call.timeline if typ in (E.COMPLETED, E.FAILED)]
        if final and final[0] <= now:
            return  # already over
        call.hung_up_at = now
        answered = call.answered_at is not None and call.answered_at <= now
        talk = (now - call.answered_at) if answered else 0.0
        # Drop every event that has not happened yet and end the call now.
        call.timeline = [(t, typ, pl) for t, typ, pl in call.timeline if t <= now]
        call.timeline.append((now, E.COMPLETED, {"answered": answered, "talk_duration_s": talk, "hangup": True}))
        self._schedule_delivery(call, len(call.timeline))

    def get_call_status(self, client_ref: str) -> Optional[ProviderCallStatus]:
        if self._outage:
            raise ProviderTimeout(f"{self.name}: status API unreachable")
        pcid = self._by_ref.get(client_ref)
        if pcid is None:
            return None
        call = self._calls[pcid]
        now = self.scheduler.clock.now()
        happened = [(i + 1, typ, pl) for i, (t, typ, pl) in enumerate(call.timeline) if t <= now]
        if not happened:
            return ProviderCallStatus(pcid, client_ref, E.INITIATED.value, 0, False, None)
        seq, typ, payload = happened[-1]
        answered = any(t == E.ANSWERED for _, t, _ in happened)
        return ProviderCallStatus(pcid, client_ref, typ.value, seq, answered, payload.get("talk_duration_s"))

    # ---------------------------------------------------------------- internals
    def _place(self, request: CallRequest, now: float, latency_ms: float) -> _NetworkCall:
        self._counter += 1
        pcid = f"{self.name}-{self._instance}-{self._counter:07d}"
        call = _NetworkCall(pcid, request.client_ref, now)
        p = self.profile
        t = now + latency_ms / 1000.0
        call.timeline.append((t, E.INITIATED, {}))
        if self.rng.random() < p.network_failure_prob:
            call.timeline.append((t + self.rng.uniform(0.5, 3.0), E.FAILED, {"reason": "NETWORK_ERROR"}))
        else:
            outcome = self.outcome_model.sample(now, self.rng)
            ring_start = t + 0.5
            call.timeline.append((ring_start, E.RINGING, {}))
            if outcome.answered and outcome.ring_s < p.ring_timeout_s:
                answer_at = ring_start + outcome.ring_s
                call.answered_at = answer_at
                call.timeline.append((answer_at, E.ANSWERED, {}))
                call.timeline.append((answer_at + outcome.talk_s, E.COMPLETED,
                                      {"answered": True, "talk_duration_s": outcome.talk_s}))
            elif outcome.busy:
                call.timeline.append((ring_start + self.rng.uniform(1.0, 3.0), E.FAILED, {"reason": "BUSY"}))
            else:
                call.timeline.append((ring_start + p.ring_timeout_s, E.COMPLETED,
                                      {"answered": False, "talk_duration_s": 0.0, "reason": "NO_ANSWER"}))
        self._calls[pcid] = call
        self._by_ref[request.client_ref] = pcid
        for seq in range(1, len(call.timeline) + 1):
            self._schedule_delivery(call, seq)
        return call

    def _schedule_delivery(self, call: _NetworkCall, seq: int) -> None:
        p = self.profile
        occurred_at, typ, payload = call.timeline[seq - 1]
        event = ProviderEvent(
            provider=self.name, event_id=f"{call.provider_call_id}:{seq}:{typ.value}", client_ref=call.client_ref,
            provider_call_id=call.provider_call_id, event_type=typ, sequence=seq,
            occurred_at=occurred_at, payload=dict(payload),
        )
        delay = max(0.0, self.rng.gauss(p.event_delay_s, p.event_delay_jitter_s))
        if self.rng.random() < p.out_of_order_prob:
            delay += self.rng.uniform(*p.out_of_order_extra_delay_s)
            self.stats["reordered"] += 1
        hangup_marker = call.hung_up_at

        def deliver() -> None:
            # Events scheduled before a hangup, for moments after it, never happen.
            if call.hung_up_at is not None and hangup_marker is None and occurred_at > call.hung_up_at:
                return
            self._deliver(event)

        self.scheduler.schedule_at(occurred_at + delay, deliver)
        if self.rng.random() < p.duplicate_prob:
            self.stats["duplicates_sent"] += 1
            self.scheduler.schedule_at(occurred_at + delay + self.rng.uniform(0.05, 3.0), deliver)

    def _deliver(self, event: ProviderEvent) -> None:
        if self._webhook_outage:
            self._buffer.append(event)
            return
        self.stats["events_sent"] += 1
        self._emit(event)
