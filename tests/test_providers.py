"""Mock providers, circuit breaker and routing."""
from __future__ import annotations

import pytest

from app.clock import Scheduler, SimClock
from app.config import CircuitBreakerConfig
from app.models.provider_event import ProviderEventType as E
from app.providers.base_provider import CallRequest, ProviderRejected, ProviderTimeout, TelecomProvider
from app.providers.circuit_breaker import CircuitBreaker
from app.providers.mock_provider import StaticOutcomeModel
from app.providers.provider_a import ProviderA
from app.providers.provider_b import ProviderB
from app.providers.router import ProviderRouter


def _run(provider_cls, seed=1, calls=300, answer=0.5):
    clock = SimClock()
    sched = Scheduler(clock)
    p = provider_cls(sched, StaticOutcomeModel(answer, 60.0), seed=seed)
    events = []
    p.set_event_sink(events.append)
    outcomes = {"ok": 0, "rejected": 0, "timeout": 0}
    latencies = []
    for i in range(calls):
        try:
            res = p.initiate_call(CallRequest(f"c{i}", "+15550100000", "camp"))
            outcomes["ok"] += 1
            latencies.append(res.latency_ms)
        except ProviderRejected:
            outcomes["rejected"] += 1
        except ProviderTimeout:
            outcomes["timeout"] += 1
    sched.run_until(10_000)
    return p, events, outcomes, latencies


def test_both_providers_implement_the_same_interface():
    assert issubclass(ProviderA, TelecomProvider) and issubclass(ProviderB, TelecomProvider)


def test_provider_a_is_fast_and_well_behaved():
    p, events, outcomes, lat = _run(ProviderA)
    assert sum(lat) / len(lat) < 150
    assert outcomes["timeout"] == 0 and p.stats["duplicates_sent"] == 0 and p.stats["reordered"] == 0
    # events for each call arrive in sequence order
    by_call = {}
    for e in events:
        by_call.setdefault(e.client_ref, []).append(e.sequence)
    assert all(seq == sorted(seq) for seq in by_call.values())


def test_provider_b_is_slow_flaky_duplicating_and_reordering():
    p, events, outcomes, lat = _run(ProviderB)
    assert sum(lat) / len(lat) > 500
    assert outcomes["timeout"] > 0 and outcomes["rejected"] > 0
    assert p.stats["duplicates_sent"] > 0 and p.stats["reordered"] > 0
    ids = [e.event_id for e in events]
    assert len(ids) > len(set(ids))                       # duplicates really delivered
    by_call = {}
    for e in events:
        by_call.setdefault(e.client_ref, []).append(e.sequence)
    assert any(seq != sorted(seq) for seq in by_call.values())   # some out of order


def test_seeded_runs_are_reproducible():
    _, e1, o1, _ = _run(ProviderB, seed=7)
    _, e2, o2, _ = _run(ProviderB, seed=7)
    assert o1 == o2 and [x.event_id for x in e1] == [x.event_id for x in e2]
    _, e3, _, _ = _run(ProviderB, seed=8)
    assert [x.event_id for x in e1] != [x.event_id for x in e3]


def test_outage_times_out_and_buffers_webhooks_when_requested():
    clock = SimClock()
    sched = Scheduler(clock)
    p = ProviderA(sched, StaticOutcomeModel(1.0, 60.0), seed=1)
    events = []
    p.set_event_sink(events.append)
    p.initiate_call(CallRequest("c1", "+1", "camp"))
    p.set_outage(True, webhooks=True)
    with pytest.raises(ProviderTimeout):
        p.initiate_call(CallRequest("c2", "+1", "camp"))
    with pytest.raises(ProviderTimeout):
        p.get_call_status("c1")
    sched.run_until(200)
    assert events == []                    # call happened, but webhooks are held back
    p.set_outage(False)
    sched.run_until(400)
    assert [e.event_type for e in events][-1] == E.COMPLETED


def test_hangup_before_and_after_answer():
    clock = SimClock()
    sched = Scheduler(clock)
    p = ProviderA(sched, StaticOutcomeModel(1.0, 100.0, min_ring_s=5, max_ring_s=5), seed=1)
    events = []
    p.set_event_sink(events.append)
    r = p.initiate_call(CallRequest("c1", "+1", "camp"))
    sched.run_until(2)
    p.hangup(r.provider_call_id)
    sched.run_until(500)
    assert events[-1].event_type == E.COMPLETED and events[-1].payload["answered"] is False
    assert E.ANSWERED not in [e.event_type for e in events]
    r2 = p.initiate_call(CallRequest("c2", "+1", "camp"))
    sched.run_until(clock.now() + 20)
    p.hangup(r2.provider_call_id)
    sched.run_until(clock.now() + 500)
    last = [e for e in events if e.client_ref == "c2"][-1]
    assert last.payload["answered"] is True and last.payload["talk_duration_s"] > 0


class TestCircuitBreaker:
    def _breaker(self, runtime_factory, **cfg):
        rt = runtime_factory()
        b = CircuitBreaker(rt.db, rt.clock, CircuitBreakerConfig(**cfg), seed=1)
        b.ensure("p")
        return rt, b

    def test_closed_open_half_open_closed(self, runtime_factory):
        rt, b = self._breaker(runtime_factory, failure_threshold=3, base_open_s=10, jitter_fraction=0.0)
        for _ in range(3):
            b.record_failure("p", 5000, timeout=True)
        assert b.get("p")["state"] == "OPEN" and not b.allow_request("p")
        rt.clock.advance_to(rt.clock.now() + 10.5)
        assert b.allow_request("p")                       # probe granted -> HALF_OPEN
        assert b.get("p")["state"] == "HALF_OPEN"
        assert not b.allow_request("p")                   # only one probe at a time
        b.record_success("p", 80)
        assert b.get("p")["state"] == "CLOSED" and b.allow_request("p")

    def test_failed_probe_reopens_with_longer_backoff(self, runtime_factory):
        rt, b = self._breaker(runtime_factory, failure_threshold=1, base_open_s=10, jitter_fraction=0.0)
        b.record_failure("p", 1, timeout=True)
        first = b.get("p")["retry_at"] - rt.clock.now()
        rt.clock.advance_to(rt.clock.now() + 11)
        assert b.allow_request("p")
        b.record_failure("p", 1, timeout=True)
        second = b.get("p")["retry_at"] - rt.clock.now()
        assert first == pytest.approx(10) and second == pytest.approx(20)

    def test_jitter_spreads_cooldowns(self, runtime_factory):
        rt, b = self._breaker(runtime_factory, base_open_s=100, jitter_fraction=0.2)
        values = {round(b._cooldown(0), 3) for _ in range(20)}
        assert len(values) > 1 and all(80 <= v <= 120 for v in values)

    def test_router_skips_open_provider(self, runtime_factory):
        rt = runtime_factory(weights={"provider_a": 1.0, "provider_b": 1.0})
        rt.db.execute("UPDATE provider_health SET state='OPEN', retry_at=? WHERE provider='provider_a'",
                      (rt.clock.now() + 100,))
        assert {rt.router.select().name for _ in range(20)} == {"provider_b"}
