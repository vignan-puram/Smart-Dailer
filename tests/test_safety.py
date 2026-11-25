"""Safety Controller: hard admission limits, fallback, and no bypass."""
from __future__ import annotations

import dataclasses
import json
import sqlite3
import threading

import pytest

from app.config import SafetyConfig
from app.models.call import Binding
from app.models.campaign import DialMode
from app.models.safety import Decision, DialPermit, PacingRecommendation, SafetyMode
from app.services.call_allocator import UnauthorizedDialError
from app.services.safety_controller import binomial_tail_above, max_dials_within_risk
from tests.conftest import capture_events, make_runtime


def rec(n: int, binding=Binding.UNBOUND, engine="PREDICTIVE") -> PacingRecommendation:
    return PacingRecommendation("camp", engine, n, binding, ("test",))


class TestHardAdmissionLimit:
    def test_predictive_engine_requests_100_with_10_agents(self, runtime_factory):
        rt = runtime_factory(agents=10, borrowers=1000, mode=DialMode.PREDICTIVE)
        d = rt.safety.evaluate(rec(100))
        assert d.decision == Decision.REDUCED
        assert (d.requested, d.approved, d.rejected) == (100, 10, 90)
        assert "agent_capacity" in d.reason
        assert d.safety_mode == SafetyMode.STRICT

    def test_in_flight_pooled_calls_consume_capacity(self, runtime_factory):
        rt = runtime_factory(agents=10, borrowers=1000, mode=DialMode.PREDICTIVE)
        capture_events(rt)
        rt.allocator.execute(rt.safety.evaluate(rec(6)).permit)
        d = rt.safety.evaluate(rec(100))
        assert d.approved == 4 and d.capacity.unbound_in_flight == 6
        rt.allocator.execute(d.permit)
        d = rt.safety.evaluate(rec(100))
        assert d.decision == Decision.REJECTED and d.approved == 0

    def test_outstanding_permits_count_as_committed(self, runtime_factory):
        rt = runtime_factory(agents=10, borrowers=1000, mode=DialMode.PREDICTIVE)
        first = rt.safety.evaluate(rec(7))
        second = rt.safety.evaluate(rec(7))      # first permit not yet executed
        assert first.approved == 7 and second.approved == 3

    def test_repeated_excessive_requests_never_exceed_capacity(self, runtime_factory):
        rt = runtime_factory(agents=25, borrowers=5000, mode=DialMode.PREDICTIVE)
        capture_events(rt)
        for _ in range(50):
            d = rt.safety.evaluate(rec(1000))
            if d.permit:
                rt.allocator.execute(d.permit)
        assert rt.metrics.unbound_in_flight() <= 25

    def test_wrapup_lookahead_only_for_agents_free_before_earliest_answer(self, runtime_factory):
        rt = runtime_factory(agents=4, borrowers=100, mode=DialMode.PREDICTIVE)
        now = rt.clock.now()
        rt.db.execute("UPDATE agents SET status='WRAP_UP', wrap_up_until=? WHERE id='agent-00000'", (now + 2,))
        rt.db.execute("UPDATE agents SET status='WRAP_UP', wrap_up_until=? WHERE id='agent-00001'", (now + 10,))
        d = rt.safety.evaluate(rec(100))
        # 2 idle + 1 wrap-up ending within the 3s guaranteed answer delay; the 10s one does not count.
        assert d.approved == 3 and d.capacity.wrapup_lookahead_agents == 1

    def test_campaign_concurrency_and_global_limits(self, runtime_factory):
        rt = runtime_factory(agents=50, borrowers=1000, mode=DialMode.PREDICTIVE, max_concurrent=7)
        d = rt.safety.evaluate(rec(100))
        assert d.approved == 7 and "campaign_concurrency_limit" in d.reason
        rt2 = runtime_factory(agents=50, borrowers=1000, mode=DialMode.PREDICTIVE,
                              safety=SafetyConfig(max_outstanding_calls=5))
        assert rt2.safety.evaluate(rec(100)).approved == 5

    def test_unspent_permits_count_against_global_and_campaign_limits(self, runtime_factory):
        """Regression (found by the 10k load test): permits granted but not yet spent
        must count against the outstanding-call caps, or back-to-back evaluations
        by different workers each get the full headroom and the cap is exceeded."""
        rt = runtime_factory(agents=50, borrowers=1000, max_concurrent=7)
        assert rt.safety.evaluate(rec(30, Binding.BOUND, "PROGRESSIVE")).approved == 7
        second = rt.safety.evaluate(rec(30, Binding.BOUND, "PROGRESSIVE"))
        assert second.approved == 0 and "campaign_concurrency_limit" in second.reason
        rt2 = runtime_factory(agents=50, borrowers=1000, safety=SafetyConfig(max_outstanding_calls=5))
        assert rt2.safety.evaluate(rec(30, Binding.BOUND, "PROGRESSIVE")).approved == 5
        second = rt2.safety.evaluate(rec(30, Binding.BOUND, "PROGRESSIVE"))
        assert second.approved == 0 and "global_outstanding_limit" in second.reason

    def test_campaign_not_running_is_rejected(self, runtime_factory):
        rt = runtime_factory(agents=5, borrowers=10, start=False)
        d = rt.safety.evaluate(rec(3))
        assert d.decision == Decision.REJECTED and d.permit is None


class TestProviderAndFallback:
    def test_provider_outage_rejects_all_new_calls(self, runtime_factory):
        rt = runtime_factory(agents=10, borrowers=100, mode=DialMode.PREDICTIVE)
        rt.db.execute("UPDATE provider_health SET state='OPEN', retry_at=?", (rt.clock.now() + 60,))
        d = rt.safety.evaluate(rec(5))
        assert d.decision == Decision.REJECTED and "circuits OPEN" in d.reason

    def test_degraded_provider_falls_back_to_progressive(self, runtime_factory):
        rt = runtime_factory(agents=10, borrowers=100, mode=DialMode.PREDICTIVE)
        rt.db.execute("UPDATE provider_health SET ewma_failure_rate=0.4 WHERE provider='provider_a'")
        d = rt.safety.evaluate(rec(30))
        assert d.decision == Decision.FALLBACK_TO_PROGRESSIVE
        assert d.binding == Binding.BOUND and d.approved == 10
        capture_events(rt)
        rt.allocator.execute(d.permit)
        assert rt.metrics.agent_counts()["DIALING"] + rt.metrics.agent_counts()["RESERVED"] == 10

    def test_half_open_allows_only_a_probe(self, runtime_factory):
        rt = runtime_factory(agents=10, borrowers=100, mode=DialMode.PREDICTIVE)
        rt.db.execute("UPDATE provider_health SET state='OPEN', retry_at=? WHERE provider='provider_a'",
                      (rt.clock.now() - 1,))
        d = rt.safety.evaluate(rec(30))
        assert d.approved == 1

    def test_availability_drop_triggers_fallback(self, runtime_factory):
        rt = runtime_factory(agents=100, borrowers=1000, mode=DialMode.PREDICTIVE)
        rt.safety.observe_agents()
        for i in range(40):
            rt.agents.disappear(f"agent-{i:05d}")
        d = rt.safety.evaluate(rec(200))
        assert d.decision == Decision.FALLBACK_TO_PROGRESSIVE and "dropped 100 -> 60" in d.reason
        assert d.approved <= 60


class TestBoundedRisk:
    def test_insufficient_samples_fall_back(self, runtime_factory):
        rt = runtime_factory(agents=20, borrowers=1000, mode=DialMode.PREDICTIVE,
                             safety=SafetyConfig(mode="BOUNDED_RISK"))
        d = rt.safety.evaluate(rec(100))
        assert d.decision == Decision.FALLBACK_TO_PROGRESSIVE and d.approved <= 20

    def test_overdial_respects_risk_budget(self, runtime_factory):
        rt = runtime_factory(agents=20, borrowers=1000, mode=DialMode.PREDICTIVE,
                             safety=SafetyConfig(mode="BOUNDED_RISK", abandon_risk_epsilon=0.01))
        rt.db.execute("UPDATE pacing_stats SET answer_rate_ewma=0.2, answer_rate_fast=0.2, answer_samples=1000")
        d = rt.safety.evaluate(rec(500))
        assert d.decision == Decision.REDUCED and d.approved > 20          # over-dial is allowed...
        p_hi = d.capacity.answer_rate_upper
        assert binomial_tail_above(d.approved, p_hi, 20) <= 0.01           # ...but stays inside the budget
        assert binomial_tail_above(d.approved + 1, p_hi, 20) > 0.01

    def test_binomial_helpers(self):
        assert max_dials_within_risk(10, 0.3, 0.0, 1000) == 10              # epsilon 0 == deterministic
        assert max_dials_within_risk(0, 0.3, 0.01, 1000) == 0
        assert max_dials_within_risk(10, 1.0, 0.5, 1000) == 10
        a = max_dials_within_risk(10, 0.3, 0.01, 1000)
        b = max_dials_within_risk(10, 0.3, 0.05, 1000)
        assert 10 < a <= b
        assert binomial_tail_above(5, 0.5, 5) == 0.0


class TestNoBypass:
    def test_allocator_refuses_forged_permit(self, runtime_factory):
        rt = runtime_factory(agents=5, borrowers=10)
        forged = DialPermit("not-issued", "camp", 5, Binding.UNBOUND, 0.0, 1e9)
        with pytest.raises(UnauthorizedDialError):
            rt.allocator.execute(forged)
        with pytest.raises(UnauthorizedDialError):
            rt.allocator.execute(rec(5))                   # a recommendation is not a permit
        assert rt.db.scalar("SELECT COUNT(*) FROM calls") == 0

    def test_allocator_refuses_expired_or_reused_permit(self, runtime_factory):
        rt = runtime_factory(agents=5, borrowers=10)
        capture_events(rt)
        d = rt.safety.evaluate(rec(2, Binding.BOUND, "PROGRESSIVE"))
        rt.allocator.execute(d.permit)
        with pytest.raises(UnauthorizedDialError):
            rt.allocator.execute(d.permit)                 # exhausted
        d2 = rt.safety.evaluate(rec(2, Binding.BOUND, "PROGRESSIVE"))
        rt.clock.advance_to(rt.clock.now() + 60)
        with pytest.raises(UnauthorizedDialError):
            rt.allocator.execute(d2.permit)                # expired

    def test_permit_binding_comes_from_database_not_caller(self, runtime_factory):
        rt = runtime_factory(agents=5, borrowers=10)
        capture_events(rt)
        d = rt.safety.evaluate(rec(2, Binding.BOUND, "PROGRESSIVE"))
        tampered = dataclasses.replace(d.permit, binding=Binding.UNBOUND, approved=50)
        res = rt.allocator.execute(tampered)
        assert res.placed == 2
        assert {c.binding for c in rt.lifecycle.calls.list()} == {Binding.BOUND}

    def test_database_refuses_overspending_a_permit(self, runtime_factory):
        rt = runtime_factory(agents=5, borrowers=10)
        d = rt.safety.evaluate(rec(2, Binding.BOUND, "PROGRESSIVE"))
        with pytest.raises(sqlite3.IntegrityError):
            rt.db.execute("UPDATE dial_permits SET consumed=3 WHERE id=?", (d.permit.permit_id,))

    def test_safety_mode_cannot_be_switched_off(self, runtime_factory):
        rt = runtime_factory()
        with pytest.raises(dataclasses.FrozenInstanceError):
            rt.settings.safety.mode = "OFF"                       # type: ignore[misc]
        with pytest.raises(AttributeError):
            rt.safety.safety_mode = SafetyMode.BOUNDED_RISK       # type: ignore[misc]
        with pytest.raises(ValueError):
            make_runtime(safety=SafetyConfig(mode="DISABLED"))

    def test_every_decision_is_persisted_with_reason_and_capacity(self, runtime_factory):
        rt = runtime_factory(agents=3, borrowers=10, mode=DialMode.PREDICTIVE)
        rt.safety.evaluate(rec(10))
        row = rt.db.query_one("SELECT * FROM safety_decisions ORDER BY id DESC LIMIT 1")
        assert (row["requested"], row["approved"], row["rejected"], row["decision"]) == (10, 3, 7, "REDUCED")
        cap = json.loads(row["capacity_json"])
        assert cap["available_agents"] == 3 and "limits" in cap


def test_concurrent_evaluations_never_over_approve(file_db):
    """Two workers evaluate at the same instant: BEGIN IMMEDIATE serialises them."""
    from app.config import Settings
    from app.runtime import build_runtime
    first = make_runtime(agents=10, borrowers=100, db_path=file_db, mode=DialMode.PREDICTIVE)
    # A second, independent worker attached to the same database file.
    second = build_runtime(Settings(), db_path=file_db, seed=99,
                           provider_weights={"provider_a": 1.0, "provider_b": 0.0})
    rts = [first, second]
    barrier = threading.Barrier(2)
    approved = [0, 0]

    def work(i):
        barrier.wait()
        for _ in range(5):
            approved[i] += rts[i].safety.evaluate(rec(4)).approved

    threads = [threading.Thread(target=work, args=(i,)) for i in range(2)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert sum(approved) == 10
    for r in rts:
        r.db.close()


def test_capacity_deficit_after_agent_loss_cancels_excess(runtime_factory):
    rt = runtime_factory(agents=10, borrowers=100, mode=DialMode.PREDICTIVE)
    capture_events(rt)
    rt.allocator.execute(rt.safety.evaluate(rec(10)).permit)
    for i in range(4):
        rt.agents.disappear(f"agent-{i:05d}")
    victims = rt.safety.find_capacity_deficit()
    assert len(victims) == 4
    assert rt.safety.find_capacity_deficit() == []          # already cancelling: no double cancel
    assert rt.db.scalar("SELECT COUNT(*) FROM dial_permits WHERE status='ACTIVE'") == 0


def test_break_requested_during_lookahead_wrapup_never_strands_pooled_calls(runtime_factory):
    """Regression: an agent counted as wrap-up look-ahead capacity asks for a break.

    Previously the expired wrap-up went straight to PAUSED and the agent dropped
    out of claimable capacity at once, so the pooled calls relying on it were
    capacity-cancelled (or would have been abandoned had they been answered first).
    """
    rt = runtime_factory(agents=2, borrowers=20, mode=DialMode.PREDICTIVE)
    capture_events(rt)
    now = rt.clock.now()
    rt.db.execute("UPDATE agents SET status='WRAP_UP', wrap_up_until=? WHERE id='agent-00000'", (now + 2.0,))
    d = rt.safety.evaluate(rec(5))
    assert d.approved == 2                          # 1 idle + 1 look-ahead (wrap-up ends within 3s)
    rt.allocator.execute(d.permit)
    assert rt.db.scalar("SELECT COUNT(*) FROM calls WHERE binding='UNBOUND' AND status='RINGING'") \
        + rt.db.scalar("SELECT COUNT(*) FROM calls WHERE binding='UNBOUND' AND status='INITIATED'") == 2

    assert rt.agents.request_pause("agent-00000").value == "PENDING"
    assert rt.safety.find_capacity_deficit() == []  # still claimable while finishing wrap-up
    rt.clock.advance_to(now + 3.0)
    rt.agents.housekeeping()
    a = rt.agents.get("agent-00000")
    assert a.status.value == "AVAILABLE" and a.pause_requested   # pause waits for headroom
    assert rt.safety.find_capacity_deficit() == []
    assert rt.safety.evaluate(rec(5)).approved == 0              # but it backs no *new* calls
