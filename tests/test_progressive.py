"""Progressive dialing: one agent per call, hard capacity, cleanup."""
from __future__ import annotations

from app.models.agent import AgentStatus
from app.models.call import Binding, CallStatus
from app.models.provider_event import ProviderEventType as E
from app.models.safety import PacingRecommendation
from app.providers.mock_provider import ProviderProfile
from tests.conftest import capture_events, ev


def test_one_agent_per_outbound_call(runtime_factory):
    rt = runtime_factory(agents=50, borrowers=500)
    capture_events(rt)
    res = rt.dial_cycle()[0]
    assert res.recommendation.requested == 50 and res.recommendation.binding == Binding.BOUND
    calls = rt.lifecycle.calls.list(limit=1000)
    assert len(calls) == 50
    assert len({c.agent_id for c in calls}) == 50            # distinct agents
    assert all(c.binding == Binding.BOUND for c in calls)
    assert rt.metrics.agent_counts()["DIALING"] == 50


def test_never_more_calls_than_available_agents(runtime_factory):
    rt = runtime_factory(agents=50, borrowers=500)
    capture_events(rt)
    for _ in range(5):                                      # repeated cycles add nothing
        rt.dial_cycle()
    assert rt.db.scalar("SELECT COUNT(*) FROM calls") == 50
    # Even an inflated progressive request is capped by the Safety Controller.
    rt.agents.create_many(5, prefix="late")
    big = PacingRecommendation("camp", "PROGRESSIVE", 100, Binding.BOUND, ("inflated",))
    d = rt.safety.evaluate(big)
    assert d.approved == 5
    rt.allocator.execute(d.permit)
    assert rt.db.scalar("SELECT COUNT(*) FROM calls") == 55


def test_progressive_reserves_agent_before_provider_is_contacted(runtime_factory):
    rt = runtime_factory(agents=1, borrowers=1)
    seen = {}

    def hook(where, ref):
        if where == "before_initiate":
            seen["agent"] = rt.agents.get("agent-00000").status
            seen["call"] = rt.lifecycle.calls.get(ref).status

    rt.allocator.crash_hook = hook
    capture_events(rt)
    rt.dial_cycle()
    assert seen == {"agent": AgentStatus.RESERVED, "call": CallStatus.RESERVED}


def test_failed_setup_releases_agent_and_borrower(runtime_factory):
    rt = runtime_factory(agents=10, borrowers=50, profile_a=ProviderProfile(name="provider_a", reject_prob=1.0))
    rt.dial_cycle()
    # Five rejections trip provider_a's breaker (threshold 5); with no other routable
    # provider the allocator stops instead of hammering a failing carrier.
    assert rt.db.scalar("SELECT COUNT(*) FROM calls WHERE outcome='SETUP_FAILED'") == 5
    assert rt.db.scalar("SELECT COUNT(*) FROM calls") == 5
    assert rt.breaker.get("provider_a")["state"] == "OPEN"
    assert rt.metrics.agent_counts()["AVAILABLE"] == 10
    assert rt.db.scalar("SELECT COUNT(*) FROM borrowers WHERE status='RESERVED'") == 0


def test_no_answer_and_network_failure_cleanup(runtime_factory):
    rt = runtime_factory(agents=2, borrowers=2)
    capture_events(rt)
    rt.dial_cycle()
    c1, c2 = rt.lifecycle.calls.list()
    rt.processor.handle(ev(rt, c1.id, c1.provider_call_id, E.COMPLETED, 4, answered=False))
    rt.processor.handle(ev(rt, c2.id, c2.provider_call_id, E.FAILED, 2, reason="NETWORK_ERROR"))
    assert rt.metrics.agent_counts()["AVAILABLE"] == 2
    assert rt.lifecycle.calls.get(c1.id).outcome == "NO_ANSWER"
    assert rt.lifecycle.calls.get(c2.id).outcome == "PROVIDER_FAILED"
    assert rt.borrowers.get(c1.borrower_id).attempt_count == 1
    assert rt.borrowers.get(c2.borrower_id).attempt_count == 0     # never reached the phone


def test_full_progressive_flow_end_to_end(runtime_factory):
    rt = runtime_factory(agents=5, borrowers=200, answer_rate=0.5, talk_s=30)
    rt.advance(600)
    assert rt.db.scalar("SELECT COUNT(*) FROM calls WHERE outcome='CONNECTED'") > 20
    assert rt.db.scalar("SELECT COUNT(*) FROM calls WHERE outcome='ABANDONED'") == 0
    # Invariant at the end of the run: never more active calls than agents.
    assert rt.db.scalar("SELECT COUNT(*) FROM calls WHERE status IN "
                        "('RESERVED','INITIATED','RINGING','ANSWERED','CONNECTED')") <= 5
