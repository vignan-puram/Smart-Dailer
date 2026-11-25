"""Worker crashes, expired leases, ambiguous timeouts and reconciliation."""
from __future__ import annotations

import pytest

from app.faults import SimulatedCrash, crash_at
from app.models.agent import AgentStatus
from app.models.call import CallStatus
from app.providers.mock_provider import ProviderProfile
from tests.conftest import capture_events


def test_crash_before_initiate_expired_reservation_is_released(runtime_factory):
    rt = runtime_factory(agents=1, borrowers=1)
    rt.allocator.crash_hook = crash_at("before_initiate")
    with pytest.raises(SimulatedCrash):
        rt.dial_cycle()
    rt.allocator.crash_hook = None
    assert rt.recovery.run().get("setup_not_placed") is None          # lease not expired yet: hands off
    assert rt.agents.get("agent-00000").status == AgentStatus.RESERVED
    rt.clock.advance_to(rt.clock.now() + 11)
    assert rt.recovery.run()["setup_not_placed"] == 1
    call = rt.lifecycle.calls.list()[0]
    assert call.status == CallStatus.CANCELLED and call.outcome == "CANCELLED_NOT_PLACED"
    assert rt.agents.get("agent-00000").status == AgentStatus.AVAILABLE
    b = rt.borrowers.get(call.borrower_id)
    assert b.status.value == "RETRY_WAIT" and b.attempt_count == 0


def test_worker_restart_after_initiate_adopts_provider_state_without_duplicate(runtime_factory):
    rt = runtime_factory(agents=1, borrowers=1, answer_rate=1.0, talk_s=60)
    lost = capture_events(rt)                         # webhooks lost while worker is down
    rt.allocator.crash_hook = crash_at("after_initiate")
    with pytest.raises(SimulatedCrash):
        rt.dial_cycle()
    rt.allocator.crash_hook = None
    call = rt.lifecycle.calls.list()[0]
    assert call.status == CallStatus.RESERVED and call.provider_call_id is None
    rt.scheduler.run_until(rt.clock.now() + 12)       # the call is really ringing/answered at the provider
    assert lost
    # Another worker cannot redial: agent and borrower are still reserved.
    assert rt.dial_cycle()[0].decision is None
    for p in rt.providers.values():
        p.set_event_sink(rt.processor.handle)
    report = rt.recovery.run()
    assert report["setup_adopted"] == 1
    adopted = rt.lifecycle.calls.get(call.id)
    assert adopted.provider_call_id is not None and adopted.status in (CallStatus.RINGING, CallStatus.CONNECTED)
    rt.advance(300, dial=False)
    final = rt.lifecycle.calls.get(call.id)
    assert final.status == CallStatus.COMPLETED and final.outcome == "CONNECTED"
    assert rt.db.scalar("SELECT COUNT(*) FROM calls") == 1
    assert rt.providers["provider_a"]._counter == 1     # exactly one real call was ever placed


def test_ambiguous_initiate_timeout_reconciled_both_ways(runtime_factory):
    placed = ProviderProfile(name="provider_a", reject_prob=0.0, timeout_prob=1.0, timeout_call_placed_prob=1.0,
                             network_failure_prob=0.0)
    rt = runtime_factory(agents=1, borrowers=1, profile_a=placed, answer_rate=0.0)
    capture_events(rt)
    rt.dial_cycle()
    call = rt.lifecycle.calls.list()[0]
    assert call.status == CallStatus.RESERVED                       # kept: outcome unknown
    assert rt.agents.get("agent-00000").status == AgentStatus.RESERVED
    rt.scheduler.run_until(rt.clock.now() + 11)
    rt.recovery.run()
    assert rt.lifecycle.calls.get(call.id).status == CallStatus.RINGING   # it WAS placed

    not_placed = ProviderProfile(name="provider_a", reject_prob=0.0, timeout_prob=1.0,
                                 timeout_call_placed_prob=0.0)
    rt2 = runtime_factory(agents=1, borrowers=1, profile_a=not_placed)
    rt2.dial_cycle()
    rt2.clock.advance_to(rt2.clock.now() + 11)
    rt2.recovery.run()
    c2 = rt2.lifecycle.calls.list()[0]
    assert c2.status == CallStatus.CANCELLED and c2.outcome == "CANCELLED_NOT_PLACED"
    assert rt2.agents.get("agent-00000").status == AgentStatus.AVAILABLE


def test_unreachable_provider_keeps_reservations(runtime_factory):
    rt = runtime_factory(agents=1, borrowers=1)
    capture_events(rt)
    rt.allocator.crash_hook = crash_at("after_initiate")
    with pytest.raises(SimulatedCrash):
        rt.dial_cycle()
    rt.allocator.crash_hook = None
    rt.providers["provider_a"].set_outage(True)
    rt.clock.advance_to(rt.clock.now() + 11)
    assert rt.recovery.run()["setup_unreachable"] == 1
    assert rt.agents.get("agent-00000").status == AgentStatus.RESERVED       # never released blindly
    assert rt.lifecycle.calls.list()[0].status == CallStatus.RESERVED


def test_silent_call_is_reconciled(runtime_factory):
    rt = runtime_factory(agents=1, borrowers=1, answer_rate=0.0)
    capture_events(rt)                      # all webhooks lost
    rt.dial_cycle()
    rt.scheduler.run_until(rt.clock.now() + 200)     # provider finished the call long ago
    report = rt.recovery.run()
    assert report["silent_adopted"] == 1
    call = rt.lifecycle.calls.list()[0]
    assert call.status == CallStatus.COMPLETED and call.outcome == "NO_ANSWER"
    assert rt.agents.get("agent-00000").status == AgentStatus.AVAILABLE


def test_consistency_repair_releases_agent_of_finished_call(runtime_factory):
    rt = runtime_factory(agents=1, borrowers=1)
    capture_events(rt)
    rt.dial_cycle()
    call = rt.lifecycle.calls.list()[0]
    rt.db.execute("UPDATE calls SET status='FAILED' WHERE id=?", (call.id,))   # simulate a partial bug
    report = rt.recovery.run()
    assert report["agents_repaired"] == 1 and report["borrowers_repaired"] == 1
    assert rt.agents.get("agent-00000").status == AgentStatus.AVAILABLE


def test_stale_permits_expire(runtime_factory):
    rt = runtime_factory(agents=5, borrowers=5)
    from app.models.call import Binding
    from app.models.safety import PacingRecommendation
    rt.safety.evaluate(PacingRecommendation("camp", "PROGRESSIVE", 3, Binding.BOUND, ("x",)))
    rt.clock.advance_to(rt.clock.now() + 10)
    assert rt.recovery.run()["permits_expired"] == 1


def test_recovery_is_idempotent(runtime_factory):
    rt = runtime_factory(agents=3, borrowers=20)
    rt.advance(120)
    snapshot = (rt.metrics.agent_counts(), rt.metrics.call_counts())
    rt.recovery.run()
    rt.recovery.run()
    assert (rt.metrics.agent_counts(), rt.metrics.call_counts()) == snapshot
