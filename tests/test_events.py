"""Event processing: duplicates, ordering, late events, crash safety."""
from __future__ import annotations

import pytest

from app.faults import SimulatedCrash, crash_at
from app.models.agent import AgentStatus
from app.models.call import CallStatus
from app.models.campaign import DialMode
from app.models.provider_event import EventResult as R, ProviderEventType as E
from tests.conftest import capture_events, ev, place_single_call


def test_duplicate_answered_causes_one_transition_and_one_allocation(runtime_factory):
    rt = runtime_factory(agents=1, borrowers=1)
    call_id, pcid = place_single_call(rt)
    results = [rt.processor.handle(ev(rt, call_id, pcid, E.ANSWERED, 3)) for _ in range(3)]
    assert results == [R.APPLIED, R.DUPLICATE, R.DUPLICATE]
    assert rt.processor.handle(ev(rt, call_id, pcid, E.COMPLETED, 4, answered=True, talk_duration_s=40)) == R.APPLIED
    assert rt.db.scalar("SELECT COUNT(*) FROM provider_events WHERE call_id=?", (call_id,)) == 2
    assert rt.db.scalar("SELECT duplicate_count FROM provider_events WHERE event_type='ANSWERED'") == 2
    call = rt.lifecycle.calls.get(call_id)
    assert call.status == CallStatus.COMPLETED and call.outcome == "CONNECTED"
    assert rt.agents.get("agent-00000").status == AgentStatus.WRAP_UP


def test_duplicate_answered_on_pooled_call_claims_one_agent(runtime_factory):
    rt = runtime_factory(agents=3, borrowers=1, mode=DialMode.PREDICTIVE)
    call_id, pcid = place_single_call(rt)
    for _ in range(3):
        rt.processor.handle(ev(rt, call_id, pcid, E.ANSWERED, 3))
    assert rt.metrics.agent_counts()["CONNECTED"] == 1
    assert rt.metrics.agent_counts()["AVAILABLE"] == 2


def test_out_of_order_completed_answered_ringing(runtime_factory):
    rt = runtime_factory(agents=1, borrowers=1)
    call_id, pcid = place_single_call(rt)
    seq = [ev(rt, call_id, pcid, E.COMPLETED, 4, answered=True, talk_duration_s=30),
           ev(rt, call_id, pcid, E.ANSWERED, 3), ev(rt, call_id, pcid, E.RINGING, 2)]
    assert [rt.processor.handle(e) for e in seq] == [R.APPLIED, R.TERMINAL_PROTECTED, R.TERMINAL_PROTECTED]
    call = rt.lifecycle.calls.get(call_id)
    assert call.status == CallStatus.COMPLETED and call.outcome == "CONNECTED"
    assert call.talk_duration_s == 30
    assert rt.agents.get("agent-00000").status == AgentStatus.WRAP_UP
    assert rt.borrowers.get(call.borrower_id).status.value == "COMPLETED"


def test_stale_ringing_after_answered_is_ignored(runtime_factory):
    rt = runtime_factory(agents=1, borrowers=1)
    call_id, pcid = place_single_call(rt)
    rt.processor.handle(ev(rt, call_id, pcid, E.ANSWERED, 3))
    assert rt.processor.handle(ev(rt, call_id, pcid, E.RINGING, 2)) == R.STALE
    assert rt.lifecycle.calls.get(call_id).status == CallStatus.CONNECTED


def test_late_failed_after_completed_cannot_overwrite_terminal(runtime_factory):
    rt = runtime_factory(agents=1, borrowers=1)
    call_id, pcid = place_single_call(rt)
    rt.processor.handle(ev(rt, call_id, pcid, E.COMPLETED, 4, answered=False))
    assert rt.processor.handle(ev(rt, call_id, pcid, E.FAILED, 5, reason="LATE")) == R.TERMINAL_PROTECTED
    assert rt.lifecycle.calls.get(call_id).outcome == "NO_ANSWER"


def test_late_answer_after_cancel_is_hung_up(runtime_factory):
    rt = runtime_factory(agents=1, borrowers=1)
    call_id, pcid = place_single_call(rt)
    rt.processor.handle(ev(rt, call_id, pcid, E.FAILED, 2, reason="NETWORK_ERROR"))
    rt.db.execute("UPDATE calls SET status='CANCELLED' WHERE id=?", (call_id,))
    hung = []
    rt.providers["provider_a"].hangup = lambda p: hung.append(p)
    assert rt.processor.handle(ev(rt, call_id, pcid, E.ANSWERED, 3)) == R.TERMINAL_PROTECTED
    assert hung == [pcid] and rt.processor.late_answers_after_cancel == 1


def test_worker_crash_during_answered_then_completed_then_redelivery(runtime_factory):
    rt = runtime_factory(agents=1, borrowers=1)
    call_id, pcid = place_single_call(rt)
    rt.processor.handle(ev(rt, call_id, pcid, E.RINGING, 2))
    answered = ev(rt, call_id, pcid, E.ANSWERED, 3)
    rt.processor.crash_hook = crash_at("event_before_commit")
    with pytest.raises(SimulatedCrash):
        rt.processor.handle(answered)
    rt.processor.crash_hook = None
    # The crash rolled back everything, including the inbox row.
    assert rt.lifecycle.calls.get(call_id).status == CallStatus.RINGING
    assert rt.db.scalar("SELECT COUNT(*) FROM provider_events WHERE event_type='ANSWERED'") == 0
    assert rt.processor.handle(ev(rt, call_id, pcid, E.COMPLETED, 4, answered=True, talk_duration_s=50)) == R.APPLIED
    assert rt.processor.handle(answered) == R.TERMINAL_PROTECTED
    call = rt.lifecycle.calls.get(call_id)
    assert call.status == CallStatus.COMPLETED and call.outcome == "CONNECTED"
    assert rt.agents.get("agent-00000").status == AgentStatus.WRAP_UP


def test_worker_crash_after_commit_makes_redelivery_a_duplicate(runtime_factory):
    rt = runtime_factory(agents=1, borrowers=1)
    call_id, pcid = place_single_call(rt)
    answered = ev(rt, call_id, pcid, E.ANSWERED, 3)
    assert rt.processor.handle(answered) == R.APPLIED       # committed, then the worker "dies"
    assert rt.processor.handle(answered) == R.DUPLICATE     # provider retries the webhook
    assert rt.lifecycle.calls.get(call_id).status == CallStatus.CONNECTED


def test_orphan_event_is_recorded_not_applied(runtime_factory):
    rt = runtime_factory()
    assert rt.processor.handle(ev(rt, "unknown-call", "pcid-x", E.ANSWERED, 3)) == R.ORPHAN
    assert rt.db.scalar("SELECT result FROM provider_events") == "ORPHAN"


def test_pooled_answer_without_free_agent_is_abandoned_and_counted(runtime_factory):
    rt = runtime_factory(agents=3, borrowers=5, mode=DialMode.PREDICTIVE)
    call_id, pcid = place_single_call(rt)
    for a in rt.agents.list():
        rt.agents.disappear(a.id)              # every agent vanishes involuntarily
    assert rt.processor.handle(ev(rt, call_id, pcid, E.ANSWERED, 3)) == R.APPLIED
    call = rt.lifecycle.calls.get(call_id)
    done = rt.lifecycle.calls.get(call.id)
    assert done.outcome == "ABANDONED" and done.status == CallStatus.COMPLETED
    assert rt.stats.get("camp")["abandoned_count"] == 1
    assert rt.borrowers.get(done.borrower_id).status.value == "RETRY_WAIT"


def test_sequence_and_rank_skip_states(runtime_factory):
    rt = runtime_factory(agents=1, borrowers=1)
    call_id, pcid = place_single_call(rt)
    # ANSWERED arrives as the very first event (INITIATED/RINGING lost): forward jump allowed.
    assert rt.processor.handle(ev(rt, call_id, pcid, E.ANSWERED, 3)) == R.APPLIED
    assert rt.processor.handle(ev(rt, call_id, pcid, E.INITIATED, 1)) == R.STALE
    assert rt.lifecycle.calls.get(call_id).last_event_seq == 3
