"""Call state machine and resource release."""
from __future__ import annotations

import pytest

from app.models.call import (
    CALL_TRANSITIONS,
    RANK,
    TERMINAL_STATES,
    CallOutcome,
    CallStatus as C,
    InvalidCallTransition,
    can_transition,
    validate_transition,
)
from app.models.agent import AgentStatus
from tests.conftest import place_single_call


@pytest.mark.parametrize("a,b", [
    (C.QUEUED, C.RESERVED), (C.RESERVED, C.INITIATED), (C.INITIATED, C.RINGING), (C.RINGING, C.ANSWERED),
    (C.ANSWERED, C.CONNECTED), (C.CONNECTED, C.COMPLETED), (C.RINGING, C.COMPLETED), (C.RESERVED, C.ANSWERED),
    (C.RINGING, C.CANCELLED), (C.INITIATED, C.FAILED),
])
def test_valid_call_transitions(a, b):
    assert can_transition(a, b)


@pytest.mark.parametrize("a,b", [
    (C.COMPLETED, C.ANSWERED), (C.CANCELLED, C.ANSWERED), (C.FAILED, C.RINGING), (C.CONNECTED, C.RINGING),
    (C.ANSWERED, C.RINGING), (C.ANSWERED, C.CANCELLED), (C.CONNECTED, C.CANCELLED), (C.RINGING, C.INITIATED),
])
def test_invalid_call_transitions(a, b):
    assert not can_transition(a, b)
    with pytest.raises(InvalidCallTransition):
        validate_transition(a, b)


def test_terminal_states_have_no_exits_and_rank_is_monotonic():
    for t in TERMINAL_STATES:
        assert CALL_TRANSITIONS[t] == frozenset()
    for src, targets in CALL_TRANSITIONS.items():
        for dst in targets:
            assert RANK[dst] > RANK[src], f"{src}->{dst} would move backwards"


def test_finish_releases_agent_and_borrower_once(runtime_factory):
    rt = runtime_factory(agents=1, borrowers=1)
    call_id, _ = place_single_call(rt)
    call = rt.lifecycle.calls.get(call_id)
    with rt.db.transaction():
        assert rt.lifecycle.finish(call, C.COMPLETED, CallOutcome.NO_ANSWER)
    with rt.db.transaction():   # second finish with the stale object: compare-and-set refuses
        assert not rt.lifecycle.finish(call, C.FAILED, CallOutcome.PROVIDER_FAILED)
    assert rt.lifecycle.calls.get(call_id).status == C.COMPLETED
    assert rt.agents.get("agent-00000").status == AgentStatus.AVAILABLE
    assert rt.borrowers.get(call.borrower_id).attempt_count == 1


def test_release_guard_prevents_freeing_agent_busy_on_newer_call(runtime_factory):
    rt = runtime_factory(agents=1, borrowers=2)
    old_call, _ = place_single_call(rt)
    # Force the agent onto a different (newer) call id and try to release it for the old one.
    rt.db.execute("UPDATE agents SET current_call_id='newer-call' WHERE id='agent-00000'")
    assert not rt.lifecycle.agents.release_from_call("agent-00000", old_call, AgentStatus.AVAILABLE, 0.0)
    assert rt.agents.get("agent-00000").current_call_id == "newer-call"
