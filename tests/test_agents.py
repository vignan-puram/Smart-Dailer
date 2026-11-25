"""Agent lifecycle, state machine and agent loss."""
from __future__ import annotations

import sqlite3

import pytest

from app.models.agent import AGENT_TRANSITIONS, AgentStatus as S, InvalidAgentTransition, can_transition
from app.models.call import CallStatus
from app.models.campaign import DialMode
from app.models.provider_event import ProviderEventType as E
from app.services.agent_manager import PauseResult
from tests.conftest import capture_events, ev, place_single_call


class TestStateMachine:
    @pytest.mark.parametrize("a,b", [
        (S.OFFLINE, S.AVAILABLE), (S.AVAILABLE, S.RESERVED), (S.RESERVED, S.DIALING),
        (S.DIALING, S.CONNECTED), (S.CONNECTED, S.WRAP_UP), (S.WRAP_UP, S.AVAILABLE),
        (S.AVAILABLE, S.PAUSED), (S.PAUSED, S.AVAILABLE), (S.AVAILABLE, S.CONNECTED),
        (S.RESERVED, S.AVAILABLE), (S.DIALING, S.AVAILABLE), (S.CONNECTED, S.OFFLINE),
    ])
    def test_valid_transitions(self, a, b):
        assert can_transition(a, b)

    @pytest.mark.parametrize("a,b", [
        (S.OFFLINE, S.CONNECTED), (S.OFFLINE, S.RESERVED), (S.PAUSED, S.RESERVED),
        (S.CONNECTED, S.AVAILABLE), (S.CONNECTED, S.RESERVED), (S.WRAP_UP, S.DIALING),
        (S.DIALING, S.WRAP_UP), (S.PAUSED, S.CONNECTED),
    ])
    def test_invalid_transitions(self, a, b):
        assert not can_transition(a, b)

    def test_every_state_has_an_exit_and_can_go_offline(self):
        for state, targets in AGENT_TRANSITIONS.items():
            assert targets, state
            if state != S.OFFLINE:
                assert S.OFFLINE in targets, f"{state} must allow involuntary disconnect"


class TestAgentManager:
    def test_login_pause_resume_logout(self, runtime_factory):
        rt = runtime_factory()
        rt.agents.create("a1")
        assert rt.agents.login("a1").status == S.AVAILABLE
        assert rt.agents.request_pause("a1") == PauseResult.PAUSED
        assert rt.agents.get("a1").status == S.PAUSED
        assert rt.agents.resume("a1").status == S.AVAILABLE
        assert rt.agents.logout("a1") == PauseResult.OFFLINE
        assert rt.agents.get("a1").status == S.OFFLINE

    def test_invalid_manual_transition_rejected(self, runtime_factory):
        rt = runtime_factory()
        rt.agents.create("a1")
        with pytest.raises(InvalidAgentTransition):
            rt.agents.transition("a1", S.CONNECTED)      # dialer-controlled state
        with pytest.raises(InvalidAgentTransition):
            rt.agents.resume("a1")                       # OFFLINE -> AVAILABLE must be login

    def test_db_check_constraint_rejects_reserved_without_call(self, runtime_factory):
        rt = runtime_factory(agents=1)
        with pytest.raises(sqlite3.IntegrityError):
            rt.db.execute("UPDATE agents SET status='RESERVED' WHERE id='agent-00000'")

    def test_voluntary_pause_is_deferred_when_pooled_calls_depend_on_agent(self, runtime_factory):
        rt = runtime_factory(agents=2, borrowers=10, mode=DialMode.PREDICTIVE)
        capture_events(rt)
        rt.dial_cycle()          # 2 pooled calls, both counting on the 2 idle agents
        assert rt.metrics.unbound_in_flight() == 2
        assert rt.agents.request_pause("agent-00000") == PauseResult.PENDING
        agent = rt.agents.get("agent-00000")
        assert agent.status == S.AVAILABLE and agent.pause_requested
        # It is still claimable by an answered pooled call (the commitment holds).
        call = rt.lifecycle.calls.list()[0]
        rt.processor.handle(ev(rt, call.id, call.provider_call_id, E.ANSWERED, 3))
        assert rt.lifecycle.calls.get(call.id).status == CallStatus.CONNECTED

    def test_pending_pause_applied_when_headroom_returns(self, runtime_factory):
        rt = runtime_factory(agents=2, borrowers=10, mode=DialMode.PREDICTIVE)
        capture_events(rt)
        rt.dial_cycle()
        rt.agents.request_pause("agent-00001")
        for call in rt.lifecycle.calls.list():          # both calls go unanswered
            rt.processor.handle(ev(rt, call.id, call.provider_call_id, E.COMPLETED, 4, answered=False))
        rt.campaigns.pause("camp")
        rt.dial_cycle()                                  # housekeeping applies the pause
        assert rt.agents.get("agent-00001").status == S.PAUSED


class TestAgentDisappearsDuringSetup:
    def test_bound_call_is_rebound_to_another_agent(self, runtime_factory):
        rt = runtime_factory(agents=2, borrowers=5)
        capture_events(rt)
        rt.campaigns.get("camp")
        # Only one call: reserve one agent by limiting borrowers to one eligible.
        rt.db.execute("UPDATE borrowers SET status='DO_NOT_CALL' WHERE id NOT LIKE '%000000'")
        rt.dial_cycle()
        call = rt.lifecycle.calls.list()[0]
        lost = call.agent_id
        assert rt.agents.disappear(lost) == call.id
        moved = rt.lifecycle.calls.get(call.id)
        assert moved.agent_id != lost and moved.agent_id is not None
        assert rt.agents.get(moved.agent_id).status == S.DIALING
        assert rt.agents.get(lost).status == S.OFFLINE

    def test_bound_call_cancelled_when_no_replacement(self, runtime_factory):
        rt = runtime_factory(agents=1, borrowers=1)
        call_id, pcid = place_single_call(rt)
        rt.agents.disappear("agent-00000")
        call = rt.lifecycle.calls.get(call_id)
        assert call.cancel_requested_at is not None and call.cancel_reason == "CANCELLED_AGENT_LOST"
        # The provider confirms the hangup: unanswered -> CANCELLED, borrower retried later.
        rt.processor.handle(ev(rt, call_id, pcid, E.COMPLETED, 3, answered=False, hangup=True))
        call = rt.lifecycle.calls.get(call_id)
        assert call.status == CallStatus.CANCELLED and call.outcome == "CANCELLED_AGENT_LOST"
        assert rt.borrowers.get(call.borrower_id).status.value == "RETRY_WAIT"

    def test_answer_races_the_cancel_and_finds_no_agent(self, runtime_factory):
        rt = runtime_factory(agents=1, borrowers=1)
        call_id, pcid = place_single_call(rt)
        rt.agents.disappear("agent-00000")
        # Borrower answers before the hangup lands: no agent anywhere -> ABANDONED (counted).
        rt.processor.handle(ev(rt, call_id, pcid, E.ANSWERED, 3))
        call = rt.lifecycle.calls.get(call_id)
        assert call.status == CallStatus.COMPLETED and call.outcome == "ABANDONED"

    def test_agent_vanishes_between_reservation_and_dial(self, runtime_factory):
        rt = runtime_factory(agents=1, borrowers=1)
        capture_events(rt)
        rt.allocator.crash_hook = lambda where, ref: rt.agents.disappear("agent-00000") \
            if where == "before_initiate" else None
        rt.dial_cycle()
        call = rt.lifecycle.calls.list()[0]
        # The provider was never contacted for a call that lost its agent.
        assert call.status == CallStatus.CANCELLED and call.outcome == "CANCELLED_NOT_PLACED"
        assert rt.providers["provider_a"].stats["initiated"] == 0
