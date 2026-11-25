"""Agent lifecycle state machine."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class AgentStatus(str, Enum):
    OFFLINE = "OFFLINE"
    AVAILABLE = "AVAILABLE"
    RESERVED = "RESERVED"      # bound to a call that is being set up (progressive)
    DIALING = "DIALING"        # bound call is ringing at the borrower
    CONNECTED = "CONNECTED"    # talking to a borrower
    WRAP_UP = "WRAP_UP"        # after-call work, system-enforced deadline
    PAUSED = "PAUSED"


S = AgentStatus

# Valid transitions. Anything not listed is rejected by AgentManager.
AGENT_TRANSITIONS: dict[AgentStatus, frozenset[AgentStatus]] = {
    S.OFFLINE: frozenset({S.AVAILABLE}),
    # AVAILABLE -> CONNECTED is the *pooled* path: an unbound predictive call
    # was answered and atomically claimed this agent.
    S.AVAILABLE: frozenset({S.RESERVED, S.CONNECTED, S.PAUSED, S.OFFLINE}),
    # RESERVED -> CONNECTED happens when INITIATED/RINGING events were lost or
    # arrived late and ANSWERED is the first event we see.
    S.RESERVED: frozenset({S.DIALING, S.CONNECTED, S.AVAILABLE, S.OFFLINE}),
    S.DIALING: frozenset({S.CONNECTED, S.AVAILABLE, S.OFFLINE}),
    S.CONNECTED: frozenset({S.WRAP_UP, S.OFFLINE}),
    # WRAP_UP -> CONNECTED only once the system-enforced wrap-up deadline has
    # passed (look-ahead capacity, see SafetyController).
    S.WRAP_UP: frozenset({S.AVAILABLE, S.CONNECTED, S.PAUSED, S.OFFLINE}),
    S.PAUSED: frozenset({S.AVAILABLE, S.OFFLINE}),
}

# States in which the agent is attached to a call (DB CHECK constraint mirrors this).
CALL_BOUND_STATES = frozenset({S.RESERVED, S.DIALING, S.CONNECTED})
# States that count as "logged in and eligible for work" for utilisation.
ELIGIBLE_STATES = frozenset({S.AVAILABLE, S.RESERVED, S.DIALING, S.CONNECTED, S.WRAP_UP})


class InvalidAgentTransition(Exception):
    pass


def can_transition(current: AgentStatus, target: AgentStatus) -> bool:
    return target in AGENT_TRANSITIONS[current]


def validate_transition(current: AgentStatus, target: AgentStatus) -> None:
    if not can_transition(current, target):
        raise InvalidAgentTransition(f"agent transition {current.value} -> {target.value} is not allowed")


@dataclass(frozen=True)
class Agent:
    id: str
    name: str
    status: AgentStatus
    current_call_id: Optional[str]
    reserved_by: Optional[str]
    reserved_at: Optional[float]
    reserved_until: Optional[float]
    wrap_up_until: Optional[float]
    pause_requested: bool
    last_activity_at: float
    version: int

    @staticmethod
    def from_row(row) -> "Agent":
        return Agent(
            id=row["id"], name=row["name"], status=AgentStatus(row["status"]),
            current_call_id=row["current_call_id"], reserved_by=row["reserved_by"],
            reserved_at=row["reserved_at"], reserved_until=row["reserved_until"],
            wrap_up_until=row["wrap_up_until"], pause_requested=bool(row["pause_requested"]),
            last_activity_at=row["last_activity_at"], version=row["version"],
        )
