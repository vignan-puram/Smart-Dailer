"""Call lifecycle state machine.

Transitions are validated using an explicit table *and* a monotonic rank.
Provider events may skip states (e.g. COMPLETED arrives before ANSWERED), so
forward jumps are allowed; backward moves never are, and terminal states can
never be left. This makes the processor safe against out-of-order and late events.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class CallStatus(str, Enum):
    QUEUED = "QUEUED"
    RESERVED = "RESERVED"      # agent/borrower reserved, provider not yet contacted
    INITIATED = "INITIATED"    # provider accepted the request
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"      # borrower picked up
    CONNECTED = "CONNECTED"    # borrower bridged to an agent
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


C = CallStatus

TERMINAL_STATES = frozenset({C.COMPLETED, C.FAILED, C.CANCELLED})
ACTIVE_STATES = frozenset(set(C) - TERMINAL_STATES)
PRE_ANSWER_STATES = frozenset({C.QUEUED, C.RESERVED, C.INITIATED, C.RINGING})

RANK: dict[CallStatus, int] = {
    C.QUEUED: 0, C.RESERVED: 1, C.INITIATED: 2, C.RINGING: 3,
    C.ANSWERED: 4, C.CONNECTED: 5, C.COMPLETED: 6, C.FAILED: 6, C.CANCELLED: 6,
}

CALL_TRANSITIONS: dict[CallStatus, frozenset[CallStatus]] = {
    C.QUEUED: frozenset({C.RESERVED, C.CANCELLED, C.FAILED}),
    C.RESERVED: frozenset({C.INITIATED, C.RINGING, C.ANSWERED, C.COMPLETED, C.FAILED, C.CANCELLED}),
    C.INITIATED: frozenset({C.RINGING, C.ANSWERED, C.COMPLETED, C.FAILED, C.CANCELLED}),
    C.RINGING: frozenset({C.ANSWERED, C.COMPLETED, C.FAILED, C.CANCELLED}),
    # An answered call cannot be CANCELLED any more: someone is on the line.
    C.ANSWERED: frozenset({C.CONNECTED, C.COMPLETED, C.FAILED}),
    C.CONNECTED: frozenset({C.COMPLETED, C.FAILED}),
    C.COMPLETED: frozenset(),
    C.FAILED: frozenset(),
    C.CANCELLED: frozenset(),
}


class CallOutcome(str, Enum):
    CONNECTED = "CONNECTED"                    # borrower talked to an agent
    NO_ANSWER = "NO_ANSWER"
    BUSY = "BUSY"
    SETUP_FAILED = "SETUP_FAILED"              # provider rejected / errored on initiate
    PROVIDER_FAILED = "PROVIDER_FAILED"        # network failure after initiate
    ABANDONED = "ABANDONED"                    # answered, no agent available (compliance event)
    AGENT_DROPPED = "AGENT_DROPPED"            # agent disconnected mid-conversation
    CANCELLED_AGENT_LOST = "CANCELLED_AGENT_LOST"
    CANCELLED_CAPACITY = "CANCELLED_CAPACITY"
    CANCELLED_NOT_PLACED = "CANCELLED_NOT_PLACED"  # recovery: provider never saw it
    UNRESOLVED = "UNRESOLVED"                  # provider unreachable past hard deadline


class Binding(str, Enum):
    BOUND = "BOUND"        # agent reserved before dialing (progressive)
    UNBOUND = "UNBOUND"    # pooled: agent claimed atomically at answer time


class InvalidCallTransition(Exception):
    pass


def can_transition(current: CallStatus, target: CallStatus) -> bool:
    return target in CALL_TRANSITIONS[current]


def validate_transition(current: CallStatus, target: CallStatus) -> None:
    if not can_transition(current, target):
        raise InvalidCallTransition(f"call transition {current.value} -> {target.value} is not allowed")


@dataclass(frozen=True)
class Call:
    id: str
    campaign_id: str
    borrower_id: str
    agent_id: Optional[str]
    binding: Binding
    permit_id: Optional[str]
    attempt_seq: int
    provider: Optional[str]
    provider_call_id: Optional[str]
    status: CallStatus
    outcome: Optional[str]
    cancel_reason: Optional[str]
    cancel_requested_at: Optional[float]
    last_event_seq: int
    last_event_at: Optional[float]
    lease_owner: Optional[str]
    lease_until: Optional[float]
    created_at: float
    initiated_at: Optional[float]
    answered_at: Optional[float]
    connected_at: Optional[float]
    ended_at: Optional[float]
    talk_duration_s: Optional[float]

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATES

    @staticmethod
    def from_row(row) -> "Call":
        return Call(
            id=row["id"], campaign_id=row["campaign_id"], borrower_id=row["borrower_id"],
            agent_id=row["agent_id"], binding=Binding(row["binding"]), permit_id=row["permit_id"],
            attempt_seq=row["attempt_seq"], provider=row["provider"],
            provider_call_id=row["provider_call_id"], status=CallStatus(row["status"]),
            outcome=row["outcome"], cancel_reason=row["cancel_reason"],
            cancel_requested_at=row["cancel_requested_at"], last_event_seq=row["last_event_seq"],
            last_event_at=row["last_event_at"], lease_owner=row["lease_owner"],
            lease_until=row["lease_until"], created_at=row["created_at"],
            initiated_at=row["initiated_at"], answered_at=row["answered_at"],
            connected_at=row["connected_at"], ended_at=row["ended_at"],
            talk_duration_s=row["talk_duration_s"],
        )
