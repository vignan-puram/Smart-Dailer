"""Normalised provider event (what every telecom adapter must emit)."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class ProviderEventType(str, Enum):
    INITIATED = "INITIATED"
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class EventResult(str, Enum):
    APPLIED = "APPLIED"                      # caused a state transition
    DUPLICATE = "DUPLICATE"                  # same event_id seen before (inbox)
    STALE = "STALE"                          # older than current state, ignored
    TERMINAL_PROTECTED = "TERMINAL_PROTECTED"  # call already terminal
    ORPHAN = "ORPHAN"                        # no matching call
    REJECTED = "REJECTED"                    # invalid for the current state


@dataclass(frozen=True)
class ProviderEvent:
    provider: str
    event_id: str                 # provider's unique id, used for idempotency
    client_ref: Optional[str]     # our call id, echoed back by the provider
    provider_call_id: Optional[str]
    event_type: ProviderEventType
    sequence: int                 # per-call monotonic sequence from the provider
    occurred_at: float
    payload: dict[str, Any] = field(default_factory=dict)
