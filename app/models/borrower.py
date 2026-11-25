"""Borrower (call target) model. Only dummy data is ever used."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class BorrowerStatus(str, Enum):
    PENDING = "PENDING"          # never attempted
    RESERVED = "RESERVED"        # has exactly one active call attempt
    RETRY_WAIT = "RETRY_WAIT"    # eligible again at next_eligible_at
    COMPLETED = "COMPLETED"      # reached an agent; done for this campaign
    EXHAUSTED = "EXHAUSTED"      # max attempts used
    DO_NOT_CALL = "DO_NOT_CALL"


DIALABLE_STATES = frozenset({BorrowerStatus.PENDING, BorrowerStatus.RETRY_WAIT})


@dataclass(frozen=True)
class Borrower:
    id: str
    campaign_id: str
    phone: str
    status: BorrowerStatus
    priority: int
    attempt_count: int
    call_count: int
    last_attempt_at: Optional[float]
    next_eligible_at: float
    active_call_id: Optional[str]

    @staticmethod
    def from_row(row) -> "Borrower":
        return Borrower(
            id=row["id"], campaign_id=row["campaign_id"], phone=row["phone"],
            status=BorrowerStatus(row["status"]), priority=row["priority"],
            attempt_count=row["attempt_count"], call_count=row["call_count"],
            last_attempt_at=row["last_attempt_at"], next_eligible_at=row["next_eligible_at"],
            active_call_id=row["active_call_id"],
        )
