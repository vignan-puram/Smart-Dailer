"""Campaign model."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class DialMode(str, Enum):
    PROGRESSIVE = "PROGRESSIVE"
    PREDICTIVE = "PREDICTIVE"


class CampaignStatus(str, Enum):
    DRAFT = "DRAFT"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    COMPLETED = "COMPLETED"


@dataclass(frozen=True)
class Campaign:
    id: str
    name: str
    mode: DialMode
    status: CampaignStatus
    max_attempts: int
    retry_delay_s: float
    max_concurrent_calls: int

    @staticmethod
    def from_row(row) -> "Campaign":
        return Campaign(
            id=row["id"], name=row["name"], mode=DialMode(row["mode"]),
            status=CampaignStatus(row["status"]), max_attempts=row["max_attempts"],
            retry_delay_s=row["retry_delay_s"], max_concurrent_calls=row["max_concurrent_calls"],
        )
