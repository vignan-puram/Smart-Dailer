"""Value objects exchanged across the pacing / safety / allocation boundary."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Optional

from app.models.call import Binding


class SafetyMode(str, Enum):
    STRICT = "STRICT"
    BOUNDED_RISK = "BOUNDED_RISK"


class Decision(str, Enum):
    APPROVED = "APPROVED"
    REDUCED = "REDUCED"
    REJECTED = "REJECTED"
    FALLBACK_TO_PROGRESSIVE = "FALLBACK_TO_PROGRESSIVE"


class ProviderHealthLevel(str, Enum):
    GOOD = "GOOD"
    DEGRADED = "DEGRADED"
    DOWN = "DOWN"


@dataclass(frozen=True)
class PacingRecommendation:
    """What a pacing engine *wants*. It carries no authority."""
    campaign_id: str
    engine: str                      # PROGRESSIVE | PREDICTIVE
    requested: int
    binding: Binding
    explanation: tuple[str, ...]
    inputs: dict[str, Any] = field(default_factory=dict)

    def explain(self) -> str:
        lines = [f"Requested {self.requested} calls because:"]
        lines += [f"  * {line}" for line in self.explanation]
        return "\n".join(lines)


@dataclass(frozen=True)
class CapacitySnapshot:
    """The controller's *own* view of the system, read from the database."""
    available_agents: int
    pause_pending_agents: int
    wrapup_lookahead_agents: int
    claimable_capacity: int           # agents that can take an answered unbound call
    reserved_agents: int
    dialing_agents: int
    connected_agents: int
    logged_in_agents: int
    unbound_in_flight: int            # unanswered pooled calls
    outstanding_permit_slots: int     # approved but not yet consumed
    active_calls: int            # calls in flight + unspent permit slots (future calls)
    campaign_active_calls: int   # same, for this campaign
    eligible_borrowers: int
    provider_health: str
    answer_rate_upper: float
    answer_samples: int
    max_unbound_in_flight: int        # hard bound computed by the controller

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DialPermit:
    """Capability token. The allocator places calls *only* against a permit,
    and the permit row in the DB enforces ``consumed <= approved``."""
    permit_id: str
    campaign_id: str
    approved: int
    binding: Binding
    issued_at: float
    expires_at: float


@dataclass(frozen=True)
class SafetyDecision:
    decision_id: int
    campaign_id: str
    engine: str
    requested: int
    approved: int
    rejected: int
    decision: Decision
    reason: str
    safety_mode: SafetyMode
    binding: Binding
    capacity: CapacitySnapshot
    permit: Optional[DialPermit]

    def as_dict(self) -> dict[str, Any]:
        return {
            "decision_id": self.decision_id, "campaign_id": self.campaign_id,
            "engine": self.engine, "requested": self.requested, "approved": self.approved,
            "rejected": self.rejected, "decision": self.decision.value, "reason": self.reason,
            "safety_mode": self.safety_mode.value, "binding": self.binding.value,
            "capacity": self.capacity.as_dict(),
            "permit_id": self.permit.permit_id if self.permit else None,
        }
