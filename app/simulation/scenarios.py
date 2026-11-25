"""Simulation scenarios (the "world" the dialer runs against).

Assumptions, documented and fixed per scenario:
* Answer probability and talk time come from the scenario phase active at
  dial time. Talk time is exponential around the phase mean, clipped to
  [0.2x, 3x] the mean.
* Ring time before an answer is uniform 3-15s. Unanswered calls ring until
  the 30s ring timeout. 5% of unanswered calls are BUSY.
* Wrap-up is a fixed, system-enforced 15s.
* Traffic is split 70% Provider A (fast, reliable) / 30% Provider B (slow,
  timeouts, duplicate and out-of-order webhooks). Both are subject to the
  circuit breaker.
* Agent fluctuation: every 60s about 2% of logged-in agents take a voluntary
  break of 2-5 minutes.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Optional

from app.providers.mock_provider import SampledOutcome


@dataclass(frozen=True)
class Phase:
    start_s: float
    answer_rate: float
    mean_talk_s: float


@dataclass
class PhasedOutcomeModel:
    phases: list[Phase]
    busy_rate: float = 0.05
    min_ring_s: float = 3.0
    max_ring_s: float = 15.0

    def phase_at(self, now: float) -> Phase:
        current = self.phases[0]
        for ph in self.phases:
            if now >= ph.start_s:
                current = ph
        return current

    def sample(self, now: float, rng: random.Random) -> SampledOutcome:
        ph = self.phase_at(now)
        answered = rng.random() < ph.answer_rate
        busy = (not answered) and rng.random() < self.busy_rate
        talk = min(max(rng.expovariate(1.0 / ph.mean_talk_s), 0.2 * ph.mean_talk_s), 3.0 * ph.mean_talk_s)
        return SampledOutcome(answered, busy, rng.uniform(self.min_ring_s, self.max_ring_s), talk)


@dataclass(frozen=True)
class ProviderOutage:
    provider: str
    start_s: float
    end_s: float


@dataclass(frozen=True)
class AgentDrop:
    at_s: float
    fraction: float
    over_s: float          # agents disappear spread over this many seconds
    return_at_s: Optional[float]


@dataclass(frozen=True)
class Scenario:
    key: str
    name: str
    description: str
    phases: tuple[Phase, ...]
    duration_s: float = 3600.0
    agents: int = 50
    borrowers: int = 8000
    break_rate_per_min: float = 0.02
    provider_weights: dict = field(default_factory=lambda: {"provider_a": 0.7, "provider_b": 0.3})
    outages: tuple[ProviderOutage, ...] = ()
    agent_drops: tuple[AgentDrop, ...] = ()

    def outcome_model(self) -> PhasedOutcomeModel:
        return PhasedOutcomeModel(list(self.phases))


SCENARIOS: dict[str, Scenario] = {
    "A": Scenario("A", "Low answer rate", "20% answer rate, 120s average talk time",
                  (Phase(0, 0.20, 120.0),)),
    "B": Scenario("B", "Medium answer rate", "50% answer rate, 90s average talk time",
                  (Phase(0, 0.50, 90.0),)),
    "C": Scenario("C", "High answer rate, long calls", "70% answer rate, 180s average talk time",
                  (Phase(0, 0.70, 180.0),)),
    "D": Scenario(
        "D", "Changing conditions",
        "answer rate and talk time change every 20 min (incl. a 70% -> 10% collapse), "
        "Provider A outage 50:00-55:00, 30% of agents vanish at 66:40 and return at 73:20",
        (Phase(0, 0.50, 90.0), Phase(1200, 0.70, 180.0), Phase(2400, 0.10, 60.0),
         Phase(3600, 0.35, 150.0)),
        duration_s=4800.0,
        outages=(ProviderOutage("provider_a", 3000.0, 3300.0),),
        agent_drops=(AgentDrop(4000.0, 0.30, 5.0, 4400.0),),
    ),
}
