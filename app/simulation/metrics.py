"""Simulation metrics.

Agent utilisation (the headline metric)
---------------------------------------
    utilisation = time agents spend CONNECTED / eligible agent time

*Eligible agent time* is the time agents are logged in and working:
AVAILABLE + RESERVED + DIALING + CONNECTED + WRAP_UP. OFFLINE and PAUSED
(breaks) are excluded, so agent breaks do not distort the comparison.
Agent states are sampled every pacing tick (1s of virtual time). The same
interval and formula are used for every mode.

Also reported:
* occupancy = (CONNECTED + WRAP_UP) / eligible time;
* average agent wait per connected call = (AVAILABLE + RESERVED + DIALING)
  agent-seconds / connected calls. This is the idle time an agent spends
  waiting for each conversation.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import Any

ELIGIBLE = ("AVAILABLE", "RESERVED", "DIALING", "CONNECTED", "WRAP_UP")
WAITING = ("AVAILABLE", "RESERVED", "DIALING")


@dataclass
class TimePoint:
    t: float
    utilisation_window: float
    true_answer_rate: float
    answer_rate_ewma: float
    requested: int
    approved: int
    unbound_in_flight: int
    available: int
    logged_in: int


@dataclass
class SimulationResult:
    scenario: str
    mode: str
    safety_mode: str
    seed: int
    duration_s: float
    total_agents: int
    avg_available_agents: float
    calls_created: int
    calls_initiated: int
    calls_answered: int
    calls_connected: int
    calls_completed: int
    calls_failed: int
    calls_cancelled: int
    abandoned_calls: int
    agent_utilisation_pct: float
    occupancy_pct: float
    avg_agent_wait_per_connect_s: float
    connects_per_agent_hour: float
    avg_setup_latency_ms: dict[str, float]
    safety_decisions: dict[str, int]
    rejected_requests: int
    calls_requested_total: int
    calls_approved_total: int
    duplicate_events: int
    event_results: dict[str, int]
    outcomes: dict[str, int]
    capacity_cancellations: int
    circuit_openings: int
    wall_time_s: float
    timeline: list[TimePoint] = field(default_factory=list)

    def as_dict(self, include_timeline: bool = False) -> dict[str, Any]:
        d = asdict(self)
        if not include_timeline:
            d.pop("timeline")
        return d


class AgentTimeAccumulator:
    """Integrates agent-seconds per state from periodic snapshots."""

    def __init__(self) -> None:
        self.seconds: Counter[str] = Counter()
        self.samples = 0
        self.available_sum = 0
        self._window: Counter[str] = Counter()

    def add(self, counts: dict[str, int], dt: float) -> None:
        self.samples += 1
        self.available_sum += counts.get("AVAILABLE", 0)
        for state, n in counts.items():
            self.seconds[state] += n * dt
            self._window[state] += n * dt

    def take_window_utilisation(self) -> float:
        eligible = sum(self._window[s] for s in ELIGIBLE)
        util = (self._window["CONNECTED"] / eligible) if eligible else 0.0
        self._window = Counter()
        return util

    def utilisation(self) -> float:
        eligible = sum(self.seconds[s] for s in ELIGIBLE)
        return self.seconds["CONNECTED"] / eligible if eligible else 0.0

    def occupancy(self) -> float:
        eligible = sum(self.seconds[s] for s in ELIGIBLE)
        return (self.seconds["CONNECTED"] + self.seconds["WRAP_UP"]) / eligible if eligible else 0.0

    def waiting_seconds(self) -> float:
        return sum(self.seconds[s] for s in WAITING)

    def eligible_hours(self) -> float:
        return sum(self.seconds[s] for s in ELIGIBLE) / 3600.0
