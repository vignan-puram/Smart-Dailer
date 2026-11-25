"""Discrete-event simulator.

Runs the *real* services (pacing engines, Safety Controller, allocator,
event processor, recovery, SQLite) against mock providers in virtual time.
Nothing about dialing behaviour is simulated separately. The simulator only:
* advances the clock (1s pacing ticks),
* injects world events (agent breaks, outages, agent drops),
* samples metrics.

Every source of randomness is seeded, so the same (scenario, mode, seed)
gives identical results.
"""
from __future__ import annotations

import logging
import random
import time
from typing import Optional

from app.config import SafetyConfig, Settings
from app.logging_setup import configure_logging
from app.models.campaign import DialMode
from app.runtime import Runtime, build_runtime
from app.simulation.metrics import AgentTimeAccumulator, SimulationResult, TimePoint
from app.simulation.scenarios import Scenario

TIMELINE_INTERVAL_S = 60.0


class Simulator:
    def __init__(self, scenario: Scenario, mode: DialMode, safety_mode: str = "STRICT", seed: int = 42,
                 epsilon: float = 0.01, db_path: str = ":memory:", quiet: bool = True) -> None:
        self.scenario = scenario
        self.mode = mode
        self.safety_mode = safety_mode
        self.seed = seed
        if quiet:
            configure_logging("WARNING")
        settings = Settings(random_seed=seed, safety=SafetyConfig(mode=safety_mode, abandon_risk_epsilon=epsilon))
        self.rt: Runtime = build_runtime(settings, db_path=db_path, outcome_model=scenario.outcome_model(),
                                         provider_weights=dict(scenario.provider_weights), seed=seed,
                                         fast_db=True)
        self.rng = random.Random(seed * 7919 + 17)
        self.acc = AgentTimeAccumulator()
        self.timeline: list[TimePoint] = []
        self._on_break: set[str] = set()
        self._dropped: list[str] = []
        self._setup()

    # ---------------------------------------------------------------- setup
    def _setup(self) -> None:
        rt, sc = self.rt, self.scenario
        rt.campaigns.create("camp", f"Scenario {sc.key}", self.mode, max_attempts=3, retry_delay_s=300.0,
                            max_concurrent_calls=10_000)
        rt.agents.create_many(sc.agents, prefix="agent")
        rt.borrowers.create_many("camp", sc.borrowers, seed=self.seed)
        rt.campaigns.start("camp")
        for outage in sc.outages:
            prov = rt.providers[outage.provider]
            rt.scheduler.schedule_at(outage.start_s, lambda p=prov: p.set_outage(True))
            rt.scheduler.schedule_at(outage.end_s, lambda p=prov: p.set_outage(False))
        for drop in sc.agent_drops:
            rt.scheduler.schedule_at(drop.at_s, lambda d=drop: self._agent_drop(d))
            if drop.return_at_s is not None:
                rt.scheduler.schedule_at(drop.return_at_s, self._agents_return)

    # --------------------------------------------------------- world events
    def _breaks(self) -> None:
        """Voluntary breaks. Pauses are deferred by AgentManager when pooled calls depend on the agent."""
        rt = self.rt
        logged = [a.id for a in rt.agents.list() if a.status.value not in ("OFFLINE", "PAUSED")
                  and a.id not in self._on_break and a.id not in self._dropped]
        k = sum(1 for _ in logged if self.rng.random() < self.scenario.break_rate_per_min)
        for agent_id in self.rng.sample(logged, min(k, len(logged))):
            rt.agents.request_pause(agent_id)
            self._on_break.add(agent_id)
            rt.scheduler.schedule_in(self.rng.uniform(120, 300), lambda a=agent_id: self._end_break(a))

    def _end_break(self, agent_id: str) -> None:
        self._on_break.discard(agent_id)
        agent = self.rt.agents.get(agent_id)
        if agent.status.value == "PAUSED" or agent.pause_requested:
            self.rt.agents.resume(agent_id)

    def _agent_drop(self, drop) -> None:
        logged = [a.id for a in self.rt.agents.list() if a.status.value not in ("OFFLINE", "PAUSED")]
        victims = self.rng.sample(logged, int(len(logged) * drop.fraction))
        for i, agent_id in enumerate(victims):
            self.rt.scheduler.schedule_in(drop.over_s * i / max(1, len(victims)),
                                          lambda a=agent_id: self.rt.agents.disappear(a))
        self._dropped.extend(victims)

    def _agents_return(self) -> None:
        for agent_id in self._dropped:
            if self.rt.agents.get(agent_id).status.value == "OFFLINE":
                self.rt.agents.login(agent_id)
        self._dropped.clear()

    # ------------------------------------------------------------------ run
    def run(self) -> SimulationResult:
        rt, sc = self.rt, self.scenario
        wall = time.perf_counter()
        step = rt.settings.dialer.pacing_interval_s
        next_break, next_point = 60.0, TIMELINE_INTERVAL_S
        last_decision_id = 0
        while rt.clock.now() < sc.duration_s - 1e-9:
            rt.advance(step)
            self.acc.add(rt.metrics.agent_counts(), step)
            now = rt.clock.now()
            if now >= next_break:
                self._breaks()
                next_break += 60.0
            if now >= next_point:
                last_decision_id = self._timepoint(now, last_decision_id)
                next_point += TIMELINE_INTERVAL_S
        result = self._collect(time.perf_counter() - wall)
        return result

    def _timepoint(self, now: float, last_id: int) -> int:
        rt = self.rt
        row = rt.db.query_one("SELECT COALESCE(SUM(requested),0) r, COALESCE(SUM(approved),0) a, MAX(id) m "
                              "FROM safety_decisions WHERE id > ?", (last_id,))
        stats = rt.stats.get("camp")
        counts = rt.metrics.agent_counts()
        self.timeline.append(TimePoint(
            t=now, utilisation_window=round(self.acc.take_window_utilisation(), 4),
            true_answer_rate=self.scenario.outcome_model().phase_at(now).answer_rate,
            answer_rate_ewma=round(stats["answer_rate_ewma"], 4), requested=row["r"], approved=row["a"],
            unbound_in_flight=rt.metrics.unbound_in_flight(), available=counts["AVAILABLE"],
            logged_in=sum(v for k, v in counts.items() if k not in ("OFFLINE", "PAUSED"))))
        return row["m"] or last_id

    def _collect(self, wall_s: float) -> SimulationResult:
        rt, db = self.rt, self.rt.db
        q = db.scalar
        decisions = {r["decision"]: r["n"] for r in
                     db.query("SELECT decision, COUNT(*) n FROM safety_decisions GROUP BY decision")}
        latency = {r["provider"]: round(r["l"], 1) for r in db.query(
            "SELECT provider, AVG(setup_latency_ms) l FROM calls WHERE setup_latency_ms IS NOT NULL "
            "GROUP BY provider")}
        connected = q("SELECT COUNT(*) FROM calls WHERE connected_at IS NOT NULL")
        acc = self.acc
        return SimulationResult(
            scenario=self.scenario.key, mode=self.mode.value, safety_mode=self.safety_mode, seed=self.seed,
            duration_s=self.scenario.duration_s, total_agents=self.scenario.agents,
            avg_available_agents=round(acc.available_sum / max(1, acc.samples), 2),
            calls_created=q("SELECT COUNT(*) FROM calls"),
            calls_initiated=q("SELECT COUNT(*) FROM calls WHERE initiated_at IS NOT NULL OR provider_call_id IS NOT NULL"),
            calls_answered=q("SELECT COUNT(*) FROM calls WHERE answered_at IS NOT NULL"),
            calls_connected=connected,
            calls_completed=q("SELECT COUNT(*) FROM calls WHERE status='COMPLETED'"),
            calls_failed=q("SELECT COUNT(*) FROM calls WHERE status='FAILED'"),
            calls_cancelled=q("SELECT COUNT(*) FROM calls WHERE status='CANCELLED'"),
            abandoned_calls=q("SELECT COUNT(*) FROM calls WHERE outcome='ABANDONED'"),
            agent_utilisation_pct=round(100 * acc.utilisation(), 2),
            occupancy_pct=round(100 * acc.occupancy(), 2),
            avg_agent_wait_per_connect_s=round(acc.waiting_seconds() / max(1, connected), 1),
            connects_per_agent_hour=round(connected / max(1e-9, acc.eligible_hours()), 2),
            avg_setup_latency_ms=latency,
            safety_decisions=decisions,
            rejected_requests=decisions.get("REJECTED", 0),
            calls_requested_total=q("SELECT COALESCE(SUM(requested),0) FROM safety_decisions"),
            calls_approved_total=q("SELECT COALESCE(SUM(approved),0) FROM safety_decisions"),
            duplicate_events=q("SELECT COALESCE(SUM(duplicate_count),0) FROM provider_events"),
            event_results={r["result"]: r["n"] for r in
                           db.query("SELECT result, COUNT(*) n FROM provider_events GROUP BY result")},
            outcomes=rt.lifecycle.calls.counts_by_outcome(),
            capacity_cancellations=rt.worker.cancelled_for_capacity,
            circuit_openings=q("SELECT COALESCE(SUM(total_openings),0) FROM provider_health"),
            wall_time_s=round(wall_s, 1),
            timeline=self.timeline,
        )


def run_scenario(scenario: Scenario, mode: DialMode, safety_mode: str = "STRICT", seed: int = 42,
                 epsilon: float = 0.01, duration_override: Optional[float] = None) -> SimulationResult:
    if duration_override is not None:
        from dataclasses import replace
        scenario = replace(scenario, duration_s=duration_override)
    return Simulator(scenario, mode, safety_mode, seed, epsilon).run()


logging.getLogger("matplotlib").setLevel(logging.WARNING)
