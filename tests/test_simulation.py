"""Discrete-event simulation: determinism and safety properties over many calls."""
from __future__ import annotations

import pytest

from app.models.campaign import DialMode
from app.simulation.scenarios import SCENARIOS
from app.simulation.simulator import run_scenario


def _strip(d: dict) -> dict:
    d = dict(d)
    d.pop("wall_time_s")
    return d


def test_same_seed_gives_identical_results():
    a = run_scenario(SCENARIOS["B"], DialMode.PREDICTIVE, "STRICT", seed=7, duration_override=300).as_dict()
    b = run_scenario(SCENARIOS["B"], DialMode.PREDICTIVE, "STRICT", seed=7, duration_override=300).as_dict()
    assert _strip(a) == _strip(b)
    c = run_scenario(SCENARIOS["B"], DialMode.PREDICTIVE, "STRICT", seed=8, duration_override=300).as_dict()
    assert _strip(a) != _strip(c)


@pytest.mark.parametrize("scenario", ["A", "B", "C"])
@pytest.mark.parametrize("mode", [DialMode.PROGRESSIVE, DialMode.PREDICTIVE])
def test_strict_mode_never_abandons(scenario, mode):
    r = run_scenario(SCENARIOS[scenario], mode, "STRICT", seed=3, duration_override=400)
    assert r.calls_connected > 0
    assert r.abandoned_calls == 0
    assert r.calls_approved_total <= r.calls_requested_total
    # In STRICT mode the in-flight pooled calls never exceed the agents that could take them.
    assert all(p.unbound_in_flight <= p.logged_in for p in r.timeline)


def test_progressive_creates_only_bound_calls():
    r = run_scenario(SCENARIOS["A"], DialMode.PROGRESSIVE, "STRICT", seed=5, duration_override=300)
    assert all(p.unbound_in_flight == 0 for p in r.timeline)
    assert r.capacity_cancellations == 0


def test_dynamic_scenario_through_outage_and_agent_drop():
    """Scenario D up to just after the provider outage window (3000-3300 s)."""
    r = run_scenario(SCENARIOS["D"], DialMode.PREDICTIVE, "STRICT", seed=11, duration_override=3400)
    assert r.abandoned_calls == 0
    assert r.calls_connected > 100
    assert r.circuit_openings >= 1
    assert r.duplicate_events > 0                     # provider B duplicates were deduplicated
