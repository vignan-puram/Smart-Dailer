"""Predictive pacing engine: estimates, explanations and the advisory-only boundary."""
from __future__ import annotations

import inspect

import pytest

import app.services.predictive_engine as predictive_module
from app.models.call import Binding
from app.models.campaign import DialMode
from app.models.safety import ProviderHealthLevel
from app.services.predictive_engine import PredictivePacingEngine
from tests.conftest import capture_events


def _engine(rt, health=ProviderHealthLevel.GOOD) -> PredictivePacingEngine:
    return PredictivePacingEngine(rt.metrics, rt.stats, rt.lifecycle.borrowers, lambda: health, rt.clock,
                                  rt.settings.pacing)


def _set_stats(rt, slow, fast=None, samples=500, talk=120.0, setup=8.0):
    rt.db.execute("UPDATE pacing_stats SET answer_rate_ewma=?, answer_rate_fast=?, answer_samples=?, "
                  "talk_time_ewma=?, setup_time_ewma=? WHERE campaign_id='camp'",
                  (slow, slow if fast is None else fast, samples, talk, setup))


def test_lower_answer_rate_means_more_dials(runtime_factory):
    rt = runtime_factory(agents=20, borrowers=1000, mode=DialMode.PREDICTIVE)
    camp = rt.campaigns.get("camp")
    _set_stats(rt, 0.2)
    low = _engine(rt).recommend(camp).requested
    _set_stats(rt, 0.5)
    high = _engine(rt).recommend(camp).requested
    assert low > high > 0
    assert low == pytest.approx(20 / 0.2 - 1, abs=1) or low == int(4.0 * 20) - 1   # capped at max_dial_ratio


def test_longer_talk_time_means_fewer_agents_expected_free(runtime_factory):
    rt = runtime_factory(agents=10, borrowers=1000, mode=DialMode.PREDICTIVE)
    camp = rt.campaigns.get("camp")
    # Put 6 agents in conversation so expected completions depend on talk time.
    rt.db.execute("UPDATE agents SET status='CONNECTED', current_call_id='x'||id WHERE id IN "
                  "(SELECT id FROM agents ORDER BY id LIMIT 6)")
    rt.db.execute("INSERT INTO calls (id, campaign_id, borrower_id, binding, attempt_seq, status, created_at, "
                  "updated_at, connected_at) SELECT 'x'||a.id, 'camp', b.id, 'UNBOUND', 1, 'CONNECTED', 0, 0, 0 "
                  "FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) r FROM agents WHERE status='CONNECTED') a "
                  "JOIN (SELECT id, ROW_NUMBER() OVER (ORDER BY id) r FROM borrowers) b ON a.r=b.r")
    _set_stats(rt, 0.4, talk=60.0, setup=20.0)
    short = _engine(rt).recommend(camp)
    _set_stats(rt, 0.4, talk=600.0, setup=20.0)
    long = _engine(rt).recommend(camp)
    assert short.inputs["expected_free"] > long.inputs["expected_free"]
    assert short.requested > long.requested


def test_provider_degradation_reduces_and_outage_stops_requests(runtime_factory):
    rt = runtime_factory(agents=20, borrowers=1000, mode=DialMode.PREDICTIVE)
    camp = rt.campaigns.get("camp")
    _set_stats(rt, 0.3)
    good = _engine(rt, ProviderHealthLevel.GOOD).recommend(camp)
    degraded = _engine(rt, ProviderHealthLevel.DEGRADED).recommend(camp)
    down = _engine(rt, ProviderHealthLevel.DOWN).recommend(camp)
    assert good.requested > degraded.requested > down.requested == 0
    assert any("DEGRADED" in line for line in degraded.explanation)


def test_changing_agent_availability(runtime_factory):
    rt = runtime_factory(agents=40, borrowers=2000, mode=DialMode.PREDICTIVE)
    camp = rt.campaigns.get("camp")
    _set_stats(rt, 0.3)
    before = _engine(rt).recommend(camp).requested
    for i in range(20):
        rt.agents.disappear(f"agent-{i:05d}")
    after = _engine(rt).recommend(camp).requested
    assert after < before


def test_conservative_with_insufficient_data(runtime_factory):
    rt = runtime_factory(agents=20, borrowers=1000, mode=DialMode.PREDICTIVE)
    camp = rt.campaigns.get("camp")
    _set_stats(rt, 0.2, samples=5)
    rec = _engine(rt).recommend(camp)
    assert rec.inputs["answer_rate_used"] == pytest.approx(0.6)     # assumes HIGH answer rate -> fewer dials
    assert "only 5 answer samples" in rec.explain()
    _set_stats(rt, 0.2, samples=500)
    assert _engine(rt).recommend(camp).requested > rec.requested


def test_rising_answer_rate_uses_fast_estimate(runtime_factory):
    rt = runtime_factory(agents=20, borrowers=1000, mode=DialMode.PREDICTIVE)
    _set_stats(rt, 0.2, fast=0.6)
    rec = _engine(rt).recommend(rt.campaigns.get("camp"))
    assert rec.inputs["answer_rate_used"] == pytest.approx(0.6)
    assert any("unstable" in line for line in rec.explanation)


def test_explanation_is_complete(runtime_factory):
    rt = runtime_factory(agents=15, borrowers=1000, mode=DialMode.PREDICTIVE)
    _set_stats(rt, 0.4)
    text = _engine(rt).recommend(rt.campaigns.get("camp")).explain()
    for needle in ("Requested", "agents expected free", "answer rate", "target in-flight", "provider health",
                   "already ringing", "safety margin"):
        assert needle in text


def test_ewma_answer_rate_update(runtime_factory):
    rt = runtime_factory()
    rt.db.execute("UPDATE pacing_stats SET answer_rate_ewma=0.5, answer_rate_fast=0.5 WHERE campaign_id='camp'")
    rt.stats.record_dial_outcome("camp", True, 0.1, 0.5, 0.0)
    row = rt.stats.get("camp")
    assert row["answer_rate_ewma"] == pytest.approx(0.55) and row["answer_rate_fast"] == pytest.approx(0.75)
    rt.stats.record_dial_outcome("camp", False, 0.1, 0.5, 0.0)
    assert rt.stats.get("camp")["answer_rate_ewma"] == pytest.approx(0.495)


def test_engine_adapts_when_answer_rate_collapses(runtime_factory):
    """70% -> 10%: the estimate follows the data instead of staying stuck."""
    rt = runtime_factory(mode=DialMode.PREDICTIVE)
    cfg = rt.settings.pacing
    rt.db.execute("UPDATE pacing_stats SET answer_rate_ewma=0.7, answer_rate_fast=0.7, answer_samples=500")
    for i in range(100):
        rt.stats.record_dial_outcome("camp", i % 10 == 0, cfg.ewma_alpha, cfg.fast_ewma_alpha, 0.0)
    row = rt.stats.get("camp")
    assert row["answer_rate_fast"] < 0.15 and row["answer_rate_ewma"] < 0.15


class TestAdvisoryOnly:
    def test_engine_returns_unbound_recommendation_and_places_nothing(self, runtime_factory):
        rt = runtime_factory(agents=10, borrowers=100, mode=DialMode.PREDICTIVE)
        rec = rt.predictive.recommend(rt.campaigns.get("camp"))
        assert rec.binding == Binding.UNBOUND and rec.requested > 0
        assert rt.db.scalar("SELECT COUNT(*) FROM calls") == 0
        assert rt.db.scalar("SELECT COUNT(*) FROM dial_permits") == 0

    def test_engine_has_no_path_to_allocator_or_providers(self, runtime_factory):
        rt = runtime_factory()
        source = inspect.getsource(predictive_module)
        for forbidden in ("call_allocator", "CallAllocator", "providers.provider_a", "initiate_call",
                          "safety_controller", "SafetyConfig", "dial_permits"):
            assert forbidden not in source, forbidden
        held = [type(v).__name__ for v in vars(rt.predictive).values()]
        assert not any(name in held for name in ("CallAllocator", "ProviderRouter", "SafetyController",
                                                 "MockTelecomProvider", "ProviderA", "ProviderB"))
