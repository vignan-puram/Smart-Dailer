"""Predictive pacing engine: transparent, statistical, and *advisory only*.

This module has no reference to the Call Allocator or to any telecom
provider. Its only output is a ``PacingRecommendation``, a number plus a
human-readable explanation. Whether any call is actually placed is decided by
the Safety Controller.

Algorithm (evaluated every pacing tick)
---------------------------------------
1. Answer-rate estimate ``p``
   * slow EWMA (alpha=0.05) for stability, fast EWMA (alpha=0.10) for change detection;
   * insufficient data (< min_samples) -> assume a *high* answer rate
     (``conservative_answer_rate``). A high assumed rate means fewer dials;
   * if the fast EWMA is above the slow one (answers rising) use the fast one
     (more answers = more risk). Otherwise average the two, adapting downward
     cautiously.
2. Horizon ``H`` = EWMA of dial-to-answer time (how far ahead we must plan).
3. Agents expected free within H:
       idle + wrap-ups ending within H + connected * min(1, H / avg_talk_time)
   (memoryless approximation of conversations ending in the next H seconds).
4. Target in-flight dials = expected_free / p, capped at ``max_dial_ratio * expected_free``.
5. Provider health factor: GOOD 1.0, DEGRADED 0.5, DOWN 0.0. An unstable
   answer rate multiplies by 0.7.
6. Recommendation = target - calls already ringing - safety margin, clipped to
   borrower supply and campaign headroom.
"""
from __future__ import annotations

import math
from typing import Callable

from app.clock import Clock
from app.config import PacingConfig
from app.database.repositories import BorrowerRepository, MetricsReader, PacingStatsRepository
from app.models.call import Binding
from app.models.campaign import Campaign
from app.models.safety import PacingRecommendation, ProviderHealthLevel

HEALTH_FACTOR = {ProviderHealthLevel.GOOD: 1.0, ProviderHealthLevel.DEGRADED: 0.5, ProviderHealthLevel.DOWN: 0.0}


class PredictivePacingEngine:
    engine_name = "PREDICTIVE"

    def __init__(self, metrics: MetricsReader, stats: PacingStatsRepository, borrowers: BorrowerRepository,
                 provider_health: Callable[[], ProviderHealthLevel], clock: Clock, config: PacingConfig) -> None:
        self._metrics = metrics
        self._stats = stats
        self._borrowers = borrowers
        self._provider_health = provider_health
        self._clock = clock
        self._cfg = config

    def estimate_answer_rate(self, slow: float, fast: float, samples: int) -> tuple[float, str]:
        cfg = self._cfg
        if samples < cfg.min_samples:
            p = max(slow, cfg.conservative_answer_rate)
            return p, (f"only {samples} answer samples (< {cfg.min_samples}): assuming a conservative "
                       f"{p:.0%} answer rate so we dial fewer calls")
        if fast > slow:
            return fast, f"answer rate rising (fast EWMA {fast:.1%} > slow {slow:.1%}): using {fast:.1%}"
        p = (slow + fast) / 2.0
        return p, f"historical answer rate is {p:.1%} (slow EWMA {slow:.1%}, fast EWMA {fast:.1%})"

    def recommend(self, campaign: Campaign) -> PacingRecommendation:
        now = self._clock.now()
        cfg = self._cfg
        self._stats.ensure(campaign.id, now)
        st = self._stats.get(campaign.id)
        slow, fast, samples = st["answer_rate_ewma"], st["answer_rate_fast"], st["answer_samples"]
        talk = max(1.0, st["talk_time_ewma"])
        horizon = max(1.0, st["setup_time_ewma"])

        p, p_reason = self.estimate_answer_rate(slow, fast, samples)
        p = min(max(p, 0.01), 1.0)

        agents = self._metrics.agent_counts()
        idle = agents["AVAILABLE"]
        wrapups_soon = sum(1 for r in self._metrics.wrapup_remaining(now) if r <= horizon)
        connected = agents["CONNECTED"]
        ending_soon = connected * min(1.0, horizon / talk)
        expected_free = idle + wrapups_soon + ending_soon

        in_flight = self._metrics.unbound_in_flight()
        ringing = self._metrics.ringing_calls()
        target = expected_free / p
        target = min(target, cfg.max_dial_ratio * expected_free)

        health = self._provider_health()
        factor = HEALTH_FACTOR[health]
        volatility = abs(fast - slow) / max(slow, 0.05)
        unstable = samples >= cfg.min_samples and volatility > 0.5
        if unstable:
            factor *= 0.7

        eligible = self._borrowers.count_eligible(campaign.id, now, campaign.max_attempts)
        headroom = max(0, campaign.max_concurrent_calls - self._metrics.campaign_active_calls(campaign.id))
        raw = math.floor(target * factor) - in_flight - cfg.safety_margin_calls
        requested = max(0, min(raw, eligible, headroom))

        explanation = [
            f"{idle} agents idle, {wrapups_soon} finish wrap-up within {horizon:.0f}s, "
            f"~{ending_soon:.1f} of {connected} conversations end within {horizon:.0f}s "
            f"(avg talk {talk:.0f}s) -> {expected_free:.1f} agents expected free",
            p_reason,
            f"target in-flight dials = {expected_free:.1f} / {p:.2f} = {expected_free / p:.1f}"
            + (f" (capped at {cfg.max_dial_ratio:.0f}x)" if target < expected_free / p else ""),
            f"provider health is {health.value} (factor {HEALTH_FACTOR[health]:.1f})"
            + (f"; answer rate unstable (volatility {volatility:.0%}) -> factor x0.7" if unstable else ""),
            f"{in_flight} pooled calls already ringing ({ringing} ringing in total)",
            f"safety margin is {cfg.safety_margin_calls}",
            f"{eligible} eligible borrowers, campaign headroom {headroom}",
        ]
        inputs = {
            "idle_agents": idle, "wrapups_soon": wrapups_soon, "connected": connected,
            "expected_free": round(expected_free, 2), "answer_rate_used": round(p, 4),
            "answer_rate_slow": round(slow, 4), "answer_rate_fast": round(fast, 4), "samples": samples,
            "avg_talk_s": round(talk, 1), "horizon_s": round(horizon, 2), "in_flight": in_flight,
            "provider_health": health.value, "factor": round(factor, 2), "target": round(target, 2),
        }
        return PacingRecommendation(campaign.id, self.engine_name, requested, Binding.UNBOUND,
                                    tuple(explanation), inputs)
