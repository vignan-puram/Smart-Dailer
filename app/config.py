"""Central configuration.

All configuration objects are *frozen* dataclasses. This matters for safety:
the Safety Controller receives its ``SafetyConfig`` at construction time from
the composition root (``app/runtime.py``). No other component holds a
reference to it, and because it is immutable nobody (in particular the
predictive engine) can flip the safety mode at runtime.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return float(raw) if raw not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw not in (None, "") else default


@dataclass(frozen=True)
class SafetyConfig:
    # STRICT: an answered borrower always has an agent (deterministic, default).
    # BOUNDED_RISK: operator-approved statistical over-dial, P(abandon) <= epsilon.
    mode: str = "STRICT"
    abandon_risk_epsilon: float = 0.01
    # BOUNDED_RISK only over-dials when the answer-rate estimate is backed by
    # at least this many samples. Otherwise it falls back to progressive.
    min_samples_for_overdial: int = 200
    # Upper confidence bound multiplier applied to the answer-rate estimate
    # before computing the over-dial bound (pessimistic = safer).
    answer_rate_confidence_z: float = 2.0
    # Earliest time after dialing at which a borrower can possibly answer
    # (setup latency + minimum ring). An agent whose system-enforced wrap-up
    # ends inside this window is guaranteed free before any answer can arrive,
    # so it can be counted as capacity *deterministically*.
    guaranteed_min_answer_delay_s: float = 3.0
    enable_wrapup_lookahead: bool = True
    # Global hard cap on simultaneously active calls.
    max_outstanding_calls: int = 5000
    # Agents deliberately held back from predictive capacity.
    capacity_reserve_agents: int = 0
    permit_ttl_s: float = 5.0
    # If logged-in agent count drops by more than this fraction within
    # ``availability_drop_window_s`` the controller stops pooled dialing.
    availability_drop_threshold: float = 0.2
    availability_drop_window_s: float = 10.0


@dataclass(frozen=True)
class PacingConfig:
    initial_answer_rate: float = 0.30
    ewma_alpha: float = 0.05           # slow estimate (stable)
    fast_ewma_alpha: float = 0.10      # fast estimate (change detection)
    min_samples: int = 30
    # With insufficient data assume a *high* answer rate: dial fewer calls.
    conservative_answer_rate: float = 0.60
    max_dial_ratio: float = 4.0        # never more than N dials per expected free agent
    safety_margin_calls: int = 1
    default_talk_time_s: float = 120.0
    default_setup_time_s: float = 8.0


@dataclass(frozen=True)
class DialerConfig:
    wrap_up_s: float = 15.0
    agent_reservation_lease_s: float = 45.0
    setup_lease_s: float = 10.0
    ring_timeout_s: float = 30.0
    max_call_duration_s: float = 3600.0
    event_silence_reconcile_s: float = 60.0
    retry_base_delay_s: float = 300.0
    retry_max_delay_s: float = 3600.0
    setup_failure_retry_s: float = 30.0
    max_attempts: int = 3
    pacing_interval_s: float = 1.0
    recovery_interval_s: float = 5.0


@dataclass(frozen=True)
class CircuitBreakerConfig:
    failure_threshold: int = 5          # consecutive failures -> OPEN
    failure_rate_threshold: float = 0.5  # EWMA failure rate -> OPEN
    min_requests_for_rate: int = 20
    base_open_s: float = 10.0
    max_open_s: float = 300.0
    jitter_fraction: float = 0.2
    half_open_max_probes: int = 1
    ewma_alpha: float = 0.1
    degraded_failure_rate: float = 0.15
    degraded_latency_ms: float = 1500.0


@dataclass(frozen=True)
class Settings:
    database_path: str = "data/smartdialer.db"
    random_seed: int = 42
    safety: SafetyConfig = field(default_factory=SafetyConfig)
    pacing: PacingConfig = field(default_factory=PacingConfig)
    dialer: DialerConfig = field(default_factory=DialerConfig)
    breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)

    @staticmethod
    def from_env() -> "Settings":
        safety = SafetyConfig(
            mode=os.getenv("SMARTDIALER_SAFETY_MODE", "STRICT").upper(),
            abandon_risk_epsilon=_env_float("SMARTDIALER_ABANDON_EPSILON", 0.01),
            max_outstanding_calls=_env_int("SMARTDIALER_MAX_OUTSTANDING_CALLS", 5000),
        )
        return Settings(
            database_path=os.getenv("SMARTDIALER_DB_PATH", "data/smartdialer.db"),
            random_seed=_env_int("SMARTDIALER_SEED", 42),
            safety=safety,
        )
