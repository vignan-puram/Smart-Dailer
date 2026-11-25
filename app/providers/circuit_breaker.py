"""Persisted per-provider circuit breaker: CLOSED -> OPEN -> HALF_OPEN -> CLOSED.

State lives in the ``provider_health`` table, so every dialer worker shares the
same view. All state changes are conditional UPDATEs, so two workers cannot both
win the single HALF_OPEN probe slot.

* CLOSED: requests flow. Trips to OPEN after ``failure_threshold`` consecutive
  failures, or when the EWMA failure rate exceeds ``failure_rate_threshold``.
* OPEN: no new calls to this provider until ``retry_at``. The cool-down grows
  exponentially with each consecutive opening (``base * 2^n``, capped) and has
  +/- jitter, so workers don't all probe at the same instant (no retry storm).
* HALF_OPEN: at most ``half_open_max_probes`` trial calls. Success -> CLOSED,
  failure -> OPEN with a longer cool-down.
"""
from __future__ import annotations

import random
from typing import Optional

from app.clock import Clock
from app.config import CircuitBreakerConfig
from app.database.connection import Database
from app.logging_setup import get_logger, log_event
from app.models.safety import ProviderHealthLevel

log = get_logger("circuit_breaker")


class CircuitBreaker:
    def __init__(self, db: Database, clock: Clock, config: CircuitBreakerConfig, seed: int = 7) -> None:
        self.db = db
        self.clock = clock
        self.cfg = config
        self.rng = random.Random(seed)

    def ensure(self, provider: str) -> None:
        self.db.execute(
            "INSERT OR IGNORE INTO provider_health (provider, state, updated_at) VALUES (?, 'CLOSED', ?)",
            (provider, self.clock.now()),
        )

    def get(self, provider: str):
        return self.db.query_one("SELECT * FROM provider_health WHERE provider=?", (provider,))

    def all(self):
        return self.db.query("SELECT * FROM provider_health ORDER BY provider")

    # ------------------------------------------------------------- admission
    def allow_request(self, provider: str) -> bool:
        now = self.clock.now()
        row = self.get(provider)
        if row is None:
            self.ensure(provider)
            return True
        if row["state"] == "CLOSED":
            return True
        if row["state"] == "OPEN":
            if row["retry_at"] is None or now < row["retry_at"]:
                return False
            moved = self.db.execute(
                "UPDATE provider_health SET state='HALF_OPEN', half_open_in_flight=0, updated_at=? "
                "WHERE provider=? AND state='OPEN' AND retry_at <= ?", (now, provider, now)).rowcount
            if moved:
                log_event(log, "circuit_half_open", provider=provider, at=now)
        # HALF_OPEN: atomically take one of the limited probe slots.
        took = self.db.execute(
            """UPDATE provider_health SET half_open_in_flight = half_open_in_flight + 1, updated_at=?
               WHERE provider=? AND state='HALF_OPEN' AND half_open_in_flight < ?""",
            (now, provider, self.cfg.half_open_max_probes)).rowcount
        return took == 1

    def is_closed(self, provider: str) -> bool:
        row = self.get(provider)
        return row is None or row["state"] == "CLOSED"

    # ------------------------------------------------------------- feedback
    def record_success(self, provider: str, latency_ms: float) -> None:
        now = self.clock.now()
        a = self.cfg.ewma_alpha
        with self.db.transaction() as conn:
            row = conn.execute("SELECT state FROM provider_health WHERE provider=?", (provider,)).fetchone()
            conn.execute(
                """UPDATE provider_health SET total_requests=total_requests+1, consecutive_failures=0,
                       ewma_latency_ms = CASE WHEN total_requests=0 THEN ? ELSE (1-?)*ewma_latency_ms + ?*? END,
                       ewma_failure_rate = (1-?)*ewma_failure_rate, updated_at=?
                   WHERE provider=?""",
                (latency_ms, a, a, latency_ms, a, now, provider))
            if row and row["state"] == "HALF_OPEN":
                conn.execute(
                    """UPDATE provider_health SET state='CLOSED', open_count=0, half_open_in_flight=0,
                           ewma_failure_rate=0, retry_at=NULL, updated_at=? WHERE provider=? AND state='HALF_OPEN'""",
                    (now, provider))
                log_event(log, "circuit_closed", provider=provider, at=now)

    def record_failure(self, provider: str, latency_ms: float, timeout: bool) -> None:
        now = self.clock.now()
        a = self.cfg.ewma_alpha
        with self.db.transaction() as conn:
            conn.execute(
                """UPDATE provider_health SET total_requests=total_requests+1, total_failures=total_failures+1,
                       total_timeouts=total_timeouts+?, consecutive_failures=consecutive_failures+1,
                       ewma_latency_ms = (1-?)*ewma_latency_ms + ?*?,
                       ewma_failure_rate = (1-?)*ewma_failure_rate + ?, updated_at=?
                   WHERE provider=?""",
                (1 if timeout else 0, a, a, latency_ms, a, a, now, provider))
            row = conn.execute("SELECT * FROM provider_health WHERE provider=?", (provider,)).fetchone()
            if row is None:
                return
            should_open = row["state"] == "HALF_OPEN" or (
                row["state"] == "CLOSED" and (
                    row["consecutive_failures"] >= self.cfg.failure_threshold
                    or (row["total_requests"] >= self.cfg.min_requests_for_rate
                        and row["ewma_failure_rate"] >= self.cfg.failure_rate_threshold)))
            if should_open:
                cooldown = self._cooldown(row["open_count"])
                conn.execute(
                    """UPDATE provider_health SET state='OPEN', opened_at=?, retry_at=?, open_count=open_count+1,
                           total_openings=total_openings+1, half_open_in_flight=0, updated_at=?
                       WHERE provider=?""",
                    (now, now + cooldown, now, provider))
                log_event(log, "circuit_opened", provider=provider, at=now, cooldown_s=round(cooldown, 2),
                          consecutive_failures=row["consecutive_failures"],
                          ewma_failure_rate=round(row["ewma_failure_rate"], 3))

    def _cooldown(self, open_count: int) -> float:
        """Exponential backoff with jitter: base * 2^n, capped, then +/- jitter."""
        base = min(self.cfg.max_open_s, self.cfg.base_open_s * (2 ** open_count))
        jitter = base * self.cfg.jitter_fraction
        return max(1.0, base + self.rng.uniform(-jitter, jitter))

    # --------------------------------------------------------------- health
    def health_level(self, provider: str) -> ProviderHealthLevel:
        row = self.get(provider)
        if row is None:
            return ProviderHealthLevel.GOOD
        return self._level(row)

    def _level(self, row) -> ProviderHealthLevel:
        if row["state"] == "OPEN":
            # Once the cool-down has elapsed a probe is allowed. Reporting DOWN
            # here would make pacing request nothing, so the circuit could never
            # be probed and would stay open forever.
            if row["retry_at"] is not None and row["retry_at"] <= self.clock.now():
                return ProviderHealthLevel.DEGRADED
            return ProviderHealthLevel.DOWN
        if row["state"] == "HALF_OPEN":
            return ProviderHealthLevel.DEGRADED
        if (row["ewma_failure_rate"] >= self.cfg.degraded_failure_rate
                or row["ewma_latency_ms"] >= self.cfg.degraded_latency_ms):
            return ProviderHealthLevel.DEGRADED
        return ProviderHealthLevel.GOOD

    def overall_health(self, providers: Optional[list[str]] = None) -> ProviderHealthLevel:
        rows = self.all()
        if providers is not None:
            rows = [r for r in rows if r["provider"] in providers]
        if not rows:
            return ProviderHealthLevel.GOOD
        levels = [self._level(r) for r in rows]
        if ProviderHealthLevel.GOOD in levels:
            return ProviderHealthLevel.GOOD
        if ProviderHealthLevel.DEGRADED in levels:
            return ProviderHealthLevel.DEGRADED
        return ProviderHealthLevel.DOWN
