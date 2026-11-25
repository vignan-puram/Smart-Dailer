"""Shared fixtures. Every test gets a fresh database and seeded randomness."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Callable

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import CircuitBreakerConfig, SafetyConfig, Settings  # noqa: E402
from app.logging_setup import configure_logging  # noqa: E402
from app.models.campaign import DialMode  # noqa: E402
from app.models.provider_event import ProviderEvent, ProviderEventType  # noqa: E402
from app.providers.mock_provider import ProviderProfile, StaticOutcomeModel  # noqa: E402
from app.runtime import Runtime, build_runtime  # noqa: E402

configure_logging("ERROR")

# A perfectly reliable provider: makes tests about *our* logic deterministic.
RELIABLE_A = ProviderProfile(name="provider_a", reject_prob=0.0, network_failure_prob=0.0,
                             setup_latency_jitter_ms=0.0, event_delay_jitter_s=0.0)


def make_runtime(*, agents: int = 0, borrowers: int = 0, mode: DialMode = DialMode.PROGRESSIVE,
                 db_path: str = ":memory:", answer_rate: float = 0.3, talk_s: float = 120.0,
                 safety: SafetyConfig | None = None, breaker: CircuitBreakerConfig | None = None,
                 profile_a: ProviderProfile = RELIABLE_A, weights: dict | None = None, seed: int = 42,
                 max_attempts: int = 3, max_concurrent: int = 100_000, start: bool = True) -> Runtime:
    settings = Settings(random_seed=seed, safety=safety or SafetyConfig(),
                        breaker=breaker or CircuitBreakerConfig())
    rt = build_runtime(settings, db_path=db_path, outcome_model=StaticOutcomeModel(answer_rate, talk_s),
                       provider_weights=weights or {"provider_a": 1.0, "provider_b": 0.0},
                       profile_a=profile_a, seed=seed, fast_db=True)
    rt.campaigns.create("camp", "test campaign", mode, max_attempts=max_attempts,
                        max_concurrent_calls=max_concurrent)
    if agents:
        rt.agents.create_many(agents)
    if borrowers:
        rt.borrowers.create_many("camp", borrowers)
    if start:
        rt.campaigns.start("camp")
    return rt


@pytest.fixture
def runtime_factory() -> Callable[..., Runtime]:
    created: list[Runtime] = []

    def factory(**kwargs) -> Runtime:
        rt = make_runtime(**kwargs)
        created.append(rt)
        return rt

    yield factory
    for rt in created:
        rt.db.close()


@pytest.fixture
def file_db(tmp_path) -> str:
    return str(tmp_path / "smartdialer_test.db")


def capture_events(rt: Runtime) -> list[ProviderEvent]:
    """Detach providers from the processor so a test can inject events by hand."""
    captured: list[ProviderEvent] = []
    for p in rt.providers.values():
        p.set_event_sink(captured.append)
    return captured


def place_single_call(rt: Runtime) -> tuple[str, str]:
    """Run one dial cycle with events captured; return (call_id, provider_call_id)."""
    capture_events(rt)
    results = rt.dial_cycle()
    alloc = next(r.allocation for r in results if r.allocation)
    call_id = alloc.call_ids[0]
    return call_id, rt.lifecycle.calls.get(call_id).provider_call_id


def ev(rt: Runtime, call_id: str, pcid: str, typ: ProviderEventType, seq: int, *, event_id: str | None = None,
       provider: str = "provider_a", **payload) -> ProviderEvent:
    return ProviderEvent(provider, event_id or f"{pcid}:{seq}:{typ.value}", call_id, pcid, typ, seq,
                         rt.clock.now(), payload)
