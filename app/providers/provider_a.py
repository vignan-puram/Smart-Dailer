"""Provider A: fast, reliable, low failure rate, predictable latency, well-behaved webhooks."""
from __future__ import annotations

from app.clock import Scheduler
from app.providers.mock_provider import MockTelecomProvider, OutcomeModel, ProviderProfile

PROVIDER_A_PROFILE = ProviderProfile(
    name="provider_a",
    setup_latency_ms=80.0,
    setup_latency_jitter_ms=10.0,
    reject_prob=0.005,
    timeout_prob=0.0,
    network_failure_prob=0.005,
    event_delay_s=0.1,
    event_delay_jitter_s=0.02,
    duplicate_prob=0.0,
    out_of_order_prob=0.0,
)


class ProviderA(MockTelecomProvider):
    def __init__(self, scheduler: Scheduler, outcome_model: OutcomeModel, seed: int,
                 profile: ProviderProfile = PROVIDER_A_PROFILE) -> None:
        super().__init__(profile, scheduler, outcome_model, seed)
