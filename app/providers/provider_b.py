"""Provider B: slower, occasional (ambiguous) timeouts, higher failure rate,
duplicate webhooks and out-of-order delivery. Same interface as Provider A."""
from __future__ import annotations

from app.clock import Scheduler
from app.providers.mock_provider import MockTelecomProvider, OutcomeModel, ProviderProfile

PROVIDER_B_PROFILE = ProviderProfile(
    name="provider_b",
    setup_latency_ms=900.0,
    setup_latency_jitter_ms=400.0,
    reject_prob=0.03,
    timeout_prob=0.03,
    timeout_ms=5000.0,
    timeout_call_placed_prob=0.5,
    network_failure_prob=0.03,
    event_delay_s=1.0,
    event_delay_jitter_s=0.8,
    duplicate_prob=0.15,
    out_of_order_prob=0.10,
    out_of_order_extra_delay_s=(2.0, 12.0),
)


class ProviderB(MockTelecomProvider):
    def __init__(self, scheduler: Scheduler, outcome_model: OutcomeModel, seed: int,
                 profile: ProviderProfile = PROVIDER_B_PROFILE) -> None:
        super().__init__(profile, scheduler, outcome_model, seed)
