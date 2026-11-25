"""Progressive pacing: one available agent -> one outbound call.

The progressive dialer *recommends* ``min(idle agents, eligible borrowers,
campaign headroom)`` calls with ``Binding.BOUND``. The Safety Controller
approves them, and the Call Allocator then runs the progressive sequence for
each approved slot, inside one transaction:

    reserve agent -> reserve borrower -> create call row -> (commit) -> initiate

Because every call needs an agent reservation that succeeded *before* the
provider is contacted, 50 idle agents can never produce more than 50
agent-bound calls. The 51st reservation finds no AVAILABLE row and the slot
is discarded.
"""
from __future__ import annotations

from app.clock import Clock
from app.database.repositories import BorrowerRepository, MetricsReader
from app.models.call import Binding
from app.models.campaign import Campaign
from app.models.safety import PacingRecommendation


class ProgressiveDialer:
    engine_name = "PROGRESSIVE"

    def __init__(self, metrics: MetricsReader, borrowers: BorrowerRepository, clock: Clock) -> None:
        self._metrics = metrics
        self._borrowers = borrowers
        self._clock = clock

    def recommend(self, campaign: Campaign) -> PacingRecommendation:
        now = self._clock.now()
        idle = self._metrics.db.scalar(
            "SELECT COUNT(*) FROM agents WHERE status='AVAILABLE' AND pause_requested=0")
        eligible = self._borrowers.count_eligible(campaign.id, now, campaign.max_attempts)
        headroom = max(0, campaign.max_concurrent_calls - self._metrics.campaign_active_calls(campaign.id))
        requested = max(0, min(idle, eligible, headroom))
        explanation = (
            f"{idle} agents are AVAILABLE right now (one call per idle agent)",
            f"{eligible} borrowers are eligible to be called",
            f"campaign concurrency headroom is {headroom}",
            "each call reserves its agent before the provider is contacted (BOUND)",
        )
        return PacingRecommendation(campaign.id, self.engine_name, requested, Binding.BOUND, explanation,
                                    {"idle_agents": idle, "eligible_borrowers": eligible, "headroom": headroom})
