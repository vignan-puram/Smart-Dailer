"""One dialer worker's pacing cycle.

    housekeeping -> observe agents -> enforce capacity -> for each running campaign:
        engine.recommend()  ->  safety.evaluate()  ->  allocator.execute(permit)

The worker is the only object that connects these components. The pacing
engine returns a plain recommendation, the Safety Controller returns a
decision (and maybe a permit), and only the permit is handed to the allocator.
Several workers can run this loop concurrently. All coordination happens
through the database.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol

from app.models.call import CallOutcome
from app.models.campaign import Campaign, DialMode
from app.models.safety import PacingRecommendation, SafetyDecision
from app.services.agent_manager import AgentManager
from app.services.call_allocator import AllocationResult, CallAllocator
from app.services.campaign_manager import CampaignManager
from app.services.safety_controller import SafetyController


class PacingEngine(Protocol):
    engine_name: str

    def recommend(self, campaign: Campaign) -> PacingRecommendation: ...


@dataclass
class CycleResult:
    campaign_id: str
    recommendation: PacingRecommendation
    decision: Optional[SafetyDecision]
    allocation: Optional[AllocationResult]


class DialerWorker:
    def __init__(self, campaigns: CampaignManager, engines: dict[DialMode, PacingEngine],
                 safety: SafetyController, allocator: CallAllocator, agents: AgentManager) -> None:
        self._campaigns = campaigns
        self._engines = engines
        self._safety = safety
        self._allocator = allocator
        self._agents = agents
        self.cancelled_for_capacity = 0

    def run_cycle(self) -> list[CycleResult]:
        self._agents.housekeeping()
        self._safety.observe_agents()
        victims = self._safety.find_capacity_deficit()
        if victims:
            self.cancelled_for_capacity += len(victims)
            self._allocator.request_cancel(victims, CallOutcome.CANCELLED_CAPACITY)

        results: list[CycleResult] = []
        for campaign in self._campaigns.running():
            rec = self._engines[campaign.mode].recommend(campaign)
            if rec.requested <= 0:
                results.append(CycleResult(campaign.id, rec, None, None))
                continue
            decision = self._safety.evaluate(rec)
            allocation = self._allocator.execute(decision.permit) if decision.permit else None
            results.append(CycleResult(campaign.id, rec, decision, allocation))
        return results
