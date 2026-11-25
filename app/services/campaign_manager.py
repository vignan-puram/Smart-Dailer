"""Campaign management."""
from __future__ import annotations

from app.clock import Clock
from app.database.connection import Database
from app.database.repositories import CampaignRepository, PacingStatsRepository
from app.models.campaign import Campaign, CampaignStatus, DialMode


class CampaignNotFound(LookupError):
    pass


class CampaignManager:
    def __init__(self, db: Database, clock: Clock, stats: PacingStatsRepository) -> None:
        self._repo = CampaignRepository(db)
        self._stats = stats
        self._clock = clock

    def create(self, campaign_id: str, name: str, mode: DialMode = DialMode.PROGRESSIVE, max_attempts: int = 3,
               retry_delay_s: float = 300.0, max_concurrent_calls: int = 1000) -> Campaign:
        now = self._clock.now()
        self._repo.create(campaign_id, name, mode, max_attempts, retry_delay_s, max_concurrent_calls, now)
        self._stats.ensure(campaign_id, now)
        return self.get(campaign_id)

    def get(self, campaign_id: str) -> Campaign:
        c = self._repo.get(campaign_id)
        if c is None:
            raise CampaignNotFound(campaign_id)
        return c

    def list(self) -> list[Campaign]:
        return self._repo.list()

    def start(self, campaign_id: str) -> Campaign:
        self.get(campaign_id)
        self._repo.set_status(campaign_id, CampaignStatus.RUNNING, self._clock.now())
        return self.get(campaign_id)

    def pause(self, campaign_id: str) -> Campaign:
        self.get(campaign_id)
        self._repo.set_status(campaign_id, CampaignStatus.PAUSED, self._clock.now())
        return self.get(campaign_id)

    def set_mode(self, campaign_id: str, mode: DialMode) -> Campaign:
        self.get(campaign_id)
        self._repo.set_mode(campaign_id, mode, self._clock.now())
        return self.get(campaign_id)

    def running(self) -> list[Campaign]:
        return [c for c in self._repo.list() if c.status == CampaignStatus.RUNNING]
