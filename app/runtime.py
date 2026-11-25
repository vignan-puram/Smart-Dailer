"""Composition root: the one place where components are wired together.

Wiring is where the safety boundary becomes structural:

* the predictive engine gets only read-only metrics, statistics and a health
  callback. It never receives the allocator, the router or a provider;
* only the SafetyController gets the (frozen) ``SafetyConfig``;
* the allocator gets the router (and therefore providers), but it only acts on
  permits.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from app.clock import Scheduler, SimClock
from app.config import Settings
from app.database.connection import Database
from app.database.repositories import BorrowerRepository, MetricsReader, PacingStatsRepository
from app.models.campaign import DialMode
from app.providers.base_provider import TelecomProvider
from app.providers.circuit_breaker import CircuitBreaker
from app.providers.mock_provider import MockTelecomProvider, OutcomeModel, ProviderProfile, StaticOutcomeModel
from app.providers.provider_a import PROVIDER_A_PROFILE, ProviderA
from app.providers.provider_b import PROVIDER_B_PROFILE, ProviderB
from app.providers.router import ProviderRouter
from app.services.agent_manager import AgentManager
from app.services.borrower_manager import BorrowerManager
from app.services.call_allocator import CallAllocator
from app.services.call_lifecycle import CallLifecycle
from app.services.campaign_manager import CampaignManager
from app.services.dialer_worker import CycleResult, DialerWorker
from app.services.event_processor import EventProcessor
from app.services.predictive_engine import PredictivePacingEngine
from app.services.progressive_dialer import ProgressiveDialer
from app.services.recovery_manager import RecoveryManager
from app.services.safety_controller import SafetyController


@dataclass
class Runtime:
    settings: Settings
    db: Database
    clock: SimClock
    scheduler: Scheduler
    providers: dict[str, MockTelecomProvider]
    router: ProviderRouter
    breaker: CircuitBreaker
    stats: PacingStatsRepository
    metrics: MetricsReader
    lifecycle: CallLifecycle
    processor: EventProcessor
    allocator: CallAllocator
    safety: SafetyController
    agents: AgentManager
    borrowers: BorrowerManager
    campaigns: CampaignManager
    progressive: ProgressiveDialer
    predictive: PredictivePacingEngine
    worker: DialerWorker
    recovery: RecoveryManager
    outcome_model: OutcomeModel
    last_cycle: list[CycleResult] = field(default_factory=list)
    _last_recovery: float = 0.0

    def dial_cycle(self) -> list[CycleResult]:
        self.last_cycle = self.worker.run_cycle()
        return self.last_cycle

    def advance(self, seconds: float, *, dial: bool = True) -> None:
        """Advance virtual time: deliver due provider events, run pacing cycles
        every ``pacing_interval_s`` and recovery every ``recovery_interval_s``."""
        step = self.settings.dialer.pacing_interval_s
        target = self.clock.now() + seconds
        while self.clock.now() < target - 1e-9:
            self.scheduler.run_until(min(target, self.clock.now() + step))
            if dial:
                self.dial_cycle()
            if self.clock.now() - self._last_recovery >= self.settings.dialer.recovery_interval_s:
                self.recovery.run()
                self._last_recovery = self.clock.now()

    def drain(self, max_seconds: float = 3600.0) -> None:
        """Let in-flight calls finish without dialing new ones."""
        end = self.clock.now() + max_seconds
        while self.clock.now() < end:
            active = self.db.scalar("SELECT COUNT(*) FROM calls WHERE status IN "
                                    "('QUEUED','RESERVED','INITIATED','RINGING','ANSWERED','CONNECTED')")
            if not active and self.scheduler.pending() == 0:
                break
            self.advance(5.0, dial=False)


def build_runtime(settings: Optional[Settings] = None, *, db_path: Optional[str] = None,
                  outcome_model: Optional[OutcomeModel] = None,
                  provider_weights: Optional[dict[str, float]] = None,
                  profile_a: ProviderProfile = PROVIDER_A_PROFILE,
                  profile_b: ProviderProfile = PROVIDER_B_PROFILE,
                  seed: Optional[int] = None, fast_db: bool = False, start_time: float = 0.0) -> Runtime:
    settings = settings or Settings()
    seed = settings.random_seed if seed is None else seed
    db = Database(db_path or settings.database_path, fast=fast_db)
    db.init_schema()
    clock = SimClock(start_time)
    scheduler = Scheduler(clock)
    outcome_model = outcome_model or StaticOutcomeModel()

    provider_a = ProviderA(scheduler, outcome_model, seed=seed * 1000 + 1, profile=profile_a)
    provider_b = ProviderB(scheduler, outcome_model, seed=seed * 1000 + 2, profile=profile_b)
    providers: dict[str, MockTelecomProvider] = {provider_a.name: provider_a, provider_b.name: provider_b}
    breaker = CircuitBreaker(db, clock, settings.breaker, seed=seed + 5)
    router = ProviderRouter(list(providers.values()), breaker,
                            weights=provider_weights or {provider_a.name: 0.7, provider_b.name: 0.3},
                            seed=seed + 6)

    pcfg = settings.pacing
    stats = PacingStatsRepository(db, pcfg.initial_answer_rate, pcfg.default_talk_time_s, pcfg.default_setup_time_s)
    metrics = MetricsReader(db)
    lifecycle = CallLifecycle(db, clock, settings.dialer, pcfg, stats, seed=seed + 3)
    processor = EventProcessor(db, clock, lifecycle, router, settings.dialer, pcfg, stats)
    for p in providers.values():
        p.set_event_sink(processor.handle)
    allocator = CallAllocator(db, clock, router, lifecycle, settings.dialer, pcfg, stats)
    safety = SafetyController(db, clock, settings.safety, breaker, router.routable_names(), pcfg)
    agents = AgentManager(db, clock, headroom=safety.pooled_headroom)
    borrowers = BorrowerManager(db, clock)
    campaigns = CampaignManager(db, clock, stats)
    borrower_repo = BorrowerRepository(db)
    progressive = ProgressiveDialer(metrics, borrower_repo, clock)
    predictive = PredictivePacingEngine(metrics, stats, borrower_repo,
                                        lambda: breaker.overall_health(router.routable_names()), clock, pcfg)
    worker = DialerWorker(campaigns, {DialMode.PROGRESSIVE: progressive, DialMode.PREDICTIVE: predictive},
                          safety, allocator, agents)
    recovery = RecoveryManager(db, clock, router, processor, allocator, lifecycle, settings.dialer)
    agents.set_agent_lost_handler(recovery.handle_agent_lost)
    return Runtime(settings, db, clock, scheduler, providers, router, breaker, stats, metrics, lifecycle,
                   processor, allocator, safety, agents, borrowers, campaigns, progressive, predictive,
                   worker, recovery, outcome_model, _last_recovery=start_time)


def provider_list(rt: Runtime) -> list[TelecomProvider]:
    return list(rt.providers.values())
