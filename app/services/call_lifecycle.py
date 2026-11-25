"""Shared logic for ending a call: one place decides how resources are released.

Used by the allocator (setup failures), the event processor (provider
outcomes) and the recovery manager (reconciliation). It must run inside the
caller's transaction, so the call, agent, borrower and statistics change together.
"""
from __future__ import annotations

import random
from typing import Optional

from app.clock import Clock
from app.config import DialerConfig, PacingConfig
from app.database.connection import Database
from app.database.repositories import (
    AgentRepository,
    BorrowerRepository,
    CallRepository,
    PacingStatsRepository,
)
from app.models.agent import AgentStatus
from app.models.borrower import BorrowerStatus
from app.models.call import Call, CallOutcome, CallStatus
from app.services.borrower_manager import retry_delay

# Outcomes that never reached the borrower's phone: they don't burn an attempt.
NOT_AN_ATTEMPT = {CallOutcome.SETUP_FAILED, CallOutcome.CANCELLED_NOT_PLACED, CallOutcome.PROVIDER_FAILED}
SHORT_RETRY = {CallOutcome.SETUP_FAILED, CallOutcome.CANCELLED_NOT_PLACED, CallOutcome.PROVIDER_FAILED,
               CallOutcome.CANCELLED_CAPACITY, CallOutcome.CANCELLED_AGENT_LOST, CallOutcome.UNRESOLVED}


class CallLifecycle:
    def __init__(self, db: Database, clock: Clock, dialer_cfg: DialerConfig, pacing_cfg: PacingConfig,
                 stats: PacingStatsRepository, seed: int = 3) -> None:
        self.db = db
        self.clock = clock
        self.dcfg = dialer_cfg
        self.pcfg = pacing_cfg
        self.stats = stats
        self.calls = CallRepository(db)
        self.agents = AgentRepository(db)
        self.borrowers = BorrowerRepository(db)
        self.rng = random.Random(seed)

    def finish(self, call: Call, status: CallStatus, outcome: CallOutcome, *, talk_s: Optional[float] = None,
               answered: bool = False) -> bool:
        """Move ``call`` to a terminal state and release its agent and borrower.

        Returns False (and changes nothing) if another worker already moved the
        call. The compare-and-set on the call status is the guard.
        """
        now = self.clock.now()
        moved = self.calls.transition(call.id, call.status, status, now, outcome=outcome.value, ended_at=now,
                                      talk_duration_s=talk_s, lease_owner=None, lease_until=None)
        if not moved:
            return False
        talked = outcome in (CallOutcome.CONNECTED, CallOutcome.AGENT_DROPPED) or (
            answered and call.status == CallStatus.CONNECTED)
        if call.agent_id:
            if talked:
                self.agents.release_from_call(call.agent_id, call.id, AgentStatus.WRAP_UP, now,
                                              wrap_up_until=now + self.dcfg.wrap_up_s)
            else:
                self.agents.release_from_call(call.agent_id, call.id, AgentStatus.AVAILABLE, now)
        self._release_borrower(call, outcome, now)
        if talked and talk_s:
            self.stats.record_talk_time(call.campaign_id, talk_s, self.pcfg.ewma_alpha, now)
        return True

    def _release_borrower(self, call: Call, outcome: CallOutcome, now: float) -> None:
        row = self.db.query_one(
            "SELECT b.attempt_count, c.max_attempts, c.retry_delay_s FROM borrowers b "
            "JOIN campaigns c ON c.id=b.campaign_id WHERE b.id=?", (call.borrower_id,))
        if row is None:
            return
        counts = outcome not in NOT_AN_ATTEMPT
        attempts_after = row["attempt_count"] + (1 if counts else 0)
        if outcome == CallOutcome.CONNECTED:
            new_status, next_at = BorrowerStatus.COMPLETED, now
        elif attempts_after >= row["max_attempts"]:
            new_status, next_at = BorrowerStatus.EXHAUSTED, now
        elif outcome == CallOutcome.ABANDONED:
            new_status, next_at = BorrowerStatus.RETRY_WAIT, now + 60.0   # prompt call-back
        elif outcome in SHORT_RETRY:
            new_status, next_at = BorrowerStatus.RETRY_WAIT, now + self.dcfg.setup_failure_retry_s
        else:
            new_status = BorrowerStatus.RETRY_WAIT
            next_at = now + retry_delay(row["retry_delay_s"], attempts_after, self.dcfg.retry_max_delay_s, self.rng)
        self.borrowers.release(call.borrower_id, call.id, new_status, now, next_at, counts)
