"""Agent lifecycle management.

Voluntary vs involuntary unavailability
---------------------------------------
* **Voluntary** (pause, logout): if pooled calls are already counting on this
  agent, the request is *deferred* (``pause_requested``). The agent stays
  claimable until enough headroom exists. Voluntary actions therefore never
  break the safety invariant.
* **Involuntary** (``disappear``: browser crash, network loss): applied
  immediately. Whatever call the agent was attached to is handed to the
  recovery manager (re-bind to another agent, or cancel), and the next dialer
  cycle recomputes capacity from the database.
"""
from __future__ import annotations

from enum import Enum
from typing import Callable, Optional

from app.clock import Clock
from app.database.connection import Database
from app.database.repositories import AgentRepository
from app.logging_setup import get_logger, log_event
from app.models.agent import Agent, AgentStatus, InvalidAgentTransition, validate_transition

log = get_logger("agent_manager")
S = AgentStatus


class PauseResult(str, Enum):
    PAUSED = "PAUSED"
    PENDING = "PENDING"        # will pause once commitments allow
    OFFLINE = "OFFLINE"


class AgentNotFound(LookupError):
    pass


class AgentManager:
    def __init__(self, db: Database, clock: Clock, headroom: Callable[[], int]) -> None:
        self._db = db
        self._agents = AgentRepository(db)
        self._clock = clock
        self._headroom = headroom
        self._on_agent_lost: Optional[Callable[[str, str], None]] = None

    def set_agent_lost_handler(self, handler: Callable[[str, str], None]) -> None:
        self._on_agent_lost = handler

    # ------------------------------------------------------------ creation
    def create(self, agent_id: str, name: Optional[str] = None, status: AgentStatus = S.OFFLINE) -> Agent:
        self._agents.create(agent_id, name or agent_id, status, self._clock.now())
        return self.get(agent_id)

    def create_many(self, count: int, prefix: str = "agent", status: AgentStatus = S.AVAILABLE) -> list[str]:
        ids = [f"{prefix}-{i:05d}" for i in range(count)]
        self._agents.create_many([(i, i, status) for i in ids], self._clock.now())
        return ids

    def create_ids(self, ids: list[str], status: AgentStatus = S.AVAILABLE) -> list[str]:
        self._agents.create_many([(i, i, status) for i in ids], self._clock.now())
        return ids

    def get(self, agent_id: str) -> Agent:
        agent = self._agents.get(agent_id)
        if agent is None:
            raise AgentNotFound(agent_id)
        return agent

    def list(self, status: Optional[AgentStatus] = None) -> list[Agent]:
        return self._agents.list(status)

    # --------------------------------------------------------- transitions
    def login(self, agent_id: str) -> Agent:
        return self._simple(agent_id, S.AVAILABLE, {S.OFFLINE})

    def resume(self, agent_id: str) -> Agent:
        agent = self.get(agent_id)
        if agent.pause_requested:
            self._agents.set_pause_requested(agent_id, False, self._clock.now())
            return self.get(agent_id)
        return self._simple(agent_id, S.AVAILABLE, {S.PAUSED})

    def request_pause(self, agent_id: str) -> PauseResult:
        """Voluntary pause, deferred if it would strand a pooled call."""
        now = self._clock.now()
        agent = self.get(agent_id)
        if agent.status == S.PAUSED:
            return PauseResult.PAUSED
        if agent.status == S.AVAILABLE and self._headroom() >= 1:
            # Conditional update: if a pooled call claimed the agent meanwhile,
            # the status is no longer AVAILABLE and we fall through to deferral.
            if self._agents.set_status(agent_id, [S.AVAILABLE], S.PAUSED, now, pause_requested=False):
                log_event(log, "agent_paused", agent_id=agent_id, at=now)
                return PauseResult.PAUSED
        if agent.status == S.OFFLINE:
            raise InvalidAgentTransition("cannot pause an OFFLINE agent")
        self._agents.set_pause_requested(agent_id, True, now)
        log_event(log, "agent_pause_deferred", agent_id=agent_id, status=agent.status.value, at=now)
        return PauseResult.PENDING

    def logout(self, agent_id: str) -> PauseResult:
        """Voluntary logout. Same deferral rule as pause."""
        now = self._clock.now()
        agent = self.get(agent_id)
        if agent.status in (S.PAUSED, S.OFFLINE):
            self._agents.set_status(agent_id, [S.PAUSED, S.OFFLINE], S.OFFLINE, now, pause_requested=False)
            return PauseResult.OFFLINE
        if agent.status == S.AVAILABLE and self._headroom() >= 1:
            if self._agents.set_status(agent_id, [S.AVAILABLE], S.OFFLINE, now, pause_requested=False):
                return PauseResult.OFFLINE
        self._agents.set_pause_requested(agent_id, True, now)
        return PauseResult.PENDING

    def disappear(self, agent_id: str) -> Optional[str]:
        """Involuntary loss (crash or network). Returns the affected call id, if any."""
        now = self._clock.now()
        agent = self.get(agent_id)
        if agent.status == S.OFFLINE:
            return None
        # Detach in one conditional statement keyed on the call we saw. If the
        # agent changed in between, retry with the fresh row.
        for _ in range(5):
            agent = self.get(agent_id)
            ok = self._db.execute(
                """UPDATE agents SET status='OFFLINE', current_call_id=NULL, reserved_by=NULL, reserved_at=NULL,
                       reserved_until=NULL, wrap_up_until=NULL, pause_requested=0, last_activity_at=?,
                       version=version+1
                   WHERE id=? AND version=?""", (now, agent_id, agent.version)).rowcount
            if ok:
                break
        else:  # pragma: no cover - extreme contention
            raise RuntimeError(f"could not mark agent {agent_id} offline")
        log_event(log, "agent_disappeared", agent_id=agent_id, previous=agent.status.value,
                  call_id=agent.current_call_id, at=now)
        if agent.current_call_id and self._on_agent_lost is not None:
            self._on_agent_lost(agent_id, agent.current_call_id)
        return agent.current_call_id

    def transition(self, agent_id: str, target: AgentStatus) -> Agent:
        """Generic validated transition for API use (no call-bound states)."""
        if target in (S.RESERVED, S.DIALING, S.CONNECTED, S.WRAP_UP):
            raise InvalidAgentTransition(f"{target.value} is controlled by the dialer, not set manually")
        agent = self.get(agent_id)
        if target == S.PAUSED:
            self.request_pause(agent_id)
            return self.get(agent_id)
        if target == S.OFFLINE and agent.status != S.OFFLINE:
            self.logout(agent_id)
            return self.get(agent_id)
        return self._simple(agent_id, target, {agent.status})

    def _simple(self, agent_id: str, target: AgentStatus, allowed_from: set[AgentStatus]) -> Agent:
        agent = self.get(agent_id)
        if agent.status not in allowed_from:
            raise InvalidAgentTransition(f"agent {agent_id} is {agent.status.value}; cannot go to {target.value}")
        validate_transition(agent.status, target)
        if not self._agents.set_status(agent_id, [agent.status], target, self._clock.now(), pause_requested=False):
            raise InvalidAgentTransition(f"agent {agent_id} changed concurrently; retry")
        return self.get(agent_id)

    # ---------------------------------------------------------- housekeeping
    def housekeeping(self) -> dict[str, int]:
        """End expired wrap-ups and apply deferred pauses where headroom allows."""
        now = self._clock.now()
        ended = self._agents.end_expired_wrapups(now)
        paused = 0
        pending = self._db.query("SELECT id FROM agents WHERE status='AVAILABLE' AND pause_requested=1 "
                                 "ORDER BY last_activity_at")
        for row in pending:
            if self._headroom() < 1:
                break
            if self._agents.set_status(row["id"], [S.AVAILABLE], S.PAUSED, now, pause_requested=False):
                paused += 1
        return {"wrapups_ended": ended, "pending_pauses_applied": paused}
