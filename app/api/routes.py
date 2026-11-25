"""HTTP API for demonstrating the SmartDialer.

The API drives one in-process ``Runtime`` whose clock is *virtual*: nothing
happens until you trigger a dial cycle or advance time. A global lock
serialises requests, because the API is a demo console. Concurrency
correctness is enforced by the database and is demonstrated by the
multi-threaded tests, not by this lock.
"""
from __future__ import annotations

import json
import threading
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query

from app.api.schemas import (
    AdvanceTime,
    AgentStatusChange,
    CreateAgents,
    CreateBorrowers,
    CreateCampaign,
    Outage,
    RunSimulation,
    SetMode,
)
from app.config import Settings
from app.models.agent import AgentStatus, InvalidAgentTransition
from app.models.call import CallStatus
from app.models.campaign import DialMode
from app.runtime import Runtime, build_runtime
from app.services.agent_manager import AgentNotFound
from app.services.campaign_manager import CampaignNotFound

router = APIRouter()
_lock = threading.RLock()
_state: dict[str, Optional[Runtime]] = {"rt": None}


def init_runtime(settings: Optional[Settings] = None, db_path: Optional[str] = None) -> Runtime:
    with _lock:
        if _state["rt"] is not None:
            _state["rt"].db.close()
        settings = settings or Settings.from_env()
        path = db_path or settings.database_path
        if path != ":memory:":
            import os
            for suffix in ("", "-wal", "-shm"):
                if os.path.exists(path + suffix):
                    os.remove(path + suffix)   # each API start is a fresh demo database
        _state["rt"] = build_runtime(settings, db_path=path)
        return _state["rt"]


def rt() -> Runtime:
    if _state["rt"] is None:
        init_runtime()
    return _state["rt"]


def _agent(a) -> dict[str, Any]:
    return {"id": a.id, "status": a.status.value, "current_call_id": a.current_call_id,
            "reserved_until": a.reserved_until, "wrap_up_until": a.wrap_up_until,
            "pause_requested": a.pause_requested, "last_activity_at": a.last_activity_at}


def _call(c) -> dict[str, Any]:
    return {"id": c.id, "status": c.status.value, "outcome": c.outcome, "binding": c.binding.value,
            "agent_id": c.agent_id, "borrower_id": c.borrower_id, "provider": c.provider,
            "provider_call_id": c.provider_call_id, "created_at": c.created_at, "answered_at": c.answered_at,
            "connected_at": c.connected_at, "ended_at": c.ended_at, "talk_duration_s": c.talk_duration_s,
            "cancel_reason": c.cancel_reason}


# ------------------------------------------------------------------ system
@router.get("/health")
def health() -> dict:
    return {"status": "ok", "virtual_time": rt().clock.now(), "safety_mode": rt().safety.safety_mode.value}


@router.post("/reset")
def reset() -> dict:
    init_runtime()
    return {"status": "reset"}


@router.post("/time/advance")
def advance(body: AdvanceTime) -> dict:
    with _lock:
        rt().advance(body.seconds, dial=body.dial)
        return {"virtual_time": rt().clock.now(), "agents": rt().metrics.agent_counts(),
                "calls": rt().metrics.call_counts()}


# ------------------------------------------------------------------ agents
@router.post("/agents", status_code=201)
def create_agents(body: CreateAgents) -> dict:
    with _lock:
        existing = rt().db.scalar("SELECT COUNT(*) FROM agents WHERE id LIKE ?", (body.prefix + "-%",))
        ids = [f"{body.prefix}-{i:05d}" for i in range(existing, existing + body.count)]
        rt().agents.create_ids(ids, AgentStatus(body.status))
        return {"created": len(ids), "ids": ids[:50]}


@router.get("/agents")
def list_agents(status: Optional[str] = Query(None)) -> dict:
    with _lock:
        st = AgentStatus(status) if status else None
        return {"counts": rt().metrics.agent_counts(), "agents": [_agent(a) for a in rt().agents.list(st)][:500]}


@router.post("/agents/{agent_id}/status")
def change_agent_status(agent_id: str, body: AgentStatusChange) -> dict:
    with _lock:
        try:
            agent = rt().agents.get(agent_id)
            if body.status == "AVAILABLE":
                result = (rt().agents.login(agent_id) if agent.status == AgentStatus.OFFLINE
                          else rt().agents.resume(agent_id))
                return _agent(result)
            if body.status == "PAUSED":
                return {"result": rt().agents.request_pause(agent_id).value, "agent": _agent(rt().agents.get(agent_id))}
            return {"result": rt().agents.logout(agent_id).value, "agent": _agent(rt().agents.get(agent_id))}
        except AgentNotFound:
            raise HTTPException(404, f"agent {agent_id} not found")
        except InvalidAgentTransition as exc:
            raise HTTPException(409, str(exc))


@router.post("/agents/{agent_id}/disappear")
def agent_disappear(agent_id: str) -> dict:
    with _lock:
        try:
            call_id = rt().agents.disappear(agent_id)
        except AgentNotFound:
            raise HTTPException(404, f"agent {agent_id} not found")
        return {"agent": _agent(rt().agents.get(agent_id)), "affected_call": call_id}


# --------------------------------------------------------------- campaigns
@router.post("/campaigns", status_code=201)
def create_campaign(body: CreateCampaign) -> dict:
    with _lock:
        try:
            c = rt().campaigns.create(body.id, body.name, DialMode(body.mode), body.max_attempts,
                                      body.retry_delay_s, body.max_concurrent_calls)
        except Exception as exc:  # sqlite IntegrityError (duplicate id)
            raise HTTPException(409, f"cannot create campaign: {exc}")
        return c.__dict__ | {"mode": c.mode.value, "status": c.status.value}


@router.get("/campaigns")
def list_campaigns() -> list[dict]:
    with _lock:
        return [c.__dict__ | {"mode": c.mode.value, "status": c.status.value} for c in rt().campaigns.list()]


@router.post("/campaigns/{campaign_id}/borrowers", status_code=201)
def create_borrowers(campaign_id: str, body: CreateBorrowers) -> dict:
    with _lock:
        try:
            rt().campaigns.get(campaign_id)
        except CampaignNotFound:
            raise HTTPException(404, "campaign not found")
        start = rt().db.scalar("SELECT COUNT(*) FROM borrowers WHERE campaign_id=?", (campaign_id,))
        ids = rt().borrowers.create_many(campaign_id, body.count, start_index=start)
        return {"created": len(ids), "counts": rt().borrowers.counts(campaign_id)}


@router.get("/borrowers")
def list_borrowers(campaign_id: Optional[str] = None, limit: int = Query(100, le=1000)) -> dict:
    with _lock:
        return {"counts": rt().borrowers.counts(campaign_id),
                "borrowers": [b.__dict__ | {"status": b.status.value}
                              for b in rt().borrowers.list(campaign_id, limit)]}


def _campaign_action(campaign_id: str, fn) -> dict:
    with _lock:
        try:
            c = fn(campaign_id)
        except CampaignNotFound:
            raise HTTPException(404, "campaign not found")
        return c.__dict__ | {"mode": c.mode.value, "status": c.status.value}


@router.post("/campaigns/{campaign_id}/start")
def start_campaign(campaign_id: str) -> dict:
    return _campaign_action(campaign_id, rt().campaigns.start)


@router.post("/campaigns/{campaign_id}/pause")
def pause_campaign(campaign_id: str) -> dict:
    return _campaign_action(campaign_id, rt().campaigns.pause)


@router.put("/campaigns/{campaign_id}/mode")
def set_mode(campaign_id: str, body: SetMode) -> dict:
    return _campaign_action(campaign_id, lambda cid: rt().campaigns.set_mode(cid, DialMode(body.mode)))


# ------------------------------------------------------------------ dialer
@router.post("/dialer/cycle")
def dial_cycle() -> list[dict]:
    """Run one pacing cycle: recommendation -> safety decision -> allocation."""
    with _lock:
        out = []
        for r in rt().dial_cycle():
            out.append({
                "campaign_id": r.campaign_id,
                "recommendation": {"engine": r.recommendation.engine, "requested": r.recommendation.requested,
                                   "explanation": r.recommendation.explain(), "inputs": r.recommendation.inputs},
                "decision": r.decision.as_dict() if r.decision else None,
                "allocation": r.allocation.__dict__ if r.allocation else None,
            })
        return out


@router.get("/calls")
def list_calls(status: Optional[str] = None, limit: int = Query(100, le=1000)) -> dict:
    with _lock:
        st = CallStatus(status) if status else None
        return {"counts": rt().metrics.call_counts(),
                "calls": [_call(c) for c in rt().lifecycle.calls.list(st, limit)]}


@router.get("/calls/{call_id}")
def get_call(call_id: str) -> dict:
    with _lock:
        c = rt().lifecycle.calls.get(call_id)
        if c is None:
            raise HTTPException(404, "call not found")
        events = [dict(e) for e in rt().db.query(
            "SELECT event_id, event_type, sequence, result, detail, duplicate_count, received_at "
            "FROM provider_events WHERE call_id=? ORDER BY id", (call_id,))]
        return _call(c) | {"events": events}


@router.get("/providers/health")
def providers_health() -> list[dict]:
    with _lock:
        return [dict(r) | {"level": rt().breaker.health_level(r["provider"]).value,
                           "in_outage": rt().providers[r["provider"]].in_outage}
                for r in rt().breaker.all()]


@router.post("/providers/{name}/outage")
def provider_outage(name: str, body: Outage) -> dict:
    with _lock:
        prov = rt().providers.get(name)
        if prov is None:
            raise HTTPException(404, "provider not found")
        prov.set_outage(body.active, webhooks=body.webhooks)
        return {"provider": name, "in_outage": body.active}


@router.get("/safety/decisions")
def safety_decisions(limit: int = Query(50, le=1000)) -> dict:
    with _lock:
        rows = rt().db.query("SELECT * FROM safety_decisions ORDER BY id DESC LIMIT ?", (limit,))
        summary = {r["decision"]: r["n"] for r in
                   rt().db.query("SELECT decision, COUNT(*) n FROM safety_decisions GROUP BY decision")}
        return {"summary": summary, "decisions": [
            dict(r) | {"capacity": json.loads(r["capacity_json"]), "explanation": json.loads(r["explanation_json"])}
            for r in rows]}


@router.get("/metrics")
def metrics() -> dict:
    with _lock:
        db = rt().db
        connected = db.scalar("SELECT COUNT(*) FROM calls WHERE connected_at IS NOT NULL")
        stats = rt().stats.get(rt().campaigns.list()[0].id) if rt().campaigns.list() else None
        return {
            "virtual_time": rt().clock.now(), "agents": rt().metrics.agent_counts(),
            "calls": rt().metrics.call_counts(), "outcomes": rt().lifecycle.calls.counts_by_outcome(),
            "connected_total": connected,
            "abandoned_total": db.scalar("SELECT COUNT(*) FROM calls WHERE outcome='ABANDONED'"),
            "duplicate_events": db.scalar("SELECT COALESCE(SUM(duplicate_count),0) FROM provider_events"),
            "event_results": {r["result"]: r["n"] for r in
                              db.query("SELECT result, COUNT(*) n FROM provider_events GROUP BY result")},
            "pacing_stats": dict(stats) if stats else None,
        }


@router.post("/recovery/run")
def run_recovery() -> dict:
    with _lock:
        return {"report": rt().recovery.run()}


# --------------------------------------------------------- simulations
@router.post("/simulations/run")
def run_simulation(body: RunSimulation) -> dict:
    from app.simulation.scenarios import SCENARIOS
    from app.simulation.simulator import run_scenario
    r = run_scenario(SCENARIOS[body.scenario], DialMode(body.mode), body.safety_mode, seed=body.seed,
                     duration_override=body.duration_s)
    return r.as_dict()


@router.post("/failures/{name}")
def run_failure(name: str) -> dict:
    from app.simulation.failure_scenarios import FAILURE_SCENARIOS
    fn = FAILURE_SCENARIOS.get(name)
    if fn is None:
        raise HTTPException(404, f"unknown failure scenario; choose one of {sorted(FAILURE_SCENARIOS)}")
    return fn()
