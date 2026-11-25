"""Scripted failure scenarios. Each one builds an isolated in-memory runtime,
injects a fault, and returns a step-by-step report of what the system did.

Used by ``scripts/demo.py`` and the ``POST /failures/{name}`` API endpoint.
Each report is produced by running the real services.
"""
from __future__ import annotations

from typing import Any, Callable

from app.config import CircuitBreakerConfig, Settings
from app.faults import SimulatedCrash, crash_at
from app.logging_setup import configure_logging
from app.models.campaign import DialMode
from app.models.provider_event import ProviderEvent, ProviderEventType
from app.providers.mock_provider import StaticOutcomeModel
from app.runtime import Runtime, build_runtime

Report = dict[str, Any]


def _rt(agents: int, borrowers: int, mode: DialMode, *, answer_rate: float = 0.35, talk: float = 120.0,
        weights: dict | None = None, settings: Settings | None = None) -> Runtime:
    configure_logging("WARNING")
    rt = build_runtime(settings or Settings(), db_path=":memory:", fast_db=True,
                       outcome_model=StaticOutcomeModel(answer_rate, talk),
                       provider_weights=weights or {"provider_a": 1.0, "provider_b": 0.0})
    rt.campaigns.create("camp", "failure demo", mode, max_concurrent_calls=10_000)
    rt.agents.create_many(agents)
    rt.borrowers.create_many("camp", borrowers)
    rt.campaigns.start("camp")
    return rt


def _call_state(rt: Runtime, call_id: str) -> dict:
    c = rt.lifecycle.calls.get(call_id)
    agent = rt.agents.get(c.agent_id) if c.agent_id else None
    b = rt.borrowers.get(c.borrower_id)
    return {"call": c.status.value, "outcome": c.outcome, "agent": agent.id if agent else None,
            "agent_status": agent.status.value if agent else None, "borrower_status": b.status.value,
            "borrower_attempts": b.attempt_count}


def _place_one_call(rt: Runtime) -> tuple[str, str]:
    """Place a single progressive call while capturing (not processing) provider events."""
    captured: list[ProviderEvent] = []
    for p in rt.providers.values():
        p.set_event_sink(captured.append)
    results = rt.dial_cycle()
    call_id = results[0].allocation.call_ids[0]
    return call_id, rt.lifecycle.calls.get(call_id).provider_call_id


def _event(rt: Runtime, call_id: str, pcid: str, typ: ProviderEventType, seq: int, **payload) -> ProviderEvent:
    return ProviderEvent("provider_a", f"{pcid}:{seq}:{typ.value}", call_id, pcid, typ, seq, rt.clock.now(),
                         payload)


# ------------------------------------------------------------------ scenarios
def worker_crash() -> Report:
    """Agent reserved > borrower reserved > call initiated > worker crashes."""
    rt = _rt(agents=1, borrowers=2, mode=DialMode.PROGRESSIVE, answer_rate=1.0, talk=60)
    steps = []
    lost_events: list[ProviderEvent] = []
    rt.allocator.crash_hook = crash_at("after_initiate")
    for p in rt.providers.values():          # while the worker is dead its webhooks are lost
        p.set_event_sink(lost_events.append)
    try:
        rt.dial_cycle()
    except SimulatedCrash as exc:
        steps.append(f"worker crashed: {exc}")
    call = rt.lifecycle.calls.list()[0]
    steps.append(f"persisted state after crash: {_call_state(rt, call.id)} (provider_call_id={call.provider_call_id})")
    rt.allocator.crash_hook = None
    rt.scheduler.run_until(rt.clock.now() + 12)   # lease (10s) expires; webhooks were lost
    steps.append(f"{len(lost_events)} provider webhooks were lost while the worker was down")
    second = rt.dial_cycle()
    steps.append("a second worker runs a dial cycle: " + (
        f"decision={second[0].decision.decision.value if second[0].decision else 'no demand'}, "
        f"placed={second[0].allocation.placed if second[0].allocation else 0} "
        "(agent still RESERVED, borrower still RESERVED -> no duplicate call)"))
    for p in rt.providers.values():
        p.set_event_sink(rt.processor.handle)
    report = rt.recovery.run()
    steps.append(f"recovery run: {report} -> provider status API consulted, state adopted: {_call_state(rt, call.id)}")
    rt.advance(120, dial=False)
    steps.append(f"after the call finishes: {_call_state(rt, call.id)}")
    total_calls = rt.db.scalar("SELECT COUNT(*) FROM calls WHERE borrower_id=?", (call.borrower_id,))
    return {"scenario": "worker_crash", "steps": steps, "calls_for_borrower": total_calls,
            "final": _call_state(rt, call.id)}


def worker_crash_before_initiate() -> Report:
    rt = _rt(agents=1, borrowers=1, mode=DialMode.PROGRESSIVE)
    steps = []
    rt.allocator.crash_hook = crash_at("before_initiate")
    try:
        rt.dial_cycle()
    except SimulatedCrash as exc:
        steps.append(f"worker crashed before contacting the provider: {exc}")
    call = rt.lifecycle.calls.list()[0]
    steps.append(f"after crash: {_call_state(rt, call.id)}")
    rt.allocator.crash_hook = None
    rt.clock.advance_to(rt.clock.now() + 12)
    steps.append(f"recovery: {rt.recovery.run()} -> {_call_state(rt, call.id)}")
    return {"scenario": "worker_crash_before_initiate", "steps": steps, "final": _call_state(rt, call.id)}


def provider_outage() -> Report:
    """The only provider starts timing out for 3 minutes while calls are active."""
    settings = Settings(breaker=CircuitBreakerConfig(failure_threshold=3, base_open_s=10.0))
    rt = _rt(agents=20, borrowers=3000, mode=DialMode.PREDICTIVE, settings=settings)
    rt.advance(120)
    prov = rt.providers["provider_a"]
    active_before = rt.db.scalar("SELECT COUNT(*) FROM calls WHERE status IN ('RINGING','CONNECTED')")
    timeline = []
    prov.set_outage(True)
    start = rt.clock.now()
    timeouts_before = prov.stats["timeouts"]
    for i in range(36):                    # 180s of outage, sampled every 5s
        rt.advance(5)
        h = rt.breaker.get("provider_a")
        last = rt.db.query_one("SELECT decision, reason FROM safety_decisions ORDER BY id DESC LIMIT 1")
        timeline.append({"t": round(rt.clock.now() - start), "circuit": h["state"],
                         "initiate_timeouts": prov.stats["timeouts"] - timeouts_before,
                         "active_calls": rt.metrics.call_counts()["CONNECTED"],
                         "last_decision": last["decision"] if last else None})
    prov.set_outage(False)
    for _ in range(48):                    # observe recovery: HALF_OPEN probe -> CLOSED
        rt.advance(5)
        h = rt.breaker.get("provider_a")
        last = rt.db.query_one("SELECT decision FROM safety_decisions ORDER BY id DESC LIMIT 1")
        timeline.append({"t": round(rt.clock.now() - start), "circuit": h["state"],
                         "initiate_timeouts": prov.stats["timeouts"] - timeouts_before,
                         "active_calls": rt.metrics.call_counts()["CONNECTED"],
                         "last_decision": last["decision"] if last else None})
    h = rt.breaker.get("provider_a")
    closed_at = next((p["t"] for p in timeline[36:] if p["circuit"] == "CLOSED"), None)
    return {"scenario": "provider_outage", "active_calls_when_outage_started": active_before,
            "outage_duration_s": 180, "circuit_closed_again_at_t": closed_at,
            "calls_after_recovery": rt.db.scalar("SELECT COUNT(*) FROM calls WHERE created_at > ?",
                                                 (start + 180,)),
            "total_initiate_timeouts_during_outage": timeline[35]["initiate_timeouts"],
            "circuit_openings": h["total_openings"], "abandoned": rt.db.scalar(
                "SELECT COUNT(*) FROM calls WHERE outcome='ABANDONED'"),
            "unresolved": rt.db.scalar("SELECT COUNT(*) FROM calls WHERE outcome='UNRESOLVED'"),
            "timeline": timeline}


def agent_drop(agents: int = 100, drop: int = 40) -> Report:
    """100 agents available; 40 disappear within ~3 seconds."""
    rt = _rt(agents=agents, borrowers=20000, mode=DialMode.PREDICTIVE, answer_rate=0.3, talk=120)
    rt.advance(300)
    import random
    rng = random.Random(5)
    logged = [a.id for a in rt.agents.list() if a.status.value != "OFFLINE"]
    victims = rng.sample(logged, drop)
    timeline = []
    t0 = rt.clock.now()

    def snap(label: str) -> None:
        last = rt.db.query_one("SELECT decision, requested, approved, capacity_json FROM safety_decisions "
                               "ORDER BY id DESC LIMIT 1")
        timeline.append({"t": round(rt.clock.now() - t0, 1), "event": label,
                         "logged_in": rt.db.scalar("SELECT COUNT(*) FROM agents WHERE status NOT IN ('OFFLINE','PAUSED')"),
                         "available": rt.metrics.agent_counts()["AVAILABLE"],
                         "pooled_calls_ringing": rt.metrics.unbound_in_flight(),
                         "last_decision": f"{last['decision']} {last['approved']}/{last['requested']}" if last else None,
                         "abandoned_so_far": rt.db.scalar("SELECT COUNT(*) FROM calls WHERE outcome='ABANDONED'")})

    snap("before drop")
    for i, agent_id in enumerate(victims):
        rt.agents.disappear(agent_id)
        if i % 14 == 13:
            rt.advance(1)
            snap(f"{i + 1} agents gone")
    for _ in range(10):
        rt.advance(1)
        snap("after drop")
    rt.advance(120)
    snap("+2 min")
    return {"scenario": "agent_drop", "dropped": drop, "timeline": timeline,
            "capacity_cancellations": rt.worker.cancelled_for_capacity,
            "calls_rebound_or_cancelled": rt.db.scalar(
                "SELECT COUNT(*) FROM calls WHERE outcome IN ('CANCELLED_AGENT_LOST','CANCELLED_CAPACITY')"),
            "abandoned": rt.db.scalar("SELECT COUNT(*) FROM calls WHERE outcome='ABANDONED'")}


def duplicate_events() -> Report:
    rt = _rt(agents=1, borrowers=1, mode=DialMode.PROGRESSIVE)
    call_id, pcid = _place_one_call(rt)
    seq = [_event(rt, call_id, pcid, ProviderEventType.RINGING, 2),
           _event(rt, call_id, pcid, ProviderEventType.ANSWERED, 3),
           _event(rt, call_id, pcid, ProviderEventType.ANSWERED, 3),
           _event(rt, call_id, pcid, ProviderEventType.ANSWERED, 3)]
    results = [(e.event_type.value, rt.processor.handle(e).value) for e in seq]
    mid = _call_state(rt, call_id)
    done = _event(rt, call_id, pcid, ProviderEventType.COMPLETED, 4, answered=True, talk_duration_s=42.0)
    results.append((done.event_type.value, rt.processor.handle(done).value))
    return {"scenario": "duplicate_events", "results": results, "state_after_answers": mid,
            "agent_connections": rt.db.scalar("SELECT COUNT(*) FROM calls WHERE connected_at IS NOT NULL"),
            "final": _call_state(rt, call_id)}


def out_of_order_events() -> Report:
    rt = _rt(agents=1, borrowers=1, mode=DialMode.PROGRESSIVE)
    call_id, pcid = _place_one_call(rt)
    seq = [_event(rt, call_id, pcid, ProviderEventType.COMPLETED, 4, answered=True, talk_duration_s=30.0),
           _event(rt, call_id, pcid, ProviderEventType.ANSWERED, 3),
           _event(rt, call_id, pcid, ProviderEventType.RINGING, 2)]
    results = [(e.event_type.value, rt.processor.handle(e).value) for e in seq]
    return {"scenario": "out_of_order_events", "results": results, "final": _call_state(rt, call_id)}


def crash_after_answered() -> Report:
    """Worker crashes while processing ANSWERED; COMPLETED then arrives; ANSWERED is redelivered."""
    rt = _rt(agents=1, borrowers=1, mode=DialMode.PROGRESSIVE)
    call_id, pcid = _place_one_call(rt)
    steps = []
    rt.processor.handle(_event(rt, call_id, pcid, ProviderEventType.RINGING, 2))
    rt.processor.crash_hook = crash_at("event_before_commit")
    answered = _event(rt, call_id, pcid, ProviderEventType.ANSWERED, 3)
    try:
        rt.processor.handle(answered)
    except SimulatedCrash:
        steps.append(f"crash while processing ANSWERED (transaction rolled back): {_call_state(rt, call_id)}")
    rt.processor.crash_hook = None
    r = rt.processor.handle(_event(rt, call_id, pcid, ProviderEventType.COMPLETED, 4, answered=True,
                                   talk_duration_s=55.0))
    steps.append(f"COMPLETED(answered=True) arrives -> {r.value}: {_call_state(rt, call_id)}")
    r = rt.processor.handle(answered)
    steps.append(f"provider redelivers ANSWERED -> {r.value}: {_call_state(rt, call_id)}")
    return {"scenario": "crash_after_answered", "steps": steps, "final": _call_state(rt, call_id)}


FAILURE_SCENARIOS: dict[str, Callable[[], Report]] = {
    "worker_crash": worker_crash,
    "worker_crash_before_initiate": worker_crash_before_initiate,
    "provider_outage": provider_outage,
    "agent_drop": agent_drop,
    "duplicate_events": duplicate_events,
    "out_of_order_events": out_of_order_events,
    "crash_after_answered": crash_after_answered,
}
