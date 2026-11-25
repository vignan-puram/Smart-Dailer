"""Basic load test: 100 / 1,000 / 10,000 agents against a file-backed SQLite (WAL).

Measures, on THIS machine:
  * agent reservation throughput + latency with N concurrent worker threads
  * borrower reservation throughput + latency
  * end-to-end call initiation (pacing -> safety -> allocator -> mock provider)
    with several independent dialer workers sharing one database
  * Safety Controller evaluate() latency
  * event processing throughput (with 20% duplicate webhooks, concurrent)
and verifies the safety invariants after every phase (no double allocation).

Usage:
    python scripts/run_load_test.py                 # 100, 1000, 10000 agents, 8 workers
    python scripts/run_load_test.py --agents 100 1000 --workers 4
"""
from __future__ import annotations

import argparse
import dataclasses
import json
import platform
import shutil
import statistics
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.config import SafetyConfig, Settings  # noqa: E402
from app.database.connection import Database  # noqa: E402
from app.database.repositories import AgentRepository, BorrowerRepository  # noqa: E402
from app.logging_setup import configure_logging  # noqa: E402
from app.models.agent import AgentStatus  # noqa: E402
from app.models.call import Binding  # noqa: E402
from app.models.campaign import DialMode  # noqa: E402
from app.models.provider_event import ProviderEvent, ProviderEventType  # noqa: E402
from app.models.safety import PacingRecommendation  # noqa: E402
from app.providers.mock_provider import ProviderProfile, StaticOutcomeModel  # noqa: E402
from app.runtime import build_runtime  # noqa: E402

FAST_A = ProviderProfile(name="provider_a", reject_prob=0.0, network_failure_prob=0.0)


def pct(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    return values[min(len(values) - 1, int(round(p / 100 * (len(values) - 1))))]


def lat_summary(samples_s: list[float]) -> dict:
    ms = [s * 1000 for s in samples_s]
    return {"p50_ms": round(pct(ms, 50), 3), "p95_ms": round(pct(ms, 95), 3), "p99_ms": round(pct(ms, 99), 3),
            "max_ms": round(max(ms), 3) if ms else 0.0}


def run_threads(n: int, target) -> float:
    barrier = threading.Barrier(n)
    threads = [threading.Thread(target=target, args=(i, barrier)) for i in range(n)]
    t0 = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return time.perf_counter() - t0


def median_run(fn, invariant_key: str, repeats: int = 3) -> dict:
    """Run a repeatable phase several times; keep the median-throughput run.
    The invariant must hold in *every* repetition."""
    runs = sorted((fn() for _ in range(repeats)), key=lambda r: r["ops_per_s"])
    chosen = dict(runs[len(runs) // 2])
    chosen[invariant_key] = all(r[invariant_key] for r in runs)
    chosen["ops_per_s_all_runs"] = [r["ops_per_s"] for r in runs]
    return chosen


def seed_db(path: str, agents: int, borrowers: int) -> None:
    rt = build_runtime(Settings(), db_path=path, profile_a=FAST_A,
                       provider_weights={"provider_a": 1.0, "provider_b": 0.0})
    rt.campaigns.create("load", "load test", DialMode.PROGRESSIVE, max_concurrent_calls=1_000_000)
    rt.agents.create_many(agents, prefix="agent")
    rt.borrowers.create_many("load", borrowers)
    rt.campaigns.start("load")
    rt.db.close()


# -------------------------------------------------------------------- phases
def phase_agent_reservation(path: str, n_agents: int, workers: int) -> dict:
    db = Database(path)
    lat: list[list[float]] = [[] for _ in range(workers)]
    wins = [0] * workers

    def work(i: int, barrier: threading.Barrier) -> None:
        repo = AgentRepository(db)
        barrier.wait()
        k = 0
        while True:
            t = time.perf_counter()
            got = repo.reserve_any(f"lt-a-{i}-{k}", f"w{i}", 0.0, 1e12)
            lat[i].append(time.perf_counter() - t)
            if got is None:
                break
            wins[i] += 1
            k += 1

    wall = run_threads(workers, work)
    reserved = db.scalar("SELECT COUNT(*) FROM agents WHERE status='RESERVED'")
    distinct = db.scalar("SELECT COUNT(DISTINCT current_call_id) FROM agents WHERE status='RESERVED'")
    # reset for later phases
    db.execute("UPDATE agents SET status='AVAILABLE', current_call_id=NULL, reserved_by=NULL, reserved_at=NULL, "
               "reserved_until=NULL")
    db.close()
    total = sum(wins)
    return {"reservations": total, "wall_s": round(wall, 3), "ops_per_s": round(total / wall, 1),
            "latency": lat_summary([x for l in lat for x in l]),
            "invariant_each_agent_reserved_once": reserved == n_agents == total == distinct}


def phase_borrower_reservation(path: str, n_borrowers: int, workers: int) -> dict:
    db = Database(path)
    lat: list[list[float]] = [[] for _ in range(workers)]
    wins = [0] * workers

    def work(i: int, barrier: threading.Barrier) -> None:
        repo = BorrowerRepository(db)
        barrier.wait()
        k = 0
        while True:
            t = time.perf_counter()
            got = repo.reserve_next("load", f"lt-b-{i}-{k}", f"w{i}", 0.0, 1e12, 3)
            lat[i].append(time.perf_counter() - t)
            if got is None:
                break
            wins[i] += 1
            k += 1

    wall = run_threads(workers, work)
    reserved = db.scalar("SELECT COUNT(*) FROM borrowers WHERE status='RESERVED'")
    distinct = db.scalar("SELECT COUNT(DISTINCT active_call_id) FROM borrowers WHERE status='RESERVED'")
    db.execute("UPDATE borrowers SET status='PENDING', active_call_id=NULL, reserved_by=NULL, reserved_until=NULL, "
               "call_count=0")
    db.close()
    total = sum(wins)
    return {"reservations": total, "wall_s": round(wall, 3), "ops_per_s": round(total / wall, 1),
            "latency": lat_summary([x for l in lat for x in l]),
            "invariant_each_borrower_reserved_once": reserved == n_borrowers == total == distinct}


def phase_call_initiation(path: str, n_agents: int, workers: int) -> dict:
    """Independent dialer workers (own runtime, own provider mock) sharing one database."""
    configure_logging("ERROR")
    # Lift the global outstanding-call cap (default 5,000) so this phase measures
    # throughput, not a configured limit. The cap itself is covered by tests/test_safety.py.
    settings = dataclasses.replace(Settings(), safety=dataclasses.replace(SafetyConfig(),
                                                                          max_outstanding_calls=10 ** 7))
    runtimes = [build_runtime(settings, db_path=path, seed=100 + i, profile_a=FAST_A,
                              outcome_model=StaticOutcomeModel(0.3, 120),
                              provider_weights={"provider_a": 1.0, "provider_b": 0.0}) for i in range(workers)]
    for i, r in enumerate(runtimes):
        r.allocator.worker_id = f"worker-{i}"
    eval_lat: list[list[float]] = [[] for _ in range(workers)]
    placed = [0] * workers

    def work(i: int, barrier: threading.Barrier) -> None:
        r = runtimes[i]
        camp = r.campaigns.get("load")
        barrier.wait()
        # Stop only when no idle agent is left. A rejected decision usually means other
        # workers hold the remaining capacity in their permits right now (contention),
        # not that dialing is finished, so it gets a much longer patience budget.
        idle_rounds = 0
        contended_rounds = 0
        while idle_rounds < 3 and contended_rounds < 200:
            rec = r.progressive.recommend(camp)
            if rec.requested == 0:
                idle_rounds += 1
                continue
            idle_rounds = 0
            # cap each cycle so workers interleave like real pacing ticks
            rec = PacingRecommendation(rec.campaign_id, rec.engine, min(rec.requested, 50), rec.binding,
                                       rec.explanation, rec.inputs)
            t = time.perf_counter()
            d = r.safety.evaluate(rec)
            eval_lat[i].append(time.perf_counter() - t)
            if d.permit:
                placed[i] += r.allocator.execute(d.permit).placed
                contended_rounds = 0
            else:
                contended_rounds += 1
                time.sleep(0.01)   # back off: real pacing ticks once per second, not in a hot loop

    wall = run_threads(workers, work)
    db = runtimes[0].db
    active = db.scalar("SELECT COUNT(*) FROM calls WHERE status IN ('RESERVED','INITIATED','RINGING')")
    max_per_agent = db.scalar("SELECT COALESCE(MAX(n),0) FROM (SELECT agent_id, COUNT(*) n FROM calls "
                              "WHERE status IN ('RESERVED','INITIATED','RINGING','ANSWERED','CONNECTED') "
                              "GROUP BY agent_id)")
    max_per_borrower = db.scalar("SELECT COALESCE(MAX(n),0) FROM (SELECT borrower_id, COUNT(*) n FROM calls "
                                 "WHERE status IN ('RESERVED','INITIATED','RINGING','ANSWERED','CONNECTED') "
                                 "GROUP BY borrower_id)")
    decisions = db.scalar("SELECT COUNT(*) FROM safety_decisions")
    total = sum(placed)
    for r in runtimes:
        r.db.close()
    return {"calls_initiated": total, "wall_s": round(wall, 3), "calls_per_s": round(total / wall, 1),
            "safety_evaluations": decisions, "safety_eval_latency": lat_summary([x for l in eval_lat for x in l]),
            "invariant_calls_le_agents": active <= n_agents and total <= n_agents,
            "invariant_max_active_calls_per_agent": max_per_agent,
            "invariant_max_active_calls_per_borrower": max_per_borrower}


def phase_safety_latency(path: str, iterations: int = 300) -> dict:
    configure_logging("ERROR")
    r = build_runtime(Settings(), db_path=path, profile_a=FAST_A,
                      provider_weights={"provider_a": 1.0, "provider_b": 0.0})
    rec = PacingRecommendation("load", "PREDICTIVE", 10, Binding.UNBOUND, ("load test",))
    lat = []
    for _ in range(iterations):
        t = time.perf_counter()
        d = r.safety.evaluate(rec)
        lat.append(time.perf_counter() - t)
        if d.permit:  # do not let permits pile up and change the workload
            r.db.execute("UPDATE dial_permits SET status='EXPIRED' WHERE id=?", (d.permit.permit_id,))
    r.db.close()
    return {"iterations": iterations, "latency": lat_summary(lat)}


def phase_events(path: str, workers: int) -> dict:
    """Process RINGING/ANSWERED/COMPLETED for every in-flight call, 20% duplicated,
    from several threads at once (like several webhook handlers)."""
    configure_logging("ERROR")
    runtimes = [build_runtime(Settings(), db_path=path, seed=300 + i, profile_a=FAST_A,
                              provider_weights={"provider_a": 1.0, "provider_b": 0.0}) for i in range(workers)]
    calls = runtimes[0].db.query("SELECT id, provider_call_id FROM calls WHERE status='INITIATED' "
                                 "OR status='RINGING'")
    events: list[ProviderEvent] = []
    for c in calls:
        pcid = c["provider_call_id"]
        for seq, typ, payload in ((2, ProviderEventType.RINGING, {}), (3, ProviderEventType.ANSWERED, {}),
                                  (4, ProviderEventType.COMPLETED, {"answered": True, "talk_duration_s": 60.0})):
            ev = ProviderEvent("provider_a", f"lt:{pcid}:{seq}", c["id"], pcid, typ, seq, 0.0, payload)
            events.append(ev)
    import random
    rnd = random.Random(1)
    dupes = [e for e in events if rnd.random() < 0.2]
    stream = events + dupes
    # Keep per-call order inside each worker; spread calls across workers.
    buckets: list[list[ProviderEvent]] = [[] for _ in range(workers)]
    for e in stream:
        buckets[hash(e.client_ref) % workers].append(e)
    lat: list[list[float]] = [[] for _ in range(workers)]

    def work(i: int, barrier: threading.Barrier) -> None:
        proc = runtimes[i].processor
        barrier.wait()
        for e in buckets[i]:
            t = time.perf_counter()
            proc.handle(e)
            lat[i].append(time.perf_counter() - t)

    wall = run_threads(workers, work)
    db = runtimes[0].db
    results = {r["result"]: r["n"] for r in db.query(
        "SELECT result, COUNT(*) n FROM provider_events WHERE event_id LIKE 'lt:%' GROUP BY result")}
    dup_total = db.scalar("SELECT COALESCE(SUM(duplicate_count),0) FROM provider_events WHERE event_id LIKE 'lt:%'")
    connected_twice = db.scalar("SELECT COUNT(*) FROM agents WHERE status='CONNECTED'")
    for r in runtimes:
        r.db.close()
    return {"events_processed": len(stream), "wall_s": round(wall, 3),
            "events_per_s": round(len(stream) / wall, 1), "latency": lat_summary([x for l in lat for x in l]),
            "results": results, "duplicates_detected": dup_total, "duplicates_sent": len(dupes),
            "invariant_duplicates_all_detected": dup_total == len(dupes),
            "agents_left_connected": connected_twice}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--agents", nargs="+", type=int, default=[100, 1000, 10000])
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--out", default=str(ROOT / "results"))
    args = ap.parse_args()
    configure_logging("ERROR")

    machine = {"python": platform.python_version(), "platform": platform.platform(),
               "processor": platform.processor(), "workers": args.workers,
               "sqlite": __import__("sqlite3").sqlite_version}
    print(json.dumps(machine))
    report = {"machine": machine, "runs": []}
    for n in args.agents:
        tmp = tempfile.mkdtemp(prefix="smartdialer-load-")
        path = str(Path(tmp) / "load.db")
        try:
            t0 = time.perf_counter()
            seed_db(path, n, 2 * n)
            run = {"agents": n, "borrowers": 2 * n, "seed_s": round(time.perf_counter() - t0, 2)}
            # The reservation phases take well under a second at small N, so a single
            # run is dominated by thread start-up noise: report the median of 3.
            run["agent_reservation"] = median_run(lambda: phase_agent_reservation(path, n, args.workers),
                                                  "invariant_each_agent_reserved_once")
            run["borrower_reservation"] = median_run(lambda: phase_borrower_reservation(path, 2 * n, args.workers),
                                                     "invariant_each_borrower_reserved_once")
            run["call_initiation"] = phase_call_initiation(path, n, args.workers)
            run["safety_controller"] = phase_safety_latency(path)
            run["event_processing"] = phase_events(path, args.workers)
            report["runs"].append(run)
            print(json.dumps(run), flush=True)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "load_test_results.json").write_text(json.dumps(report, indent=1), encoding="utf-8")
    lines = [f"# Load test results", "", f"Machine: `{json.dumps(machine)}`", "",
             "Generated by `python scripts/run_load_test.py`. All numbers are measured, not estimated.", "",
             "| Agents | Agent reserve ops/s | p99 ms | Borrower reserve ops/s | p99 ms | Calls placed | "
             "Calls initiated/s (8 workers) | Safety eval under contention p50/p99 ms | "
             "Safety eval alone p50/p99 ms | Events/s | Event p99 ms | Invariants |",
             "|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in report["runs"]:
        inv = all([r["agent_reservation"]["invariant_each_agent_reserved_once"],
                   r["borrower_reservation"]["invariant_each_borrower_reserved_once"],
                   r["call_initiation"]["invariant_calls_le_agents"],
                   r["call_initiation"]["invariant_max_active_calls_per_agent"] <= 1,
                   r["call_initiation"]["invariant_max_active_calls_per_borrower"] <= 1,
                   r["event_processing"]["invariant_duplicates_all_detected"]])
        lines.append(
            f"| {r['agents']:,} | {r['agent_reservation']['ops_per_s']:,} | {r['agent_reservation']['latency']['p99_ms']} "
            f"| {r['borrower_reservation']['ops_per_s']:,} | {r['borrower_reservation']['latency']['p99_ms']} "
            f"| {r['call_initiation']['calls_initiated']:,} | {r['call_initiation']['calls_per_s']:,} "
            f"| {r['call_initiation']['safety_eval_latency']['p50_ms']} / {r['call_initiation']['safety_eval_latency']['p99_ms']} "
            f"| {r['safety_controller']['latency']['p50_ms']} / {r['safety_controller']['latency']['p99_ms']} "
            f"| {r['event_processing']['events_per_s']:,} | {r['event_processing']['latency']['p99_ms']} "
            f"| {'PASS' if inv else 'FAIL'} |")
    (out / "load_test_results.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
