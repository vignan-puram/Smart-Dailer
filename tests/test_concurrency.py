"""Real multi-threaded races against a file-backed SQLite database (WAL).

Each worker thread has its own connection, like separate dialer processes.
"""
from __future__ import annotations

import threading

from app.config import Settings
from app.database.connection import Database
from app.database.repositories import AgentRepository, BorrowerRepository
from app.models.campaign import DialMode
from app.models.provider_event import EventResult, ProviderEventType as E
from app.runtime import build_runtime
from tests.conftest import RELIABLE_A, capture_events, ev, make_runtime, place_single_call


def race(n: int, fn) -> list:
    barrier = threading.Barrier(n)
    results = [None] * n

    def run(i: int) -> None:
        barrier.wait()
        results[i] = fn(i)

    threads = [threading.Thread(target=run, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results


def test_two_workers_reserving_the_same_agent(file_db):
    rt = make_runtime(agents=1, db_path=file_db)
    db = Database(file_db)
    repo = AgentRepository(db)
    for round_ in range(20):
        db.execute("UPDATE agents SET status='AVAILABLE', current_call_id=NULL")
        wins = race(2, lambda i: repo.try_reserve("agent-00000", f"call-{round_}-{i}", f"w{i}", 0.0, 99.0))
        assert sorted(wins) == [False, True], wins
    db.close()
    rt.db.close()


def test_many_workers_reserving_from_a_pool_never_double_allocate(file_db):
    rt = make_runtime(agents=30, db_path=file_db)
    db = Database(file_db)
    repo = AgentRepository(db)

    def grab(i):
        got = []
        while (a := repo.reserve_any(f"c-{i}-{len(got)}", f"w{i}", 0.0, 99.0)) is not None:
            got.append(a)
        return got

    results = race(8, grab)
    allocated = [a for r in results for a in r]
    assert len(allocated) == 30 and len(set(allocated)) == 30
    db.close()
    rt.db.close()


def test_naive_read_then_write_is_racy_but_conditional_update_is_not(runtime_factory):
    """Deterministic interleaving of two workers, showing *why* we use conditional updates."""
    rt = runtime_factory(agents=1)
    db = rt.db
    # Naive: both read AVAILABLE, then both write.
    seen_1 = db.scalar("SELECT status FROM agents WHERE id='agent-00000'")
    seen_2 = db.scalar("SELECT status FROM agents WHERE id='agent-00000'")
    winners = 0
    for seen, call in ((seen_1, "call-1"), (seen_2, "call-2")):
        if seen == "AVAILABLE":
            db.execute("UPDATE agents SET status='RESERVED', current_call_id=? WHERE id='agent-00000'", (call,))
            winners += 1
    assert winners == 2                         # double allocation: call-1 silently overwritten
    # Correct: compare-and-set in one statement.
    db.execute("UPDATE agents SET status='AVAILABLE', current_call_id=NULL")
    repo = AgentRepository(db)
    assert [repo.try_reserve("agent-00000", c, "w", 0.0, 9.0) for c in ("call-1", "call-2")] == [True, False]


def test_two_workers_reserving_the_same_borrower(file_db):
    rt = make_runtime(borrowers=1, db_path=file_db)
    db = Database(file_db)
    repo = BorrowerRepository(db)
    wins = race(4, lambda i: repo.try_reserve("b-camp-000000", f"call-{i}", f"w{i}", 0.0, 99.0, 3))
    assert wins.count(True) == 1
    db.close()
    rt.db.close()


def test_concurrent_borrower_selection_is_disjoint(file_db):
    rt = make_runtime(borrowers=200, db_path=file_db)
    db = Database(file_db)
    repo = BorrowerRepository(db)

    def grab(i):
        got = []
        while (row := repo.reserve_next("camp", f"c{i}-{len(got)}", f"w{i}", 0.0, 99.0, 3)) is not None:
            got.append(row["id"])
        return got

    results = race(6, grab)
    ids = [b for r in results for b in r]
    assert len(ids) == 200 and len(set(ids)) == 200
    db.close()
    rt.db.close()


def test_multiple_workers_processing_the_same_event(file_db):
    rt = make_runtime(agents=3, borrowers=1, db_path=file_db, mode=DialMode.PREDICTIVE)
    call_id, pcid = place_single_call(rt)
    workers = [build_runtime(Settings(), db_path=file_db, seed=10 + i,
                             provider_weights={"provider_a": 1.0, "provider_b": 0.0}) for i in range(4)]
    event = ev(rt, call_id, pcid, E.ANSWERED, 3)
    results = race(4, lambda i: workers[i].processor.handle(event))
    assert results.count(EventResult.APPLIED) == 1 and results.count(EventResult.DUPLICATE) == 3
    assert rt.db.scalar("SELECT COUNT(*) FROM agents WHERE status='CONNECTED'") == 1
    for w in workers:
        w.db.close()
    rt.db.close()


def test_concurrent_dialer_workers_respect_agent_capacity(file_db):
    """Four independent workers run full dial cycles on the same campaign at once."""
    rt = make_runtime(agents=20, borrowers=500, db_path=file_db)
    # RELIABLE_A: no random provider rejections, so this test isolates concurrency.
    # (With the default profile a rejected setup legitimately frees the agent, who is
    # then re-reserved for another borrower; that is correct, but makes counts vary.)
    workers = [build_runtime(Settings(), db_path=file_db, seed=20 + i, profile_a=RELIABLE_A,
                             provider_weights={"provider_a": 1.0, "provider_b": 0.0}) for i in range(4)]
    for i, w in enumerate(workers):
        capture_events(w)
        w.allocator.worker_id = f"worker-{i}"

    def cycles(i):
        for _ in range(5):
            workers[i].dial_cycle()

    race(4, cycles)
    active_sql = "status IN ('RESERVED','INITIATED','RINGING')"
    assert rt.db.scalar(f"SELECT COUNT(*) FROM calls WHERE {active_sql}") == 20
    # The real safety properties: nobody is double-booked among active calls.
    assert rt.db.scalar(f"SELECT COUNT(DISTINCT agent_id) FROM calls WHERE {active_sql}") == 20
    assert rt.db.scalar(f"SELECT COUNT(DISTINCT borrower_id) FROM calls WHERE {active_sql}") == 20
    assert rt.db.scalar("SELECT COUNT(*) FROM calls") == 20          # no failed or duplicate attempts
    assert rt.metrics.agent_counts()["AVAILABLE"] == 0
    for w in workers:
        w.db.close()
    rt.db.close()
