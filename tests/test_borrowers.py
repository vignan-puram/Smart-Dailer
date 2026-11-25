"""Borrower reservation, retry limits and eligibility."""
from __future__ import annotations

import sqlite3

import pytest

from app.database.repositories import BorrowerRepository
from app.models.borrower import BorrowerStatus
from app.models.call import Binding
from app.models.provider_event import ProviderEventType as E
from tests.conftest import capture_events, ev


def test_duplicate_reservation_of_same_borrower_fails(runtime_factory):
    rt = runtime_factory(borrowers=1)
    repo = BorrowerRepository(rt.db)
    bid = "b-camp-000000"
    assert repo.try_reserve(bid, "call-1", "w1", 0.0, 100.0, 3)
    assert not repo.try_reserve(bid, "call-2", "w2", 0.0, 100.0, 3)
    assert rt.borrowers.get(bid).active_call_id == "call-1"


def test_completed_and_dnc_borrowers_are_never_reserved(runtime_factory):
    rt = runtime_factory(borrowers=3)
    rt.db.execute("UPDATE borrowers SET status='COMPLETED' WHERE id='b-camp-000000'")
    rt.borrowers.mark_do_not_call("b-camp-000001")
    repo = BorrowerRepository(rt.db)
    got = [repo.reserve_next("camp", f"c{i}", "w", 0.0, 10.0, 3) for i in range(3)]
    assert [g["id"] if g else None for g in got] == ["b-camp-000002", None, None]


def test_retry_wait_respected_until_next_eligible(runtime_factory):
    rt = runtime_factory(borrowers=1)
    rt.db.execute("UPDATE borrowers SET status='RETRY_WAIT', next_eligible_at=500 WHERE 1")
    repo = BorrowerRepository(rt.db)
    assert repo.reserve_next("camp", "c1", "w", 100.0, 110.0, 3) is None
    assert repo.reserve_next("camp", "c1", "w", 500.0, 510.0, 3) is not None


def test_retry_limit_exhausts_borrower(runtime_factory):
    rt = runtime_factory(agents=1, borrowers=1, max_attempts=3)
    capture_events(rt)
    for attempt in range(3):
        rt.db.execute("UPDATE borrowers SET next_eligible_at=0")   # skip the backoff wait
        results = rt.dial_cycle()
        call_id = results[0].allocation.call_ids[0]
        call = rt.lifecycle.calls.get(call_id)
        rt.processor.handle(ev(rt, call_id, call.provider_call_id, E.COMPLETED, 4, answered=False))
        rt.advance(16, dial=False)     # agent back to AVAILABLE
    b = rt.borrowers.get("b-camp-000000")
    assert b.attempt_count == 3 and b.status == BorrowerStatus.EXHAUSTED
    rt.db.execute("UPDATE borrowers SET next_eligible_at=0")
    assert rt.dial_cycle()[0].decision is None     # nobody left to call
    assert rt.db.scalar("SELECT COUNT(*) FROM calls") == 3


def test_retry_uses_exponential_backoff(runtime_factory):
    rt = runtime_factory(agents=1, borrowers=1, max_attempts=5)
    capture_events(rt)
    waits = []
    for _ in range(3):
        rt.db.execute("UPDATE borrowers SET next_eligible_at=0")
        call_id = rt.dial_cycle()[0].allocation.call_ids[0]
        call = rt.lifecycle.calls.get(call_id)
        rt.processor.handle(ev(rt, call_id, call.provider_call_id, E.COMPLETED, 4, answered=False))
        b = rt.borrowers.get("b-camp-000000")
        waits.append(b.next_eligible_at - rt.clock.now())
    assert waits[0] < waits[1] < waits[2]          # 300s, 600s, 1200s (+/- 20% jitter)


def test_setup_failure_does_not_burn_an_attempt(runtime_factory):
    from app.providers.mock_provider import ProviderProfile
    rt = runtime_factory(agents=1, borrowers=1,
                         profile_a=ProviderProfile(name="provider_a", reject_prob=1.0))
    rt.dial_cycle()
    b = rt.borrowers.get("b-camp-000000")
    assert b.status == BorrowerStatus.RETRY_WAIT and b.attempt_count == 0


def test_database_rejects_second_active_call_for_borrower(runtime_factory):
    rt = runtime_factory(agents=2, borrowers=1)
    now = rt.clock.now()
    rt.db.execute("INSERT INTO safety_decisions (created_at, campaign_id, engine, safety_mode, binding, requested,"
                  " approved, rejected, decision, reason, capacity_json, explanation_json)"
                  " VALUES (0,'camp','T','STRICT','BOUND',2,2,0,'APPROVED','t','{}','[]')")
    rt.db.execute("INSERT INTO dial_permits VALUES ('p1','camp',1,'BOUND',2,0,'ACTIVE',0,99)")
    common = dict(campaign_id="camp", borrower_id="b-camp-000000", agent_id=None, binding=Binding.UNBOUND,
                  permit_id="p1", provider="provider_a", now=now, lease_owner="w", lease_until=now + 10)
    rt.lifecycle.calls.insert(call_id="c1", attempt_seq=1, **common)
    with pytest.raises(sqlite3.IntegrityError):          # partial unique index: one ACTIVE call per borrower
        rt.lifecycle.calls.insert(call_id="c2", attempt_seq=2, **common)
    with pytest.raises(sqlite3.IntegrityError):          # same attempt twice (duplicate job)
        rt.lifecycle.calls.insert(call_id="c3", attempt_seq=1, **common)


def test_priority_ordering(runtime_factory):
    rt = runtime_factory(borrowers=3)
    rt.db.execute("UPDATE borrowers SET priority=0")
    rt.db.execute("UPDATE borrowers SET priority=9 WHERE id='b-camp-000002'")
    got = BorrowerRepository(rt.db).reserve_next("camp", "c", "w", 0.0, 1.0, 3)
    assert got["id"] == "b-camp-000002"
