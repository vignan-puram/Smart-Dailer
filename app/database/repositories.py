"""Repositories: every SQL statement that mutates state lives here.

Rule used throughout: **never read-check-write**. Every reservation or
transition is a single conditional ``UPDATE`` whose ``WHERE`` clause restates
the precondition (``status='AVAILABLE'``, ``current_call_id=?``...). The
database applies it atomically, and ``rowcount``/``RETURNING`` tells the
caller whether it won. A loser simply sees 0 rows. It never overwrites the
winner's state.
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Iterable, Optional

from app.database.connection import Database
from app.database.schema import ACTIVE_CALL_STATES_SQL
from app.models.agent import Agent, AgentStatus
from app.models.borrower import Borrower, BorrowerStatus
from app.models.call import Binding, Call, CallStatus
from app.models.campaign import Campaign, CampaignStatus, DialMode

PRE_ANSWER_SQL = "('QUEUED','RESERVED','INITIATED','RINGING')"


# --------------------------------------------------------------------------- agents
class AgentRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def create(self, agent_id: str, name: str, status: AgentStatus, now: float) -> None:
        self.db.execute(
            "INSERT INTO agents (id, name, status, last_activity_at, created_at) VALUES (?,?,?,?,?)",
            (agent_id, name, status.value, now, now),
        )

    def create_many(self, rows: Iterable[tuple[str, str, AgentStatus]], now: float) -> None:
        with self.db.transaction() as conn:
            conn.executemany(
                "INSERT INTO agents (id, name, status, last_activity_at, created_at) VALUES (?,?,?,?,?)",
                [(a, n, s.value, now, now) for a, n, s in rows],
            )

    def get(self, agent_id: str) -> Optional[Agent]:
        row = self.db.query_one("SELECT * FROM agents WHERE id=?", (agent_id,))
        return Agent.from_row(row) if row else None

    def list(self, status: Optional[AgentStatus] = None, limit: int = 10_000) -> list[Agent]:
        if status:
            rows = self.db.query("SELECT * FROM agents WHERE status=? ORDER BY id LIMIT ?", (status.value, limit))
        else:
            rows = self.db.query("SELECT * FROM agents ORDER BY id LIMIT ?", (limit,))
        return [Agent.from_row(r) for r in rows]

    def counts_by_status(self) -> dict[str, int]:
        rows = self.db.query("SELECT status, COUNT(*) AS n FROM agents GROUP BY status")
        counts = {s.value: 0 for s in AgentStatus}
        counts.update({r["status"]: r["n"] for r in rows})
        return counts

    # -- atomic reservation ------------------------------------------------
    def try_reserve(self, agent_id: str, call_id: str, worker_id: str, now: float, lease_until: float) -> bool:
        """Compare-and-set reservation of one specific agent.

        Two workers racing on the same agent both execute this statement. SQLite
        serialises writers, so the first one flips the row to RESERVED and the
        second one's ``WHERE status='AVAILABLE'`` no longer matches. It gets
        rowcount == 0 and must pick another agent.
        """
        cur = self.db.execute(
            """UPDATE agents SET status='RESERVED', current_call_id=?, reserved_by=?, reserved_at=?,
                      reserved_until=?, last_activity_at=?, version=version+1
               WHERE id=? AND status='AVAILABLE' AND pause_requested=0""",
            (call_id, worker_id, now, lease_until, now, agent_id),
        )
        return cur.rowcount == 1

    def reserve_any(self, call_id: str, worker_id: str, now: float, lease_until: float) -> Optional[str]:
        """Reserve the longest-idle AVAILABLE agent in a single statement.

        The outer ``AND status='AVAILABLE'`` re-check is what makes this safe
        under PostgreSQL READ COMMITTED as well. There you would add ``FOR UPDATE
        SKIP LOCKED`` to the sub-select so concurrent workers pick different rows
        instead of queueing on the same one.
        """
        row = self.db.query_one(
            """UPDATE agents SET status='RESERVED', current_call_id=?, reserved_by=?, reserved_at=?,
                      reserved_until=?, last_activity_at=?, version=version+1
               WHERE id = (SELECT id FROM agents WHERE status='AVAILABLE' AND pause_requested=0
                           ORDER BY last_activity_at, id LIMIT 1)
                 AND status='AVAILABLE' AND pause_requested=0
               RETURNING id""",
            (call_id, worker_id, now, lease_until, now),
        )
        return row["id"] if row else None

    def claim_for_answered_call(self, call_id: str, now: float) -> Optional[str]:
        """Pooled (unbound) path: bind an answered borrower to any free agent.

        Free = AVAILABLE, or WRAP_UP whose system-enforced deadline has passed.
        Agents with a pending pause are still claimable. They were counted as
        capacity when the call was admitted, so the commitment must be honoured.
        """
        row = self.db.query_one(
            """UPDATE agents SET status='CONNECTED', current_call_id=?, wrap_up_until=NULL,
                      reserved_by=NULL, reserved_at=NULL, reserved_until=NULL,
                      last_activity_at=?, version=version+1
               WHERE id = (SELECT id FROM agents
                           WHERE status='AVAILABLE' OR (status='WRAP_UP' AND wrap_up_until <= ?)
                           ORDER BY (status='AVAILABLE') DESC, pause_requested ASC, last_activity_at, id
                           LIMIT 1)
                 AND (status='AVAILABLE' OR (status='WRAP_UP' AND wrap_up_until <= ?))
               RETURNING id""",
            (call_id, now, now, now),
        )
        return row["id"] if row else None

    def mark_dialing(self, agent_id: str, call_id: str, now: float) -> bool:
        cur = self.db.execute(
            """UPDATE agents SET status='DIALING', last_activity_at=?, version=version+1
               WHERE id=? AND current_call_id=? AND status='RESERVED'""",
            (now, agent_id, call_id),
        )
        return cur.rowcount == 1

    def connect_bound(self, agent_id: str, call_id: str, now: float) -> bool:
        cur = self.db.execute(
            """UPDATE agents SET status='CONNECTED', reserved_until=NULL, last_activity_at=?, version=version+1
               WHERE id=? AND current_call_id=? AND status IN ('RESERVED','DIALING')""",
            (now, agent_id, call_id),
        )
        return cur.rowcount == 1

    def release_from_call(self, agent_id: str, call_id: str, new_status: AgentStatus, now: float,
                          wrap_up_until: Optional[float] = None) -> bool:
        """Detach an agent from *this specific* call.

        Guarding on ``current_call_id`` means a stale event for an older call
        can never free an agent who is already busy on a newer call.
        """
        cur = self.db.execute(
            """UPDATE agents SET status=?, current_call_id=NULL, reserved_by=NULL, reserved_at=NULL,
                      reserved_until=NULL, wrap_up_until=?, last_activity_at=?, version=version+1
               WHERE id=? AND current_call_id=?""",
            (new_status.value, wrap_up_until, now, agent_id, call_id),
        )
        return cur.rowcount == 1

    def rebind(self, call_id: str, to_agent: str, worker_id: str, now: float,
               lease_until: float, target_status: AgentStatus) -> bool:
        """Move a pre-answer bound call onto another AVAILABLE agent (agent-lost recovery)."""
        cur = self.db.execute(
            """UPDATE agents SET status=?, current_call_id=?, reserved_by=?, reserved_at=?, reserved_until=?,
                      last_activity_at=?, version=version+1
               WHERE id=? AND status='AVAILABLE' AND pause_requested=0""",
            (target_status.value, call_id, worker_id, now, lease_until, now, to_agent),
        )
        return cur.rowcount == 1

    def set_status(self, agent_id: str, expected: Iterable[AgentStatus], new_status: AgentStatus,
                   now: float, *, clear_call: bool = False, pause_requested: Optional[bool] = None) -> bool:
        expected_vals = [s.value for s in expected]
        placeholders = ",".join("?" * len(expected_vals))
        sets = ["status=?", "last_activity_at=?", "version=version+1"]
        params: list[Any] = [new_status.value, now]
        if clear_call:
            sets += ["current_call_id=NULL", "reserved_by=NULL", "reserved_at=NULL",
                     "reserved_until=NULL", "wrap_up_until=NULL"]
        if pause_requested is not None:
            sets.append("pause_requested=?")
            params.append(1 if pause_requested else 0)
        if new_status in (AgentStatus.AVAILABLE, AgentStatus.PAUSED, AgentStatus.OFFLINE):
            sets.append("wrap_up_until=NULL")
        sql = f"UPDATE agents SET {', '.join(sets)} WHERE id=? AND status IN ({placeholders})"
        cur = self.db.execute(sql, (*params, agent_id, *expected_vals))
        return cur.rowcount == 1

    def set_pause_requested(self, agent_id: str, value: bool, now: float) -> bool:
        cur = self.db.execute(
            "UPDATE agents SET pause_requested=?, last_activity_at=?, version=version+1 WHERE id=?",
            (1 if value else 0, now, agent_id),
        )
        return cur.rowcount == 1

    def end_expired_wrapups(self, now: float) -> int:
        """System-enforced wrap-up deadline: WRAP_UP -> AVAILABLE.

        A pending pause is *kept* (``pause_requested`` stays 1) and is applied by
        ``AgentManager.housekeeping`` only when pooled-call headroom allows it.
        Going straight to PAUSED here would remove an agent that in-flight pooled
        calls were counting on (the agent may have been look-ahead capacity).
        """
        cur = self.db.execute(
            """UPDATE agents SET status='AVAILABLE', wrap_up_until=NULL, last_activity_at=?, version=version+1
               WHERE status='WRAP_UP' AND wrap_up_until <= ?""",
            (now, now),
        )
        return cur.rowcount


# ------------------------------------------------------------------------ borrowers
class BorrowerRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def create_many(self, campaign_id: str, rows: Iterable[tuple[str, str, int]], now: float) -> None:
        with self.db.transaction() as conn:
            conn.executemany(
                """INSERT INTO borrowers (id, campaign_id, phone, name, status, priority, created_at, updated_at)
                   VALUES (?,?,?,?, 'PENDING', ?, ?, ?)""",
                [(bid, campaign_id, phone, f"Borrower {bid}", prio, now, now) for bid, phone, prio in rows],
            )

    def get(self, borrower_id: str) -> Optional[Borrower]:
        row = self.db.query_one("SELECT * FROM borrowers WHERE id=?", (borrower_id,))
        return Borrower.from_row(row) if row else None

    def counts_by_status(self, campaign_id: Optional[str] = None) -> dict[str, int]:
        if campaign_id:
            rows = self.db.query("SELECT status, COUNT(*) n FROM borrowers WHERE campaign_id=? GROUP BY status",
                                 (campaign_id,))
        else:
            rows = self.db.query("SELECT status, COUNT(*) n FROM borrowers GROUP BY status")
        counts = {s.value: 0 for s in BorrowerStatus}
        counts.update({r["status"]: r["n"] for r in rows})
        return counts

    def count_eligible(self, campaign_id: str, now: float, max_attempts: int) -> int:
        return self.db.scalar(
            """SELECT COUNT(*) FROM borrowers WHERE campaign_id=? AND status IN ('PENDING','RETRY_WAIT')
               AND next_eligible_at <= ? AND attempt_count < ?""",
            (campaign_id, now, max_attempts),
        )

    def reserve_next(self, campaign_id: str, call_id: str, worker_id: str, now: float,
                     lease_until: float, max_attempts: int) -> Optional[sqlite3.Row]:
        """Atomically pick + reserve the highest-priority eligible borrower.

        Eligibility (not completed, retry time reached, attempts left) is part of
        the WHERE clause, so an ineligible borrower can never be reserved even if
        another worker changed it a microsecond earlier.

        Performance: the best row is picked per status (PENDING, RETRY_WAIT) by
        walking ``ix_borrowers_pick`` in ORDER BY order and stopping at the first
        match, then the better of the two wins. A single ``status IN (...)``
        query would sort *every* eligible borrower on every reservation
        (measured: ~9 ms per call at 20k borrowers). The ordering is identical.
        """
        return self.db.query_one(
            """UPDATE borrowers SET status='RESERVED', active_call_id=?, reserved_by=?, reserved_until=?,
                      call_count=call_count+1, last_attempt_at=?, updated_at=?
               WHERE id = (SELECT id FROM (
                               SELECT * FROM (SELECT id, priority, next_eligible_at FROM borrowers INDEXED BY ix_borrowers_pick
                                   WHERE campaign_id=? AND status='PENDING'
                                     AND next_eligible_at <= ? AND attempt_count < ?
                                   ORDER BY priority DESC, next_eligible_at, id LIMIT 1)
                               UNION ALL
                               SELECT * FROM (SELECT id, priority, next_eligible_at FROM borrowers INDEXED BY ix_borrowers_pick
                                   WHERE campaign_id=? AND status='RETRY_WAIT'
                                     AND next_eligible_at <= ? AND attempt_count < ?
                                   ORDER BY priority DESC, next_eligible_at, id LIMIT 1))
                           ORDER BY priority DESC, next_eligible_at, id LIMIT 1)
                 AND status IN ('PENDING','RETRY_WAIT') AND next_eligible_at <= ? AND attempt_count < ?
               RETURNING id, call_count, attempt_count, phone""",
            (call_id, worker_id, lease_until, now, now,
             campaign_id, now, max_attempts, campaign_id, now, max_attempts, now, max_attempts),
        )

    def try_reserve(self, borrower_id: str, call_id: str, worker_id: str, now: float,
                    lease_until: float, max_attempts: int) -> bool:
        cur = self.db.execute(
            """UPDATE borrowers SET status='RESERVED', active_call_id=?, reserved_by=?, reserved_until=?,
                      call_count=call_count+1, last_attempt_at=?, updated_at=?
               WHERE id=? AND status IN ('PENDING','RETRY_WAIT') AND next_eligible_at <= ?
                 AND attempt_count < ?""",
            (call_id, worker_id, lease_until, now, now, borrower_id, now, max_attempts),
        )
        return cur.rowcount == 1

    def release(self, borrower_id: str, call_id: str, new_status: BorrowerStatus, now: float,
                next_eligible_at: float, count_attempt: bool) -> bool:
        cur = self.db.execute(
            """UPDATE borrowers SET status=?, active_call_id=NULL, reserved_by=NULL, reserved_until=NULL,
                      next_eligible_at=?, attempt_count=attempt_count+?, updated_at=?
               WHERE id=? AND active_call_id=?""",
            (new_status.value, next_eligible_at, 1 if count_attempt else 0, now, borrower_id, call_id),
        )
        return cur.rowcount == 1


# ---------------------------------------------------------------------------- calls
class CallRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def insert(self, *, call_id: str, campaign_id: str, borrower_id: str, agent_id: Optional[str],
               binding: Binding, permit_id: str, attempt_seq: int, provider: Optional[str],
               now: float, lease_owner: str, lease_until: float) -> None:
        self.db.execute(
            """INSERT INTO calls (id, campaign_id, borrower_id, agent_id, binding, permit_id, attempt_seq,
                                  provider, status, lease_owner, lease_until, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?, 'RESERVED', ?,?,?,?)""",
            (call_id, campaign_id, borrower_id, agent_id, binding.value, permit_id, attempt_seq,
             provider, lease_owner, lease_until, now, now),
        )

    def get(self, call_id: str) -> Optional[Call]:
        row = self.db.query_one("SELECT * FROM calls WHERE id=?", (call_id,))
        return Call.from_row(row) if row else None

    def find_by_provider_ref(self, provider: str, provider_call_id: str) -> Optional[Call]:
        row = self.db.query_one("SELECT * FROM calls WHERE provider=? AND provider_call_id=?",
                                (provider, provider_call_id))
        return Call.from_row(row) if row else None

    def list(self, status: Optional[CallStatus] = None, limit: int = 200) -> list[Call]:
        if status:
            rows = self.db.query("SELECT * FROM calls WHERE status=? ORDER BY created_at DESC LIMIT ?",
                                 (status.value, limit))
        else:
            rows = self.db.query("SELECT * FROM calls ORDER BY created_at DESC LIMIT ?", (limit,))
        return [Call.from_row(r) for r in rows]

    def list_active(self) -> list[Call]:
        rows = self.db.query(f"SELECT * FROM calls WHERE status IN {ACTIVE_CALL_STATES_SQL}")
        return [Call.from_row(r) for r in rows]

    def counts_by_status(self) -> dict[str, int]:
        rows = self.db.query("SELECT status, COUNT(*) n FROM calls GROUP BY status")
        counts = {s.value: 0 for s in CallStatus}
        counts.update({r["status"]: r["n"] for r in rows})
        return counts

    def counts_by_outcome(self) -> dict[str, int]:
        rows = self.db.query("SELECT COALESCE(outcome,'IN_PROGRESS') o, COUNT(*) n FROM calls GROUP BY o")
        return {r["o"]: r["n"] for r in rows}

    def record_initiated(self, call_id: str, provider_call_id: str, now: float, latency_ms: float) -> bool:
        """Persist the provider's acceptance. Never regresses a call that events already advanced."""
        cur = self.db.execute(
            """UPDATE calls SET provider_call_id=COALESCE(provider_call_id, ?),
                      status = CASE WHEN status='RESERVED' THEN 'INITIATED' ELSE status END,
                      initiated_at=COALESCE(initiated_at, ?), setup_latency_ms=?,
                      lease_owner=NULL, lease_until=NULL, updated_at=?
               WHERE id=?""",
            (provider_call_id, now, latency_ms, now, call_id),
        )
        return cur.rowcount == 1

    def transition(self, call_id: str, expected: CallStatus, new_status: CallStatus, now: float,
                   **fields: Any) -> bool:
        """Compare-and-set call transition. ``fields`` are extra columns to set."""
        sets = ["status=?", "updated_at=?"]
        params: list[Any] = [new_status.value, now]
        for col, val in fields.items():
            sets.append(f"{col}=?")
            params.append(val)
        cur = self.db.execute(
            f"UPDATE calls SET {', '.join(sets)} WHERE id=? AND status=?",
            (*params, call_id, expected.value),
        )
        return cur.rowcount == 1

    def set_fields(self, call_id: str, now: float, **fields: Any) -> None:
        sets = ["updated_at=?"] + [f"{c}=?" for c in fields]
        self.db.execute(f"UPDATE calls SET {', '.join(sets)} WHERE id=?", (now, *fields.values(), call_id))


# ------------------------------------------------------------------------ campaigns
class CampaignRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def create(self, campaign_id: str, name: str, mode: DialMode, max_attempts: int,
               retry_delay_s: float, max_concurrent_calls: int, now: float) -> None:
        self.db.execute(
            """INSERT INTO campaigns (id, name, mode, status, max_attempts, retry_delay_s,
                                      max_concurrent_calls, created_at, updated_at)
               VALUES (?,?,?, 'DRAFT', ?,?,?,?,?)""",
            (campaign_id, name, mode.value, max_attempts, retry_delay_s, max_concurrent_calls, now, now),
        )

    def get(self, campaign_id: str) -> Optional[Campaign]:
        row = self.db.query_one("SELECT * FROM campaigns WHERE id=?", (campaign_id,))
        return Campaign.from_row(row) if row else None

    def list(self) -> list[Campaign]:
        return [Campaign.from_row(r) for r in self.db.query("SELECT * FROM campaigns ORDER BY created_at")]

    def set_status(self, campaign_id: str, status: CampaignStatus, now: float) -> bool:
        cur = self.db.execute("UPDATE campaigns SET status=?, updated_at=? WHERE id=?",
                              (status.value, now, campaign_id))
        return cur.rowcount == 1

    def set_mode(self, campaign_id: str, mode: DialMode, now: float) -> bool:
        cur = self.db.execute("UPDATE campaigns SET mode=?, updated_at=? WHERE id=?",
                              (mode.value, now, campaign_id))
        return cur.rowcount == 1


# --------------------------------------------------------------------- event inbox
class EventRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def insert_inbox(self, *, provider: str, event_id: str, call_id: Optional[str],
                     provider_call_id: Optional[str], event_type: str, sequence: int,
                     occurred_at: float, received_at: float, payload: dict) -> bool:
        """Returns False if this (provider, event_id) was already recorded."""
        try:
            self.db.execute(
                """INSERT INTO provider_events (provider, event_id, call_id, provider_call_id, event_type,
                                                sequence, occurred_at, received_at, result, payload)
                   VALUES (?,?,?,?,?,?,?,?, 'PENDING', ?)""",
                (provider, event_id, call_id, provider_call_id, event_type, sequence, occurred_at,
                 received_at, json.dumps(payload)),
            )
            return True
        except sqlite3.IntegrityError:
            self.db.execute(
                "UPDATE provider_events SET duplicate_count=duplicate_count+1 WHERE provider=? AND event_id=?",
                (provider, event_id),
            )
            return False

    def set_result(self, provider: str, event_id: str, result: str, detail: str, call_id: Optional[str],
                   processed_at: float) -> None:
        self.db.execute(
            "UPDATE provider_events SET result=?, detail=?, call_id=COALESCE(call_id, ?) "
            "WHERE provider=? AND event_id=?",
            (result, detail, call_id, provider, event_id),
        )

    def duplicate_total(self) -> int:
        return self.db.scalar("SELECT COALESCE(SUM(duplicate_count),0) FROM provider_events") or 0

    def counts_by_result(self) -> dict[str, int]:
        rows = self.db.query("SELECT result, COUNT(*) n FROM provider_events GROUP BY result")
        return {r["result"]: r["n"] for r in rows}

    def for_call(self, call_id: str) -> list[sqlite3.Row]:
        return self.db.query("SELECT * FROM provider_events WHERE call_id=? ORDER BY id", (call_id,))


# ------------------------------------------------------------------- pacing stats
class PacingStatsRepository:
    def __init__(self, db: Database, initial_answer_rate: float, default_talk_s: float,
                 default_setup_s: float) -> None:
        self.db = db
        self.initial_answer_rate = initial_answer_rate
        self.default_talk_s = default_talk_s
        self.default_setup_s = default_setup_s

    def ensure(self, campaign_id: str, now: float) -> None:
        self.db.execute(
            """INSERT OR IGNORE INTO pacing_stats (campaign_id, answer_rate_ewma, answer_rate_fast,
                   talk_time_ewma, setup_time_ewma, updated_at) VALUES (?,?,?,?,?,?)""",
            (campaign_id, self.initial_answer_rate, self.initial_answer_rate,
             self.default_talk_s, self.default_setup_s, now),
        )

    def get(self, campaign_id: str) -> Optional[sqlite3.Row]:
        return self.db.query_one("SELECT * FROM pacing_stats WHERE campaign_id=?", (campaign_id,))

    def record_dial_outcome(self, campaign_id: str, answered: bool, alpha: float, fast_alpha: float,
                            now: float) -> None:
        """EWMA update: new = (1 - alpha) * old + alpha * x, with x in {0, 1}."""
        self.ensure(campaign_id, now)
        x = 1.0 if answered else 0.0
        self.db.execute(
            """UPDATE pacing_stats SET
                   answer_rate_ewma = (1 - ?) * answer_rate_ewma + ? * ?,
                   answer_rate_fast = (1 - ?) * answer_rate_fast + ? * ?,
                   answer_samples = answer_samples + 1,
                   answered_count = answered_count + ?,
                   updated_at = ?
               WHERE campaign_id=?""",
            (alpha, alpha, x, fast_alpha, fast_alpha, x, 1 if answered else 0, now, campaign_id),
        )

    def record_talk_time(self, campaign_id: str, talk_s: float, alpha: float, now: float) -> None:
        self.ensure(campaign_id, now)
        self.db.execute(
            """UPDATE pacing_stats SET talk_time_ewma = (1 - ?) * talk_time_ewma + ? * ?,
                   talk_samples = talk_samples + 1, updated_at=? WHERE campaign_id=?""",
            (alpha, alpha, talk_s, now, campaign_id),
        )

    def record_setup_time(self, campaign_id: str, setup_s: float, alpha: float, now: float) -> None:
        self.ensure(campaign_id, now)
        self.db.execute(
            "UPDATE pacing_stats SET setup_time_ewma = (1 - ?) * setup_time_ewma + ? * ?, updated_at=? "
            "WHERE campaign_id=?",
            (alpha, alpha, setup_s, now, campaign_id),
        )

    def record_abandoned(self, campaign_id: str, now: float) -> None:
        self.ensure(campaign_id, now)
        self.db.execute("UPDATE pacing_stats SET abandoned_count=abandoned_count+1, updated_at=? "
                        "WHERE campaign_id=?", (now, campaign_id))


# -------------------------------------------------------------- read-only metrics
class MetricsReader:
    """Read-only view used by the pacing engines and the API.

    The Safety Controller deliberately does NOT use this class. It runs its own
    capacity queries, so a bug in pacing inputs cannot corrupt the safety bound.
    """

    def __init__(self, db: Database) -> None:
        self.db = db

    def agent_counts(self) -> dict[str, int]:
        return AgentRepository(self.db).counts_by_status()

    def wrapup_remaining(self, now: float) -> list[float]:
        rows = self.db.query("SELECT wrap_up_until FROM agents WHERE status='WRAP_UP' AND pause_requested=0")
        return [max(0.0, (r["wrap_up_until"] or now) - now) for r in rows]

    def connected_elapsed(self, now: float) -> list[float]:
        rows = self.db.query("SELECT connected_at FROM calls WHERE status='CONNECTED'")
        return [max(0.0, now - (r["connected_at"] or now)) for r in rows]

    def call_counts(self) -> dict[str, int]:
        return CallRepository(self.db).counts_by_status()

    def unbound_in_flight(self) -> int:
        return self.db.scalar(
            f"SELECT COUNT(*) FROM calls WHERE binding='UNBOUND' AND status IN {PRE_ANSWER_SQL}")

    def ringing_calls(self) -> int:
        return self.db.scalar("SELECT COUNT(*) FROM calls WHERE status IN ('INITIATED','RINGING')")

    def campaign_active_calls(self, campaign_id: str) -> int:
        return self.db.scalar(
            f"SELECT COUNT(*) FROM calls WHERE campaign_id=? AND status IN {ACTIVE_CALL_STATES_SQL}",
            (campaign_id,))
