"""SQLite connection management and transactions.

Concurrency model
-----------------
* File databases run in WAL mode, with one connection per thread. Readers never
  block the writer, and SQLite allows exactly one writer at a time.
* ``transaction()`` opens ``BEGIN IMMEDIATE``, which takes the write lock *up
  front*. Two workers can therefore never interleave a read-then-write
  sequence inside a transaction. The second one waits (busy_timeout) until
  the first commits, then sees the committed state.
* Single-statement conditional updates (``UPDATE ... WHERE status='AVAILABLE'``)
  are atomic on their own. ``cursor.rowcount`` tells the caller whether it won
  the race.

PostgreSQL migration: replace ``BEGIN IMMEDIATE`` with row-level locking
(``SELECT ... FOR UPDATE SKIP LOCKED``) so that writers touching *different*
rows proceed in parallel instead of serialising on one database-wide lock.
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Optional, Sequence

from app.database.schema import SCHEMA_SQL


class Database:
    def __init__(self, path: str, *, fast: bool = False) -> None:
        self.path = path
        self.fast = fast
        self.is_memory = path == ":memory:"
        self._local = threading.local()
        # An in-memory database exists per connection, so it is shared and
        # protected by a re-entrant lock (used by the single-threaded simulator).
        self._shared: Optional[sqlite3.Connection] = None
        self._shared_lock = threading.RLock()
        if not self.is_memory:
            Path(path).parent.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ setup
    def _open(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, isolation_level=None, timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        if not self.is_memory:
            conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(f"PRAGMA synchronous={'OFF' if self.fast else 'NORMAL'}")
        return conn

    @property
    def conn(self) -> sqlite3.Connection:
        if self.is_memory:
            if self._shared is None:
                self._shared = self._open()
            return self._shared
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._open()
            self._local.conn = conn
            self._local.depth = 0
        return conn

    def init_schema(self) -> None:
        with self._lock_if_memory():
            self.conn.executescript(SCHEMA_SQL)

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None
        if self._shared is not None:
            self._shared.close()
            self._shared = None

    @contextmanager
    def _lock_if_memory(self) -> Iterator[None]:
        if self.is_memory:
            with self._shared_lock:
                yield
        else:
            yield

    # ----------------------------------------------------------- transactions
    def _depth(self) -> int:
        return getattr(self._local, "depth", 0)

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """``BEGIN IMMEDIATE`` transaction. Nested calls become SAVEPOINTs."""
        with self._lock_if_memory():
            conn = self.conn
            depth = self._depth()
            if depth == 0:
                conn.execute("BEGIN IMMEDIATE")
            else:
                conn.execute(f"SAVEPOINT sp_{depth}")
            self._local.depth = depth + 1
            try:
                yield conn
            except BaseException:
                self._local.depth = depth
                if depth == 0:
                    conn.execute("ROLLBACK")
                else:
                    conn.execute(f"ROLLBACK TO sp_{depth}")
                    conn.execute(f"RELEASE sp_{depth}")
                raise
            else:
                self._local.depth = depth
                if depth == 0:
                    conn.execute("COMMIT")
                else:
                    conn.execute(f"RELEASE sp_{depth}")

    # ---------------------------------------------------------------- helpers
    def execute(self, sql: str, params: Sequence[Any] | dict = ()) -> sqlite3.Cursor:
        with self._lock_if_memory():
            return self.conn.execute(sql, params)

    def query(self, sql: str, params: Sequence[Any] | dict = ()) -> list[sqlite3.Row]:
        with self._lock_if_memory():
            return self.conn.execute(sql, params).fetchall()

    def query_one(self, sql: str, params: Sequence[Any] | dict = ()) -> Optional[sqlite3.Row]:
        with self._lock_if_memory():
            return self.conn.execute(sql, params).fetchone()

    def scalar(self, sql: str, params: Sequence[Any] | dict = ()) -> Any:
        row = self.query_one(sql, params)
        return None if row is None else row[0]
