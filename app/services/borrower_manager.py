"""Borrower management. Uses dummy data only (555-01xx style numbers)."""
from __future__ import annotations

import random
from typing import Optional

from app.clock import Clock
from app.database.connection import Database
from app.database.repositories import BorrowerRepository
from app.models.borrower import Borrower, BorrowerStatus


def retry_delay(base_s: float, attempt_count: int, max_s: float, rng: random.Random) -> float:
    """Exponential backoff with +/-20% jitter: base * 2^(attempts-1)."""
    delay = min(max_s, base_s * (2 ** max(0, attempt_count - 1)))
    return delay * rng.uniform(0.8, 1.2)


class BorrowerNotFound(LookupError):
    pass


class BorrowerManager:
    def __init__(self, db: Database, clock: Clock) -> None:
        self._db = db
        self._repo = BorrowerRepository(db)
        self._clock = clock

    @staticmethod
    def dummy_phone(index: int) -> str:
        # +1-555-01xx is reserved for fiction; we extend it with an index suffix.
        return f"+1555010{index:07d}"

    def create_many(self, campaign_id: str, count: int, prefix: str = "b", seed: int = 1,
                    start_index: int = 0) -> list[str]:
        rng = random.Random(seed)
        rows = []
        for i in range(start_index, start_index + count):
            bid = f"{prefix}-{campaign_id}-{i:06d}"
            rows.append((bid, self.dummy_phone(i), rng.choice([0, 0, 0, 1, 2])))
        self._repo.create_many(campaign_id, rows, self._clock.now())
        return [r[0] for r in rows]

    def get(self, borrower_id: str) -> Borrower:
        b = self._repo.get(borrower_id)
        if b is None:
            raise BorrowerNotFound(borrower_id)
        return b

    def list(self, campaign_id: Optional[str] = None, limit: int = 100) -> list[Borrower]:
        if campaign_id:
            rows = self._db.query("SELECT * FROM borrowers WHERE campaign_id=? ORDER BY id LIMIT ?",
                                  (campaign_id, limit))
        else:
            rows = self._db.query("SELECT * FROM borrowers ORDER BY id LIMIT ?", (limit,))
        return [Borrower.from_row(r) for r in rows]

    def counts(self, campaign_id: Optional[str] = None) -> dict[str, int]:
        return self._repo.counts_by_status(campaign_id)

    def mark_do_not_call(self, borrower_id: str) -> bool:
        """Only non-active borrowers can be marked DNC (an active call must finish first)."""
        cur = self._db.execute(
            "UPDATE borrowers SET status='DO_NOT_CALL', updated_at=? WHERE id=? AND status <> 'RESERVED'",
            (self._clock.now(), borrower_id))
        return cur.rowcount == 1

    def count_eligible(self, campaign_id: str, max_attempts: int) -> int:
        return self._repo.count_eligible(campaign_id, self._clock.now(), max_attempts)
