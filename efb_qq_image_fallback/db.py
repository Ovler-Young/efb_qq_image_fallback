"""SQLite-backed persistent retry queue.

Survives middleware restarts. One row per (chat_uid, msg_uid) pair.
"""
from __future__ import annotations

import logging
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)


SCHEMA = """
CREATE TABLE IF NOT EXISTS pending (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    hash            TEXT NOT NULL,
    msg_uid         TEXT NOT NULL,
    chat_module_id  TEXT NOT NULL,
    chat_uid        TEXT NOT NULL,
    original_url    TEXT,
    first_seen      REAL NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_try_at     REAL NOT NULL,
    UNIQUE(chat_module_id, chat_uid, msg_uid)
);
CREATE INDEX IF NOT EXISTS idx_due ON pending(next_try_at);
CREATE INDEX IF NOT EXISTS idx_hash ON pending(hash);
"""


@dataclass
class PendingRow:
    id: int
    hash: str
    msg_uid: str
    chat_module_id: str
    chat_uid: str
    original_url: Optional[str]
    first_seen: float
    attempts: int
    next_try_at: float


class Queue:
    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._con = sqlite3.connect(str(path), check_same_thread=False)
        self._con.executescript(SCHEMA)
        self._con.commit()

    def close(self) -> None:
        with self._lock:
            self._con.close()

    def enqueue(
        self,
        hash_: str,
        msg_uid: str,
        chat_module_id: str,
        chat_uid: str,
        original_url: Optional[str],
        first_try_at: float,
    ) -> bool:
        """Insert a pending entry. Returns True if inserted, False if it
        was already present (same chat+msg). Idempotent by design.
        """
        now = time.time()
        with self._lock:
            try:
                self._con.execute(
                    """
                    INSERT INTO pending
                        (hash, msg_uid, chat_module_id, chat_uid,
                         original_url, first_seen, attempts, next_try_at)
                    VALUES (?, ?, ?, ?, ?, ?, 0, ?)
                    """,
                    (hash_, msg_uid, chat_module_id, chat_uid,
                     original_url, now, first_try_at),
                )
                self._con.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def due(self, limit: int = 100) -> list[PendingRow]:
        now = time.time()
        with self._lock:
            rows = self._con.execute(
                """
                SELECT id, hash, msg_uid, chat_module_id, chat_uid,
                       original_url, first_seen, attempts, next_try_at
                FROM pending
                WHERE next_try_at <= ?
                ORDER BY next_try_at ASC
                LIMIT ?
                """,
                (now, limit),
            ).fetchall()
        return [PendingRow(*r) for r in rows]

    def reschedule(self, row_id: int, attempts: int, next_try_at: float) -> None:
        with self._lock:
            self._con.execute(
                "UPDATE pending SET attempts = ?, next_try_at = ? WHERE id = ?",
                (attempts, next_try_at, row_id),
            )
            self._con.commit()

    def remove(self, row_id: int) -> None:
        with self._lock:
            self._con.execute("DELETE FROM pending WHERE id = ?", (row_id,))
            self._con.commit()

    def count(self) -> int:
        with self._lock:
            return self._con.execute("SELECT COUNT(*) FROM pending").fetchone()[0]

    def earliest_due(self) -> Optional[float]:
        with self._lock:
            r = self._con.execute(
                "SELECT MIN(next_try_at) FROM pending"
            ).fetchone()
        return r[0] if r and r[0] is not None else None
