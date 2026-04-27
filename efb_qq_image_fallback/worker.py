"""Background retry worker.

Runs in its own thread with its own blocking HTTP client (httpx.Client
via fetch_sync). Polls the SQLite queue, fetches fallback URLs, and
emits edit messages via coordinator.send_message.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Callable, Optional

from .config import Config
from .db import PendingRow, Queue
from .fetch import build_url, fetch_sync

log = logging.getLogger(__name__)


EditCallback = Callable[[PendingRow, "object"], bool]
"""Takes (row, open_file) and returns True iff the edit was delivered
successfully. Worker removes the DB row on True, reschedules on False."""


class RetryWorker(threading.Thread):
    def __init__(
        self,
        cfg: Config,
        queue: Queue,
        edit_callback: EditCallback,
        name: str = "qqimg-fallback-worker",
    ):
        super().__init__(name=name, daemon=True)
        self.cfg = cfg
        self.queue = queue
        self.edit_callback = edit_callback
        self._stop_event = threading.Event()
        self._refresh_lock = threading.Lock()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        log.info("retry worker started")
        while not self._stop_event.is_set():
            try:
                self._tick()
            except Exception:
                log.exception("unexpected error in worker tick")
            # Sleep in small slices so stop() is responsive
            self._sleep(self.cfg.poll_interval_seconds)
        log.info("retry worker stopped")

    def _sleep(self, total_seconds: float) -> None:
        end = time.monotonic() + total_seconds
        while not self._stop_event.is_set():
            left = end - time.monotonic()
            if left <= 0:
                return
            time.sleep(min(1.0, left))

    def _tick(self) -> None:
        if not self._refresh_lock.acquire(blocking=False):
            log.debug("worker: refresh already running; skipping scheduled tick")
            return
        try:
            rows = self.queue.due(limit=100)
            if not rows:
                return
            log.debug("worker: %d due entries", len(rows))
            self._process_rows(rows, reschedule_misses=True)
        finally:
            self._refresh_lock.release()

    def refresh_all_pending(self) -> int:
        """Immediately try every pending row, ignoring next_try_at."""
        if not self._refresh_lock.acquire(blocking=False):
            log.info("manual refresh skipped because another refresh is running")
            return 0
        try:
            rows = self.queue.all_pending()
            if not rows:
                return 0
            log.info("manual refresh: %d pending entries", len(rows))
            self._process_rows(rows, reschedule_misses=False)
            return len(rows)
        finally:
            self._refresh_lock.release()

    def _process_rows(
        self, rows: list[PendingRow], reschedule_misses: bool
    ) -> None:
        if not rows:
            return

        # Group by hash so we only fetch each hash once per tick.
        by_hash: dict[str, list[PendingRow]] = {}
        for r in rows:
            by_hash.setdefault(r.hash, []).append(r)

        for h, group in by_hash.items():
            if self._stop_event.is_set():
                break
            url = build_url(self.cfg.server_base_url, h)
            file = fetch_sync(
                url,
                timeout=self.cfg.worker_timeout_seconds,
                max_bytes=self.cfg.max_bytes,
                headers=self.cfg.request_headers,
            )
            if file is None:
                if reschedule_misses:
                    self._reschedule_all(group)
                continue

            # Success: the same bytes go out to every row in the group.
            # We close() after each callback so subsequent rows need a
            # fresh handle each time. Rather than re-fetching, we buffer
            # the bytes.
            try:
                data = file.read()
            finally:
                try:
                    file.close()
                except Exception:
                    pass

            for r in group:
                if self._stop_event.is_set():
                    break
                import io, tempfile
                tmp = tempfile.NamedTemporaryFile(
                    prefix="qqimg-fb-", delete=False
                )
                tmp.write(data)
                tmp.seek(0)
                try:
                    ok = self.edit_callback(r, tmp)
                except Exception:
                    log.exception("edit callback raised for row %s", r.id)
                    ok = False
                finally:
                    try:
                        tmp.close()
                    except Exception:
                        pass

                if ok:
                    self.queue.remove(r.id)
                else:
                    # Treat as transient: bump and reschedule
                    self._reschedule_one(r)

    def _reschedule_all(self, rows: list[PendingRow]) -> None:
        for r in rows:
            self._reschedule_one(r)

    def _reschedule_one(self, r: PendingRow) -> None:
        new_attempts = r.attempts + 1
        delay = self.cfg.next_delay(new_attempts)
        if delay is None:
            log.info(
                "giving up on hash=%s msg=%s after %d attempts",
                r.hash, r.msg_uid, new_attempts,
            )
            self.queue.remove(r.id)
            return
        next_at = time.time() + delay
        self.queue.reschedule(r.id, new_attempts, next_at)
