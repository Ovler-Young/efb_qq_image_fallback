"""efb-qq-image-fallback-middleware

EFB middleware that recovers images which the QQ slave failed to
download. Recognises the slave's failure-text marker, replaces it
with a placeholder image, and schedules retry fetches against a
hash-addressed fallback HTTP server.

Config: ~/.ehforwarderbot/profiles/<profile>/qqimg.fallback/config.yaml
"""
from __future__ import annotations

import asyncio
import logging
import threading
from pathlib import Path
from typing import Optional

from ehforwarderbot import Message, MsgType, Status, coordinator
from ehforwarderbot.middleware import Middleware
from ehforwarderbot.types import InstanceID, ModuleID
from ehforwarderbot.utils import get_config_path, get_data_path

from .config import Config
from .db import PendingRow, Queue
from .fetch import build_url, fetch_sync
from .hash_extract import extract_hash_from_text, extract_hash_from_url
from .placeholder import placeholder_file
from .worker import RetryWorker

__version__ = "0.1.0"

log = logging.getLogger(__name__)


# Failure-text markers emitted by the QQ slave (after applying the
# provided 1-line patch that always includes the URL).
_FAIL_MARKERS = (
    "[Image download failed",    # catches the patched form
)


class QQImageFallbackMiddleware(Middleware):
    middleware_id = "qqimg.fallback"
    middleware_name = "QQ Image Fallback Middleware"
    __version__ = __version__

    def __init__(self, instance_id: Optional[InstanceID] = None):
        super().__init__(instance_id)

        cfg_path = get_config_path(self.middleware_id) / "config.yaml"
        self.cfg = Config.load(cfg_path)

        db_path = get_data_path(self.middleware_id) / "queue.sqlite"
        self.queue = Queue(db_path)

        self.worker: Optional[RetryWorker] = None
        self._started = False
        self._start_lock = threading.Lock()

        if self.cfg.enabled():
            log.info(
                "%s v%s enabled; server=%s, pending=%d",
                self.middleware_id, __version__,
                self.cfg.server_base_url, self.queue.count(),
            )
        else:
            log.warning(
                "%s loaded but disabled (server_base_url is empty). "
                "Edit %s to enable.", self.middleware_id, cfg_path,
            )

    # --- framework hooks ------------------------------------------------

    def process_message(self, message: Message) -> Optional[Message]:
        """Intercept image-failure messages from the QQ slave; return
        either the rewritten message (placeholder image) or the
        original message for any other case.
        """
        if not self.cfg.enabled():
            return message

        if not self._started:
            self._lazy_start()

        # Filter: only slave-originating messages that look like the
        # slave's image-failure text.
        if message.type != MsgType.Text:
            return message
        if not message.text or not any(m in message.text for m in _FAIL_MARKERS):
            return message
        if self.cfg.slave_module_ids:
            chan = getattr(message.chat, "module_id", None)
            if chan and chan not in self.cfg.slave_module_ids:
                return message

        hash_ = extract_hash_from_text(message.text)
        if not hash_:
            log.debug("failure text seen but no hash extractable: %r", message.text)
            return message

        return self._rewrite_as_placeholder(message, hash_)

    def process_status(self, status: Status) -> Optional[Status]:
        return status

    # --- lifecycle ------------------------------------------------------

    def _lazy_start(self) -> None:
        """Start the worker the first time we actually see a message.
        Doing this in __init__ would be too early — coordinator may not
        yet be fully set up.
        """
        with self._start_lock:
            if self._started:
                return
            self.worker = RetryWorker(
                cfg=self.cfg,
                queue=self.queue,
                edit_callback=self._deliver_edit,
            )
            self.worker.start()
            self._started = True

    def stop_polling(self) -> None:
        """Optional EFB hook. Not abstract on Middleware, but if the
        framework invokes it we honour it.
        """
        if self.worker is not None:
            self.worker.stop()
            self.worker.join(timeout=5)
        self.queue.close()

    # --- rewrite path ---------------------------------------------------

    def _rewrite_as_placeholder(self, message: Message, hash_: str) -> Message:
        """Replace the failure text with a placeholder image and enqueue
        a retry. If sync_attempt_on_arrival is on, try the fallback
        server once before falling back to the placeholder.
        """
        original_url = _extract_url_from_text(message.text)

        if self.cfg.sync_attempt_on_arrival:
            # Synchronous attempt — bounded by sync_timeout. Happens in
            # whatever thread the slave is using; fine because httpx
            # Client releases the GIL during network I/O.
            url = build_url(self.cfg.server_base_url, hash_)
            file = fetch_sync(
                url,
                timeout=self.cfg.sync_timeout_seconds,
                max_bytes=self.cfg.max_bytes,
                headers=self.cfg.request_headers,
            )
            if file is not None:
                log.info("sync fallback hit: hash=%s chat=%s msg=%s",
                         hash_, message.chat.uid, message.uid)
                _attach_image(message, file)
                return message
            log.debug("sync fallback miss: hash=%s", hash_)

        # Miss or skipped: placeholder + enqueue.
        _attach_image(message, placeholder_file())

        delay = self.cfg.next_delay(0)
        if delay is None:
            # Empty schedule → no retries. Ship the placeholder and
            # stop there. At least the master no longer shows a text
            # error; the user can still open the original URL.
            log.debug("retry schedule empty; shipping placeholder only")
            return message

        import time as _t
        chat_module_id = getattr(message.chat, "module_id", "") or ""
        inserted = self.queue.enqueue(
            hash_=hash_,
            msg_uid=str(message.uid),
            chat_module_id=str(chat_module_id),
            chat_uid=str(message.chat.uid),
            original_url=original_url,
            first_try_at=_t.time() + delay,
        )
        if inserted:
            log.info(
                "enqueued hash=%s msg=%s chat=%s first_try=+%ds",
                hash_, message.uid, message.chat.uid, delay,
            )
        return message

    # --- edit delivery (called from worker thread) ----------------------

    def _deliver_edit(self, row: PendingRow, fresh_file) -> bool:
        """Called by RetryWorker when a fetch succeeds. Builds an edit
        Message and hands it to coordinator.send_message.
        """
        try:
            slave = coordinator.slaves.get(ModuleID(row.chat_module_id))
            if slave is None:
                log.warning(
                    "slave %s not registered; dropping retry for msg=%s",
                    row.chat_module_id, row.msg_uid,
                )
                return True  # Don't keep retrying; just drop.

            try:
                chat = slave.get_chat(row.chat_uid)
            except Exception as e:
                log.info(
                    "get_chat(%s) failed on %s: %s; will retry later",
                    row.chat_uid, row.chat_module_id, e,
                )
                return False

            edit = Message()
            edit.chat = chat
            edit.author = chat.self or _synthetic_author(chat)
            edit.uid = row.msg_uid
            edit.type = MsgType.Image
            edit.file = fresh_file
            edit.filename = f"{row.hash}.jpg"
            edit.path = fresh_file.name
            edit.mime = "image/jpeg"
            edit.edit = True
            edit.edit_media = True
            edit.deliver_to = coordinator.master

            coordinator.send_message(edit)
            log.info(
                "delivered edit hash=%s msg=%s chat=%s",
                row.hash, row.msg_uid, row.chat_uid,
            )
            return True
        except Exception:
            log.exception("edit delivery failed for row %s", row.id)
            return False


# --- helpers ----------------------------------------------------------


def _extract_url_from_text(text: str) -> Optional[str]:
    import re
    m = re.search(r"https?://\S+", text or "")
    return m.group(0) if m else None


def _attach_image(message: Message, file) -> None:
    """Mutate message in place: text failure → image with the given file."""
    message.type = MsgType.Image
    message.text = ""
    message.file = file
    message.path = file.name
    try:
        import magic
        mime = magic.from_file(file.name, mime=True)
        if isinstance(mime, bytes):
            mime = mime.decode()
    except Exception:
        mime = "image/png"
    message.mime = mime
    ext = (mime.split("/")[-1] if "/" in mime else "bin")
    message.filename = f"qqfallback.{ext}"


def _synthetic_author(chat):
    """Fallback author when chat.self isn't populated.
    Most masters ignore author on edit, but set something sensible."""
    try:
        from ehforwarderbot.chat import SelfChatMember
        return SelfChatMember(chat)
    except Exception:
        return chat
