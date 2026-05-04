"""efb-qq-image-fallback-middleware

EFB middleware that recovers images which the QQ slave failed to
download. Recognises the slave's failure-text marker, replaces it
with a placeholder image, and schedules retry fetches against a
hash-addressed fallback HTTP server.

Config: ~/.ehforwarderbot/profiles/<profile>/qqimg.fallback/config.yaml
"""
from __future__ import annotations

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional
from urllib.parse import parse_qs, urlparse

from ehforwarderbot import Message, MsgType, Status, coordinator
from ehforwarderbot.middleware import Middleware
from ehforwarderbot.types import InstanceID, ModuleID
from ehforwarderbot.utils import get_config_path, get_data_path

from .config import Config
from .db import PendingRow, Queue
from .fetch import build_url, fetch_sync
from .hash_extract import extract_hash_from_text
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

        cfg_path = get_config_path(self.middleware_id)
        self.cfg = Config.load(cfg_path)

        db_path = get_data_path(self.middleware_id) / "queue.sqlite"
        self.queue = Queue(db_path)

        self.worker: Optional[RetryWorker] = None
        self._started = False
        self._start_lock = threading.Lock()
        self._refresh_server: Optional[ThreadingHTTPServer] = None
        self._refresh_server_thread: Optional[threading.Thread] = None
        self._manual_refresh_thread: Optional[threading.Thread] = None
        self._manual_refresh_lock = threading.Lock()

        if self.cfg.enabled():
            log.info(
                "%s v%s enabled; server=%s, pending=%d",
                self.middleware_id, __version__,
                self.cfg.server_base_url, self.queue.count(),
            )
            self._start_refresh_api()
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
        self._stop_refresh_api()
        if self.worker is not None:
            self.worker.stop()
            self.worker.join(timeout=5)
        if self._manual_refresh_thread is not None:
            self._manual_refresh_thread.join(timeout=5)
        self.queue.close()

    # --- local refresh API ---------------------------------------------

    def _start_refresh_api(self) -> None:
        if not self.cfg.refresh_api_enabled:
            return

        middleware = self

        class RefreshHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                self._handle_refresh()

            def do_POST(self) -> None:
                self._handle_refresh()

            def log_message(self, fmt: str, *args) -> None:
                log.debug("refresh api: " + fmt, *args)

            def _handle_refresh(self) -> None:
                parsed = urlparse(self.path)
                if parsed.path != "/refresh":
                    self._write_json(404, {"ok": False, "error": "not_found"})
                    return
                if not middleware._refresh_api_authorized(parsed.query, self.headers):
                    self._write_json(403, {"ok": False, "error": "forbidden"})
                    return

                hashes, error = self._refresh_hashes_from_body()
                if error is not None:
                    self._write_json(400, {"ok": False, "error": error})
                    return

                accepted = middleware._trigger_manual_refresh(hashes)
                status = "accepted" if accepted else "already_running"
                scope = "ids" if hashes is not None else "all"
                self._write_json(202, {"ok": True, "status": status, "scope": scope})

            def _refresh_hashes_from_body(
                self,
            ) -> tuple[Optional[list[str]], Optional[str]]:
                try:
                    length = int(self.headers.get("Content-Length", "0") or "0")
                except ValueError:
                    return None, "invalid_content_length"
                if length <= 0:
                    return None, None

                try:
                    raw = self.rfile.read(length)
                    payload = json.loads(raw.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    return None, "invalid_json"

                ids = payload.get("ids") if isinstance(payload, dict) else payload
                if not isinstance(ids, list):
                    return None, "ids_must_be_array"

                hashes: list[str] = []
                seen: set[str] = set()
                for item in ids:
                    if not isinstance(item, str):
                        return None, "ids_must_be_strings"
                    hash_ = item.lower()
                    if not _is_hash_id(hash_):
                        return None, "invalid_id"
                    if hash_ not in seen:
                        hashes.append(hash_)
                        seen.add(hash_)
                return hashes, None

            def _write_json(self, status: int, body: dict) -> None:
                data = json.dumps(body).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        try:
            self._refresh_server = ThreadingHTTPServer(
                (self.cfg.refresh_api_host, self.cfg.refresh_api_port),
                RefreshHandler,
            )
        except OSError:
            log.exception(
                "failed to start refresh API on %s:%s",
                self.cfg.refresh_api_host, self.cfg.refresh_api_port,
            )
            return

        self._refresh_server_thread = threading.Thread(
            target=self._refresh_server.serve_forever,
            name="qqimg-fallback-refresh-api",
            daemon=True,
        )
        self._refresh_server_thread.start()
        log.info(
            "refresh API listening on http://%s:%s/refresh",
            self.cfg.refresh_api_host,
            self.cfg.refresh_api_port,
        )

    def _stop_refresh_api(self) -> None:
        if self._refresh_server is None:
            return
        self._refresh_server.shutdown()
        self._refresh_server.server_close()
        if self._refresh_server_thread is not None:
            self._refresh_server_thread.join(timeout=5)
        self._refresh_server = None
        self._refresh_server_thread = None

    def _refresh_api_authorized(self, query: str, headers) -> bool:
        token = self.cfg.refresh_api_token
        if not token:
            return True
        query_token = parse_qs(query).get("token", [""])[0]
        header_token = headers.get("X-Refresh-Token", "")
        auth_header = headers.get("Authorization", "")
        bearer = f"Bearer {token}"
        return token in (query_token, header_token) or auth_header == bearer

    def _trigger_manual_refresh(self, hashes: Optional[list[str]] = None) -> bool:
        with self._manual_refresh_lock:
            if (
                self._manual_refresh_thread is not None
                and self._manual_refresh_thread.is_alive()
            ):
                return False
            if not self._started:
                self._lazy_start()

            self._manual_refresh_thread = threading.Thread(
                target=self._run_manual_refresh,
                args=(hashes,),
                name="qqimg-fallback-manual-refresh",
                daemon=True,
            )
            self._manual_refresh_thread.start()
            return True

    def _run_manual_refresh(self, hashes: Optional[list[str]]) -> None:
        if self.worker is None:
            return
        try:
            if hashes is None:
                self.worker.refresh_all_pending()
            else:
                self.worker.refresh_pending_hashes(hashes)
        except Exception:
            log.exception("manual refresh failed")

    # --- rewrite path ---------------------------------------------------

    def _rewrite_as_placeholder(self, message: Message, hash_: str) -> Message:
        """Replace the failure text with a placeholder image and enqueue
        a retry. If sync_attempt_on_arrival is on, try the fallback
        server once before falling back to the placeholder.
        """
        original_url = _extract_url_from_text(message.text)
        original_text = message.text or ""
        author_uid, author_name, author_alias, author_kind = _author_info(
            message.author
        )

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
            message_text=original_text,
            author_uid=author_uid,
            author_name=author_name,
            author_alias=author_alias,
            author_kind=author_kind,
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
            edit.author = _resolve_author(chat, row)
            edit.uid = row.msg_uid
            edit.text = row.message_text or ""
            _attach_image(edit, fresh_file, filename_stem=row.hash)
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


def _is_hash_id(value: str) -> bool:
    return len(value) == 32 and all(c in "0123456789abcdef" for c in value)


def _attach_image(message: Message, file, filename_stem: str = "qqfallback") -> None:
    """Mutate message in place: text failure → image with the given file."""
    message.type = MsgType.Image
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
    message.filename = f"{filename_stem}.{ext}"


def _author_info(author) -> tuple[Optional[str], Optional[str], Optional[str], str]:
    kind = "member"
    try:
        from ehforwarderbot.chat import SelfChatMember, SystemChatMember
        if isinstance(author, SelfChatMember):
            kind = "self"
        elif isinstance(author, SystemChatMember):
            kind = "system"
    except Exception:
        pass

    uid = getattr(author, "uid", None)
    name = getattr(author, "name", None)
    alias = getattr(author, "alias", None)
    return (
        str(uid) if uid else None,
        str(name) if name else None,
        str(alias) if alias else None,
        kind,
    )


def _resolve_author(chat, row: PendingRow):
    if row.author_uid:
        try:
            return chat.get_member(row.author_uid)
        except KeyError:
            pass

        name = row.author_name or row.author_uid
        alias = row.author_alias or None
        if row.author_kind == "self":
            if getattr(chat, "self", None) is not None:
                return chat.self
            return chat.add_self()
        if row.author_kind == "system":
            return chat.add_system_member(name=name, alias=alias, uid=row.author_uid)
        return chat.add_member(name=name, alias=alias, uid=row.author_uid)

    return _best_effort_author(chat)


def _best_effort_author(chat):
    try:
        from ehforwarderbot.chat import SelfChatMember
        other = getattr(chat, "other", None)
        if other is not None and not isinstance(other, SelfChatMember):
            return other
        for member in getattr(chat, "members", ()):
            if not isinstance(member, SelfChatMember):
                return member
    except Exception:
        pass
    return getattr(chat, "self", None) or _synthetic_author(chat)


def _synthetic_author(chat):
    """Fallback author when chat.self isn't populated.
    Most masters ignore author on edit, but set something sensible."""
    try:
        from ehforwarderbot.chat import SelfChatMember
        return SelfChatMember(chat)
    except Exception:
        return chat
