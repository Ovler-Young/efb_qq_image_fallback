"""Load the middleware's YAML config.

Default config path follows EFB's convention:
  ~/.ehforwarderbot/profiles/<profile>/<middleware_id>/config.yaml
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

log = logging.getLogger(__name__)

# 5 min, 1 h, 6 h, 1 d, 3 d, 7 d
DEFAULT_SCHEDULE = [300, 3600, 21600, 86400, 259200, 604800]


@dataclass
class Config:
    # Base URL of the fallback image server, e.g. "https://img.example.com/img".
    # The middleware will GET {base}/{hash[:2]}/{hash[2:4]}/{hash}.
    server_base_url: str = ""

    # Retry schedule as delays (seconds) from first-seen time.
    # Empty list disables retries; only synchronous fetch at message arrival
    # is attempted.
    retry_schedule_seconds: list[int] = field(
        default_factory=lambda: list(DEFAULT_SCHEDULE)
    )

    # How often the background worker wakes to check for due entries.
    poll_interval_seconds: int = 30

    # Attempt a synchronous fallback fetch at message-arrival time.
    # If true and the fetch succeeds, the master sees a real image
    # right away (no placeholder).
    sync_attempt_on_arrival: bool = True

    # Maximum time to spend on the synchronous arrival-time fetch.
    sync_timeout_seconds: float = 8.0

    # Timeout for background-worker fetches.
    worker_timeout_seconds: float = 15.0

    # If a fetched image exceeds this many bytes, drop it. 0 disables.
    max_bytes: int = 50 * 1024 * 1024

    # HTTP headers to attach to the fallback request, e.g. for basic auth.
    request_headers: dict = field(default_factory=dict)

    # Restrict processing to these slave channel module IDs.
    # Empty list = process any slave (but the marker check still requires
    # the go-cqhttp failure text format).
    slave_module_ids: list[str] = field(default_factory=list)

    # Master module ID, used as Message.deliver_to at edit time.
    # If empty, the middleware uses coordinator.master at runtime.
    master_module_id: str = ""

    # Optional local endpoint for uploaders to trigger an immediate
    # refresh of all pending rows after new files arrive on the server.
    refresh_api_enabled: bool = False
    refresh_api_host: str = "127.0.0.1"
    refresh_api_port: int = 8765
    refresh_api_token: str = ""

    @classmethod
    def load(cls, path: Path) -> "Config":
        if not path.exists():
            log.warning(
                "config file not found at %s, using defaults (middleware will be a no-op "
                "until you set server_base_url)", path,
            )
            return cls()
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        kwargs = {}
        for k in (
            "server_base_url", "retry_schedule_seconds", "poll_interval_seconds",
            "sync_attempt_on_arrival", "sync_timeout_seconds", "worker_timeout_seconds",
            "max_bytes", "request_headers", "slave_module_ids", "master_module_id",
            "refresh_api_enabled", "refresh_api_host", "refresh_api_port",
            "refresh_api_token",
        ):
            if k in data:
                kwargs[k] = data[k]

        c = cls(**kwargs)
        # Normalise URL: no trailing slash
        c.server_base_url = c.server_base_url.rstrip("/")
        return c

    def enabled(self) -> bool:
        return bool(self.server_base_url)

    def next_delay(self, attempts: int) -> Optional[int]:
        """Delay (seconds) before the (attempts+1)-th try, where attempts
        is the number of prior attempts. Returns None when exhausted.

        attempts=0 → schedule[0]
        attempts=1 → schedule[1]
        ...
        """
        if attempts < 0:
            return None
        if attempts >= len(self.retry_schedule_seconds):
            return None
        return int(self.retry_schedule_seconds[attempts])
