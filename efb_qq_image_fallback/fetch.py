"""Fetch fallback URL. Returns a temp file on HTTP 200, None otherwise.

Sync and async variants — sync is used by the worker thread, async by the
optional arrival-time fetch from the middleware's process_message.
"""
from __future__ import annotations

import logging
import tempfile
from typing import IO, Optional

import httpx

log = logging.getLogger(__name__)


def build_url(server_base: str, hash_: str) -> str:
    base = server_base.rstrip("/")
    return f"{base}/{hash_[:2]}/{hash_[2:4]}/{hash_}"


def _store_stream(resp: httpx.Response, max_bytes: int) -> Optional[IO]:
    tmp = tempfile.NamedTemporaryFile(prefix="qqimg-fb-", delete=False)
    total = 0
    try:
        for chunk in resp.iter_bytes():
            total += len(chunk)
            if max_bytes > 0 and total > max_bytes:
                tmp.close()
                log.warning("fallback response exceeded max_bytes=%d", max_bytes)
                return None
            tmp.write(chunk)
        if total == 0:
            tmp.close()
            return None
        tmp.seek(0)
        return tmp
    except Exception:
        tmp.close()
        raise


def fetch_sync(
    url: str, timeout: float, max_bytes: int = 0, headers: Optional[dict] = None,
) -> Optional[IO]:
    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            with client.stream("GET", url, headers=headers or {}) as resp:
                if resp.status_code == 404:
                    log.debug("fallback 404 for %s", url)
                    return None
                if resp.status_code != 200:
                    log.info("fallback HTTP %d for %s", resp.status_code, url)
                    return None
                return _store_stream(resp, max_bytes)
    except httpx.HTTPError as e:
        log.info("fallback fetch error for %s: %s", url, e)
        return None
    except Exception as e:
        log.warning("fallback unexpected error for %s: %s", url, e)
        return None


async def _store_stream_async(
    resp: httpx.Response, max_bytes: int
) -> Optional[IO]:
    tmp = tempfile.NamedTemporaryFile(prefix="qqimg-fb-", delete=False)
    total = 0
    try:
        async for chunk in resp.aiter_bytes():
            total += len(chunk)
            if max_bytes > 0 and total > max_bytes:
                tmp.close()
                return None
            tmp.write(chunk)
        if total == 0:
            tmp.close()
            return None
        tmp.seek(0)
        return tmp
    except Exception:
        tmp.close()
        raise


async def fetch_async(
    url: str, timeout: float, max_bytes: int = 0, headers: Optional[dict] = None,
) -> Optional[IO]:
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as c:
            async with c.stream("GET", url, headers=headers or {}) as resp:
                if resp.status_code == 404:
                    return None
                if resp.status_code != 200:
                    log.info("fallback HTTP %d for %s", resp.status_code, url)
                    return None
                return await _store_stream_async(resp, max_bytes)
    except httpx.HTTPError as e:
        log.info("fallback fetch error for %s: %s", url, e)
        return None
    except Exception as e:
        log.warning("fallback unexpected error for %s: %s", url, e)
        return None
