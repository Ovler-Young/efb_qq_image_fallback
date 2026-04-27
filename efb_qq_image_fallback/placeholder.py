"""Solid-color placeholder PNG, stdlib only.

A small grey 30x200 PNG shown while the real image is being fetched
via the fallback server or while we wait for it to appear there.
"""
from __future__ import annotations

import struct
import tempfile
import zlib
from typing import IO

_CACHE: bytes | None = None


def _encode_png(width: int, height: int, rgb: tuple[int, int, int]) -> bytes:
    def chunk(tag: bytes, data: bytes) -> bytes:
        return (
            struct.pack(">I", len(data))
            + tag + data
            + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)
        )

    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)  # 8-bit truecolor
    row = b"\x00" + bytes(rgb) * width  # filter byte + pixel data
    idat = zlib.compress(row * height, 9)
    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", ihdr)
        + chunk(b"IDAT", idat)
        + chunk(b"IEND", b"")
    )


def placeholder_bytes() -> bytes:
    global _CACHE
    if _CACHE is None:
        _CACHE = _encode_png(30, 200, (220, 220, 220))
    return _CACHE


def placeholder_file() -> IO:
    """Open temp file, position at 0, containing the placeholder PNG.
    The caller is responsible for closing it (or, as EFB does, letting
    the master channel close it after send).
    """
    tmp = tempfile.NamedTemporaryFile(
        prefix="qqimg-placeholder-", suffix=".png", delete=False
    )
    tmp.write(placeholder_bytes())
    tmp.seek(0)
    return tmp
