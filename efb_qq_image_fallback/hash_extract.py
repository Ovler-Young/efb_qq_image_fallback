"""Extract a 32-hex MD5 hash from a QQ image URL.

Supported QQ URL shapes we've seen fail and need fallback for:

  /gchatpic_new/1/1-1-<HEX32>/0?term=...
  /gchatpic_new/0/0-0-<HEX32>/0?term=...
  /offpic_new/0/.../{<UUID-WITH-DASHES>}/0
  /c2cpicdw/...<HEX32>...

NT new-link URLs (multimedia.nt.qq.com.cn/download?fileid=...) are explicitly
out of scope; per the user, those don't fail.
"""
from __future__ import annotations

import re
from typing import Optional

# Try these in order. First match wins.
_PATTERNS = [
    # gchatpic_new explicit hash
    re.compile(r"/gchatpic_new/\d+/\d+-\d+-([0-9A-Fa-f]{32})", re.I),
    # offpic_new {UUID-with-dashes} -> strip dashes, take 32 hex
    # handled specially below
]

_OFFPIC_UUID = re.compile(r"\{([0-9A-Fa-f\-]{36,})\}")
# Generic fallback: any 32-hex run in the URL. Intentionally last because
# it can trip on unrelated hex in query params.
_ANY_HEX32 = re.compile(r"(?<![0-9A-Fa-f])([0-9A-Fa-f]{32})(?![0-9A-Fa-f])")


def extract_hash_from_url(url: str) -> Optional[str]:
    """Return the 32-char lowercase hex hash embedded in a QQ URL, or None."""
    if not url:
        return None

    for p in _PATTERNS:
        m = p.search(url)
        if m:
            return m.group(1).lower()

    m = _OFFPIC_UUID.search(url)
    if m:
        stripped = m.group(1).replace("-", "")
        if len(stripped) >= 32 and all(c in "0123456789abcdefABCDEF" for c in stripped):
            return stripped[:32].lower()

    m = _ANY_HEX32.search(url)
    if m:
        return m.group(1).lower()

    return None


def extract_hash_from_text(text: str) -> Optional[str]:
    """The slave's failure Text message contains the original URL on a line
    by itself; pull the URL out and hand off to extract_hash_from_url.
    """
    if not text:
        return None
    # Find the first http(s):// token, take until whitespace.
    m = re.search(r"https?://\S+", text)
    if not m:
        # Maybe the hash is just sitting in the text (defensive)
        return extract_hash_from_url(text)
    return extract_hash_from_url(m.group(0))
