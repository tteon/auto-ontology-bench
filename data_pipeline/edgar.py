"""Minimal SEC EDGAR client: declared User-Agent, <=8 req/s, raw responses cached under raw/."""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path

import httpx
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

RAW = Path(__file__).resolve().parents[1] / "raw" / "edgar"
_UA = os.environ["SEC_UA"]
_MIN_INTERVAL = 1 / 8
_lock = threading.Lock()
_last = 0.0
_client = httpx.Client(headers={"User-Agent": _UA, "Accept-Encoding": "gzip"}, timeout=60)


def get(url: str, cache: str | None = None, *, binary: bool = False) -> bytes:
    """GET with rate limit and on-disk cache (cache is a path relative to raw/edgar)."""
    global _last
    path = RAW / cache if cache else None
    if path and path.exists():
        return path.read_bytes()
    for attempt in range(4):
        with _lock:
            wait = _MIN_INTERVAL - (time.monotonic() - _last)
            if wait > 0:
                time.sleep(wait)
            _last = time.monotonic()
        r = _client.get(url)
        if r.status_code in (429, 503):
            time.sleep(2**attempt)
            continue
        r.raise_for_status()
        if path:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(r.content)
        return r.content
    raise RuntimeError(f"EDGAR kept refusing {url}")


def get_json(url: str, cache: str | None = None) -> dict:
    return json.loads(get(url, cache))


def submissions(cik: int) -> dict:
    return get_json(
        f"https://data.sec.gov/submissions/CIK{cik:010d}.json", f"submissions/{cik:010d}.json"
    )


def companyfacts(cik: int) -> dict:
    return get_json(
        f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json",
        f"companyfacts/{cik:010d}.json",
    )
