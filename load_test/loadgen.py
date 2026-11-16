"""Async load generator: AIPerf's load model (fixed concurrency or Poisson arrivals,
warm-up window, per-request records) without its LLM-serving metrics.

The process pins itself to CPUs 4-7,12-15 so it never competes with the
benchmark Postgres container (cpuset 0-3,8-11).
"""

from __future__ import annotations

import asyncio
import itertools
import os
import random
import time
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import asdict, dataclass, field
from typing import Any

LOADGEN_CPUS = {4, 5, 6, 7, 12, 13, 14, 15}


def pin_cpus() -> None:
    try:
        os.sched_setaffinity(0, LOADGEN_CPUS)
    except (AttributeError, OSError):
        pass


@dataclass
class Record:
    request_id: str
    item_id: str
    worker: int
    t_start: float           # seconds since run start (monotonic)
    t_end: float
    latency_ms: float
    ok: bool
    error_type: str | None = None
    warmup: bool = False
    extra: dict[str, Any] = field(default_factory=dict)


Request = Callable[[Any, str], Awaitable[dict[str, Any]]]


async def run_load(
    request: Request,
    items: Sequence[Any],
    *,
    item_id: Callable[[Any], str],
    concurrency: int = 1,
    rate_per_s: float | None = None,
    n_requests: int | None = None,
    duration_s: float | None = None,
    warmup_s: float = 0.0,
    seed: int = 0,
) -> list[Record]:
    """Drive `request(item, request_id)` until n_requests or duration_s.

    Fixed-concurrency mode (rate_per_s None): `concurrency` workers each send the
    next item as soon as their previous request returns (closed loop).
    Poisson mode: arrivals with exponential gaps at `rate_per_s`, at most
    `concurrency` in flight (open loop with a cap, so a slow server shows up as
    queueing delay in the record's latency rather than as lost requests).
    `request` returns a dict that may hold "error_type"; it must not raise for
    expected failures, but an exception is recorded as UNHANDLED.
    """
    assert n_requests or duration_s, "need a stop condition"
    rng = random.Random(seed)
    order = list(items)
    rng.shuffle(order)
    feed = itertools.cycle(order)
    records: list[Record] = []
    t0 = time.monotonic()
    sent = 0
    lock = asyncio.Lock()

    def should_stop() -> bool:
        if n_requests is not None and sent >= n_requests:
            return True
        return duration_s is not None and time.monotonic() - t0 >= duration_s

    async def one(item: Any, worker: int, scheduled: float) -> None:
        rid = uuid.uuid4().hex[:12]
        start = time.monotonic()
        try:
            extra = await request(item, rid)
            err = extra.pop("error_type", None)
        except Exception as e:  # noqa: BLE001 -- recorded, not swallowed
            extra, err = {"exception": repr(e)[:300]}, "UNHANDLED"
        end = time.monotonic()
        records.append(
            Record(rid, item_id(item), worker, scheduled - t0, end - t0, (end - scheduled) * 1000, err is None, err,
                   scheduled - t0 < warmup_s, {**extra, "service_ms": (end - start) * 1000})
        )

    if rate_per_s is None:
        async def worker(w: int) -> None:
            nonlocal sent
            while True:
                async with lock:
                    if should_stop():
                        return
                    sent += 1
                    item = next(feed)
                await one(item, w, time.monotonic())

        await asyncio.gather(*(worker(w) for w in range(concurrency)))
    else:
        sem = asyncio.Semaphore(concurrency)
        tasks = []
        next_t = time.monotonic()
        while not should_stop():
            next_t += rng.expovariate(rate_per_s)
            await asyncio.sleep(max(0.0, next_t - time.monotonic()))
            sent += 1
            item = next(feed)
            scheduled = time.monotonic()

            async def guarded(it: Any = item, sch: float = scheduled) -> None:
                async with sem:
                    await one(it, 0, sch)

            tasks.append(asyncio.create_task(guarded()))
        await asyncio.gather(*tasks)
    return sorted(records, key=lambda r: r.t_start)


def pct(vals: list[float], p: float) -> float | None:
    if not vals:
        return None
    s = sorted(vals)
    k = (len(s) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def summarize(records: list[Record], key: str = "latency_ms") -> dict[str, Any]:
    rs = [r for r in records if not r.warmup]
    if not rs:
        return {"n": 0}
    span = max(r.t_end for r in rs) - min(r.t_start for r in rs)
    vals = [getattr(r, key) if hasattr(r, key) else r.extra.get(key) for r in rs if r.ok]
    vals = [v for v in vals if v is not None]
    errors: dict[str, int] = {}
    for r in rs:
        if not r.ok:
            errors[r.error_type or "?"] = errors.get(r.error_type or "?", 0) + 1
    return {
        "n": len(rs),
        "ok": sum(r.ok for r in rs),
        "throughput_rps": len(rs) / span if span > 0 else None,
        "p50": pct(vals, 0.5), "p90": pct(vals, 0.9), "p99": pct(vals, 0.99),
        "mean": sum(vals) / len(vals) if vals else None,
        "errors": errors,
    }


def to_rows(records: list[Record]) -> list[dict[str, Any]]:
    return [asdict(r) for r in records]
