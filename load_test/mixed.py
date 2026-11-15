"""Mixed workload: does runtime routing + Enterprise transaction control protect interactive latency?

    uv run python -m load_test.mixed --db finder_scale --duration 60 --tag mixed-s30k

Three client classes run at the same time against one engine:
  light     24 clients  W1 point lookup / W4a 6-hop reachability       (interactive)
  analytic   2 clients  W2 global aggregation / W3 3-hop co-holding     (batch analytics)
  llm        4 clients  replay of every query the model generated in Stage A (ad-hoc, unvetted)

Neo4j policies
  N1 all-pipelined                every class on the default pipelined runtime
  N2 all-parallel                 every class on the parallel runtime
  N3 routed                       light + llm pipelined; analytic parallel behind a semaphore of 2
  N4 routed + watchdog            N3 plus an Enterprise watchdog: every 0.5 s, SHOW TRANSACTIONS filtered on
                                  tx metadata (cls = 'llm') and TERMINATE TRANSACTIONS for any running > 2 s
Postgres policies (same classes, same queries in SQL)
  P1 default                      server default (max_parallel_workers_per_gather = 2)
  P2 all-single                   no parallel workers
  P3 routed                       light + llm single; analytic 4 workers
  P4 routed + statement_timeout   P3 plus SET LOCAL statement_timeout = 2 s for llm

Per class: throughput, client and server latency p50/p99, errors (TIMEOUT / TERMINATED);
per policy: Enterprise JMX deltas + SHOW TRANSACTIONS sampling (Neo4j) or container CPU (Postgres).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

from enterprise.conn import DATABASE, async_driver, driver  # noqa: E402
from enterprise.metrics import JmxSnapshot, TxSampler  # noqa: E402
from gateway.executors import NeoExecutor, PgExecutor  # noqa: E402
from load_test.loadgen import pct, pin_cpus  # noqa: E402
from load_test.runtime_study import W, anchors, neo_params, pg_cpu_seconds  # noqa: E402

CLASSES = {"light": 24, "analytic": 2, "llm": 4}
WATCHDOG_MS = 2000


def llm_queries(engine: str) -> list[str]:
    qs: set[str] = set()
    for tag in ("test-noonto", "test-onto"):
        p = ROOT / "results" / "stage_a" / tag / "records.jsonl"
        if not p.exists():
            continue
        for line in p.read_text().splitlines():
            r = json.loads(line)
            if r.get("engine") == engine and r.get("query") and (r.get("db") or {}).get("ok"):
                qs.add(r["query"])
    return sorted(qs)


class Watchdog:
    """Terminates llm-class transactions of this run that exceed WATCHDOG_MS (Enterprise TERMINATE TRANSACTIONS)."""

    def __init__(self, run: str) -> None:
        self.run, self.terminated, self._stop = run, 0, asyncio.Event()

    async def loop(self) -> None:
        async with async_driver() as d:
            while not self._stop.is_set():
                try:
                    recs, _, _ = await d.execute_query(
                        "SHOW TRANSACTIONS YIELD transactionId, metaData, elapsedTime "
                        "WHERE metaData.bench = $run AND metaData.cls = 'llm' AND elapsedTime.milliseconds > $ms "
                        "RETURN collect(transactionId) AS ids", run=self.run, ms=WATCHDOG_MS, database_=DATABASE)
                    ids = recs[0]["ids"]
                    if ids:
                        await d.execute_query("TERMINATE TRANSACTIONS $ids", ids=ids, database_=DATABASE)
                        self.terminated += len(ids)
                except Exception:
                    pass
                await asyncio.sleep(0.5)

    def stop(self) -> None:
        self._stop.set()


async def run_policy(engine: str, policy: str, ex: Any, anc: dict, llm: list[str], duration: float, warmup: float,
                     tag: str) -> dict[str, Any]:
    run_id = f"{tag}:{policy}"
    rng = random.Random(policy)
    sem = asyncio.Semaphore(2)
    t_end = time.monotonic() + warmup + duration
    t_meas = time.monotonic() + warmup
    recs: dict[str, list[dict]] = {c: [] for c in CLASSES}

    def runtime_for(cls: str) -> str:
        if engine == "neo":
            return {"N1": "pipelined", "N2": "parallel"}.get(policy) or ("parallel" if cls == "analytic" else "pipelined")
        return {"P1": "default", "P2": "single"}.get(policy) or ("parallel" if cls == "analytic" else "single")

    async def one(cls: str) -> None:
        if cls == "light":
            w = rng.choice(["W1", "W4a"])
            a = rng.choice(anc[w])
        elif cls == "analytic":
            w = rng.choice(["W2", "W3"])
            a = rng.choice(anc[w])
        else:
            w, a = "llm", None
        rt = runtime_for(cls)
        t0 = time.monotonic()
        if engine == "neo":
            meta = {"bench": run_id, "cls": cls}
            if cls == "llm":
                r = await ex.run(rng.choice(llm), runtime=rt, meta=meta)
            elif cls == "analytic" and policy in ("N3", "N4"):
                async with sem:
                    r = await ex.run(W[w]["cypher"], runtime=rt, params=neo_params(w, a), meta=meta)
            else:
                r = await ex.run(W[w]["cypher"], runtime=rt, params=neo_params(w, a), meta=meta)
        else:
            if cls == "llm":
                extra = ["SET LOCAL statement_timeout = '2s'"] if policy == "P4" else []
                r = await ex.run(rng.choice(llm), runtime=rt, pre=extra)
            else:
                r = await ex.run(W[w]["sql"], runtime=rt, args=a)
        if t0 >= t_meas:
            recs[cls].append({"ok": r.get("ok"), "err": r.get("error_type"), "client_ms": (time.monotonic() - t0) * 1000,
                              "server_ms": r.get("server_ms"), "w": w})

    async def client(cls: str) -> None:
        while time.monotonic() < t_end:
            await one(cls)

    extra: dict[str, Any] = {}
    wd = Watchdog(run_id) if policy == "N4" else None
    if engine == "neo":
        with driver() as d:
            jmx = JmxSnapshot()
            jmx.start(d)
        async with async_driver() as ad:
            sampler = TxSampler(ad, run_id, interval_s=0.25)
            sampler.start()
            wd_task = asyncio.create_task(wd.loop()) if wd else None
            await asyncio.gather(*(client(c) for c, n in CLASSES.items() for _ in range(n)))
            if wd:
                wd.stop()
                await wd_task
            await sampler.stop()
        with driver() as d:
            jmx.stop(d)
        extra = {"jmx": jmx.summary(), "tx": {k: v for k, v in sampler.summary().items() if k != "lock_snapshots"},
                 "watchdog_terminated": wd.terminated if wd else 0}
    else:
        c0, t0 = pg_cpu_seconds(), time.monotonic()
        await asyncio.gather(*(client(c) for c, n in CLASSES.items() for _ in range(n)))
        extra = {"pg_cpu_cores_avg": (pg_cpu_seconds() - c0) / (time.monotonic() - t0)}
    out = {"engine": engine, "policy": policy, "duration_s": duration, **extra, "classes": {}}
    for cls, rs in recs.items():
        ok = [r for r in rs if r["ok"]]
        errs: dict[str, int] = {}
        for r in rs:
            if not r["ok"]:
                errs[r["err"] or "?"] = errs.get(r["err"] or "?", 0) + 1
        out["classes"][cls] = {
            "n": len(rs), "ok": len(ok), "qps": len(ok) / duration, "errors": errs,
            "client_ms": {"p50": pct([r["client_ms"] for r in ok], .5), "p99": pct([r["client_ms"] for r in ok], .99)},
            "server_ms": {"p50": pct([r["server_ms"] for r in ok], .5), "p99": pct([r["server_ms"] for r in ok], .99)},
        }
    return out


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="finder_scale")
    ap.add_argument("--duration", type=float, default=60)
    ap.add_argument("--warmup", type=float, default=10)
    ap.add_argument("--policies", default="N1,N2,N3,N4,P1,P2,P3,P4")
    ap.add_argument("--tag", default=time.strftime("mixed-%Y%m%d-%H%M%S"))
    a = ap.parse_args()
    pin_cpus()
    out = ROOT / "results" / "mixed" / a.tag
    out.mkdir(parents=True, exist_ok=True)
    size = sum(CLASSES.values()) + 4
    pg, neo = PgExecutor(a.db, size), NeoExecutor(size)
    await pg.start()
    await neo.start()
    anc = await anchors(pg)
    llm = {"neo": llm_queries("neo"), "pg": llm_queries("pg")}
    print({k: len(v) for k, v in llm.items()}, "replayed LLM queries", flush=True)
    with (out / "policies.jsonl").open("w") as f:
        for policy in a.policies.split(","):
            engine = "neo" if policy.startswith("N") else "pg"
            res = await run_policy(engine, policy, neo if engine == "neo" else pg, anc, llm[engine], a.duration, a.warmup, a.tag)
            f.write(json.dumps(res, default=str) + "\n")
            f.flush()
            c = res["classes"]
            print(f"{policy}: light p50/p99={c['light']['client_ms']['p50']:.0f}/{c['light']['client_ms']['p99']:.0f}ms "
                  f"qps={c['light']['qps']:.0f} | analytic qps={c['analytic']['qps']:.2f} p50={c['analytic']['client_ms']['p50']} | "
                  f"llm qps={c['llm']['qps']:.1f} err={c['llm']['errors']} | terminated={res.get('watchdog_terminated')}", flush=True)
    await pg.close()
    await neo.close()


if __name__ == "__main__":
    asyncio.run(main())
