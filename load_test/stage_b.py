"""Stage B: DB-only replay of Stage A queries that were verified correct (LLM bypassed).

    uv run python -m load_test.stage_b --from-tag stageA --db finder_real --conc 1,8,32,64,128,256 --duration 30 --tag stageB-real

For each engine, the replay set is every distinct query an LLM generated in
Stage A that scored correct on that engine. Each concurrency level drives the
set in a closed loop; per level we record throughput, server-time percentiles
(by tier as well), errors, and the Enterprise metrics (Neo4j JMX deltas and
SHOW TRANSACTIONS sampling, i.e. the server-side concurrency actually reached)
or Postgres container CPU and pg_stat_statements.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

from enterprise.conn import async_driver, driver  # noqa: E402
from enterprise.metrics import JmxSnapshot, TxSampler  # noqa: E402
from gateway.executors import NeoExecutor, PgExecutor, pg_stats_delta, pg_stats_snapshot  # noqa: E402
from load_test.loadgen import pct, pin_cpus, run_load  # noqa: E402
from load_test.runtime_study import pg_cpu_seconds  # noqa: E402


def replay_set(tag: str) -> dict[str, list[dict]]:
    recs = [json.loads(line) for line in (ROOT / "results" / "stage_a" / tag / "records.jsonl").read_text().splitlines()]
    out: dict[str, dict[str, dict]] = {"pg": {}, "neo": {}}
    for r in recs:
        if r.get("score", {}).get("correct") and r.get("query") and r.get("engine") in out:
            out[r["engine"]].setdefault(r["query"], {"query": r["query"], "tier": r["tier"], "cq_id": r["cq_id"]})
    return {e: list(v.values()) for e, v in out.items()}


async def level(engine: str, ex: Any, items: list[dict], conc: int, duration: float, warmup: float, tag: str) -> dict:
    run_id = f"{tag}:{engine}:{conc}"

    async def req(it: dict, rid: str) -> dict:
        if engine == "pg":
            r = await ex.run(it["query"])
        else:
            r = await ex.run(it["query"], meta={"bench": run_id, "cq": it["cq_id"]})
        if not r.get("ok"):
            return {"error_type": r.get("error_type"), "tier": it["tier"]}
        return {"server_ms": r["server_ms"], "wall_ms": r["client_wall_ms"], "tier": it["tier"]}

    extra: dict[str, Any] = {}
    if engine == "neo":
        with driver() as d:
            jmx = JmxSnapshot()
            jmx.start(d)
        async with async_driver() as ad:
            sampler = TxSampler(ad, run_id, interval_s=0.2)
            sampler.start()
            recs = await run_load(req, items, item_id=lambda i: i["cq_id"], concurrency=conc, duration_s=duration + warmup, warmup_s=warmup)
            await sampler.stop()
        with driver() as d:
            jmx.stop(d)
        extra = {"jmx": jmx.summary(), "tx": sampler.summary()}
    else:
        s0, c0, t0 = await pg_stats_snapshot(ex.db), pg_cpu_seconds(), time.monotonic()
        recs = await run_load(req, items, item_id=lambda i: i["cq_id"], concurrency=conc, duration_s=duration + warmup, warmup_s=warmup)
        el = time.monotonic() - t0
        s1, c1 = await pg_stats_snapshot(ex.db), pg_cpu_seconds()
        extra = {"pg_cpu_cores_avg": (c1 - c0) / el,
                 "pg_stat_statements_top": sorted(pg_stats_delta(s0, s1), key=lambda d: -d["calls"] * d["mean_exec_ms"])[:5]}
    meas = [r for r in recs if not r.warmup]
    ok = [r for r in meas if r.ok]
    span = (max(r.t_end for r in meas) - min(r.t_start for r in meas)) if meas else 0
    by_tier = {}
    for t in sorted({r.extra.get("tier") for r in ok}):
        v = [r.extra["server_ms"] for r in ok if r.extra.get("tier") == t]
        by_tier[t] = {"n": len(v), "p50": pct(v, .5), "p99": pct(v, .99)}
    srv = [r.extra["server_ms"] for r in ok]
    errors: dict[str, int] = {}
    for r in meas:
        if not r.ok:
            errors[r.error_type or "?"] = errors.get(r.error_type or "?", 0) + 1
    extra["host_loadavg_1m"] = __import__("os").getloadavg()[0]  # background-noise indicator (unpinned containers)
    return {"engine": engine, "concurrency": conc, "n": len(meas), "ok": len(ok), "throughput_qps": len(ok) / span if span else 0,
            "server_ms": {"p50": pct(srv, .5), "p90": pct(srv, .9), "p99": pct(srv, .99)},
            "wall_ms": {"p50": pct([r.extra["wall_ms"] for r in ok], .5), "p99": pct([r.extra["wall_ms"] for r in ok], .99)},
            "client_latency_ms": {"p50": pct([r.latency_ms for r in ok], .5), "p99": pct([r.latency_ms for r in ok], .99)},
            "by_tier": by_tier, "errors": errors, **extra}


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-tag", required=True)
    ap.add_argument("--db", default="finder_real")
    ap.add_argument("--conc", default="1,8,32,64,128,256")
    ap.add_argument("--duration", type=float, default=30)
    ap.add_argument("--warmup", type=float, default=5)
    ap.add_argument("--tag", default=time.strftime("stageB-%Y%m%d-%H%M%S"))
    a = ap.parse_args()
    pin_cpus()
    sets = replay_set(a.from_tag)
    concs = [int(c) for c in a.conc.split(",")]
    out = ROOT / "results" / "stage_b" / a.tag
    out.mkdir(parents=True, exist_ok=True)
    pg, neo = PgExecutor(a.db, max(concs)), NeoExecutor(max(concs))
    await pg.start()
    await neo.start()
    print({e: len(v) for e, v in sets.items()}, "verified queries", flush=True)
    with (out / "levels.jsonl").open("w") as f:
        for conc in concs:
            for engine, ex in (("pg", pg), ("neo", neo)):
                if not sets[engine]:
                    continue
                res = await level(engine, ex, sets[engine], conc, a.duration, a.warmup, a.tag)
                f.write(json.dumps(res, default=str) + "\n")
                f.flush()
                print(f"{engine} c={conc}: qps={res['throughput_qps']:.1f} server p50={res['server_ms']['p50']} "
                      f"p99={res['server_ms']['p99']} errors={res['errors']}", flush=True)
    await pg.close()
    await neo.close()


if __name__ == "__main__":
    asyncio.run(main())
