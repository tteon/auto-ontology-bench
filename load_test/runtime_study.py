"""Runtime study: PG-Single / PG-Parallel vs Neo4j slotted / pipelined / parallel, by workload and concurrency.

    uv run python -m load_test.runtime_study --db finder_scale --duration 45 --conc 1,8,32,64 --tag rt1

Workloads (fixed, verified queries; anchors rotate per request):
  W1  point lookup            one FY metric of one company
  W2  financial aggregation   FY revenue summed by country over every company (global scan + 3-way join)
  W3  2-3 hop                 co-holding: companies most often held by the anchor's institutional holders
  W4a 4-6 hop reachability    distinct upstream suppliers within 6 tiers (natural var-length Cypher)
  W4b 5 hop aggregation       risk categories of upstream suppliers within 4 tiers
  W4r W4a rewritten           APOC BFS (subgraphNodes) instead of path enumeration -- "rewrite before runtime"

Method (from the aura-runtime study): timing runs never use PROFILE; Neo4j time is
server time (result_available_after + result_consumed_after), Postgres time is
wall minus the loopback baseline; plans are captured in a separate pass; the
order of (workload, runtime, concurrency) cells is shuffled with a fixed seed.
Per cell the Enterprise metrics (JMX deltas, SHOW TRANSACTIONS sampling) are
captured for Neo4j and cgroup CPU seconds for the Postgres container.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import subprocess
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

from benchmarks.verifier import compare  # noqa: E402
from enterprise.conn import async_driver, driver  # noqa: E402
from enterprise.metrics import JmxSnapshot, TxSampler  # noqa: E402
from gateway.executors import NeoExecutor, PgExecutor, pg_stats_delta, pg_stats_snapshot  # noqa: E402
from load_test.loadgen import pct, pin_cpus, run_load  # noqa: E402

W: dict[str, dict[str, Any]] = {
    "W1": {
        "sql": "SELECT f.value FROM financial_metrics f WHERE f.company_id = $1 AND f.metric_id = $2 AND f.fiscal_year = $3 AND f.fiscal_period = 'FY'",
        "cypher": ("MATCH (c:Company {company_id: $c})-[:REPORTED]->(f:FinancialMetric {fiscal_year: $y, fiscal_period: 'FY'})"
                   "-[:OF_METRIC]->(:MetricDefinition {metric_id: $m}) RETURN f.value"),
        "ordered": False,
    },
    "W2": {
        "sql": ("SELECT c.country, sum(f.value) FROM financial_metrics f JOIN metric_definitions m ON m.metric_id = f.metric_id "
                "AND m.metric_code = 'revenue' JOIN companies c ON c.company_id = f.company_id "
                "WHERE f.fiscal_year = $1 AND f.fiscal_period = 'FY' GROUP BY c.country"),
        "cypher": ("MATCH (:MetricDefinition {metric_code: 'revenue'})<-[:OF_METRIC]-(f:FinancialMetric {fiscal_year: $y, fiscal_period: 'FY'})"
                   "<-[:REPORTED]-(c:Company) RETURN c.country, sum(f.value)"),
        "ordered": False,
    },
    "W3": {
        "sql": ("SELECT h2.company_id, count(DISTINCT h1.institution_id) AS n FROM institutional_holdings h1 "
                "JOIN institutional_holdings h2 ON h2.institution_id = h1.institution_id AND h2.report_period = h1.report_period "
                "AND h2.company_id <> h1.company_id WHERE h1.company_id = $1 GROUP BY h2.company_id ORDER BY n DESC, h2.company_id LIMIT 20"),
        "cypher": ("MATCH (c:Company {company_id: $c})<-[h1:HOLDS]-(i:Institution)-[h2:HOLDS]->(o:Company) "
                   "WHERE h2.report_period = h1.report_period AND o <> c "
                   "RETURN o.company_id, count(DISTINCT i) AS n ORDER BY n DESC, o.company_id LIMIT 20"),
        "ordered": True,
    },
    "W4a": {
        "sql": ("WITH RECURSIVE up(s, d) AS (SELECT supplier_company_id, 1 FROM supply_chain_relations WHERE customer_company_id = $1 "
                "UNION SELECT r.supplier_company_id, up.d + 1 FROM up JOIN supply_chain_relations r ON r.customer_company_id = up.s WHERE up.d < 6) "
                "SELECT count(DISTINCT s) FROM up WHERE s <> $1"),
        "cypher": "MATCH (s:Company)-[:SUPPLIES*1..6]->(c:Company {company_id: $c}) WHERE s <> c RETURN count(DISTINCT s)",
        "ordered": False,
    },
    "W4b": {
        "sql": ("WITH RECURSIVE up(s, d) AS (SELECT supplier_company_id, 1 FROM supply_chain_relations WHERE customer_company_id = $1 "
                "UNION SELECT r.supplier_company_id, up.d + 1 FROM up JOIN supply_chain_relations r ON r.customer_company_id = up.s WHERE up.d < 4) "
                "SELECT rc.name, count(*) FROM (SELECT DISTINCT s FROM up WHERE s <> $1) u JOIN risk_factors r ON r.company_id = u.s "
                "JOIN risk_categories rc ON rc.risk_category_id = r.risk_category_id GROUP BY rc.name"),
        "cypher": ("MATCH (s:Company)-[:SUPPLIES*1..4]->(c:Company {company_id: $c}) WHERE s <> c WITH DISTINCT s "
                   "MATCH (s)-[:HAS_RISK]->(:RiskFactor)-[:IN_CATEGORY]->(rc:RiskCategory) RETURN rc.name, count(*)"),
        "ordered": False,
    },
    "W4r": {
        "sql": None,  # same SQL as W4a; this workload isolates the Cypher rewrite
        "cypher": ("MATCH (c:Company {company_id: $c}) CALL apoc.path.subgraphNodes(c, {relationshipFilter: '<SUPPLIES', maxLevel: 6}) "
                   "YIELD node WHERE node <> c RETURN count(node)"),
        "ordered": False,
        "same_as": "W4a",
    },
}
PG_RT = ("single", "parallel")
NEO_RT = ("slotted", "pipelined", "parallel")


def pg_cpu_seconds() -> float:
    """CPU seconds consumed by the finder-pg container (cgroup v2 cpu.stat)."""
    out = subprocess.run(["docker", "exec", "finder-pg", "cat", "/sys/fs/cgroup/cpu.stat"], capture_output=True, text=True).stdout
    for line in out.splitlines():
        if line.startswith("usage_usec"):
            return int(line.split()[1]) / 1e6
    return float("nan")


async def anchors(pg: PgExecutor) -> dict[str, list]:
    oems = (await pg.run("SELECT company_id FROM companies WHERE name LIKE 'SynOEM%' ORDER BY company_id LIMIT 24"))["rows"]
    co = (await pg.run("SELECT company_id FROM companies WHERE name LIKE 'SynCo T3-%' ORDER BY company_id LIMIT 200"))["rows"]
    held = (await pg.run("SELECT company_id FROM institutional_holdings GROUP BY 1 ORDER BY count(*) DESC, 1 LIMIT 40"))["rows"]
    return {
        "W1": [(c[0], m, y) for c in co[:100] for m, y in ((1, 2024), (5, 2023), (14, 2022))],
        "W2": [(y,) for y in range(2019, 2026)],
        "W3": [(h[0],) for h in held],
        "W4a": [(o[0],) for o in oems], "W4b": [(o[0],) for o in oems], "W4r": [(o[0],) for o in oems],
    }


def neo_params(w: str, a: tuple) -> dict[str, Any]:
    if w == "W1":
        return {"c": a[0], "m": a[1], "y": a[2]}
    if w == "W2":
        return {"y": a[0]}
    return {"c": a[0]}


async def correctness(pg: PgExecutor, neo: NeoExecutor, anc: dict[str, list], per: int = 3) -> list[dict]:
    out = []
    for w, spec in W.items():
        sql = spec["sql"] or W[spec.get("same_as", w)]["sql"]
        for a in anc[w][:per]:
            ref = await pg.run(sql, runtime="single", args=a)
            row = {"workload": w, "anchor": list(a), "pg_single_rows": ref.get("row_count"), "checks": {}}
            for rt in PG_RT[1:]:
                r = await pg.run(sql, runtime=rt, args=a)
                row["checks"][f"pg-{rt}"] = r.get("ok") and compare(ref["rows"], r["rows"], spec["ordered"])["strict"]
            for rt in NEO_RT:
                r = await neo.run(spec["cypher"], runtime=rt, params=neo_params(w, a), meta={"bench": "rt-correctness"})
                row["checks"][f"neo-{rt}"] = (r.get("ok") and compare(ref["rows"], r["rows"], spec["ordered"])["strict"]) or r.get("error_type")
            out.append(row)
            print(f"  correctness {w} {a}: {row['checks']}", flush=True)
    return out


async def plans(pg: PgExecutor, neo: NeoExecutor, anc: dict[str, list]) -> list[dict]:
    out = []
    for w, spec in W.items():
        a = anc[w][0]
        for rt in NEO_RT:
            r = await neo.run(spec["cypher"], runtime=rt, params=neo_params(w, a), profile=True, meta={"bench": "rt-plan"})
            out.append({"workload": w, "engine": "neo", "runtime": rt, "ok": r.get("ok"), "plan": r.get("plan"),
                        "profile_server_ms": r.get("server_ms"), "error": r.get("error")})
        if spec["sql"]:
            for rt in PG_RT:
                r = await pg.run("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + spec["sql"], runtime=rt, args=a)
                p = json.loads(r["rows"][0][0])[0] if r.get("ok") else {}
                nodes, workers = [], []

                def walk(n: dict) -> None:
                    nodes.append(n.get("Node Type"))
                    if "Workers Launched" in n:
                        workers.append(n["Workers Launched"])
                    for c in n.get("Plans", []):
                        walk(c)

                if p:
                    walk(p["Plan"])
                out.append({"workload": w, "engine": "pg", "runtime": rt, "ok": r.get("ok"),
                            "execution_ms": p.get("Execution Time"), "planning_ms": p.get("Planning Time"),
                            "workers_launched": sum(workers), "parallel_nodes": [n for n in nodes if n and n.startswith(("Gather", "Parallel"))],
                            "shared_hit": p.get("Plan", {}).get("Shared Hit Blocks"), "shared_read": p.get("Plan", {}).get("Shared Read Blocks"),
                            "error": r.get("error")})
        print(f"  plans {w}: " + ", ".join(f"{x['engine']}-{x['runtime']}={x.get('plan', {}).get('actual_runtime') if x['engine'] == 'neo' and x.get('plan') else x.get('workers_launched')}"
                                          for x in out if x["workload"] == w), flush=True)
    return out


async def cell(pg: PgExecutor, neo: NeoExecutor, w: str, engine: str, rt: str, conc: int, anc: list, duration: float,
               warmup: float, tag: str) -> dict[str, Any]:
    spec = W[w]
    run_id = f"{tag}:{w}:{engine}:{rt}:{conc}"
    sql = spec["sql"] or W[spec.get("same_as", w)]["sql"]

    async def req(a: tuple, rid: str) -> dict:
        if engine == "pg":
            r = await pg.run(sql, runtime=rt, args=a)
        else:
            r = await neo.run(spec["cypher"], runtime=rt, params=neo_params(w, a), meta={"bench": run_id, "rid": rid})
        if not r.get("ok"):
            return {"error_type": r.get("error_type")}
        return {"server_ms": r["server_ms"], "wall_ms": r["client_wall_ms"], "pool_wait_ms": r.get("pool_wait_ms"), "rows": r["row_count"]}

    extra: dict[str, Any] = {}
    if engine == "neo":
        with driver() as d:
            jmx = JmxSnapshot()
            jmx.start(d)
        async with async_driver() as ad:
            sampler = TxSampler(ad, run_id, interval_s=0.2)
            sampler.start()
            recs = await run_load(req, anc, item_id=str, concurrency=conc, duration_s=duration + warmup, warmup_s=warmup, seed=conc)
            await sampler.stop()
        with driver() as d:
            jmx.stop(d)
        extra = {"jmx": jmx.summary(), "tx": sampler.summary()}
    else:
        s0, c0 = await pg_stats_snapshot(pg.db), pg_cpu_seconds()
        t0 = time.monotonic()
        recs = await run_load(req, anc, item_id=str, concurrency=conc, duration_s=duration + warmup, warmup_s=warmup, seed=conc)
        el = time.monotonic() - t0
        s1, c1 = await pg_stats_snapshot(pg.db), pg_cpu_seconds()
        delta = pg_stats_delta(s0, s1)
        extra = {"pg_cpu_cores_avg": (c1 - c0) / el, "pg_stat_statements": [d for d in delta if d["calls"] >= 3][:3]}
    meas = [r for r in recs if not r.warmup]
    ok = [r for r in meas if r.ok]
    srv = [r.extra["server_ms"] for r in ok]
    span = (max(r.t_end for r in meas) - min(r.t_start for r in meas)) if meas else 0
    errors: dict[str, int] = {}
    for r in meas:
        if not r.ok:
            errors[r.error_type or "?"] = errors.get(r.error_type or "?", 0) + 1
    extra["host_loadavg_1m"] = __import__("os").getloadavg()[0]  # background-noise indicator (unpinned containers)
    return {"workload": w, "engine": engine, "runtime": rt, "concurrency": conc, "n": len(meas), "ok": len(ok),
            "throughput_qps": len(ok) / span if span else 0,
            "server_ms": {"p50": pct(srv, .5), "p90": pct(srv, .9), "p99": pct(srv, .99), "mean": sum(srv) / len(srv) if srv else None},
            "wall_ms": {"p50": pct([r.extra["wall_ms"] for r in ok], .5), "p99": pct([r.extra["wall_ms"] for r in ok], .99)},
            "client_latency_ms": {"p50": pct([r.latency_ms for r in ok], .5), "p99": pct([r.latency_ms for r in ok], .99)},
            "errors": errors, **extra}


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="finder_scale")
    ap.add_argument("--duration", type=float, default=45)
    ap.add_argument("--warmup", type=float, default=8)
    ap.add_argument("--conc", default="1,8,32,64")
    ap.add_argument("--workloads", default=",".join(W))
    ap.add_argument("--tag", default=time.strftime("rt-%Y%m%d-%H%M%S"))
    ap.add_argument("--skip-timing", action="store_true")
    a = ap.parse_args()
    pin_cpus()
    out = ROOT / "results" / "runtime" / a.tag
    out.mkdir(parents=True, exist_ok=True)
    concs = [int(c) for c in a.conc.split(",")]
    pg, neo = PgExecutor(a.db, max(concs)), NeoExecutor(max(concs))
    await pg.start()
    await neo.start()
    anc = await anchors(pg)
    print("correctness gate", flush=True)
    corr = await correctness(pg, neo, anc)
    (out / "correctness.json").write_text(json.dumps(corr, indent=1, default=str))
    print("plan pass", flush=True)
    pl = await plans(pg, neo, anc)
    (out / "plans.json").write_text(json.dumps(pl, indent=1, default=str))
    if not a.skip_timing:
        cells = [(w, "pg", rt, c) for w in a.workloads.split(",") if W[w]["sql"] for rt in PG_RT for c in concs]
        cells += [(w, "neo", rt, c) for w in a.workloads.split(",") for rt in NEO_RT for c in concs]
        random.Random(1115).shuffle(cells)
        with (out / "cells.jsonl").open("a") as f:
            for k, (w, e, rt, c) in enumerate(cells):
                res = await cell(pg, neo, w, e, rt, c, anc[w], a.duration, a.warmup, a.tag)
                f.write(json.dumps(res, default=str) + "\n")
                f.flush()
                print(f"[{k + 1}/{len(cells)}] {w} {e}-{rt} c={c}: qps={res['throughput_qps']:.1f} "
                      f"server p50={res['server_ms']['p50']} p99={res['server_ms']['p99']} errors={res['errors']}", flush=True)
    await pg.close()
    await neo.close()


if __name__ == "__main__":
    asyncio.run(main())
