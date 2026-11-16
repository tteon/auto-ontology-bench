"""Tuning tracks: is each engine's worst workload slow by nature, or just untuned?

    uv run python -m load_test.tuning --db finder_s100k --tag tuning-s100k

For each workload, a baseline and tuned variants (data model / index / rewrite / planner setting).
Every variant must return the baseline's result on every anchor (strict compare) before it is timed.
Timing: concurrency 1 (3 anchors x 5 reps, median server ms) and a 20 s closed loop at concurrency 16.
Setup DDL time is recorded; tuned structures are left in place at the end (they only add).

  W2 global revenue by country
    neo-base       MetricDefinition -> FinancialMetric -> Company (3.5M db hits at 30k)
    neo-denorm     metric_code copied onto FinancialMetric + composite range index (metric_code, fiscal_year, fiscal_period)
    pg-base        3-way join, server default
    pg-cover       covering index (metric_id, fiscal_year, fiscal_period) INCLUDE (company_id, value) -> index-only scan
    pg-cover-jit   same with JIT on
  W3 co-holding fan-out
    neo-base / neo-rewrite (DISTINCT (institution, period) before the second expand)
    pg-base / pg-cover (index (institution_id, report_period) INCLUDE (company_id))
  W4a 6-hop reachability
    pg-base / pg-cover (index (customer_company_id) INCLUDE (supplier_company_id))
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

import psycopg  # noqa: E402

from benchmarks.verifier import compare  # noqa: E402
from enterprise.conn import DATABASE, driver  # noqa: E402
from gateway.executors import NeoExecutor, PgExecutor  # noqa: E402
from load_test.loadgen import pct, pin_cpus, run_load  # noqa: E402
from load_test.runtime_study import W, anchors, neo_params  # noqa: E402

NEO_SETUP = {
    "neo-denorm": [
        "CYPHER 25 MATCH (m:MetricDefinition)<-[:OF_METRIC]-(f:FinancialMetric) WHERE f.metric_code IS NULL "
        "CALL (f, m) { SET f.metric_code = m.metric_code } IN CONCURRENT TRANSACTIONS OF 10000 ROWS ON ERROR RETRY FOR 120 SECONDS THEN FAIL",
        "CREATE INDEX fm_code_year_period IF NOT EXISTS FOR (f:FinancialMetric) ON (f.metric_code, f.fiscal_year, f.fiscal_period)",
        "CALL db.awaitIndexes(1800)",
    ],
}
PG_SETUP = {
    "W2:pg-cover": ["CREATE INDEX IF NOT EXISTS fm_cover ON financial_metrics (metric_id, fiscal_year, fiscal_period) INCLUDE (company_id, value)"],
    "W3:pg-cover": ["CREATE INDEX IF NOT EXISTS ih_cover ON institutional_holdings (institution_id, report_period) INCLUDE (company_id)"],
    "W4a:pg-cover": ["CREATE INDEX IF NOT EXISTS scr_cover ON supply_chain_relations (customer_company_id) INCLUDE (supplier_company_id)"],
}

VARIANTS: dict[str, list[dict[str, Any]]] = {
    "W2": [
        {"name": "neo-base", "engine": "neo", "q": W["W2"]["cypher"], "runtime": "pipelined"},
        {"name": "neo-base-parallel", "engine": "neo", "q": W["W2"]["cypher"], "runtime": "parallel"},
        {"name": "neo-denorm", "engine": "neo", "runtime": "pipelined",
         "q": "MATCH (c:Company)-[:REPORTED]->(f:FinancialMetric {metric_code: 'revenue', fiscal_year: $y, fiscal_period: 'FY'}) "
              "RETURN c.country, sum(f.value)"},
        {"name": "neo-denorm-parallel", "engine": "neo", "runtime": "parallel",
         "q": "MATCH (c:Company)-[:REPORTED]->(f:FinancialMetric {metric_code: 'revenue', fiscal_year: $y, fiscal_period: 'FY'}) "
              "RETURN c.country, sum(f.value)"},
        {"name": "pg-base", "engine": "pg", "q": W["W2"]["sql"], "runtime": "single"},
        {"name": "pg-cover", "engine": "pg", "q": W["W2"]["sql"], "runtime": "single"},
        {"name": "pg-cover-jit", "engine": "pg", "q": W["W2"]["sql"], "runtime": "single", "pre": ["SET LOCAL jit = on"]},
    ],
    "W3": [
        {"name": "neo-base", "engine": "neo", "q": W["W3"]["cypher"], "runtime": "pipelined"},
        {"name": "neo-rewrite", "engine": "neo", "runtime": "pipelined",
         "q": "MATCH (c:Company {company_id: $c})<-[h1:HOLDS]-(i:Institution) WITH DISTINCT c, i, h1.report_period AS p "
              "MATCH (i)-[h2:HOLDS]->(o:Company) WHERE h2.report_period = p AND o <> c "
              "RETURN o.company_id, count(DISTINCT i) AS n ORDER BY n DESC, o.company_id LIMIT 20"},
        {"name": "pg-base", "engine": "pg", "q": W["W3"]["sql"], "runtime": "single"},
        {"name": "pg-cover", "engine": "pg", "q": W["W3"]["sql"], "runtime": "single"},
    ],
    "W4a": [
        {"name": "neo-base", "engine": "neo", "q": W["W4a"]["cypher"], "runtime": "pipelined"},
        {"name": "pg-base", "engine": "pg", "q": W["W4a"]["sql"], "runtime": "single"},
        {"name": "pg-cover", "engine": "pg", "q": W["W4a"]["sql"], "runtime": "single"},
    ],
}


async def run_one(v: dict, w: str, a: tuple, pg: PgExecutor, neo: NeoExecutor, meta: dict | None = None) -> dict:
    if v["engine"] == "neo":
        return await neo.run(v["q"], runtime=v["runtime"], params=neo_params(w, a), meta=meta or {"bench": "tuning"})
    return await pg.run(v["q"], runtime=v["runtime"], args=a, pre=v.get("pre", ()))


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="finder_s100k")
    ap.add_argument("--tag", default=time.strftime("tuning-%Y%m%d-%H%M%S"))
    a = ap.parse_args()
    pin_cpus()
    out_dir = ROOT / "results" / "tuning" / a.tag
    out_dir.mkdir(parents=True, exist_ok=True)
    pg, neo = PgExecutor(a.db, 16), NeoExecutor(16)
    await pg.start()
    await neo.start()
    anc = await anchors(pg)
    results = []
    for w, variants in VARIANTS.items():
        ws = anc[w][:3]
        base = {}
        for v in variants:
            setup_s = 0.0
            if v["engine"] == "neo" and v["name"].split("-parallel")[0] in NEO_SETUP:
                t = time.time()
                with driver() as d:
                    for stmt in NEO_SETUP[v["name"].split("-parallel")[0]]:
                        with d.session(database=DATABASE) as s:
                            s.run(stmt).consume()
                setup_s = time.time() - t
            if v["engine"] == "pg" and f"{w}:{v['name'].removesuffix('-jit')}" in PG_SETUP:
                t = time.time()
                with psycopg.connect(host="127.0.0.1", port=int(__import__("os").environ["BENCH_PG_PORT"]), user="bench",
                                     password=__import__("os").environ["BENCH_PG_PASSWORD"], dbname=a.db, autocommit=True) as c:
                    for stmt in PG_SETUP[f"{w}:{v['name'].removesuffix('-jit')}"]:
                        c.execute(stmt)
                    c.execute("VACUUM (ANALYZE) financial_metrics, institutional_holdings, supply_chain_relations")
                setup_s = time.time() - t
            # correctness against the engine-independent baseline (first variant's rows per anchor)
            ok = True
            for anchor in ws:
                r = await run_one(v, w, anchor, pg, neo)
                if not r.get("ok"):
                    ok = False
                    break
                if anchor not in base:
                    base[anchor] = r["rows"]
                elif not compare(base[anchor], r["rows"], W[w]["ordered"])["strict"]:
                    ok = False
            times = []
            if ok:
                for anchor in ws:
                    for _ in range(5):
                        r = await run_one(v, w, anchor, pg, neo)
                        times.append(r["server_ms"])

            async def req(anchor: tuple, rid: str, v: dict = v) -> dict:
                r = await run_one(v, w, anchor, pg, neo, {"bench": f"{a.tag}:{w}:{v['name']}", "rid": rid})
                return {"server_ms": r["server_ms"]} if r.get("ok") else {"error_type": r.get("error_type")}

            load = await run_load(req, anc[w], item_id=str, concurrency=16, duration_s=26, warmup_s=6) if ok else []
            meas = [r for r in load if not r.warmup and r.ok]
            span = (max(r.t_end for r in meas) - min(r.t_start for r in meas)) if meas else 0
            row = {"workload": w, "variant": v["name"], "engine": v["engine"], "runtime": v["runtime"], "equal_to_baseline": ok,
                   "setup_s": round(setup_s, 1), "c1_server_ms_median": pct(times, .5),
                   "c16_qps": len(meas) / span if span else None, "c16_server_ms_p50": pct([r.extra["server_ms"] for r in meas], .5),
                   "c16_server_ms_p99": pct([r.extra["server_ms"] for r in meas], .99)}
            if v["engine"] == "neo":
                p = await neo.run(v["q"], runtime=v["runtime"], params=neo_params(w, ws[0]), profile=True)
                pl = p.get("plan") or {}
                row.update(db_hits=pl.get("db_hits"), operators=pl.get("operator_types"))
            else:
                e = await pg.run("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + v["q"], runtime=v["runtime"], args=ws[0], pre=v.get("pre", ()))
                if e.get("ok"):
                    pj = json.loads(e["rows"][0][0])[0]
                    nodes: list[str] = []

                    def walk(n: dict) -> None:
                        nodes.append(n.get("Node Type"))
                        for ch in n.get("Plans", []):
                            walk(ch)

                    walk(pj["Plan"])
                    row.update(shared_hit=pj["Plan"].get("Shared Hit Blocks"), operators=sorted(set(nodes)),
                               jit=bool(pj.get("JIT")))
            results.append(row)
            print(f"{w} {v['name']:22} equal={ok} setup={row['setup_s']}s c1={row['c1_server_ms_median']} "
                  f"c16 qps={row['c16_qps']} p99={row['c16_server_ms_p99']} hits/buffers={row.get('db_hits', row.get('shared_hit'))}", flush=True)
            (out_dir / "variants.json").write_text(json.dumps(results, indent=1, default=str))
    await pg.close()
    await neo.close()


if __name__ == "__main__":
    asyncio.run(main())
