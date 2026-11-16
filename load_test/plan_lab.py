"""Plan lab: the same light and heavy queries on every runtime, before and after tuning, with plans and E2E latency.

    uv run python -m load_test.plan_lab --db finder_real  --tag plan-real
    uv run python -m load_test.plan_lab --db finder_scale --tag plan-s30k

For every (state, query, engine-runtime) cell:
  1. EXPLAIN   -> estimated plan tree, requested vs actual runtime (Neo4j) / estimated plan (Postgres)
  2. PROFILE   -> actual rows, db hits, page-cache hits/misses, memory per operator (Neo4j) /
                  EXPLAIN (ANALYZE, BUFFERS) actual rows, time, buffers, workers (Postgres) -- separate pass, never timed
  3. c=1 timed runs (no PROFILE): server time, client E2E time (session + request + network + decoding), overhead = E2E - server
  4. c=8 closed loop: QPS, server and E2E p50/p99
Plan trees are written to plans/*.txt; metrics to cells.jsonl. Network baseline (`RETURN 1` / `SELECT 1`) is recorded so
E2E can be split into server + network + driver.

States
  before: keys and plain indexes only. Neo4j property existence/type constraints are dropped; no denormalized metric code.
          Postgres: foreign-key indexes only.
  after:  Neo4j existence/type constraints back, FinancialMetric.metric_code denormalized + composite range index
          (metric_code, fiscal_year, fiscal_period), H1/H5 rewritten to use it. Postgres: covering indexes for H1/H2/L4.
At the end the database is returned to the loader's default state (constraints present, tuning removed).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

import psycopg  # noqa: E402

from benchmarks.verifier import compare  # noqa: E402
from neo4j import Query  # noqa: E402

from enterprise.conn import DATABASE, driver  # noqa: E402
from gateway.executors import NeoExecutor, PgExecutor  # noqa: E402
from load_test.loadgen import pct, pin_cpus, run_load  # noqa: E402

# ----------------------------------------------------------------------------- queries
Q: dict[str, dict[str, Any]] = {
    "L1 point lookup": {
        "kind": "light",
        "cypher": ("MATCH (c:Company {company_id: $c})-[:REPORTED]->(f:FinancialMetric {fiscal_year: $y, fiscal_period: 'FY'})"
                   "-[:OF_METRIC]->(:MetricDefinition {metric_id: $m}) RETURN f.value"),
        "sql": "SELECT f.value FROM financial_metrics f WHERE f.company_id = $1 AND f.metric_id = $3 AND f.fiscal_year = $2 AND f.fiscal_period = 'FY'",
        "params": "metric_anchor",
    },
    "L2 anchored 2-hop": {
        "kind": "light",
        "cypher": ("MATCH (s:Company)-[:SUPPLIES]->(:Company {company_id: $c}) WITH DISTINCT s "
                   "OPTIONAL MATCH (s)-[:PARTY_TO]->(l:LegalProceeding) RETURN s.name, count(l) ORDER BY s.name"),
        "sql": ("SELECT s.name, count(l.legal_proceeding_id) FROM (SELECT DISTINCT supplier_company_id FROM supply_chain_relations "
                "WHERE customer_company_id = $1) u JOIN companies s ON s.company_id = u.supplier_company_id "
                "LEFT JOIN legal_proceedings l ON l.company_id = s.company_id GROUP BY s.company_id, s.name ORDER BY s.name"),
        "params": "customer_anchor", "ordered": True,
    },
    "L3 index-ordered top-10": {
        "kind": "light",
        "cypher": "MATCH (c:Company) WHERE c.name >= $p RETURN c.name ORDER BY c.name LIMIT 10",
        "sql": "SELECT name FROM companies WHERE name >= $1 ORDER BY name LIMIT 10",
        "params": "name_prefix", "ordered": True,
    },
    "L4 6-hop reachability": {
        "kind": "light",
        "cypher": "MATCH (s:Company)-[:SUPPLIES*1..6]->(c:Company {company_id: $c}) WHERE s <> c RETURN count(DISTINCT s)",
        "sql": ("WITH RECURSIVE up(s, d) AS (SELECT supplier_company_id, 1 FROM supply_chain_relations WHERE customer_company_id = $1 "
                "UNION SELECT r.supplier_company_id, up.d + 1 FROM up JOIN supply_chain_relations r ON r.customer_company_id = up.s "
                "WHERE up.d < 6) SELECT count(DISTINCT s) FROM up WHERE s <> $1"),
        "params": "customer_anchor",
    },
    "H1 global revenue by country": {
        "kind": "heavy",
        "cypher": ("MATCH (:MetricDefinition {metric_code: 'revenue'})<-[:OF_METRIC]-(f:FinancialMetric {fiscal_year: $y, fiscal_period: 'FY'})"
                   "<-[:REPORTED]-(c:Company) RETURN c.country, sum(f.value)"),
        "cypher_after": ("MATCH (c:Company)-[:REPORTED]->(f:FinancialMetric {metric_code: 'revenue', fiscal_year: $y, fiscal_period: 'FY'}) "
                         "RETURN c.country, sum(f.value)"),
        "sql": ("SELECT c.country, sum(f.value) FROM financial_metrics f JOIN metric_definitions m ON m.metric_id = f.metric_id "
                "AND m.metric_code = 'revenue' JOIN companies c ON c.company_id = f.company_id "
                "WHERE f.fiscal_year = $1 AND f.fiscal_period = 'FY' GROUP BY c.country"),
        "params": "year",
    },
    "H2 3-hop co-holding fan-out": {
        "kind": "heavy",
        "cypher": ("MATCH (c:Company {company_id: $c})<-[h1:HOLDS]-(i:Institution)-[h2:HOLDS]->(o:Company) "
                   "WHERE h2.report_period = h1.report_period AND o <> c "
                   "RETURN o.company_id, count(DISTINCT i) AS n ORDER BY n DESC, o.company_id LIMIT 20"),
        "sql": ("SELECT h2.company_id, count(DISTINCT h1.institution_id) AS n FROM institutional_holdings h1 "
                "JOIN institutional_holdings h2 ON h2.institution_id = h1.institution_id AND h2.report_period = h1.report_period "
                "AND h2.company_id <> h1.company_id WHERE h1.company_id = $1 GROUP BY h2.company_id ORDER BY n DESC, h2.company_id LIMIT 20"),
        "params": "held_anchor", "ordered": True,
    },
    "H3 downstream paths (6 hops)": {
        "kind": "heavy",
        "cypher": "MATCH p = (:Company {company_id: $c})-[:SUPPLIES*1..6]->(:Company) RETURN count(p)",
        "sql": ("WITH RECURSIVE dn(s, d, path) AS (SELECT customer_company_id, 1, ARRAY[$1::int, customer_company_id] "
                "FROM supply_chain_relations WHERE supplier_company_id = $1 UNION ALL SELECT r.customer_company_id, dn.d + 1, "
                "dn.path || r.customer_company_id FROM dn JOIN supply_chain_relations r ON r.supplier_company_id = dn.s "
                "WHERE dn.d < 6 AND NOT r.customer_company_id = ANY(dn.path)) SELECT count(*) FROM dn"),
        "params": "hub_anchor", "compare": False,  # trail (Cypher) vs node-unique path (SQL) semantics differ on cycles
    },
    "H4 global count IS NOT NULL": {
        "kind": "heavy",
        "cypher": "MATCH (f:FinancialMetric) WHERE f.value IS NOT NULL RETURN count(*)",
        "sql": "SELECT count(*) FROM financial_metrics WHERE value IS NOT NULL",
        "params": "none",
    },
    "H5 global top-10 revenue": {
        "kind": "heavy",
        "cypher": ("MATCH (:MetricDefinition {metric_code: 'revenue'})<-[:OF_METRIC]-(f:FinancialMetric {fiscal_year: $y, fiscal_period: 'FY'})"
                   "<-[:REPORTED]-(c:Company) RETURN c.name, f.value ORDER BY f.value DESC, c.name LIMIT 10"),
        "cypher_after": ("MATCH (c:Company)-[:REPORTED]->(f:FinancialMetric {metric_code: 'revenue', fiscal_year: $y, fiscal_period: 'FY'}) "
                         "RETURN c.name, f.value ORDER BY f.value DESC, c.name LIMIT 10"),
        "sql": ("SELECT c.name, f.value FROM financial_metrics f JOIN metric_definitions m ON m.metric_id = f.metric_id "
                "AND m.metric_code = 'revenue' JOIN companies c ON c.company_id = f.company_id "
                "WHERE f.fiscal_year = $1 AND f.fiscal_period = 'FY' ORDER BY f.value DESC, c.name LIMIT 10"),
        "params": "year", "ordered": True,
    },
}
NEO_RT = ("slotted", "pipelined", "parallel")
SYNC: dict[str, Any] = {}  # shared sync driver for EXPLAIN / PROFILE
PG_RT = ("single", "parallel")

NEO_BEFORE = [  # drop property existence/type constraints (keys stay)
    "DROP CONSTRAINT fm_value_exists IF EXISTS", "DROP CONSTRAINT fm_value_type IF EXISTS",
    "DROP CONSTRAINT fm_year_exists IF EXISTS", "DROP CONSTRAINT fm_year_type IF EXISTS",
    "DROP CONSTRAINT company_name_exists IF EXISTS", "DROP CONSTRAINT company_filer_type IF EXISTS",
    "DROP INDEX fm_code_year_period IF EXISTS",
]
NEO_AFTER = [
    "CREATE CONSTRAINT fm_value_exists IF NOT EXISTS FOR (f:FinancialMetric) REQUIRE f.value IS NOT NULL",
    "CREATE CONSTRAINT fm_value_type IF NOT EXISTS FOR (f:FinancialMetric) REQUIRE f.value IS :: FLOAT",
    "CREATE CONSTRAINT fm_year_exists IF NOT EXISTS FOR (f:FinancialMetric) REQUIRE f.fiscal_year IS NOT NULL",
    "CREATE CONSTRAINT fm_year_type IF NOT EXISTS FOR (f:FinancialMetric) REQUIRE f.fiscal_year IS :: INTEGER",
    "CREATE CONSTRAINT company_name_exists IF NOT EXISTS FOR (c:Company) REQUIRE c.name IS NOT NULL",
    "CREATE CONSTRAINT company_filer_type IF NOT EXISTS FOR (c:Company) REQUIRE c.is_sec_filer IS :: BOOLEAN",
    ("CYPHER 25 MATCH (m:MetricDefinition)<-[:OF_METRIC]-(f:FinancialMetric) WHERE f.metric_code IS NULL "
     "CALL (f, m) { SET f.metric_code = m.metric_code } IN CONCURRENT TRANSACTIONS OF 10000 ROWS ON ERROR RETRY FOR 120 SECONDS THEN FAIL"),
    "CREATE INDEX fm_code_year_period IF NOT EXISTS FOR (f:FinancialMetric) ON (f.metric_code, f.fiscal_year, f.fiscal_period)",
    "CALL db.awaitIndexes(1800)",
]
NEO_RESET = [  # loader default: constraints present, tuning removed
    *NEO_AFTER[:6],
    "DROP INDEX fm_code_year_period IF EXISTS",
    ("CYPHER 25 MATCH (f:FinancialMetric) WHERE f.metric_code IS NOT NULL "
     "CALL (f) { REMOVE f.metric_code } IN CONCURRENT TRANSACTIONS OF 10000 ROWS ON ERROR RETRY FOR 120 SECONDS THEN FAIL"),
]
PG_AFTER = [
    "CREATE INDEX IF NOT EXISTS fm_cover ON financial_metrics (metric_id, fiscal_year, fiscal_period) INCLUDE (company_id, value)",
    "CREATE INDEX IF NOT EXISTS ih_cover ON institutional_holdings (institution_id, report_period) INCLUDE (company_id)",
    "CREATE INDEX IF NOT EXISTS scr_cover ON supply_chain_relations (customer_company_id) INCLUDE (supplier_company_id)",
]
PG_BEFORE = ["DROP INDEX IF EXISTS fm_cover", "DROP INDEX IF EXISTS ih_cover", "DROP INDEX IF EXISTS scr_cover"]


# ----------------------------------------------------------------------------- plan rendering
def render_neo(plan: dict, profile: bool) -> str:
    lines: list[str] = []

    def walk(p: dict, depth: int) -> None:
        a = p.get("args", {})
        bits = [f"est {a.get('EstimatedRows', 0):,.0f}"]
        if profile:
            bits += [f"rows {p.get('rows', 0):,}", f"hits {p.get('dbHits', 0):,}",
                     f"pc {p.get('pageCacheHits', 0):,}/{p.get('pageCacheMisses', 0):,}"]
            if a.get("Memory"):
                bits.append(f"mem {a['Memory']:,}B")
            if p.get("time"):
                bits.append(f"time {p['time'] / 1e6:,.1f}ms")
        if a.get("PipelineInfo"):
            bits.append(a["PipelineInfo"])
        det = (a.get("Details") or "")[:90]
        lines.append(f"{'  ' * depth}+{p.get('operatorType', '?').split('@')[0]}  {det}  | " + " | ".join(bits))
        for c in p.get("children", []):
            walk(c, depth + 1)

    a = plan.get("args", {})
    lines.append(f"runtime={a.get('runtime')} impl={a.get('runtime-impl')} planner={a.get('planner')} "
                 f"workers={a.get('AvailableWorkers', '-')} globalMemory={a.get('GlobalMemory', '-')}")
    walk(plan, 0)
    return "\n".join(lines)


def render_pg(plan: dict, analyze: bool) -> str:
    lines: list[str] = []

    def walk(n: dict, depth: int) -> None:
        bits = [f"est {n.get('Plan Rows', 0):,}", f"cost {n.get('Total Cost', 0):,.0f}"]
        if analyze:
            bits += [f"rows {n.get('Actual Rows', 0):,} x{n.get('Actual Loops', 1)}", f"time {n.get('Actual Total Time', 0):,.2f}ms",
                     f"hit {n.get('Shared Hit Blocks', 0):,} read {n.get('Shared Read Blocks', 0):,}"]
            if "Workers Launched" in n:
                bits.append(f"workers {n['Workers Launched']}/{n.get('Workers Planned')}")
        name = n.get("Node Type", "?") + (f" ({n['Join Type']})" if n.get("Join Type") else "") + \
            (f" on {n['Relation Name']}" if n.get("Relation Name") else "") + (f" using {n['Index Name']}" if n.get("Index Name") else "")
        lines.append(f"{'  ' * depth}-> {name}  | " + " | ".join(bits))
        for c in n.get("Plans", []):
            walk(c, depth + 1)

    walk(plan["Plan"], 0)
    if analyze:
        lines.append(f"planning {plan.get('Planning Time', 0):.2f} ms, execution {plan.get('Execution Time', 0):.2f} ms"
                     + (f", JIT {plan['JIT']['Timing']['Total']:.1f} ms" if plan.get("JIT") else ""))
    return "\n".join(lines)


def neo_totals(p: dict) -> dict[str, Any]:
    tot = {"db_hits": 0, "pc_hits": 0, "pc_misses": 0, "operators": []}

    def walk(n: dict) -> None:
        tot["db_hits"] += n.get("dbHits", 0) or 0
        tot["pc_hits"] += n.get("pageCacheHits", 0) or 0
        tot["pc_misses"] += n.get("pageCacheMisses", 0) or 0
        tot["operators"].append(n.get("operatorType", "?").split("@")[0])
        for c in n.get("children", []):
            walk(c)

    walk(p)
    a = p.get("args", {})
    fused = 0

    def count_fused(n: dict) -> None:
        nonlocal fused
        fused += (n.get("args", {}).get("PipelineInfo") or "").startswith("Fused")
        for c in n.get("children", []):
            count_fused(c)

    count_fused(p)
    tot.update(actual_runtime=a.get("runtime"), global_memory=a.get("GlobalMemory"), est_root=a.get("EstimatedRows"),
               rows_root=p.get("rows"), fused_operators=fused, operator_count=len(tot["operators"]))
    return tot


# ----------------------------------------------------------------------------- anchors and parameters
async def anchors(pg: PgExecutor) -> dict[str, list]:
    async def rows(sql: str) -> list:
        return (await pg.run(sql))["rows"]

    year = (await rows("SELECT fiscal_year FROM financial_metrics WHERE fiscal_period = 'FY' GROUP BY 1 ORDER BY count(*) DESC, 1 DESC LIMIT 1"))[0][0]
    return {
        "metric_anchor": [tuple(r) for r in await rows(
            "SELECT company_id, fiscal_year, metric_id FROM financial_metrics WHERE fiscal_period = 'FY' ORDER BY company_id DESC, metric_id, fiscal_year LIMIT 3")],
        "customer_anchor": [tuple(r) for r in await rows(
            "SELECT customer_company_id FROM supply_chain_relations GROUP BY 1 ORDER BY count(*) DESC, 1 LIMIT 3")],
        "hub_anchor": [tuple(r) for r in await rows(
            "SELECT supplier_company_id FROM supply_chain_relations GROUP BY 1 ORDER BY count(*) DESC, 1 LIMIT 3")],
        "held_anchor": [tuple(r) for r in await rows(
            "SELECT company_id FROM institutional_holdings GROUP BY 1 ORDER BY count(*) DESC, 1 LIMIT 3")],
        "name_prefix": [("M",), ("S",), ("T",)],
        "year": [(year,)],
        "none": [()],
    }


def neo_params(kind: str, a: tuple) -> dict[str, Any]:
    return {"metric_anchor": lambda: {"c": a[0], "y": a[1], "m": a[2]}, "customer_anchor": lambda: {"c": a[0]},
            "hub_anchor": lambda: {"c": a[0]}, "held_anchor": lambda: {"c": a[0]}, "name_prefix": lambda: {"p": a[0]},
            "year": lambda: {"y": a[0]}, "none": lambda: {}}[kind]()


# ----------------------------------------------------------------------------- measurement
async def timed(fn, reps_max: int = 10, budget_s: float = 20.0) -> list[dict]:
    out, t0 = [], time.monotonic()
    while len(out) < reps_max and (len(out) < 3 or time.monotonic() - t0 < budget_s):
        out.append(await fn())
        if not out[-1].get("ok"):
            break
    return out


async def cell(state: str, qname: str, engine: str, rt: str, pg: PgExecutor, neo: NeoExecutor, anc: dict,
               plans_dir: Path, tag: str, conc: int) -> dict[str, Any]:
    q = Q[qname]
    cy = q.get("cypher_after", q["cypher"]) if state == "after" else q["cypher"]
    args_list = anc[q["params"]]
    a0 = args_list[0]
    slug = f"{state}_{qname.split()[0]}_{engine}-{rt}"
    row: dict[str, Any] = {"state": state, "query": qname, "kind": q["kind"], "engine": engine, "runtime": rt,
                           "query_text": cy if engine == "neo" else q["sql"]}
    if engine == "neo":
        params = neo_params(q["params"], a0)
        try:
            _, s_ex, _ = SYNC["d"].execute_query(f"CYPHER runtime={rt} EXPLAIN {cy}", params, database_=DATABASE)
            explain_txt = render_neo(s_ex.plan, profile=False) if s_ex.plan else "(no plan)"
        except Exception as e:  # noqa: BLE001
            explain_txt = f"(explain failed: {type(e).__name__}: {str(e)[:200]})"
        try:
            with SYNC["d"].session(database=DATABASE, default_access_mode="READ") as sess:
                res = sess.run(Query(f"CYPHER runtime={rt} PROFILE {cy}", timeout=60), params)
                list(res)
                s_pr = res.consume()
            profile_txt = render_neo(s_pr.profile, profile=True)
            row["profile"] = neo_totals(s_pr.profile)
        except Exception as e:  # noqa: BLE001
            profile_txt, row["profile"] = f"(profile failed: {type(e).__name__}: {str(e)[:200]})", {}

        async def once(a: tuple = a0) -> dict:
            t = time.perf_counter()
            r = await neo.run(cy, runtime=rt, params=neo_params(q["params"], a), meta={"bench": tag})
            r["e2e_ms"] = (time.perf_counter() - t) * 1000
            return r
    else:
        e = await pg.run("EXPLAIN (FORMAT JSON) " + q["sql"], runtime=rt, args=a0)
        explain_txt = render_pg(json.loads(e["rows"][0][0])[0], analyze=False) if e.get("ok") else f"(explain failed: {e.get('error')})"
        e = await pg.run("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + q["sql"], runtime=rt, args=a0)
        if e.get("ok"):
            pj = json.loads(e["rows"][0][0])[0]
            profile_txt = render_pg(pj, analyze=True)
            row["profile"] = {"exec_ms": pj.get("Execution Time"), "planning_ms": pj.get("Planning Time"),
                              "shared_hit": pj["Plan"].get("Shared Hit Blocks"), "shared_read": pj["Plan"].get("Shared Read Blocks"),
                              "rows_root": pj["Plan"].get("Actual Rows"), "est_root": pj["Plan"].get("Plan Rows")}
        else:
            profile_txt, row["profile"] = f"(analyze failed: {e.get('error')})", {}

        async def once(a: tuple = a0) -> dict:
            t = time.perf_counter()
            r = await pg.run(q["sql"], runtime=rt, args=a)
            r["e2e_ms"] = (time.perf_counter() - t) * 1000
            return r
    (plans_dir / f"{slug}_explain.txt").write_text(row["query_text"] + "\n\n" + explain_txt + "\n")
    (plans_dir / f"{slug}_profile.txt").write_text(row["query_text"] + "\n\n" + profile_txt + "\n")
    row["explain_txt"], row["profile_txt"] = explain_txt, profile_txt

    runs = await timed(once)
    ok = [r for r in runs if r.get("ok")]
    row["c1"] = {"n": len(runs), "ok": len(ok), "error": None if len(ok) == len(runs) else runs[-1].get("error_type"),
                 "server_p50": pct([r["server_ms"] for r in ok], .5), "e2e_p50": pct([r["e2e_ms"] for r in ok], .5),
                 "e2e_p90": pct([r["e2e_ms"] for r in ok], .9), "rows": ok[0]["row_count"] if ok else None}
    if ok:
        row["c1"]["overhead_p50"] = row["c1"]["e2e_p50"] - row["c1"]["server_p50"]
        row["_rows"] = ok[0]["rows"]

    async def req(a: tuple, rid: str) -> dict:
        r = await once(a)
        return {"server_ms": r["server_ms"], "e2e_ms": r["e2e_ms"]} if r.get("ok") else {"error_type": r.get("error_type")}

    if ok:
        load = await run_load(req, args_list, item_id=str, concurrency=conc, duration_s=15, warmup_s=3)
        meas = [r for r in load if not r.warmup]
        good = [r for r in meas if r.ok]
        span = (max(r.t_end for r in meas) - min(r.t_start for r in meas)) if meas else 0
        errs: dict[str, int] = {}
        for r in meas:
            if not r.ok:
                errs[r.error_type or "?"] = errs.get(r.error_type or "?", 0) + 1
        row[f"c{conc}"] = {"qps": len(good) / span if span else 0, "errors": errs,
                           "server_p50": pct([r.extra["server_ms"] for r in good], .5), "server_p99": pct([r.extra["server_ms"] for r in good], .99),
                           "e2e_p50": pct([r.extra["e2e_ms"] for r in good], .5), "e2e_p99": pct([r.extra["e2e_ms"] for r in good], .99)}
    return row


def apply_state(state: str, db: str) -> dict[str, float]:
    t = {}
    t0 = time.time()
    with driver() as d:
        for stmt in (NEO_BEFORE if state == "before" else NEO_AFTER):
            with d.session(database=DATABASE) as s:
                s.run(stmt).consume()
    t["neo_s"] = round(time.time() - t0, 1)
    t0 = time.time()
    with psycopg.connect(host="127.0.0.1", port=int(os.environ["BENCH_PG_PORT"]), user="bench",
                         password=os.environ["BENCH_PG_PASSWORD"], dbname=db, autocommit=True) as c:
        for stmt in (PG_BEFORE if state == "before" else PG_AFTER):
            c.execute(stmt)
        c.execute("VACUUM (ANALYZE) financial_metrics, institutional_holdings, supply_chain_relations, companies")
    t["pg_s"] = round(time.time() - t0, 1)
    return t


def reset(db: str) -> None:
    with driver() as d:
        for stmt in NEO_RESET:
            with d.session(database=DATABASE) as s:
                s.run(stmt).consume()
    with psycopg.connect(host="127.0.0.1", port=int(os.environ["BENCH_PG_PORT"]), user="bench",
                         password=os.environ["BENCH_PG_PASSWORD"], dbname=db, autocommit=True) as c:
        for stmt in PG_BEFORE:
            c.execute(stmt)


async def network_baseline(pg: PgExecutor, neo: NeoExecutor) -> dict[str, float]:
    ne, pe = [], []
    for _ in range(15):
        t = time.perf_counter()
        r = await neo.run("RETURN 1")
        ne.append(((time.perf_counter() - t) * 1000, r["server_ms"]))
        t = time.perf_counter()
        await pg.run("SELECT 1")
        pe.append((time.perf_counter() - t) * 1000)
    return {"neo_e2e_ms": pct([x[0] for x in ne], .5), "neo_server_ms": pct([x[1] for x in ne], .5), "pg_e2e_ms": pct(pe, .5)}


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="finder_real")
    ap.add_argument("--tag", default=time.strftime("plan-%Y%m%d-%H%M%S"))
    ap.add_argument("--conc", type=int, default=8)
    ap.add_argument("--queries", default=",".join(Q))
    a = ap.parse_args()
    pin_cpus()
    out = ROOT / "results" / "plan_lab" / a.tag
    (out / "plans").mkdir(parents=True, exist_ok=True)
    pg, neo = PgExecutor(a.db, a.conc + 2), NeoExecutor(a.conc + 2)
    SYNC["d"] = driver()
    await pg.start()
    await neo.start()
    anc = await anchors(pg)
    meta = {"db": a.db, "anchors": {k: [list(x) for x in v] for k, v in anc.items()}, "network": await network_baseline(pg, neo)}
    print("network baseline:", meta["network"], flush=True)
    rows: list[dict] = []
    queries = [q for q in Q if q in a.queries.split(",")]
    for state in ("before", "after"):
        meta[f"setup_{state}"] = apply_state(state, a.db)
        print(f"=== state {state} (setup {meta[f'setup_{state}']})", flush=True)
        for qname in queries:
            ref = None
            for engine, rts in (("pg", PG_RT), ("neo", NEO_RT)):
                for rt in rts:
                    r = await cell(state, qname, engine, rt, pg, neo, anc, out / "plans", a.tag, a.conc)
                    got = r.pop("_rows", None)
                    if got is not None and Q[qname].get("compare", True):
                        ref = got if ref is None else ref
                        r["equal_to_reference"] = compare(ref, got, Q[qname].get("ordered", False))["strict"]
                    rows.append(r)
                    c1, cc = r["c1"], r.get(f"c{a.conc}", {})
                    print(f"  {qname:30} {engine}-{rt:9} srv={c1['server_p50']} e2e={c1['e2e_p50']} "
                          f"c{a.conc} qps={cc.get('qps', 0):.1f} e2e_p99={cc.get('e2e_p99')} eq={r.get('equal_to_reference')} "
                          f"err={c1['error'] or cc.get('errors') or ''}", flush=True)
                    (out / "cells.jsonl").write_text("\n".join(json.dumps(x, default=str) for x in rows) + "\n")
    reset(a.db)
    meta["reset"] = "loader default restored (constraints present, metric_code removed, covering indexes dropped)"
    (out / "meta.json").write_text(json.dumps(meta, indent=1, default=str))
    await pg.close()
    await neo.close()
    SYNC["d"].close()


if __name__ == "__main__":
    asyncio.run(main())
