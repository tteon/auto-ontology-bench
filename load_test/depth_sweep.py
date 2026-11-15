"""Hop-depth sweep: how traversal cost grows with depth k on each engine.

    uv run python -m load_test.depth_sweep --db finder_scale --kmax 8 --anchors 3 --reps 3 --tag depth-s30k

Variants (all upstream of an anchor company over SUPPLIES, depth 1..k):
  neo-distinct   count(DISTINCT s) over [:SUPPLIES*1..k]      -> planner can use VarLengthExpand(Pruning,BFS)
  neo-paths      count(p) over the same pattern                -> must enumerate every trail
  neo-qpp        quantified path pattern {1,k} + DISTINCT     -> Cypher 5 QPP planning
  neo-shortest   SHORTEST 1 path to each upstream node         -> shortest-path planner
  pg-union       recursive CTE, UNION on (node, depth)         -> dedupes per level (BFS-like)
  pg-unionall    recursive CTE, UNION ALL + cycle check on path-> enumerates paths (comparable to neo-paths)
Neo4j variants run on pipelined and parallel; Postgres on single (recursive CTEs are not parallel-safe).
Each cell: anchors x reps at concurrency 1, server time; then one PROFILE / EXPLAIN (ANALYZE, BUFFERS)
for db hits and buffers. A variant stops growing k after its first timeout (30 s).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

from gateway.executors import NeoExecutor, PgExecutor  # noqa: E402
from load_test.loadgen import pct, pin_cpus  # noqa: E402

NEO = {
    "neo-distinct": "MATCH (s:Company)-[:SUPPLIES*1..{k}]->(c:Company {{company_id: $c}}) WHERE s <> c RETURN count(DISTINCT s)",
    "neo-paths": "MATCH p = (:Company)-[:SUPPLIES*1..{k}]->(:Company {{company_id: $c}}) RETURN count(p)",
    "neo-qpp": "MATCH (s:Company) (()-[:SUPPLIES]->()){{1,{k}}} (c:Company {{company_id: $c}}) WHERE s <> c RETURN count(DISTINCT s)",
    "neo-shortest": (
        "MATCH p = SHORTEST 1 (s:Company)-[:SUPPLIES]->{{1,{k}}}(c:Company {{company_id: $c}}) WHERE s <> c "
        "RETURN count(DISTINCT s)"
    ),
}
# Downstream from a hub supplier (risk propagation: who is ultimately exposed to supplier X). Hubs supply hundreds
# of customers, so this is where path enumeration explodes and dedup strategy decides the cost.
NEO_DOWN = {
    "neo-down-distinct": "MATCH (h:Company {{company_id: $c}})-[:SUPPLIES*1..{k}]->(x:Company) WHERE x <> h RETURN count(DISTINCT x)",
    "neo-down-paths": "MATCH p = (:Company {{company_id: $c}})-[:SUPPLIES*1..{k}]->(:Company) RETURN count(p)",
}
PG_DOWN = {
    "pg-down-union": (
        "WITH RECURSIVE dn(s, d) AS (SELECT customer_company_id, 1 FROM supply_chain_relations WHERE supplier_company_id = $1 "
        "UNION SELECT r.customer_company_id, dn.d + 1 FROM dn JOIN supply_chain_relations r ON r.supplier_company_id = dn.s "
        "WHERE dn.d < {k}) SELECT count(DISTINCT s) FROM dn WHERE s <> $1"
    ),
    "pg-down-unionall": (
        "WITH RECURSIVE dn(s, d, path) AS (SELECT customer_company_id, 1, ARRAY[$1::int, customer_company_id] "
        "FROM supply_chain_relations WHERE supplier_company_id = $1 "
        "UNION ALL SELECT r.customer_company_id, dn.d + 1, dn.path || r.customer_company_id FROM dn "
        "JOIN supply_chain_relations r ON r.supplier_company_id = dn.s "
        "WHERE dn.d < {k} AND NOT r.customer_company_id = ANY(dn.path)) SELECT count(*) FROM dn"
    ),
}
PG = {
    "pg-union": (
        "WITH RECURSIVE up(s, d) AS (SELECT supplier_company_id, 1 FROM supply_chain_relations WHERE customer_company_id = $1 "
        "UNION SELECT r.supplier_company_id, up.d + 1 FROM up JOIN supply_chain_relations r ON r.customer_company_id = up.s "
        "WHERE up.d < {k}) SELECT count(DISTINCT s) FROM up WHERE s <> $1"
    ),
    "pg-unionall": (
        "WITH RECURSIVE up(s, d, path) AS (SELECT supplier_company_id, 1, ARRAY[$1::int, supplier_company_id] "
        "FROM supply_chain_relations WHERE customer_company_id = $1 "
        "UNION ALL SELECT r.supplier_company_id, up.d + 1, up.path || r.supplier_company_id FROM up "
        "JOIN supply_chain_relations r ON r.customer_company_id = up.s "
        "WHERE up.d < {k} AND NOT r.supplier_company_id = ANY(up.path)) SELECT count(*) FROM up"
    ),
}


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="finder_scale")
    ap.add_argument("--kmax", type=int, default=8)
    ap.add_argument("--anchors", type=int, default=3)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--tag", default=time.strftime("depth-%Y%m%d-%H%M%S"))
    ap.add_argument("--direction", choices=["up", "down", "both"], default="both")
    a = ap.parse_args()
    pin_cpus()
    out = ROOT / "results" / "depth" / a.tag
    out.mkdir(parents=True, exist_ok=True)
    pg, neo = PgExecutor(a.db, 2), NeoExecutor(2)
    await pg.start()
    await neo.start()
    oems = [
        r[0]
        for r in (
            await pg.run(
                "SELECT company_id FROM companies WHERE name LIKE 'SynOEM%' ORDER BY company_id LIMIT $1", args=(a.anchors,)
            )
        )["rows"]
    ]
    hubs = [
        r[0]
        for r in (
            await pg.run(
                "SELECT supplier_company_id FROM supply_chain_relations GROUP BY 1 ORDER BY count(*) DESC, 1 LIMIT $1",
                args=(a.anchors,),
            )
        )["rows"]
    ]
    cells, dead = [], set()
    for k in range(1, a.kmax + 1):
        for neo_map, pg_map, anchors in ((NEO, PG, oems), (NEO_DOWN, PG_DOWN, hubs)):
            if a.direction == "up" and neo_map is NEO_DOWN or a.direction == "down" and neo_map is NEO:
                continue
            for name, tmpl in neo_map.items():
                for rt in ("pipelined", "parallel"):
                    key = f"{name}/{rt}"
                    if key in dead:
                        continue
                    times, rows, errs = [], None, []
                    for anc in anchors:
                        for _ in range(a.reps):
                            r = await neo.run(tmpl.format(k=k), runtime=rt, params={"c": anc}, meta={"bench": a.tag})
                            if r.get("ok"):
                                times.append(r["server_ms"])
                                rows = r["rows"][0][0] if anc == anchors[0] else rows
                            else:
                                errs.append(r.get("error_type"))
                                break
                        if errs:
                            break
                    prof = await neo.run(tmpl.format(k=k), runtime=rt, params={"c": anchors[0]}, profile=True) if not errs else {}
                    pl = prof.get("plan") or {}
                    cell = {
                        "k": k,
                        "variant": name,
                        "runtime": rt,
                        "server_ms_p50": pct(times, 0.5),
                        "server_ms_max": max(times, default=None),
                        "result_anchor0": rows,
                        "errors": errs,
                        "db_hits": pl.get("db_hits"),
                        "actual_runtime": pl.get("actual_runtime"),
                        "operators": pl.get("operator_types"),
                        "global_memory_bytes": pl.get("global_memory_bytes"),
                    }
                    if errs:
                        dead.add(key)
                    cells.append(cell)
                    print(
                        f"k={k} {key:24} p50={cell['server_ms_p50']} rows={rows} hits={cell['db_hits']} {errs or ''}", flush=True
                    )
            for name, tmpl in pg_map.items():
                key = f"{name}/single"
                if key in dead:
                    continue
                times, rows, errs = [], None, []
                for anc in anchors:
                    for _ in range(a.reps):
                        r = await pg.run(tmpl.format(k=k), runtime="single", args=(anc,))
                        if r.get("ok"):
                            times.append(r["server_ms"])
                            rows = r["rows"][0][0] if anc == anchors[0] else rows
                        else:
                            errs.append(r.get("error_type"))
                            break
                    if errs:
                        break
                ex = {}
                if not errs:
                    e = await pg.run(
                        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + tmpl.format(k=k), runtime="single", args=(anchors[0],)
                    )
                    if e.get("ok"):
                        p = json.loads(e["rows"][0][0])[0]
                        ex = {
                            "exec_ms": p.get("Execution Time"),
                            "shared_hit": p["Plan"].get("Shared Hit Blocks"),
                            "temp_written": p["Plan"].get("Temp Written Blocks"),
                        }
                cell = {
                    "k": k,
                    "variant": name,
                    "runtime": "single",
                    "server_ms_p50": pct(times, 0.5),
                    "server_ms_max": max(times, default=None),
                    "result_anchor0": rows,
                    "errors": errs,
                    **ex,
                }
                if errs:
                    dead.add(key)
                cells.append(cell)
                print(
                    f"k={k} {key:24} p50={cell['server_ms_p50']} rows={rows} buffers={ex.get('shared_hit')} {errs or ''}",
                    flush=True,
                )
            (out / "cells.json").write_text(json.dumps(cells, indent=1, default=str))
    await pg.close()
    await neo.close()


if __name__ == "__main__":
    asyncio.run(main())
