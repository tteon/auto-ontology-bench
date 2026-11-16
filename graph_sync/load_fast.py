"""Faster Postgres -> Neo4j loader for the scale sweep (same graph model as graph_sync.load).

    uv run python -m graph_sync.load_fast --db finder_s10k --wipe --mode client --workers 6 --batch 20000
    uv run python -m graph_sync.load_fast --db finder_s10k --wipe --mode server --batch 50000 --tx-rows 5000

Over a remote link (a managed service is typically tens to hundreds of ms away) the sequential loader mostly waits. Two ways around it:
- client: N async sessions send batches concurrently through managed transactions (the driver retries
  transient errors such as deadlocks between relationship batches that share endpoint nodes);
- server: one session sends large batches and the server splits them with
  `CALL (r) { ... } IN CONCURRENT TRANSACTIONS OF k ROWS ON ERROR RETRY` (Cypher 25), parallelizing inside the database.
Per-table rows/s are recorded so the two modes can be compared.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from datetime import date
from pathlib import Path
from typing import Any

from enterprise.conn import DATABASE, async_driver, driver
from graph_sync.load import CONSTRAINTS, INDEXES, NODES, RELS, pg, stream, wipe

ROOT = Path(__file__).resolve().parents[1]
# Concurrent inner transactions that create relationships on shared endpoint nodes can deadlock;
# Cypher 25 retries the failed inner transaction instead of failing the whole load.
RETRY = "ON ERROR RETRY FOR 120 SECONDS THEN FAIL"


def node_cypher(label: str, mode: str, tx_rows: int) -> str:
    if mode == "server":
        return (f"CYPHER 25 UNWIND $rows AS r CALL (r) {{ CREATE (x:{label}) SET x = r }} "
                f"IN CONCURRENT TRANSACTIONS OF {tx_rows} ROWS {RETRY}")
    return f"UNWIND $rows AS r CREATE (x:{label}) SET x = r"


def rel_cypher(rtype: str, fl: str, fkey: str, tl: str, tkey: str, props: list[str], mode: str, tx_rows: int) -> str:
    body = (f"MATCH (a:{fl} {{{fkey}: r._f}}) MATCH (b:{tl} {{{tkey}: r._t}}) CREATE (a)-[x:{rtype}]->(b)"
            + (" SET x = r{" + ", ".join(f".{p}" for p in props) + "}" if props else ""))
    if mode == "server":
        return f"CYPHER 25 UNWIND $rows AS r CALL (r) {{ {body} }} IN CONCURRENT TRANSACTIONS OF {tx_rows} ROWS {RETRY}"
    return f"UNWIND $rows AS r {body}"


async def push(sql_iter, cypher: str, mode: str, workers: int) -> int:
    """Send every batch from sql_iter; client mode runs `workers` managed transactions at once."""
    total = 0
    async with async_driver(max_connection_pool_size=max(workers, 2)) as d:
        if mode == "server":
            async with d.session(database=DATABASE) as s:
                for batch in sql_iter:
                    res = await s.run(cypher, rows=batch)
                    c = (await res.consume()).counters
                    total += c.nodes_created + c.relationships_created
            return total
        q: asyncio.Queue = asyncio.Queue(maxsize=workers * 2)

        async def worker() -> None:
            nonlocal total
            while True:
                batch = await q.get()
                if batch is None:
                    return
                _, summary, _ = await d.execute_query(cypher, rows=batch, database_=DATABASE)
                total += summary.counters.nodes_created + summary.counters.relationships_created

        tasks = [asyncio.create_task(worker()) for _ in range(workers)]
        loop = asyncio.get_running_loop()
        it = iter(sql_iter)
        while True:
            batch = await loop.run_in_executor(None, next, it, None)
            if batch is None:
                break
            await q.put(batch)
        for _ in tasks:
            await q.put(None)
        await asyncio.gather(*tasks)
    return total


def wipe_fast(d: Any) -> float:
    """Delete everything server-side, label by label (largest first), in concurrent transactions, then drop schema.

    The client-side wipe deletes 50k nodes per round trip; for tens of millions of elements over a remote link the
    server-side `CALL { DETACH DELETE } IN CONCURRENT TRANSACTIONS` is far faster. Deadlocks between inner
    transactions (shared relationships) are retried.
    """
    t = time.time()
    labels = [r["label"] for r in d.execute_query("CALL db.labels() YIELD label RETURN label", database_=DATABASE)[0]]
    counts = {lab: d.execute_query(f"MATCH (n:`{lab}`) RETURN count(n) AS c", database_=DATABASE)[0][0]["c"] for lab in labels}
    for lab in sorted(counts, key=counts.get, reverse=True):
        with d.session(database=DATABASE) as s:
            s.run(f"CYPHER 25 MATCH (n:`{lab}`) CALL (n) {{ DETACH DELETE n }} IN CONCURRENT TRANSACTIONS OF 5000 ROWS {RETRY}").consume()
        print(f"  wiped {lab}: {counts[lab]:,} nodes", flush=True)
    wipe(d)  # anything left + constraints and indexes
    return time.time() - t


async def load(db: str, do_wipe: bool, mode: str, workers: int, batch: int, tx_rows: int) -> dict[str, Any]:
    import graph_sync.load as base

    base.BATCH = batch
    stats: dict[str, Any] = {"db": db, "mode": mode, "workers": workers, "batch": batch, "tx_rows": tx_rows, "tables": []}
    t0 = time.time()
    with driver() as d:
        if do_wipe:
            stats["wipe_s"] = round(wipe_fast(d), 1)
        for stmt in CONSTRAINTS + INDEXES:
            d.execute_query(stmt, database_=DATABASE)
        d.execute_query("CALL db.awaitIndexes(600)", database_=DATABASE)
    with pg(db) as conn:
        for n in NODES:
            t = time.time()
            rows = await push(stream(conn, f"SELECT {', '.join(n['cols'])} FROM {n['table']}"),
                              node_cypher(n["label"], mode, tx_rows), mode, workers)
            el = time.time() - t
            stats["tables"].append({"element": n["label"], "rows": rows, "s": round(el, 1), "rows_per_s": round(rows / el) if el else None})
            print(f"  {n['label']}: {rows} in {el:.0f}s ({rows / el if el else 0:,.0f}/s)", flush=True)
        for rtype, table, fl, fc, tl, tc, props, _ in RELS:
            fkey = next(n["key"] for n in NODES if n["label"] == fl)
            tkey = next(n["key"] for n in NODES if n["label"] == tl)
            sel = [f"{fc} AS _f", f"{tc} AS _t", *props]
            t = time.time()
            rows = await push(stream(conn, f"SELECT {', '.join(sel)} FROM {table} WHERE {fc} IS NOT NULL AND {tc} IS NOT NULL"),
                              rel_cypher(rtype, fl, fkey, tl, tkey, props, mode, tx_rows), mode, workers)
            el = time.time() - t
            stats["tables"].append({"element": f"{fl}-{rtype}->{tl}", "rows": rows, "s": round(el, 1), "rows_per_s": round(rows / el) if el else None})
            print(f"  {fl}-[:{rtype}]->{tl}: {rows} in {el:.0f}s ({rows / el if el else 0:,.0f}/s)", flush=True)
    stats["seconds"] = round(time.time() - t0, 1)
    stats["rows_total"] = sum(t["rows"] for t in stats["tables"])
    stats["verification"] = verify(db)
    return stats


def verify(db: str) -> dict[str, Any]:
    """Compare every node label and relationship type in Neo4j with its source rows in Postgres.

    The load wipes first, so a failure part-way leaves a graph with nodes but missing relationships;
    this check is what tells you the graph is complete. Raises on any mismatch.
    """
    out: dict[str, Any] = {"mismatches": []}
    with driver() as d, pg(db) as conn, conn.cursor() as cur:
        for n in NODES:
            cur.execute(f"SELECT count(*) FROM {n['table']}")
            exp = cur.fetchone()[0]
            got = d.execute_query(f"MATCH (x:{n['label']}) RETURN count(x) AS c", database_=DATABASE)[0][0]["c"]
            out[n["label"]] = {"postgres": exp, "neo4j": got}
            if exp != got:
                out["mismatches"].append(n["label"])
        for rtype, table, fl, fc, tl, tc, _props, _ in RELS:
            cur.execute(f"SELECT count(*) FROM {table} WHERE {fc} IS NOT NULL AND {tc} IS NOT NULL")
            exp = cur.fetchone()[0]
            got = d.execute_query(f"MATCH (:{fl})-[x:{rtype}]->(:{tl}) RETURN count(x) AS c", database_=DATABASE)[0][0]["c"]
            key = f"{fl}-{rtype}->{tl}"
            out[key] = {"postgres": exp, "neo4j": got}
            if exp != got:
                out["mismatches"].append(key)
    print(f"verification: {len(NODES) + len(RELS) - len(out['mismatches'])}/{len(NODES) + len(RELS)} element counts match"
          + (f"; MISMATCH: {out['mismatches']}" if out["mismatches"] else ""), flush=True)
    if out["mismatches"]:
        raise SystemExit(f"graph load incomplete: {out['mismatches']}")
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--wipe", action="store_true")
    ap.add_argument("--mode", choices=["client", "server"], default="client")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--batch", type=int, default=20000)
    ap.add_argument("--tx-rows", type=int, default=5000)
    ap.add_argument("--verify-only", action="store_true")
    ap.add_argument("--wipe-only", action="store_true")
    a = ap.parse_args()
    if a.wipe_only:
        with driver() as d:
            print(f"wiped in {wipe_fast(d):.0f}s")
        return
    if a.verify_only:
        verify(a.db)
        return
    stats = asyncio.run(load(a.db, a.wipe, a.mode, a.workers, a.batch, a.tx_rows))
    out = ROOT / "results" / f"graph_load_{a.db}_{a.mode}_{date.today()}.json"
    out.write_text(json.dumps(stats, indent=1))
    print(json.dumps({k: v for k, v in stats.items() if k != "tables"}))


if __name__ == "__main__":
    main()
