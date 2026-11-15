"""Read-only query executors for Postgres and Neo4j with per-request timing.

Timing fields per request:
- client_wall_ms: around the driver call, including result transfer and decoding
- pool_wait_ms: time to get a connection/session from the pool
- server_ms: Neo4j = result_available_after + result_consumed_after (server-side,
  network excluded). Postgres has no per-statement server timer on the wire, so
  server_ms = client_wall_ms - loopback baseline (measured `SELECT 1` wall,
  ~0.1 ms on localhost); per-template server time under load is cross-checked
  from pg_stat_statements deltas (`pg_stats_snapshot`).

Runtime selection (runtime study): Neo4j via a `CYPHER runtime=...` prefix,
Postgres via `SET LOCAL` parallel settings inside the read-only transaction.
"""

from __future__ import annotations

import asyncio
import os
import re
import time
from typing import Any

import asyncpg
from neo4j import AsyncDriver, Query
from neo4j.exceptions import ClientError, CypherSyntaxError, Neo4jError, ServiceUnavailable, SessionExpired

from enterprise.conn import DATABASE, async_driver
from enterprise.metrics import server_ms

TIMEOUT_S = 30.0

PG_RUNTIMES: dict[str, list[str]] = {
    # Single worker: no parallel plans at all.
    "single": ["SET LOCAL max_parallel_workers_per_gather = 0"],
    # Parallel: allow up to 4 workers per Gather and make parallel plans cheap to choose.
    "parallel": ["SET LOCAL max_parallel_workers_per_gather = 4", "SET LOCAL parallel_setup_cost = 0",
                 "SET LOCAL parallel_tuple_cost = 0", "SET LOCAL min_parallel_table_scan_size = 0",
                 "SET LOCAL min_parallel_index_scan_size = 0"],
    "default": [],
}
NEO_RUNTIMES = ("slotted", "pipelined", "parallel")

_CYPHER_PREFIX = re.compile(r"^\s*CYPHER\b[^\n]*?(?=\b(MATCH|OPTIONAL|WITH|UNWIND|CALL|RETURN|PROFILE|EXPLAIN)\b)", re.I)


def classify_pg(e: BaseException) -> str:
    if isinstance(e, (asyncio.TimeoutError, asyncpg.QueryCanceledError)):
        return "TIMEOUT"
    if isinstance(e, (asyncpg.PostgresSyntaxError, asyncpg.UndefinedColumnError, asyncpg.UndefinedTableError,
                      asyncpg.UndefinedFunctionError, asyncpg.AmbiguousColumnError, asyncpg.GroupingError,
                      asyncpg.DatatypeMismatchError, asyncpg.InvalidTextRepresentationError)):
        return "SYNTAX_ERROR"
    if isinstance(e, asyncpg.ReadOnlySQLTransactionError):
        return "WRITE_REJECTED"
    if isinstance(e, (OSError, asyncpg.PostgresConnectionError, asyncpg.InterfaceError)):
        return "CONNECTION_ERROR"
    return "EXECUTION_ERROR"


def classify_neo(e: BaseException) -> str:
    if isinstance(e, asyncio.TimeoutError):
        return "TIMEOUT"
    if isinstance(e, CypherSyntaxError):
        return "SYNTAX_ERROR"
    if isinstance(e, (ServiceUnavailable, SessionExpired, OSError)):
        return "CONNECTION_ERROR"
    if isinstance(e, Neo4jError) and "Terminated" in (e.code or ""):
        return "TERMINATED"  # killed by TERMINATE TRANSACTIONS (TransientError.Transaction.Terminated)
    if isinstance(e, ClientError):
        code = e.code or ""
        if "Terminated" in code:
            return "TERMINATED"  # killed by TERMINATE TRANSACTIONS (or a server-side guard)
        if "TransactionTimedOut" in code:
            return "TIMEOUT"
        if "Statement" in code and ("Semantic" in code or "Type" in code or "Argument" in code or "EntityNotFound" in code):
            return "SYNTAX_ERROR"
        if "Forbidden" in code or "WriteOnReadOnly" in code or "AccessMode" in code:
            return "WRITE_REJECTED"
    return "EXECUTION_ERROR"


class PgExecutor:
    def __init__(self, db: str, pool_size: int) -> None:
        self.db, self.pool_size = db, pool_size
        self.pool: asyncpg.Pool | None = None
        self.baseline_ms = 0.0

    async def start(self) -> None:
        self.pool = await asyncpg.create_pool(
            host="127.0.0.1", port=int(os.environ["BENCH_PG_PORT"]), user="bench_ro",
            password=os.environ["BENCH_PG_RO_PASSWORD"], database=self.db,
            min_size=self.pool_size, max_size=self.pool_size, statement_cache_size=0,
        )
        samples = []
        for _ in range(20):
            t = time.perf_counter()
            await self.pool.fetchval("SELECT 1")
            samples.append((time.perf_counter() - t) * 1000)
        self.baseline_ms = sorted(samples)[10]

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()

    async def run(self, sql: str, *, runtime: str = "default", tag: str | None = None,
                  args: tuple = (), pre: list[str] | tuple = ()) -> dict[str, Any]:
        assert self.pool
        t0 = time.perf_counter()
        try:
            async with self.pool.acquire() as conn:
                t1 = time.perf_counter()
                async with conn.transaction(readonly=True):
                    if tag:
                        await conn.execute(f"SET LOCAL application_name = '{re.sub(r'[^a-zA-Z0-9_.:-]', '', tag)[:60]}'")
                    for s in (*PG_RUNTIMES[runtime], *pre):
                        await conn.execute(s)
                    t2 = time.perf_counter()
                    records = await conn.fetch(sql.rstrip().rstrip(";"), *args, timeout=TIMEOUT_S)
                    t3 = time.perf_counter()
            wall = (t3 - t2) * 1000
            return {"ok": True, "rows": [tuple(r.values()) for r in records], "columns": list(records[0].keys()) if records else [],
                    "row_count": len(records), "client_wall_ms": wall, "server_ms": max(0.0, wall - self.baseline_ms),
                    "pool_wait_ms": (t1 - t0) * 1000, "runtime_requested": runtime}
        except Exception as e:  # noqa: BLE001 -- classified and returned
            return {"ok": False, "error_type": classify_pg(e), "error": f"{type(e).__name__}: {e}"[:400],
                    "client_wall_ms": (time.perf_counter() - t0) * 1000, "runtime_requested": runtime}


class NeoExecutor:
    def __init__(self, pool_size: int) -> None:
        self.pool_size = pool_size
        self.driver: AsyncDriver | None = None

    async def start(self) -> None:
        self.driver = async_driver(max_connection_pool_size=self.pool_size, connection_acquisition_timeout=60)
        await self.driver.verify_connectivity()

    async def close(self) -> None:
        if self.driver:
            await self.driver.close()

    @staticmethod
    def with_runtime(cypher: str, runtime: str) -> str:
        body = _CYPHER_PREFIX.sub("", cypher.strip().rstrip(";"))
        return body if runtime == "default" else f"CYPHER runtime={runtime} {body}"

    async def run(self, cypher: str, *, runtime: str = "default", meta: dict[str, Any] | None = None,
                  profile: bool = False, params: dict[str, Any] | None = None) -> dict[str, Any]:
        assert self.driver
        text = self.with_runtime(cypher, runtime)
        if profile:
            text = re.sub(r"^(CYPHER runtime=\w+ )?", lambda m: (m.group(0) or "") + "PROFILE ", text, count=1)
        t0 = time.perf_counter()
        try:
            async with self.driver.session(database=DATABASE, default_access_mode="READ") as s:
                t1 = time.perf_counter()
                result = await s.run(Query(text, metadata=meta or {}, timeout=TIMEOUT_S), params or {})
                records = [tuple(r.values()) async for r in result]
                summary = await result.consume()
                t2 = time.perf_counter()
            out = {"ok": True, "rows": records, "row_count": len(records), "client_wall_ms": (t2 - t1) * 1000,
                   "server_ms": server_ms(summary), "pool_wait_ms": (t1 - t0) * 1000, "runtime_requested": runtime,
                   "columns": list(result.keys())}
            if profile and summary.profile:
                from enterprise.metrics import plan_metrics

                out["plan"] = plan_metrics(summary.profile)
            elif summary.plan:
                out["actual_runtime"] = summary.plan.get("args", {}).get("runtime")
            return out
        except (Neo4jError, ServiceUnavailable, SessionExpired, asyncio.TimeoutError, OSError) as e:
            return {"ok": False, "error_type": classify_neo(e), "error": f"{type(e).__name__}: {e}"[:400],
                    "client_wall_ms": (time.perf_counter() - t0) * 1000, "runtime_requested": runtime}


async def pg_stats_snapshot(db: str) -> dict[int, dict[str, float]]:
    """pg_stat_statements rows for `db`, keyed by queryid (for per-run deltas)."""
    # The extension lives in the `postgres` maintenance DB (it reports every database), so the
    # benchmark databases -- which Auto-Ontology ingests -- contain only benchmark tables.
    conn = await asyncpg.connect(host="127.0.0.1", port=int(os.environ["BENCH_PG_PORT"]), user="bench",
                                 password=os.environ["BENCH_PG_PASSWORD"], database="postgres")
    try:
        rows = await conn.fetch(
            """SELECT s.queryid, s.query, s.calls, s.total_exec_time, s.shared_blks_hit, s.shared_blks_read,
                      s.temp_blks_written, s.rows
               FROM pg_stat_statements s JOIN pg_database d ON d.oid = s.dbid WHERE d.datname = $1""", db)
        return {r["queryid"]: dict(r) for r in rows}
    finally:
        await conn.close()


def pg_stats_delta(before: dict[int, dict], after: dict[int, dict]) -> list[dict[str, Any]]:
    out = []
    for qid, a in after.items():
        b = before.get(qid, {})
        calls = a["calls"] - b.get("calls", 0)
        if calls <= 0:
            continue
        out.append({
            "queryid": qid, "query": a["query"][:200], "calls": calls,
            "mean_exec_ms": (a["total_exec_time"] - b.get("total_exec_time", 0)) / calls,
            "shared_blks_hit": a["shared_blks_hit"] - b.get("shared_blks_hit", 0),
            "shared_blks_read": a["shared_blks_read"] - b.get("shared_blks_read", 0),
            "temp_blks_written": a["temp_blks_written"] - b.get("temp_blks_written", 0),
        })
    return out
