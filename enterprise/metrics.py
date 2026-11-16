"""Neo4j Enterprise observability for the benchmark.

Five sources, all Enterprise/Aura-Enterprise features verified by enterprise.probe:

1. JMX metrics registry (`dbms.queryJmx('neo4j.metrics:*')`): server-side query
   latency histogram, page cache hits/faults/hit ratio, transactions, plan cache,
   heap/GC, Bolt. `JmxSnapshot` takes before/after snapshots; counters become
   per-run deltas. The latency histogram is a sliding reservoir, so its
   percentiles describe "recent queries", not exactly one run -- keep runs
   longer than the reservoir or treat it as a cross-check of our own timings.
2. `SHOW TRANSACTIONS` sampling (`TxSampler`): per-transaction pageHits/Faults,
   allocated bytes, estimated heap, lock wait, runtime and status, matched to
   our requests through transaction metadata. Also yields the *server-side*
   concurrency actually reached, which can differ from the client's.
3. `dbms.listActiveLocks` for any sampled transaction that is Blocked.
4. `dbms.listPools` / `dbms.listConnections` snapshots, `SHOW INDEXES` readCount deltas.
5. PROFILE plans (`plan_metrics`): actual runtime, fused pipelines, DbHits,
   PageCacheHits/Misses, Memory/GlobalMemory, AvailableWorkers. PROFILE adds
   overhead (large for slotted), so it runs in a separate pass, never in the
   timed one.
"""

from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass, field
from typing import Any

from neo4j import AsyncDriver, Driver

from enterprise.conn import DATABASE

# Metrics kept from the 1285-bean registry. Matched as suffixes of the bean name.
JMX_METRICS = (
    "database.neo4j.db.query.execution.latency.millis",
    "database.neo4j.db.query.execution.success",
    "database.neo4j.db.query.execution.failure",
    "database.neo4j.transaction.committed",
    "database.neo4j.transaction.rollbacks",
    "database.neo4j.transaction.active_read",
    "database.neo4j.transaction.peak_concurrent",
    "database.neo4j.transaction.started",
    "database.neo4j.cypher.replan_events",
    "database.neo4j.pool.transaction.neo4j.used_heap",
    "database.neo4j.pool.transaction.neo4j.used_native",
    "dbms.page_cache.hits",
    "dbms.page_cache.page_faults",
    "dbms.page_cache.hit_ratio",
    "dbms.page_cache.usage_ratio",
    "dbms.page_cache.evictions",
    "dbms.vm.heap.used",
    "dbms.vm.gc.time.g1_young_generation",
    "dbms.vm.gc.time.g1_old_generation",
    "dbms.vm.thread.total",
    "dbms.bolt.connections_running",
    "dbms.bolt.connections_idle",
    "dbms.bolt.messages_received",
    "dbms.bolt.accumulated_queue_time",
    "dbms.bolt.accumulated_processing_time",
    "dbms.cypher.cache.executable_query.cache_hit",
    "dbms.cypher.cache.executable_query.cache_miss",
    "dbms.cypher.cache.ast.cache_hit",
)


def _value(attrs: dict) -> dict:
    """Flatten a JMX attribute map ({k: {value: v}} or {k: v})."""
    out = {}
    for k, v in attrs.items():
        v = v.get("value") if isinstance(v, dict) and "value" in v else v
        if isinstance(v, (int, float)):
            out[k] = v
    return out


def jmx_read(d: Driver) -> dict[str, dict]:
    records, _, _ = d.execute_query(
        "CALL dbms.queryJmx('neo4j.metrics:*') YIELD name, attributes RETURN name, attributes", database_=DATABASE
    )
    out = {}
    for r in records:
        name = r["name"].split("name=neo4j.", 1)[-1]
        if name in JMX_METRICS or name.endswith(".execution.latency.millis") and "neo4j" in name:
            out[name] = _value(r["attributes"])
    return out


@dataclass
class JmxSnapshot:
    before: dict[str, dict] = field(default_factory=dict)
    after: dict[str, dict] = field(default_factory=dict)

    def start(self, d: Driver) -> None:
        self.before = jmx_read(d)

    def stop(self, d: Driver) -> None:
        self.after = jmx_read(d)

    def summary(self) -> dict[str, Any]:
        """Counter deltas, gauge end values, and the latency histogram at the end."""
        out: dict[str, Any] = {}
        for name, a in self.after.items():
            b = self.before.get(name, {})
            short = name.replace("database.neo4j.", "db.").replace("dbms.", "")
            if "50thPercentile" in a:
                out[short] = {k: a[k] for k in ("Count", "Mean", "50thPercentile", "95thPercentile", "99thPercentile", "Max") if k in a}
                out[short + ".count_delta"] = a.get("Count", 0) - b.get("Count", 0)
            elif "Count" in a:
                out[short + ".delta"] = a["Count"] - b.get("Count", 0)
            elif "Value" in a:
                out[short] = a["Value"]
        hits, faults = out.get("page_cache.hits.delta"), out.get("page_cache.page_faults.delta")
        if hits is not None and faults is not None and hits + faults:
            out["page_cache.run_hit_ratio"] = hits / (hits + faults)
        return out


_TX_QUERY = """
SHOW TRANSACTIONS YIELD transactionId, currentQueryId, metaData, status, runtime, elapsedTime, waitTime,
     activeLockCount, currentQueryAllocatedBytes, estimatedUsedHeapMemory, pageHits, pageFaults, cpuTime
WHERE metaData.bench = $run
RETURN transactionId, currentQueryId, metaData, status, runtime, elapsedTime.milliseconds AS elapsed_ms,
       waitTime.milliseconds AS wait_ms, activeLockCount, currentQueryAllocatedBytes AS alloc_bytes,
       estimatedUsedHeapMemory AS heap_bytes, pageHits, pageFaults, cpuTime
"""


@dataclass
class TxSampler:
    """Polls SHOW TRANSACTIONS for our run's transactions on a dedicated connection.

    Note: parallel-runtime worker threads do not update tx-level pageHits live
    (observed in the aura-runtime study), so pageHits for parallel queries are
    a lower bound.
    """

    driver: AsyncDriver
    run_id: str
    interval_s: float = 0.1
    per_tx: dict[str, dict] = field(default_factory=dict)
    concurrency: list[tuple[float, int]] = field(default_factory=list)
    locks: list[dict] = field(default_factory=list)
    samples: int = 0
    _task: asyncio.Task | None = None
    _stop: asyncio.Event = field(default_factory=asyncio.Event)

    async def _loop(self) -> None:
        t0 = time.monotonic()
        while not self._stop.is_set():
            try:
                records, _, _ = await self.driver.execute_query(_TX_QUERY, run=self.run_id, database_=DATABASE)
            except Exception:
                await asyncio.sleep(self.interval_s)
                continue
            self.samples += 1
            self.concurrency.append((time.monotonic() - t0, len(records)))
            for r in records:
                tx = self.per_tx.setdefault(r["transactionId"], {"meta": dict(r["metaData"] or {}), "max": {}, "blocked": False})
                tx["runtime"] = r["runtime"]
                for k in ("elapsed_ms", "wait_ms", "activeLockCount", "alloc_bytes", "heap_bytes", "pageHits", "pageFaults"):
                    v = r[k]
                    if v is not None:
                        tx["max"][k] = max(v, tx["max"].get(k, v))
                if str(r["status"]).startswith("Blocked") and r["currentQueryId"]:
                    tx["blocked"] = True
                    try:
                        lk, _, _ = await self.driver.execute_query(
                            "CALL dbms.listActiveLocks($q) YIELD mode, resourceType, resourceId RETURN mode, resourceType, resourceId",
                            q=r["currentQueryId"], database_=DATABASE,
                        )
                        self.locks.append({"tx": r["transactionId"], "status": r["status"], "locks": [dict(x) for x in lk]})
                    except Exception:
                        pass
            await asyncio.sleep(self.interval_s)

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            await self._task

    def summary(self) -> dict[str, Any]:
        conc = [c for _, c in self.concurrency]
        maxes = [t["max"] for t in self.per_tx.values()]

        def agg(k: str) -> dict[str, float]:
            vals = sorted(m[k] for m in maxes if k in m)
            if not vals:
                return {}
            return {"n": len(vals), "p50": vals[len(vals) // 2], "p95": vals[int(len(vals) * 0.95) - 1 if len(vals) > 1 else 0], "max": vals[-1]}

        return {
            "samples": self.samples,
            "tx_observed": len(self.per_tx),
            "server_concurrency": {"max": max(conc, default=0), "mean": sum(conc) / len(conc) if conc else 0},
            "blocked_tx": sum(t["blocked"] for t in self.per_tx.values()),
            "lock_snapshots": self.locks[:20],
            "runtimes_seen": sorted({str(t.get("runtime")) for t in self.per_tx.values()}),
            **{f"tx_{k}": agg(k) for k in ("pageHits", "pageFaults", "alloc_bytes", "heap_bytes", "wait_ms", "elapsed_ms")},
        }


def pools(d: Driver) -> list[dict]:
    records, _, _ = d.execute_query("CALL dbms.listPools()", database_=DATABASE)
    return [dict(r) for r in records]


def connections(d: Driver) -> dict[str, Any]:
    records, _, _ = d.execute_query(
        "CALL dbms.listConnections() YIELD connector, clientAddress, userAgent RETURN connector, userAgent", database_=DATABASE
    )
    by = {}
    for r in records:
        by[r["connector"]] = by.get(r["connector"], 0) + 1
    return {"total": len(records), "by_connector": by}


def index_reads(d: Driver) -> dict[str, int]:
    records, _, _ = d.execute_query(
        "SHOW INDEXES YIELD name, readCount RETURN name, readCount", database_=DATABASE
    )
    return {r["name"]: r["readCount"] or 0 for r in records}


def index_read_delta(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
    return {k: v - before.get(k, 0) for k, v in after.items() if v - before.get(k, 0) > 0}


def plan_metrics(profile: dict | None) -> dict[str, Any]:
    """Summarize a PROFILE plan tree: actual runtime, pipelines/fusion, DbHits, page cache, memory."""
    if not profile:
        return {}
    args = profile.get("args", {})
    ops: list[str] = []
    pipelines: set[str] = set()
    fused: set[str] = set()
    tot = {"db_hits": 0, "page_cache_hits": 0, "page_cache_misses": 0, "rows_root": profile.get("rows", 0)}

    def walk(p: dict) -> None:
        a = p.get("args", {})
        ops.append(p.get("operatorType", "?").split("@")[0])
        tot["db_hits"] += p.get("dbHits", 0) or 0
        tot["page_cache_hits"] += p.get("pageCacheHits", 0) or 0
        tot["page_cache_misses"] += p.get("pageCacheMisses", 0) or 0
        info = a.get("PipelineInfo") or ""
        m = re.search(r"Pipeline (\d+)", info)
        if m:
            pipelines.add(m.group(1))
            if info.startswith("Fused"):
                fused.add(m.group(1))
        for c in p.get("children", []):
            walk(c)

    walk(profile)
    fused_ops = 0

    def count_fused(p: dict) -> None:
        nonlocal fused_ops
        if (p.get("args", {}).get("PipelineInfo") or "").startswith("Fused"):
            fused_ops += 1
        for c in p.get("children", []):
            count_fused(c)

    count_fused(profile)
    return {
        "actual_runtime": args.get("runtime"),
        "runtime_impl": args.get("runtime-impl"),
        "planner": args.get("planner"),
        "available_workers": args.get("AvailableWorkers"),
        "global_memory_bytes": args.get("GlobalMemory"),
        "operators": len(ops),
        "operator_types": sorted(set(ops)),
        "pipelines": len(pipelines),
        "fused_pipelines": len(fused),
        "pipeline_fused_operators": fused_ops,
        "partitioned_scan": any(o.startswith("Partitioned") for o in ops),
        "uses_top_instead_of_limit": "Top" in ops and "Limit" not in ops,
        **tot,
    }


def server_ms(summary: Any) -> int:
    """Server-side time: planning + first record + streaming the rest (excludes network)."""
    return (summary.result_available_after or 0) + (summary.result_consumed_after or 0)
