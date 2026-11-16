# Neo4j Enterprise feature audit

Question asked: *did the benchmark use Neo4j Enterprise features fully?* Short answer: the **runtime and
observability** side is used thoroughly and now drives policy (runtime routing, transaction watchdog, constraint-backed
plans); the **operations** side (clustering, Fabric, CDC, fine-grained RBAC, query log, Prometheus) is either blocked on
the Aura tier used or outside a read-heavy benchmark. Every row below says what was used, where, and the evidence.

Legend: ✅ used and measured · ➕ added in the second round · ⛔ not available on this Aura tier (probe:
`results/enterprise_probe.json`) · ➖ available but not relevant / not used, with the reason.

## Query execution

| feature | status | where / evidence |
|---|---|---|
| Pipelined runtime (operator fusion) | ✅ | default for Stage A/B; `plan_metrics` counts fused pipelines and operators |
| Parallel runtime (8 workers on Aura) | ✅ | runtime study, depth sweep, mixed workload; actual runtime checked from PROFILE (`APOC procedure → falls back to PIPELINED`) |
| Slotted runtime (for comparison) | ✅ | runtime study, scale sweep |
| Runtime routing per query class + concurrency cap | ➕ | `load_test/mixed.py` policy N3 (analytics on parallel behind a semaphore of 2, interactive on pipelined) |
| `CALL { } IN CONCURRENT TRANSACTIONS … ON ERROR RETRY` (Cypher 25) | ➕ | `graph_sync/load_fast.py` server mode: the database parallelizes the load and retries deadlocked inner transactions |
| Cypher 25 path features (`SHORTEST`, quantified path patterns) | ➕ | `load_test/depth_sweep.py` (`neo-shortest`, `neo-qpp`) |
| Pruning var-length expand (BFS) | ✅ observed | planner picks `VarLengthExpand(Pruning,BFS)` for `DISTINCT` reachability; APOC rewrite unnecessary |
| Planner hints `USING INDEX` / `USING JOIN ON` | ➖ | the cost planner already chose index seeks on every measured workload; hints would only matter for mis-planned queries |

## Schema (Enterprise constraint types)

| feature | status | where / evidence |
|---|---|---|
| NODE KEY | ✅ | every node label (`graph_sync/load.py`) |
| RELATIONSHIP KEY | ✅ | `SUPPLIES.relation_id`, `HOLDS.holding_id`, `HOLDS_ROLE.company_executive_id` |
| Property existence (node + relationship) | ✅ ➕ measured | `enterprise/constraint_ab.py`: existence lets `count(*) … IS NOT NULL` read the **count store** |
| Property type (node + relationship) | ✅ ➕ measured | removes the type `Filter`; with existence, enables index-ordered `ORDER BY … LIMIT` |
| Composite range index (data-model tuning) | ➕ | `load_test/tuning.py` `neo-denorm`: `(metric_code, fiscal_year, fiscal_period)` |
| Block store format | ✅ | `SHOW DATABASES` → `block-block-1.1` |


## Observability and control

| feature | status | where / evidence |
|---|---|---|
| JMX metrics registry `dbms.queryJmx('neo4j.metrics:*')` (1,285 beans) | ✅ | per-cell deltas: server latency histogram, page cache hits/faults/hit ratio, GC, Bolt queue time, transactions, plan cache |
| `SHOW TRANSACTIONS` (39 columns) + transaction metadata | ✅ | per-request heap, allocation, page hits/faults, lock waits, runtime; server-side concurrency actually reached |
| `TERMINATE TRANSACTIONS` | ➕ | watchdog in `load_test/mixed.py` (policy N4): kills only `cls = 'llm'` transactions over 2 s, selected by metadata |
| `dbms.listActiveLocks` | ✅ wired | sampled for any `Blocked` transaction; the workload is read-only, so none occurred |
| `dbms.listPools`, `dbms.listConnections`, `SHOW INDEXES … readCount` | ✅ | Stage A summaries (memory pools, connections, index reads per run) |
| PROFILE argument metrics (`PageCacheHits/Misses`, `Memory`, `GlobalMemory`, `AvailableWorkers`, `PipelineInfo`) | ✅ | `enterprise.metrics.plan_metrics` |
| `cpuTime` in `SHOW TRANSACTIONS` | ⛔ | `db.track_query_cpu_time` is off and not settable on Aura |
| Query log (planning/CPU/wait breakdown), Prometheus endpoint | ⛔ | Aura console / metrics integration only (needs Aura API credentials) |
| `db.stats.*`, `SHOW SETTINGS`, `dbms.listConfig` | ⛔ | forbidden for the Aura `neo4j` user |


## Operations and security

| feature | status | reason |
|---|---|---|
| Clustering, read replicas, `SHOW SERVERS` | ⛔ | single-primary Aura tier; `SHOW SERVERS` forbidden |
| Composite databases / Fabric, `CREATE DATABASE` | ⛔ | not supported on Aura |
| Change Data Capture | ⛔ | disabled; enabled from the Aura console only |
| Custom roles, property-level privileges, impersonation | ⛔ | `CREATE ROLE` / `SHOW PRIVILEGES` forbidden on this tier; built-in roles only |
| Online backup / restore | ➖ | console-managed on Aura; not part of a query benchmark |
| Parallel worker limit, transaction memory limits | ⛔ | configuration is fixed on Aura (8 workers observed) |

## What would add the most next

1. **Text / full-text index for entity grounding** (not Enterprise-only): the dominant Cypher error is an exact
   `{name: '…'}` literal; a resolver over a full-text index would attack the biggest accuracy gap.
2. **Self-hosted Neo4j Enterprise** for the blocked rows: query log with time breakdown, `cpuTime`, configurable
   `server.cypher.parallel.worker_limit` (a worker-count sweep), and same-hardware comparison with Postgres.
3. **CDC** (console switch) to measure incremental ontology refresh instead of full reloads.
