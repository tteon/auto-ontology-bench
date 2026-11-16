# Methodology

This document describes every experiment in the repository: what is varied, what is held fixed,
how it is measured, and the safeguards against measuring the wrong thing. Results for the accuracy
study are in `docs/FINDINGS.md` and `results/`.

> **About database performance results.** The database performance experiments (Stage B, the runtime study, the
> scale, depth and mixed-workload sweeps, tuning, constraint A/B) are fully implemented here, but their numbers are
> not published: Neo4j's Acceptable Use Policy for Aura prohibits benchmarking the service. If you run them, check
> the terms that apply to your Neo4j deployment before publishing what you measure.

## 1. Data

- **finder_real** — SEC EDGAR data for 10 filers organized into the FinDER 8 categories (`data_pipeline/schema.sql`):
  XBRL company facts (US-GAAP and IFRS unified through SEC calendar frames), insider Forms 3/4 (board/officer roles and
  interlocks), 13F holdings of 20 large managers, and LLM-extracted risk factors, legal proceedings, related-party
  transactions and supply relations. Every extracted item must quote evidence that occurs verbatim in the filing,
  otherwise it is dropped. Every fact row points at the filing it came from. The exact database used for the published
  results is `data/finder_real.sql.gz` (`make restore`); EDGAR changes over time, so a fresh `make data` will differ.
- **finder_s10k / finder_scale (30k) / finder_s100k** — `data_pipeline/scale.py`: finder_real plus a seeded synthetic
  expansion shaped for multi-hop work: a six-tier supply network with preferential attachment (hubs) and a small share
  of skip-tier and back edges (shortcuts and cycles), board seats drawn Zipf-style from a shared person pool
  (interlocks), overlapping 13F portfolios (institution count scales with company count, so holdings density is
  constant across scale factors), risks, litigation and FY metrics 2019–2025. Used only for database experiments.

## 2. Two engines, one dataset

- **Postgres 16** (`docker/compose.yml`): pinned to a CPU set the size of the Neo4j instance's processor count,
  `pg_stat_statements` in the maintenance database (so it is never ingested as benchmark data), `track_io_timing`,
  B-tree indexes on every foreign key.
- **Neo4j Enterprise** (AuraDB or self-hosted): join tables become relationships with properties
  (`company_executives → HOLDS_ROLE`, `institutional_holdings → HOLDS`, `supply_chain_relations → SUPPLIES`);
  NODE KEY, RELATIONSHIP KEY, property existence and property type constraints (`graph_sync/load.py`).
- **Consistency gate** (`graph_sync/consistency.py`, `graph_sync/load_fast.verify`): every node label and
  relationship type count must equal its Postgres source, and 11 aggregate queries must return identical results,
  before any experiment uses a database.

## 3. Accuracy study (Stage A): ontology × database model

- **Conditions (2×2):** `pg-no-onto`, `pg-onto`, `neo-no-onto`, `neo-onto`. `no-onto` = schema only (DDL without
  comments / graph schema). `onto` = schema + Auto-Ontology context, fetched through the REST endpoints its MCP tools
  wrap: `check_answerable` candidates, term details, column attributes with sample values, and matched certified SQL
  attributes (definitions). For Cypher the ontology's table/column names are mapped to graph elements through the
  Apache Ossie export (`ontology/ossie_bridge.py`).
- **Ontology curation** (`ontology/curate.py`): Auto-Ontology compiles one term per table but cannot see metrics stored
  as rows, and derives no calculations, so 21 metric definitions and 21 calculations/graph concepts are added as SQL
  attributes, plus term descriptions with synonyms. Every attribute defines a reusable concept (no question-specific
  filters); this curation is part of the `onto` treatment.
- **Questions** (`benchmarks/build_cq.py`, `benchmarks/cq_dataset.jsonl`): 250 competency questions, 50 per tier —
  1 term normalization / lookup, 2 certified calculations, 3 two-to-three hop, 4 three-to-five hop compound paths,
  5 unanswerable — split 10 dev / 40 test per tier. Each answerable item has a gold SQL and a gold Cypher query written
  per template; an item is kept only if both execute, are non-empty and return identical results. Gold queries are not
  written by the model under test. All items are marked for human review.
- **Generation:** one model (`LLM_MODEL`), temperature 0, two repeats; a fixed system prompt per engine; ratios are
  requested as decimal fractions in every condition.
- **Scoring** (`benchmarks/verifier.py`): result-set equality after normalization (numbers to 6 significant digits,
  case-folded strings, ISO dates, order-insensitive unless the question asks for an order). *Lenient* (primary) allows
  extra columns in the prediction; *strict* requires the same columns. Tier 5 is correct only when the system declines.
- **Statistics** (`load_test/analyze.py`): bootstrap 95% CIs per cell; paired comparisons on the same question and
  repeat with exact McNemar tests.
- **`check_answerable` gate:** a coverage threshold is calibrated on the dev split only (`load_test/calibrate.py`) and
  either applied or, if it does not help on dev, dropped.
- **Coverage caching:** Auto-Ontology serializes `check_answerable` per backend process, so it is computed once per
  question (`ontology/cache_coverage.py`) and its unloaded latency is added to the context time of every onto request.

## 4. Database performance experiments (implemented; numbers not published)

All timings are **server-side**: Neo4j `result_available_after + result_consumed_after`; Postgres wall time minus the
measured loopback baseline. Timed runs never use PROFILE (which adds overhead, very large for slotted); plans are
captured in a separate PROFILE / `EXPLAIN (ANALYZE, BUFFERS)` pass. Cell order is shuffled with a fixed seed. Plan caches
are warmed before measuring. The load generator (`load_test/loadgen.py`) uses AIPerf's load model only (closed-loop
concurrency or capped Poisson arrivals, warm-up exclusion, per-request records) and pins itself to CPUs not used by
Postgres.

| experiment | script | varies | records |
|---|---|---|---|
| Stage B | `load_test/stage_b.py` | concurrency 1–256 | replay of the LLM queries Stage A verified correct; server p50/p99, QPS, errors, server-side concurrency |
| Runtime study | `load_test/runtime_study.py` | workload W1–W4 × Neo4j slotted/pipelined/parallel × Postgres single/parallel × concurrency | correctness gate across all runtimes; actual runtime from PROFILE; fused pipelines; Postgres workers launched |
| Scale sweep | `scripts/run_phases.sh … sweep` | 10k / 30k / 100k companies | same matrix at concurrency 1 and 16 |
| Depth sweep | `load_test/depth_sweep.py` | hop depth k = 1–8 | reachability (DISTINCT), path enumeration, quantified path patterns, SHORTEST vs recursive CTE with UNION / UNION ALL |
| Mixed workload | `load_test/mixed.py` | runtime policy | interactive + analytic + ad-hoc LLM clients at once; runtime routing with a parallel cap; TERMINATE watchdog; Postgres analogues |
| Tuning tracks | `load_test/tuning.py` | data model / index / rewrite | each tuned variant must equal its baseline before timing |
| Constraint A/B | `enterprise/constraint_ab.py` | existence/type constraints on/off | plan operators (count store, index order), db hits |
| Plan lab | `load_test/plan_lab.py` | before/after tuning × every runtime × light (L1–L4) and heavy (H1–H5) queries | EXPLAIN + PROFILE / `EXPLAIN (ANALYZE, BUFFERS)` text and totals (db hits, page cache, estimated vs actual rows, operators, actual runtime); server vs client-observed (E2E) latency and their difference at concurrency 1; QPS and p99 at concurrency 8; network baseline (`RETURN 1` / `SELECT 1`); result equality across engines |

Workloads (`load_test/runtime_study.py`): W1 point lookup; W2 global revenue by country over every metric fact;
W3 three-hop co-holding fan-out; W4a distinct upstream suppliers within six tiers; W4b risk categories of upstream
suppliers within four tiers; W4r W4a via APOC BFS.

Plan lab states: *before* keeps only keys and the base indexes (existence/type constraints dropped); *after* restores the
existence/type constraints, denormalises the metric code onto metric nodes with a composite index, and adds Postgres
covering indexes. E2E = what the client sees (session, request, network, decoding); E2E − server = network + driver. On a
remote managed database the network term dominates every light query, so light queries are compared on server time and
heavy ones on both. Name ordering differs by collation (Postgres `en_US.utf8` vs Neo4j code-point order), so L3's
top-10 is not expected to match across engines.

## 5. Neo4j Enterprise observability (`enterprise/`)

Per measurement cell: deltas of the JMX metrics registry (`dbms.queryJmx('neo4j.metrics:*')` — server latency
histogram, page cache, GC, Bolt queue, transactions, plan cache), `SHOW TRANSACTIONS` sampling joined on transaction
metadata (heap, allocation, page hits/faults, lock waits, server-side concurrency), lock snapshots for blocked
transactions, pools, connections and index read counts, and PROFILE plan arguments. `enterprise/probe.py` records which
Enterprise features a given deployment exposes; `docs/ENTERPRISE_AUDIT.md` maps each to where it is used.

## 6. Known threats to validity

- One generation model; two repeats; 40 test questions per tier.
- Gold queries and ontology curation share an author (an LLM agent) — independent wording, but review is pending.
- Real data is small (maximum supply depth 2); deep traversal behaviour comes from synthetic data.
- Person names are stored in EDGAR order, questions use natural order.
- A managed Neo4j service and a local Postgres differ in hardware and network; engines are compared on server time only.
