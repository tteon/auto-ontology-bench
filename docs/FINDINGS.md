# Findings — Auto-Ontology × PostgreSQL / Neo4j Enterprise (2026-11-15)

Accuracy records and tables: `results/`. Method: `docs/METHODOLOGY.md`.

## Setup in one paragraph

SEC EDGAR data for 10 filers (Tesla, SpaceX, Rocket Lab, Lockheed Martin, Boeing, NVIDIA, Albemarle, Caterpillar, TSMC,
STMicroelectronics; Panasonic and CATL have no current SEC filings and appear only as counterparties) organized into the FinDER
8 categories: XBRL facts (US-GAAP and IFRS unified through SEC frames), insider Forms 3/4 (5 real board interlocks), 13F
holdings of 20 managers, and LLM-extracted risk factors, litigation, related-party transactions and supply relations
(`nemotron-3-ultra-550b`; every item must quote evidence found verbatim in the filing; 33 items dropped). Same data in
Postgres 16 (local, 8 pinned logical CPUs) and Neo4j AuraDB Enterprise 5.27 (8 processors, 64 GB),
checked equal by an 11-query consistency gate. 250 competency questions (Tiers 1–5, 50 each; dev 10 / test 40) whose gold SQL and
gold Cypher return identical results. Query generation: `nemotron-3.5-super-vl-120b-a12b`, temperature 0, 2 repeats.
Ontology arm: Auto-Ontology's compiled layer (13 terms, 23 semantic FKs) plus 42 curated SQL attributes, delivered through
the same REST endpoints its MCP tools wrap.

## 1. Stage A — does the ontology help, and does the graph help? (test split, n = 400 per condition)

| condition | Tier 1 | Tier 2 | Tier 3 | Tier 4 | Tier 5 | all |
|---|---|---|---|---|---|---|
| pg-no-onto | 0.78 | 0.17 | 0.49 | 0.26 | 0.90 | **0.52** |
| pg-onto | 0.78 | **0.90** | 0.26 | 0.15 | 0.86 | **0.59** |
| neo-no-onto | 0.39 | 0.07 | 0.28 | 0.16 | 0.86 | **0.35** |
| neo-onto | 0.36 | **0.60** | 0.15 | 0.12 | 0.88 | **0.42** |

Paired effects (exact McNemar on the same question and repeat):

- **Ontology, overall: +0.07 on both engines** (Postgres p = 0.017, Neo4j p = 0.008).
- **Ontology, Tier 2 (certified calculations): +0.73 on Postgres (p = 5e-15), +0.53 on Neo4j (p = 4e-10).** Margins, FCF,
  leverage, growth and payout are only answered correctly when the organization's definition is given; without it the model
  invents one (e.g. capex ÷ total assets instead of capex ÷ revenue).
- **Ontology, Tiers 3–4: negative** (Postgres Tier 3 −0.23, p = 4e-5; Tier 4 −0.11, p = 0.035; Neo4j Tier 3 −0.13, p = 0.013).
  Root cause (31 Postgres regressions inspected): the matched certified query is a *concept-level* template ("Risk exposure by
  category", "Multi-tier supplier path") and the model copies its shape — per-row output instead of distinct companies, or a
  recursive CTE adapted into a syntax error (Postgres syntax errors 13 → 38). The context also moved entity anchoring away from
  tickers (152 → 105 queries) toward name patterns such as `ILIKE '%TSMC%'` that miss `TAIWAN SEMICONDUCTOR …`.
- **Engine: Text2Cypher is 0.17 below Text2SQL in both arms** (p ≈ 1e-11 to 1e-14), concentrated in Tiers 1–3. Root cause is
  entity grounding, not traversal: Cypher's idiomatic exact property match `{name: '…'}` appears in 122–146 of ~320 queries per
  arm with ~0 % accuracy; when the Cypher anchors on the ticker its accuracy is 0.45–0.67 (SQL with ticker: 0.61–0.90).
- **Tier 5 (unanswerable)**: 0.86–0.90 declined in every arm; the ontology adds nothing here. Auto-Ontology's coverage score
  does separate them (half of Tier 5 scores 0.0), but answerable Tier 2 questions score 0.2–0.33, so on dev every threshold
  only added false rejections; the dev calibration chose **no gate**.

Cost of the ontology arm: +1.2–1.6 k prompt tokens (2.5–2.8 k vs 0.9–1.4 k), +2.1–2.7 s LLM time at p50, and the
`check_answerable` pre-flight at **54 s median**. NVIDIA's API reported 4,336 prompt-cache tokens for the onto arms (the longer,
repeated prefix) and 0 for no-onto.

### Hypotheses from the plan

| hypothesis | verdict |
|---|---|
| H1 the ontology lowers errors and raises accuracy on every engine, especially Tiers 1–2 | **Partly.** Large, significant gain on Tier 2 (definitions); none on Tier 1; **loss** on Tiers 3–4. Net +0.07. |
| H2 graph models need less context/fewer tokens and are faster to generate at Tiers 3–4 | **Not supported for accuracy** (Cypher −0.17). Prompt is smaller for the graph schema (881 vs 1,357 tokens without ontology) but completions are longer (1,000 vs 766) and LLM time is higher (7.4 s vs 5.7 s p50). |

## 2. Operational findings about Auto-Ontology (reportable upstream)

1. **`check_answerable` is serialized per backend process** (`auto_ontology/server/metadata/service.py`, `_run_lock`:
   "not safe to run in parallel"). At concurrency 8, 79 of 80 onto requests waited past a 180 s timeout, and abandoned
   requests kept running server-side. Throughput ceiling ≈ 1 call / 53 s. The benchmark therefore computed coverage once per
   question and reports its unloaded latency as part of `t_ctx`.
2. **`nemotron-3.5-lightning-30b-a3b` breaks Auto-Ontology's structured extraction** (HTTP 400 "Failed to deserialize the JSON
   body", 3 retries, ~89 s, then silent fallback to the raw question). `super-vl-120b` works.
3. **Column-centric layer**: metrics stored as rows (metric_definitions × financial_metrics) are invisible to compilation;
   it produced 0 SQL attributes. Curated SQL attributes were what made Tier 2 work.
4. **Coverage-candidate ids are attribute ids, not term ids** (the term arrives by name). This benchmark's own gateway first
   treated them as term ids, got 404s and silently sent an empty ontology context; those runs are archived as `*-buggy` and
   were re-run after the fix. Records now carry `ctx_chars` and `ctx_matched_sql` so an empty context is visible.
5. `POST /api/sql-attributes` with an invalid `source` value returns **500** (DB check constraint) instead of 422.
6. Ingestion picks up every schema visible through `pg_catalog`, including `pg_stat_statements` views; keep monitoring
   extensions out of ingested databases.

## 3. Database performance

Stage B, the runtime study and the second-round sweeps are implemented (`docs/METHODOLOGY.md` §4) but their results are not published: Neo4j's Acceptable Use Policy prohibits benchmarking the Aura service.


## 4. Neo4j Enterprise observability actually used

| feature | used for |
|---|---|
| `dbms.queryJmx('neo4j.metrics:*')` (1,285 beans) | per-cell deltas: server query-latency histogram, page-cache hits/faults/hit ratio, GC time, Bolt queue time, transactions |
| `SHOW TRANSACTIONS` (39 columns) + tx metadata | per-request heap/allocation, page hits/faults, lock waits, runtime, and the server-side concurrency actually reached |
| `dbms.listActiveLocks` | lock snapshots for any sampled transaction in `Blocked` state (none occurred: read-only workload) |
| `dbms.listPools`, `dbms.listConnections`, `SHOW INDEXES … readCount` | memory pools, connection counts, index reads per Stage A run |
| `CYPHER runtime=slotted/pipelined/parallel` + PROFILE args | runtime selection and verification (actual runtime, fused pipelines, partitioned operators, AvailableWorkers, GlobalMemory) |
| NODE KEY, property existence/type constraints (node and relationship), RELATIONSHIP KEY | schema integrity and planner hints in both graph loads |
| Block store format | confirmed (`block-block-1.1`) |

Not available on this Aura tier: `SHOW SERVERS/SETTINGS/PRIVILEGES`, `db.stats.*`, `dbms.listConfig`, `CREATE ROLE`;
CDC is off (console setting); GDS is a separate Aura Graph Analytics service.

## 5. Recommendations

1. **Use the ontology for definitions, and keep it out of the way for graph questions.** Certified calculations are worth
   +0.5–0.7 on calculation questions; for multi-hop questions, pass matched calculations only when the question asks for that
   quantity, and present them as definitions rather than full SQL templates.
2. **Ground entities before generating queries.** Most wrong answers on both engines are entity-literal mismatches. Add a
   resolver step (Auto-Ontology's data-object vector index, or a `resolve_company` tool returning ticker/id) and pass ids.
   This is the largest remaining lever and matters most for Cypher.
4. **Do not put `check_answerable` on a request path under concurrency** until its lock is removed or the service is
   replicated; treat it as an offline or cached pre-flight.

## 6. Limits

- One generation model, temperature 0, two repeats; 40 test questions per tier.
- Gold queries and the curated SQL attributes were written by the benchmark author (an LLM agent), with independent wording;
  all 250 items are marked `pending-human-review`. Curation can favour the onto arm on Tier 2.
- Real data is small (10 filers, 60 supply relations, max supply depth 2); deep-hop load behaviour comes from the synthetic
  `finder_scale`, which is seeded and shaped for fan-out, not realistic per row.
- Person names are stored in EDGAR order ("Jurvetson Stephen T") while questions use natural order — a deliberate but
  confounding value-normalization difficulty in some Tier 3/4 templates.
- Hardware differs (local 4C/8T slice vs Aura's 8 processors); engines are compared on server time. Aura is shared
  infrastructure, warm-cache only; Neo4j server time has 1 ms resolution; unpinned Auto-Ontology containers were active during
  parts of the runtime study (host load recorded per cell).
- Some `companies.country` values are US state codes (EDGAR's mixed state-or-country field); left as reported.
