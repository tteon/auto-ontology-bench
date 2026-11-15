# auto-ontology-bench

Does a business ontology help an LLM write correct database queries, and does a graph database help more than a
relational one? This benchmark measures **ontology use ([NVIDIA Auto-Ontology](https://github.com/NVIDIA/auto-ontology))
× database model (PostgreSQL vs Neo4j Enterprise)** on SEC EDGAR filings organized into the FinDER 8 categories
(Accounting, Company Overview, Financials, Footnotes, Governance, Legal, Risk, Shareholder Return), and ships the
tooling for a Neo4j runtime study (slotted / pipelined / parallel vs Postgres single / parallel workers), scale and
hop-depth sweeps, and a mixed-workload runtime-policy test with Neo4j Enterprise observability.

- Results (accuracy): [`docs/FINDINGS.md`](docs/FINDINGS.md) · data in [`results/`](results/)
- How every experiment works: [`docs/METHODOLOGY.md`](docs/METHODOLOGY.md)
- Which Neo4j Enterprise features are used, and where: [`docs/ENTERPRISE_AUDIT.md`](docs/ENTERPRISE_AUDIT.md)

> **Database performance numbers are not published here.** Neo4j's Acceptable Use Policy prohibits benchmarking the
> Aura service. All performance experiments are implemented and runnable; if you run them, check the terms that apply
> to your Neo4j deployment (Aura, or a self-hosted licence) before publishing what you measure.

## What you need

| component | notes |
|---|---|
| Docker + Compose v2, [uv](https://docs.astral.sh/uv/), Python ≥ 3.12 | |
| NVIDIA API key (`nvapi-…`, [build.nvidia.com](https://build.nvidia.com)) | query generation, extraction, embeddings (Auto-Ontology) |
| Neo4j **Enterprise** 5.x | AuraDB Professional/Enterprise or self-hosted `neo4j:5-enterprise`; APOC for the W4r workload |
| [NVIDIA Auto-Ontology](https://github.com/NVIDIA/auto-ontology) running locally | the `onto` arms call its REST API (default `http://127.0.0.1:3001`) |
| SEC User-Agent (`SEC_UA="Name email"`) | only for rebuilding the data from EDGAR |

## Quick start (published data, accuracy study)

```bash
cp .env.example .env            # fill in keys, Neo4j URI/password, Postgres passwords
make pg                         # benchmark Postgres 16 in Docker
make restore                    # finder_real from data/finder_real.sql.gz (the data the gold answers were built on)
make graph                      # load it into Neo4j (wipes the target database) + consistency gate
```

Auto-Ontology (for the `onto` arms):

1. In Auto-Ontology's `.env`, point `CONNECTION_STRINGS` at the benchmark database, e.g.
   `postgresql://bench_ro:<BENCH_PG_RO_PASSWORD>@finder-pg:5432/finder_real`; the benchmark Postgres joins
   Auto-Ontology's compose network (`AUTO_ONTOLOGY_NETWORK`, default `auto-ontology_default`).
2. Restart its backend and ingestion service, enable **Settings → Semantic Compilation**, and wait for it to finish.
3. `make ontology` — adds the curated SQL attributes, exports the Apache Ossie model, and fills the
   `check_answerable` cache. The shipped `ontology/coverage_cache.json` was built against our instance; term names
   come from Auto-Ontology's (LLM-driven) compilation and can differ on yours, so delete the file to rebuild it.
   Auto-Ontology serializes `check_answerable`, so a full rebuild takes several hours (it is resumable).

Then:

```bash
make test                       # verifier unit tests
make smoke                      # 2 questions per tier x 4 conditions
make stage-a                    # 2x2 conditions on the 200 test questions, 2 repeats
make report                     # results/REPORT.md
```

`make data` rebuilds `finder_real` from EDGAR instead of the dump (needs `SEC_UA` and the NVIDIA key; EDGAR changes
over time, so answers can differ from the published gold).

## Performance experiments (not published; run at your own discretion)

```bash
make stage-b                                        # DB-only replay of verified Stage A queries
make scale                                          # synthetic 10k / 30k / 100k-company databases in Postgres
scripts/run_phases.sh finder_s10k s10k load verify sweep depth mixed tuning
```

Each phase records server-side time, plans, and Neo4j Enterprise metrics (JMX registry deltas, `SHOW TRANSACTIONS`
sampling, PROFILE arguments). See `docs/METHODOLOGY.md` §4–5.

## Layout

| path | what |
|---|---|
| `data_pipeline/` | EDGAR fetch, structured load (XBRL frames, Forms 3/4, 13F), LLM extraction with verbatim-evidence check, synthetic scale-up, schema |
| `graph_sync/` | Postgres → Neo4j loaders (sequential and concurrent-transaction), mapping, consistency gate |
| `ontology/` | Auto-Ontology curation, Ossie export bridge, `check_answerable` cache |
| `benchmarks/` | CQ set (250 items; gold SQL and Cypher verified equal on both engines), verifier + tests |
| `gateway/` | 2×2 condition router, ontology context, streaming LLM timing, executors (server time), OpenTelemetry spans |
| `enterprise/` | Neo4j Enterprise probe, metrics collectors (JMX, `SHOW TRANSACTIONS`, locks, pools, plans), constraint A/B |
| `load_test/` | load generator, Stage A/B, runtime study, depth sweep, mixed workload, tuning, calibration, reports |
| `docker/compose.yml` | benchmark Postgres pinned to a CPU set (`PG_CPUSET`) |
| `results/` | accuracy records (per question, condition and repeat) and tables |

## Licence

Apache-2.0 (`LICENSE`). SEC EDGAR data is public; `data/finder_real.sql.gz` also contains LLM-extracted fields, each
tied to the filing accession it was extracted from.
