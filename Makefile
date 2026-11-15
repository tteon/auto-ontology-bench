# auto-ontology-bench: end-to-end pipeline. Every target reads settings from .env (see .env.example).
# Quick path (no EDGAR fetch / LLM extraction): make pg restore graph ontology cq stage-a
# Full path from EDGAR:                         make pg data graph ontology cq stage-a

include .env
export

PG = docker exec -i finder-pg psql -q -U bench
PY = uv run python -m

.PHONY: help pg restore data graph ontology cq smoke stage-a stage-b scale sweep runtime report test

help:
	@grep -hE '^[a-z-]+:.*?## ' Makefile | awk -F':.*?## ' '{printf "  %-10s %s\n", $$1, $$2}'

pg: ## start benchmark Postgres 16 (docker/compose.yml) and the read-only role
	docker compose -f docker/compose.yml --env-file .env up -d --wait
	$(PG) -d postgres -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"
	$(PG) -d postgres -c "DO \$$\$$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bench_ro') THEN \
	  CREATE ROLE bench_ro LOGIN PASSWORD '$(BENCH_PG_RO_PASSWORD)'; END IF; END \$$\$$; \
	  ALTER ROLE bench_ro SET default_transaction_read_only = on; ALTER ROLE bench_ro SET statement_timeout = '30s'; \
	  GRANT pg_read_all_stats TO bench_ro;"

restore: ## load finder_real from data/finder_real.sql.gz (the exact data the CQ gold answers were built on)
	$(PG) -d postgres -c "DROP DATABASE IF EXISTS finder_real WITH (FORCE)" -c "CREATE DATABASE finder_real"
	gunzip -c data/finder_real.sql.gz | $(PG) -d finder_real
	$(PG) -d finder_real -c "GRANT CONNECT ON DATABASE finder_real TO bench_ro; GRANT USAGE ON SCHEMA public TO bench_ro; GRANT SELECT ON ALL TABLES IN SCHEMA public TO bench_ro;"

data: ## rebuild finder_real from SEC EDGAR (needs SEC_UA; LLM extraction needs NVIDIA_API_KEY)
	$(PG) -d postgres -c "DROP DATABASE IF EXISTS finder_real WITH (FORCE)" -c "CREATE DATABASE finder_real"
	$(PG) -d finder_real < data_pipeline/schema.sql
	$(PY) data_pipeline.fetch
	$(PY) data_pipeline.load_structured
	$(PY) data_pipeline.extract --load

graph: ## load finder_real into Neo4j (wipes the target database) and run the consistency gate
	$(PY) graph_sync.load --db finder_real --wipe
	$(PY) graph_sync.consistency --db finder_real

ontology: ## curate Auto-Ontology (after it ingested finder_real + Semantic Compilation), export Ossie, cache coverage
	$(PY) ontology.curate
	$(PY) ontology.ossie_bridge
	$(PY) ontology.cache_coverage

cq: ## rebuild the CQ set (gold SQL and Cypher must agree on both engines)
	$(PY) benchmarks.build_cq

test: ## unit tests
	uv run pytest -q benchmarks/tests

smoke: ## 2 questions per tier x 4 conditions, concurrency 1
	$(PY) load_test.stage_a --split dev --limit-per-tier 2 --concurrency 1 --tag smoke

stage-a: ## accuracy: 2x2 conditions on the test split, 2 repeats
	$(PY) load_test.stage_a --split test --conditions pg-no-onto,neo-no-onto --repeats 2 --concurrency 8 --tag test-noonto
	$(PY) load_test.stage_a --split test --conditions pg-onto,neo-onto --repeats 2 --concurrency 8 --only-cached --tag test-onto

stage-b: ## DB-only replay of the verified Stage A queries
	$(PY) load_test.stage_b --from-tag test-noonto --db finder_real --tag stageB-real

scale: ## synthetic scale databases in Postgres (10k / 30k / 100k companies)
	$(PY) data_pipeline.scale --companies 10000 --db finder_s10k
	$(PY) data_pipeline.scale --companies 30000 --db finder_scale
	$(PY) data_pipeline.scale --companies 100000 --db finder_s100k

sweep: ## one scale factor end to end: make sweep DB=finder_s10k SF=s10k (loads Neo4j, then every phase)
	scripts/run_phases.sh $(DB) $(SF) load verify sweep depth mixed tuning

runtime: ## runtime study at 30k: Neo4j slotted/pipelined/parallel vs Postgres single/parallel
	$(PY) load_test.runtime_study --db finder_scale --tag rt-scale

report: ## Markdown + CSV report from results/
	$(PY) load_test.analyze --stage-a test-noonto,test-onto --out results/REPORT.md
