"""A/B: do Enterprise property existence / type constraints change Neo4j plans and cost?

    uv run python -m enterprise.constraint_ab --db finder_s10k

Each query runs (pipelined, PROFILE once for operators/db hits, then 7 timed runs without PROFILE)
with the constraints dropped, then with them created. The Postgres equivalent of each query is timed
alongside: a NOT NULL constraint does not let Postgres skip work the way a count store does.
Constraints are restored to the loader's set at the end.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

from enterprise.conn import DATABASE, driver  # noqa: E402
from gateway.executors import NeoExecutor, PgExecutor  # noqa: E402

CONSTRAINTS = {
    "fm_value_exists": "CREATE CONSTRAINT fm_value_exists IF NOT EXISTS FOR (f:FinancialMetric) REQUIRE f.value IS NOT NULL",
    "fm_year_exists": "CREATE CONSTRAINT fm_year_exists IF NOT EXISTS FOR (f:FinancialMetric) REQUIRE f.fiscal_year IS NOT NULL",
    "fm_year_type": "CREATE CONSTRAINT fm_year_type IF NOT EXISTS FOR (f:FinancialMetric) REQUIRE f.fiscal_year IS :: INTEGER",
    "company_name_exists": "CREATE CONSTRAINT company_name_exists IF NOT EXISTS FOR (c:Company) REQUIRE c.name IS NOT NULL",
    "holds_shares_exists": "CREATE CONSTRAINT holds_shares_exists IF NOT EXISTS FOR ()-[h:HOLDS]-() REQUIRE h.shares IS NOT NULL",
}
# index used by the ORDER BY query (single-property range index; the loader's composite one starts with fiscal_year too)
EXTRA_INDEX = "CREATE INDEX fm_year IF NOT EXISTS FOR (f:FinancialMetric) ON (f.fiscal_year)"

QUERIES = [
    ("count non-null metric values",
     "MATCH (f:FinancialMetric) WHERE f.value IS NOT NULL RETURN count(*)",
     "SELECT count(*) FROM financial_metrics WHERE value IS NOT NULL"),
    ("count companies with a name",
     "MATCH (c:Company) WHERE c.name IS NOT NULL RETURN count(*)",
     "SELECT count(*) FROM companies WHERE name IS NOT NULL"),
    ("count HOLDS with shares",
     "MATCH ()-[h:HOLDS]->() WHERE h.shares IS NOT NULL RETURN count(*)",
     "SELECT count(*) FROM institutional_holdings WHERE shares IS NOT NULL"),
    ("top-10 latest fiscal years (no predicate)",
     "MATCH (f:FinancialMetric) RETURN f.fiscal_year AS y ORDER BY y DESC LIMIT 10",
     "SELECT fiscal_year FROM financial_metrics ORDER BY fiscal_year DESC LIMIT 10"),
    ("type predicate",
     "MATCH (f:FinancialMetric) WHERE f.fiscal_year IS :: INTEGER RETURN count(*)",
     "SELECT count(*) FROM financial_metrics WHERE fiscal_year IS NOT NULL"),
]


async def measure(neo: NeoExecutor, pg: PgExecutor, state: str) -> list[dict]:
    out = []
    for name, cy, sql in QUERIES:
        prof = await neo.run(cy, runtime="pipelined", profile=True, meta={"bench": "constraint-ab"})
        times = []
        for _ in range(7):
            r = await neo.run(cy, runtime="pipelined", meta={"bench": "constraint-ab"})
            times.append(r["server_ms"])
        pt = []
        for _ in range(7):
            pt.append((await pg.run(sql))["server_ms"])
        pl = prof.get("plan") or {}
        row = {"state": state, "query": name, "neo_operators": pl.get("operator_types"), "neo_db_hits": pl.get("db_hits"),
               "neo_server_ms_median": sorted(times)[3], "pg_ms_median": round(sorted(pt)[3], 2)}
        out.append(row)
        print(f"[{state:16}] {name:42} hits={row['neo_db_hits']:>9} neo={row['neo_server_ms_median']:>5}ms "
              f"pg={row['pg_ms_median']:>8}ms ops={row['neo_operators']}", flush=True)
    return out


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="finder_s10k")
    a = ap.parse_args()
    pg, neo = PgExecutor(a.db, 2), NeoExecutor(2)
    await pg.start()
    await neo.start()
    with driver() as d:
        d.execute_query(EXTRA_INDEX, database_=DATABASE)
        d.execute_query("CALL db.awaitIndexes(600)", database_=DATABASE)
        for c in CONSTRAINTS:
            d.execute_query(f"DROP CONSTRAINT {c} IF EXISTS", database_=DATABASE)
    results = await measure(neo, pg, "no constraints")
    with driver() as d:
        for stmt in CONSTRAINTS.values():
            d.execute_query(stmt, database_=DATABASE)
    results += await measure(neo, pg, "with constraints")
    with driver() as d:  # the loader does not create holds_shares_exists; leave the graph as the loader built it
        d.execute_query("DROP CONSTRAINT holds_shares_exists IF EXISTS", database_=DATABASE)
        d.execute_query("DROP INDEX fm_year IF EXISTS", database_=DATABASE)
    await pg.close()
    await neo.close()
    out = ROOT / "results" / f"constraint_ab_{a.db}.json"
    out.write_text(json.dumps(results, indent=1))


if __name__ == "__main__":
    asyncio.run(main())
