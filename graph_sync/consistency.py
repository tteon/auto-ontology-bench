"""Cross-engine consistency: the same facts must come out of Postgres and Aura.

    uv run python -m graph_sync.consistency --db finder_real
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from benchmarks.verifier import compare
from gateway.executors import NeoExecutor, PgExecutor

CHECKS: list[tuple[str, str, str]] = [
    ("companies", "SELECT count(*) FROM companies", "MATCH (c:Company) RETURN count(c)"),
    ("persons", "SELECT count(*) FROM persons", "MATCH (p:Person) RETURN count(p)"),
    ("roles", "SELECT count(*) FROM company_executives", "MATCH ()-[r:HOLDS_ROLE]->() RETURN count(r)"),
    ("holdings", "SELECT count(*), sum(value_usd) FROM institutional_holdings",
     "MATCH ()-[h:HOLDS]->() RETURN count(h), sum(h.value_usd)"),
    ("supply", "SELECT count(*) FROM supply_chain_relations", "MATCH ()-[s:SUPPLIES]->() RETURN count(s)"),
    ("metrics", "SELECT count(*), sum(value) FROM financial_metrics", "MATCH (f:FinancialMetric) RETURN count(f), sum(f.value)"),
    ("revenue_2024_by_company",
     """SELECT c.name, f.value FROM financial_metrics f JOIN companies c USING (company_id)
        JOIN metric_definitions m USING (metric_id) WHERE m.metric_code = 'revenue' AND f.fiscal_year = 2024 AND f.fiscal_period = 'FY'""",
     """MATCH (c:Company)-[:REPORTED]->(f:FinancialMetric {fiscal_year: 2024, fiscal_period: 'FY'})-[:OF_METRIC]->(:MetricDefinition {metric_code: 'revenue'})
        RETURN c.name, f.value"""),
    ("interlocks",
     """SELECT p.full_name, count(*) FROM company_executives ce JOIN persons p USING (person_id)
        GROUP BY p.person_id, p.full_name HAVING count(*) > 1""",
     """MATCH (p:Person)-[:HOLDS_ROLE]->(c:Company) WITH p, count(c) AS n WHERE n > 1 RETURN p.full_name, n"""),
    ("shared_holders_tsla_nvda",
     """SELECT i.name FROM institutional_holders i
        WHERE EXISTS (SELECT 1 FROM institutional_holdings h JOIN companies c USING (company_id) WHERE h.institution_id = i.institution_id AND c.ticker = 'TSLA')
          AND EXISTS (SELECT 1 FROM institutional_holdings h JOIN companies c USING (company_id) WHERE h.institution_id = i.institution_id AND c.ticker = 'NVDA')""",
     """MATCH (i:Institution)-[:HOLDS]->(:Company {ticker: 'TSLA'}) WHERE EXISTS { (i)-[:HOLDS]->(:Company {ticker: 'NVDA'}) }
        RETURN DISTINCT i.name"""),
    ("two_hop_supply",
     """SELECT count(*) FROM supply_chain_relations a JOIN supply_chain_relations b ON a.customer_company_id = b.supplier_company_id""",
     """MATCH (:Company)-[:SUPPLIES]->(:Company)-[:SUPPLIES]->(:Company) RETURN count(*)"""),
    ("risk_categories_per_company",
     """SELECT c.name, rc.name, count(*) FROM risk_factors r JOIN companies c USING (company_id)
        JOIN risk_categories rc USING (risk_category_id) GROUP BY 1, 2""",
     """MATCH (c:Company)-[:HAS_RISK]->(:RiskFactor)-[:IN_CATEGORY]->(rc:RiskCategory) RETURN c.name, rc.name, count(*)"""),
]


async def main(db: str) -> int:
    pg, neo = PgExecutor(db, 2), NeoExecutor(2)
    await pg.start()
    await neo.start()
    results, failed = [], 0
    for name, sql, cy in CHECKS:
        a, b = await pg.run(sql), await neo.run(cy)
        verdict = compare(a.get("rows") or [], b.get("rows") if b.get("ok") else None)
        ok = a.get("ok") and b.get("ok") and verdict["strict"]
        failed += not ok
        results.append({"check": name, "ok": bool(ok), "pg_rows": a.get("row_count"), "neo_rows": b.get("row_count"),
                        "pg_error": a.get("error"), "neo_error": b.get("error"),
                        "sample": [list(map(str, r)) for r in (a.get("rows") or [])[:2]]})
        print(f"{'OK ' if ok else 'FAIL'} {name:30} pg={a.get('row_count')} neo={b.get('row_count')} {a.get('error') or ''}{b.get('error') or ''}")
    await pg.close()
    await neo.close()
    out = Path(__file__).resolve().parents[1] / "results" / f"consistency_{db}.json"
    out.write_text(json.dumps(results, indent=1))
    return failed


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="finder_real")
    raise SystemExit(asyncio.run(main(ap.parse_args().db)))
