"""Load a finder Postgres database (finder_real or finder_scale) into Aura.

    uv run python -m graph_sync.load --db finder_real [--wipe]

Graph model (the mapping is also written to graph_sync/mapping.json for the
gateway's ontology context):

  (:Company)                       <- companies
  (:Filing)-[:SUBJECT_COMPANY]->(:Company)
  (:Person)-[:HOLDS_ROLE {..}]->(:Company)                 <- company_executives (join table -> relationship)
  (:Institution)-[:HOLDS {..}]->(:Company)                 <- institutional_holdings
  (:Company)-[:SUPPLIES {..}]->(:Company)                  <- supply_chain_relations (supplier -> customer)
  (:Company)-[:REPORTED]->(:FinancialMetric)-[:OF_METRIC]->(:MetricDefinition)
  (:Company)-[:HAS_RISK]->(:RiskFactor)-[:IN_CATEGORY]->(:RiskCategory), (:RiskFactor)-[:MENTIONS]->(:Company)
  (:Company)-[:PARTY_TO]->(:LegalProceeding)-[:AGAINST]->(:Company)
  (:Company)-[:HAS_RELATED_PARTY_TXN]->(:RelatedPartyTransaction)-[:WITH_COMPANY|WITH_PERSON]->(...)
  (:FinancialMetric|RiskFactor|LegalProceeding|RelatedPartyTransaction)-[:SOURCED_FROM]->(:Filing)

Enterprise schema features used: NODE KEY, property existence and property
type constraints (node and relationship), RELATIONSHIP KEY. They double as
planner hints (count store, index-backed ORDER BY, filter removal).
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg

from enterprise.conn import DATABASE, driver

ROOT = Path(__file__).resolve().parent
BATCH = int(os.environ.get("GRAPH_BATCH", "10000"))

NODES: list[dict[str, Any]] = [
    {"label": "Company", "table": "companies", "key": "company_id",
     "cols": ["company_id", "cik", "ticker", "name", "sic_code", "sic_description", "state_of_incorporation", "country",
              "fiscal_year_end", "is_sec_filer", "annual_report_form", "entity_type"]},
    {"label": "Filing", "table": "filings", "key": "filing_id",
     "cols": ["filing_id", "accession_no", "filer_cik", "form_type", "filing_date", "report_date", "primary_doc"]},
    {"label": "Person", "table": "persons", "key": "person_id", "cols": ["person_id", "owner_cik", "full_name"]},
    {"label": "Institution", "table": "institutional_holders", "key": "institution_id", "cols": ["institution_id", "cik", "name"]},
    {"label": "MetricDefinition", "table": "metric_definitions", "key": "metric_id",
     "cols": ["metric_id", "metric_code", "name", "category", "unit", "description"]},
    {"label": "FinancialMetric", "table": "financial_metrics", "key": "financial_metric_id",
     "cols": ["financial_metric_id", "fiscal_year", "fiscal_period", "period_start", "period_end", "value", "unit", "xbrl_tag"]},
    {"label": "RiskCategory", "table": "risk_categories", "key": "risk_category_id", "cols": ["risk_category_id", "name", "description"]},
    {"label": "RiskFactor", "table": "risk_factors", "key": "risk_factor_id", "cols": ["risk_factor_id", "title", "summary"]},
    {"label": "LegalProceeding", "table": "legal_proceedings", "key": "legal_proceeding_id",
     "cols": ["legal_proceeding_id", "case_name", "proceeding_type", "forum", "counterparty", "status", "amount_usd", "description"]},
    {"label": "RelatedPartyTransaction", "table": "footnote_related_parties", "key": "related_party_id",
     "cols": ["related_party_id", "related_party_name", "relationship", "transaction_type", "amount_usd", "fiscal_year", "description"]},
]

# Relationships from FK columns: (type, table, from_label, from_col, to_label, to_col, props, rel_key)
RELS: list[tuple[str, str, str, str, str, str, list[str], str | None]] = [
    ("SUBJECT_COMPANY", "filings", "Filing", "filing_id", "Company", "company_id", [], None),
    ("HOLDS_ROLE", "company_executives", "Person", "person_id", "Company", "company_id",
     ["company_executive_id", "is_director", "is_officer", "is_ten_percent_owner", "officer_title", "first_seen_date",
      "last_seen_date", "source_filing_id"], "company_executive_id"),
    ("HOLDS", "institutional_holdings", "Institution", "institution_id", "Company", "company_id",
     ["holding_id", "report_period", "shares", "value_usd", "source_filing_id"], "holding_id"),
    ("SUPPLIES", "supply_chain_relations", "Company", "supplier_company_id", "Company", "customer_company_id",
     ["relation_id", "relation_type", "product_category", "contract_value_usd", "since_year", "evidence", "source_filing_id"], "relation_id"),
    ("REPORTED", "financial_metrics", "Company", "company_id", "FinancialMetric", "financial_metric_id", [], None),
    ("OF_METRIC", "financial_metrics", "FinancialMetric", "financial_metric_id", "MetricDefinition", "metric_id", [], None),
    ("SOURCED_FROM", "financial_metrics", "FinancialMetric", "financial_metric_id", "Filing", "source_filing_id", [], None),
    ("HAS_RISK", "risk_factors", "Company", "company_id", "RiskFactor", "risk_factor_id", [], None),
    ("IN_CATEGORY", "risk_factors", "RiskFactor", "risk_factor_id", "RiskCategory", "risk_category_id", [], None),
    ("MENTIONS", "risk_factors", "RiskFactor", "risk_factor_id", "Company", "mentioned_company_id", [], None),
    ("SOURCED_FROM", "risk_factors", "RiskFactor", "risk_factor_id", "Filing", "filing_id", [], None),
    ("PARTY_TO", "legal_proceedings", "Company", "company_id", "LegalProceeding", "legal_proceeding_id", [], None),
    ("AGAINST", "legal_proceedings", "LegalProceeding", "legal_proceeding_id", "Company", "counterparty_company_id", [], None),
    ("SOURCED_FROM", "legal_proceedings", "LegalProceeding", "legal_proceeding_id", "Filing", "filing_id", [], None),
    ("HAS_RELATED_PARTY_TXN", "footnote_related_parties", "Company", "company_id", "RelatedPartyTransaction", "related_party_id", [], None),
    ("WITH_COMPANY", "footnote_related_parties", "RelatedPartyTransaction", "related_party_id", "Company", "related_company_id", [], None),
    ("WITH_PERSON", "footnote_related_parties", "RelatedPartyTransaction", "related_party_id", "Person", "related_person_id", [], None),
    ("SOURCED_FROM", "footnote_related_parties", "RelatedPartyTransaction", "related_party_id", "Filing", "filing_id", [], None),
]

CONSTRAINTS = [
    *[f"CREATE CONSTRAINT {n['label'].lower()}_key IF NOT EXISTS FOR (x:{n['label']}) REQUIRE x.{n['key']} IS NODE KEY" for n in NODES],
    *[f"CREATE CONSTRAINT {n['label'].lower()}_key_type IF NOT EXISTS FOR (x:{n['label']}) REQUIRE x.{n['key']} IS :: INTEGER" for n in NODES],
    "CREATE CONSTRAINT company_name_exists IF NOT EXISTS FOR (c:Company) REQUIRE c.name IS NOT NULL",
    "CREATE CONSTRAINT company_filer_type IF NOT EXISTS FOR (c:Company) REQUIRE c.is_sec_filer IS :: BOOLEAN",
    "CREATE CONSTRAINT metric_code_unique IF NOT EXISTS FOR (m:MetricDefinition) REQUIRE m.metric_code IS UNIQUE",
    "CREATE CONSTRAINT fm_value_exists IF NOT EXISTS FOR (f:FinancialMetric) REQUIRE f.value IS NOT NULL",
    "CREATE CONSTRAINT fm_value_type IF NOT EXISTS FOR (f:FinancialMetric) REQUIRE f.value IS :: FLOAT",
    "CREATE CONSTRAINT fm_year_type IF NOT EXISTS FOR (f:FinancialMetric) REQUIRE f.fiscal_year IS :: INTEGER",
    "CREATE CONSTRAINT fm_year_exists IF NOT EXISTS FOR (f:FinancialMetric) REQUIRE f.fiscal_year IS NOT NULL",
    "CREATE CONSTRAINT riskcat_name_unique IF NOT EXISTS FOR (r:RiskCategory) REQUIRE r.name IS UNIQUE",
    "CREATE CONSTRAINT supplies_key IF NOT EXISTS FOR ()-[r:SUPPLIES]-() REQUIRE r.relation_id IS RELATIONSHIP KEY",
    "CREATE CONSTRAINT supplies_type_exists IF NOT EXISTS FOR ()-[r:SUPPLIES]-() REQUIRE r.relation_type IS NOT NULL",
    "CREATE CONSTRAINT holds_key IF NOT EXISTS FOR ()-[r:HOLDS]-() REQUIRE r.holding_id IS RELATIONSHIP KEY",
    "CREATE CONSTRAINT holds_shares_type IF NOT EXISTS FOR ()-[r:HOLDS]-() REQUIRE r.shares IS :: INTEGER",
    "CREATE CONSTRAINT holds_value_type IF NOT EXISTS FOR ()-[r:HOLDS]-() REQUIRE r.value_usd IS :: FLOAT",
    "CREATE CONSTRAINT role_key IF NOT EXISTS FOR ()-[r:HOLDS_ROLE]-() REQUIRE r.company_executive_id IS RELATIONSHIP KEY",
    "CREATE CONSTRAINT role_director_type IF NOT EXISTS FOR ()-[r:HOLDS_ROLE]-() REQUIRE r.is_director IS :: BOOLEAN",
]
INDEXES = [
    "CREATE INDEX company_ticker IF NOT EXISTS FOR (c:Company) ON (c.ticker)",
    "CREATE INDEX company_name IF NOT EXISTS FOR (c:Company) ON (c.name)",
    "CREATE INDEX company_cik IF NOT EXISTS FOR (c:Company) ON (c.cik)",
    "CREATE INDEX person_name IF NOT EXISTS FOR (p:Person) ON (p.full_name)",
    "CREATE INDEX fm_year_period IF NOT EXISTS FOR (f:FinancialMetric) ON (f.fiscal_year, f.fiscal_period)",
    "CREATE INDEX filing_form IF NOT EXISTS FOR (f:Filing) ON (f.form_type)",
    "CREATE INDEX legal_type IF NOT EXISTS FOR (l:LegalProceeding) ON (l.proceeding_type)",
    "CREATE INDEX supplies_type IF NOT EXISTS FOR ()-[r:SUPPLIES]-() ON (r.relation_type)",
    "CREATE INDEX holds_period IF NOT EXISTS FOR ()-[r:HOLDS]-() ON (r.report_period)",
]


def _py(v: Any) -> Any:
    if isinstance(v, Decimal):
        return float(v)
    return v


def pg(db: str) -> psycopg.Connection:
    return psycopg.connect(host="127.0.0.1", port=int(os.environ["BENCH_PG_PORT"]), user="bench",
                           password=os.environ["BENCH_PG_PASSWORD"], dbname=db)


def stream(conn: psycopg.Connection, sql: str):
    with conn.cursor(name="s") as cur:
        cur.itersize = BATCH
        cur.execute(sql)
        cols = [c.name for c in cur.description]
        batch = []
        for row in cur:
            batch.append({c: _py(v) for c, v in zip(cols, row) if v is not None})
            if len(batch) >= BATCH:
                yield batch
                batch = []
        if batch:
            yield batch


def wipe(d: Any) -> None:
    while True:
        r, _, _ = d.execute_query(
            "MATCH (n) WITH n LIMIT 50000 DETACH DELETE n RETURN count(*) AS c", database_=DATABASE)
        if r[0]["c"] == 0:
            break
    for kind in ("CONSTRAINTS", "INDEXES"):
        rows, _, _ = d.execute_query(f"SHOW {kind} YIELD name, type RETURN name, type", database_=DATABASE)
        for row in rows:
            if row["type"] in ("LOOKUP",):
                continue
            stmt = f"DROP {'CONSTRAINT' if kind == 'CONSTRAINTS' else 'INDEX'} {row['name']} IF EXISTS"
            try:
                d.execute_query(stmt, database_=DATABASE)
            except Exception:
                pass


def load(db: str, do_wipe: bool) -> dict[str, Any]:
    stats: dict[str, Any] = {"db": db, "nodes": {}, "rels": {}}
    t0 = time.time()
    with driver() as d, pg(db) as conn:
        if do_wipe:
            wipe(d)
        for stmt in CONSTRAINTS + INDEXES:
            d.execute_query(stmt, database_=DATABASE)
        for n in NODES:
            cypher = f"UNWIND $rows AS r CREATE (x:{n['label']}) SET x = r"
            total = 0
            for batch in stream(conn, f"SELECT {', '.join(n['cols'])} FROM {n['table']}"):
                _, summary, _ = d.execute_query(cypher, rows=batch, database_=DATABASE)
                total += summary.counters.nodes_created
            stats["nodes"][n["label"]] = total
            print(f"  {n['label']}: {total}", flush=True)
        for rtype, table, fl, fc, tl, tc, props, _ in RELS:
            fkey = next(n["key"] for n in NODES if n["label"] == fl)
            tkey = next(n["key"] for n in NODES if n["label"] == tl)
            sel = [f"{fc} AS _f", f"{tc} AS _t", *props]
            cypher = (
                f"UNWIND $rows AS r MATCH (a:{fl} {{{fkey}: r._f}}) MATCH (b:{tl} {{{tkey}: r._t}}) "
                f"CREATE (a)-[x:{rtype}]->(b)" + (" SET x = r{" + ", ".join(f".{p}" for p in props) + "}" if props else "")
            )
            total = 0
            for batch in stream(conn, f"SELECT {', '.join(sel)} FROM {table} WHERE {fc} IS NOT NULL AND {tc} IS NOT NULL"):
                _, summary, _ = d.execute_query(cypher, rows=batch, database_=DATABASE)
                total += summary.counters.relationships_created
            stats["rels"][f"{fl}-{rtype}->{tl}"] = total
            print(f"  {fl}-[:{rtype}]->{tl}: {total}", flush=True)
    stats["seconds"] = round(time.time() - t0, 1)
    return stats


def mapping() -> dict[str, Any]:
    """Table/column -> graph element mapping, used to translate ontology terms to Cypher context."""
    return {
        "nodes": {n["table"]: {"label": n["label"], "key": n["key"], "properties": n["cols"]} for n in NODES},
        "relationships": [
            {"type": t, "from_table": tb, "from": fl, "fk_from": fc, "to": tl, "fk_to": tc, "properties": p,
             "replaces_join_table": tb if p else None}
            for t, tb, fl, fc, tl, tc, p, _ in RELS
        ],
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="finder_real")
    ap.add_argument("--wipe", action="store_true")
    a = ap.parse_args()
    (ROOT / "mapping.json").write_text(json.dumps(mapping(), indent=1))
    stats = load(a.db, a.wipe)
    from graph_sync.load_fast import verify  # raises if any label / relationship type is incomplete

    stats["verification"] = verify(a.db)
    out = ROOT.parent / "results" / f"graph_load_{a.db}_{date.today()}.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(stats, indent=1))
    print(json.dumps(stats))


if __name__ == "__main__":
    main()
