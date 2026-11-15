"""Curate the Auto-Ontology semantic layer for finder_real through its REST API.

    uv run python -m ontology.curate

Auto-Ontology compiles one Term per table and column attributes, but our
financial metrics are rows (metric_definitions x financial_metrics), which a
column-centric layer cannot see, and it derives no calculations. A real
deployment fills that gap by curating SQL Attributes -- named, certified
queries attached to Terms -- plus term descriptions and synonyms. This script is
that curation, kept in code so it is reproducible and reviewable.

Bias control: every attribute defines a reusable concept or formula (no
question-specific filters such as a company or year), and gold queries are
written independently of these definitions. Report this curation as part of
the `onto` treatment.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import httpx
from dotenv import load_dotenv

from data_pipeline.load_structured import METRICS

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")
API = os.environ.get("AUTO_ONTOLOGY_API_URL", "http://127.0.0.1:3001")

SYNONYMS = {
    "revenue": "sales, net sales, total revenues, top line, turnover",
    "cost_of_revenue": "cost of sales, cost of goods sold, COGS",
    "gross_profit": "gross income",
    "operating_income": "operating profit, EBIT (as reported), income from operations",
    "net_income": "net earnings, profit, bottom line, net profit",
    "operating_cash_flow": "cash from operations, CFO, net cash provided by operating activities",
    "capital_expenditure": "capex, capital spending, purchases of property and equipment",
    "total_assets": "assets",
    "total_liabilities": "liabilities",
    "stockholders_equity": "shareholders' equity, book value, net worth",
    "cash_and_equivalents": "cash, cash on hand",
    "long_term_debt": "LTD, long-term borrowings",
    "inventory": "inventories, stock",
    "rd_expense": "R&D, R&D spend, research and development",
    "share_based_compensation": "SBC, stock-based compensation, equity compensation",
    "depreciation_amortization": "D&A, depreciation",
    "income_tax_expense": "tax expense, provision for income taxes",
    "eps_diluted": "diluted earnings per share, EPS",
    "dividends_paid": "cash dividends, dividend payments",
    "share_repurchases": "buybacks, stock repurchases, share buybacks",
    "dividends_per_share": "DPS, dividend per share",
}

PERIOD_NOTE = ("fiscal_year is the SEC calendar frame year (fiscal years ending in January, e.g. NVIDIA, map to the previous "
               "calendar year); fiscal_period 'FY' is the annual value, 'Q1'..'Q3' quarterly. Values are in the reporting "
               "currency given by unit (TSMC reports TWD).")


def metric_sql(code: str) -> str:
    return (f"SELECT c.name AS company, c.ticker, f.fiscal_year, f.fiscal_period, f.value AS {code}, f.unit "
            "FROM financial_metrics f JOIN metric_definitions m ON m.metric_id = f.metric_id "
            f"JOIN companies c ON c.company_id = f.company_id WHERE m.metric_code = '{code}'")


def ratio_sql(name: str, num: str, den: str, *, num_expr: str | None = None) -> str:
    """Annual ratio of two metrics per company and year, same unit."""
    n = num_expr or "n.value"
    return (
        f"SELECT c.name AS company, c.ticker, n.fiscal_year, {n} / NULLIF(d.value, 0) AS {name} "
        "FROM financial_metrics n JOIN metric_definitions mn ON mn.metric_id = n.metric_id "
        "JOIN financial_metrics d ON d.company_id = n.company_id AND d.fiscal_year = n.fiscal_year "
        "AND d.fiscal_period = n.fiscal_period AND d.unit = n.unit "
        "JOIN metric_definitions md ON md.metric_id = d.metric_id JOIN companies c ON c.company_id = n.company_id "
        f"WHERE mn.metric_code = '{num}' AND md.metric_code = '{den}' AND n.fiscal_period = 'FY'"
    )


CALCULATIONS: list[tuple[str, str, str, str]] = [
    # term, name, description, sql
    ("Financial Metric", "Gross margin", "Gross profit divided by revenue for a fiscal year. " + PERIOD_NOTE,
     ratio_sql("gross_margin", "gross_profit", "revenue")),
    ("Financial Metric", "Operating margin", "Operating income divided by revenue for a fiscal year.",
     ratio_sql("operating_margin", "operating_income", "revenue")),
    ("Financial Metric", "Net margin", "Net income divided by revenue for a fiscal year (net profit margin).",
     ratio_sql("net_margin", "net_income", "revenue")),
    ("Financial Metric", "R&D intensity", "Research and development expense divided by revenue (R&D as % of sales).",
     ratio_sql("rd_intensity", "rd_expense", "revenue")),
    ("Financial Metric", "Debt-to-equity ratio", "Long-term debt divided by stockholders' equity (leverage).",
     ratio_sql("debt_to_equity", "long_term_debt", "stockholders_equity")),
    ("Financial Metric", "Return on equity", "Net income divided by stockholders' equity at year end (ROE).",
     ratio_sql("return_on_equity", "net_income", "stockholders_equity")),
    ("Financial Metric", "Capex intensity", "Capital expenditure divided by revenue.",
     ratio_sql("capex_intensity", "capital_expenditure", "revenue")),
    ("Financial Metric", "Free cash flow",
     "Free cash flow = operating cash flow minus capital expenditure, per company and fiscal year (FCF).",
     "SELECT c.name AS company, c.ticker, o.fiscal_year, o.value - k.value AS free_cash_flow, o.unit "
     "FROM financial_metrics o JOIN metric_definitions mo ON mo.metric_id = o.metric_id "
     "JOIN financial_metrics k ON k.company_id = o.company_id AND k.fiscal_year = o.fiscal_year AND k.fiscal_period = o.fiscal_period AND k.unit = o.unit "
     "JOIN metric_definitions mk ON mk.metric_id = k.metric_id JOIN companies c ON c.company_id = o.company_id "
     "WHERE mo.metric_code = 'operating_cash_flow' AND mk.metric_code = 'capital_expenditure' AND o.fiscal_period = 'FY'"),
    ("Financial Metric", "Free cash flow margin", "Free cash flow (operating cash flow minus capex) divided by revenue.",
     "SELECT c.name AS company, c.ticker, o.fiscal_year, (o.value - k.value) / NULLIF(r.value, 0) AS fcf_margin "
     "FROM financial_metrics o JOIN metric_definitions mo ON mo.metric_id = o.metric_id AND mo.metric_code = 'operating_cash_flow' "
     "JOIN financial_metrics k ON k.company_id = o.company_id AND k.fiscal_year = o.fiscal_year AND k.fiscal_period = o.fiscal_period AND k.unit = o.unit "
     "JOIN metric_definitions mk ON mk.metric_id = k.metric_id AND mk.metric_code = 'capital_expenditure' "
     "JOIN financial_metrics r ON r.company_id = o.company_id AND r.fiscal_year = o.fiscal_year AND r.fiscal_period = o.fiscal_period AND r.unit = o.unit "
     "JOIN metric_definitions mr ON mr.metric_id = r.metric_id AND mr.metric_code = 'revenue' "
     "JOIN companies c ON c.company_id = o.company_id WHERE o.fiscal_period = 'FY'"),
    ("Financial Metric", "Revenue growth (YoY)", "Year-over-year revenue growth: (revenue / prior-year revenue) - 1.",
     "SELECT c.name AS company, c.ticker, cur.fiscal_year, cur.value / NULLIF(prev.value, 0) - 1 AS revenue_growth "
     "FROM financial_metrics cur JOIN metric_definitions m ON m.metric_id = cur.metric_id AND m.metric_code = 'revenue' "
     "JOIN financial_metrics prev ON prev.company_id = cur.company_id AND prev.metric_id = cur.metric_id "
     "AND prev.fiscal_year = cur.fiscal_year - 1 AND prev.fiscal_period = cur.fiscal_period AND prev.unit = cur.unit "
     "JOIN companies c ON c.company_id = cur.company_id WHERE cur.fiscal_period = 'FY'"),
    ("Financial Metric", "Total shareholder payout",
     "Cash returned to shareholders in a fiscal year = dividends paid + share repurchases (missing component counts as 0).",
     "SELECT c.name AS company, c.ticker, f.fiscal_year, sum(f.value) AS shareholder_payout, f.unit FROM financial_metrics f "
     "JOIN metric_definitions m ON m.metric_id = f.metric_id JOIN companies c ON c.company_id = f.company_id "
     "WHERE m.metric_code IN ('dividends_paid', 'share_repurchases') AND f.fiscal_period = 'FY' GROUP BY c.name, c.ticker, f.fiscal_year, f.unit"),
    ("Company Executive", "Board interlock",
     "A person who holds a director or officer role at two or more companies (shared board members / interlocking directorates).",
     "SELECT p.full_name, array_agg(c.name ORDER BY c.name) AS companies, count(*) AS seats FROM company_executives ce "
     "JOIN persons p ON p.person_id = ce.person_id JOIN companies c ON c.company_id = ce.company_id "
     "GROUP BY p.person_id, p.full_name HAVING count(*) > 1"),
    ("Company Executive", "Officer by title",
     "Officers of a company with their title (CEO, CFO, ...); is_officer marks officers, is_director board members, "
     "is_ten_percent_owner 10% owners. Roles come from insider Forms 3/4.",
     "SELECT c.name AS company, p.full_name, ce.officer_title, ce.is_director, ce.last_seen_date FROM company_executives ce "
     "JOIN persons p ON p.person_id = ce.person_id JOIN companies c ON c.company_id = ce.company_id WHERE ce.is_officer"),
    ("Institutional Holding", "Institutional ownership value",
     "Total market value (USD) and shares of a company held by the 13F filers in the data, per quarter-end report_period.",
     "SELECT c.name AS company, h.report_period, sum(h.value_usd) AS value_usd, sum(h.shares) AS shares, count(*) AS holders "
     "FROM institutional_holdings h JOIN companies c ON c.company_id = h.company_id GROUP BY c.name, h.report_period"),
    ("Institutional Holding", "Shared institutional holders",
     "Pairs of companies held by the same institution in the same report_period (common ownership).",
     "SELECT i.name AS institution, a.report_period, ca.name AS company_a, cb.name AS company_b FROM institutional_holdings a "
     "JOIN institutional_holdings b ON b.institution_id = a.institution_id AND b.report_period = a.report_period AND b.company_id > a.company_id "
     "JOIN institutional_holders i ON i.institution_id = a.institution_id JOIN companies ca ON ca.company_id = a.company_id "
     "JOIN companies cb ON cb.company_id = b.company_id"),
    ("Supply Chain Relation", "Direct suppliers",
     "Companies that supply (or manufacture for, license to, launch for, or partner in a JV with) a customer company. "
     "Direction: supplier_company_id supplies customer_company_id.",
     "SELECT s.name AS supplier, c.name AS customer, r.relation_type, r.product_category FROM supply_chain_relations r "
     "JOIN companies s ON s.company_id = r.supplier_company_id JOIN companies c ON c.company_id = r.customer_company_id"),
    ("Supply Chain Relation", "Multi-tier supplier path",
     "Upstream suppliers of a company through any number of tiers (supplier of a supplier ...), with the tier depth; "
     "a recursive walk over supply_chain_relations that stops on cycles.",
     "WITH RECURSIVE up(customer_id, supplier_id, depth, path) AS ("
     "SELECT customer_company_id, supplier_company_id, 1, ARRAY[customer_company_id, supplier_company_id] FROM supply_chain_relations "
     "UNION ALL SELECT up.customer_id, r.supplier_company_id, up.depth + 1, up.path || r.supplier_company_id FROM up "
     "JOIN supply_chain_relations r ON r.customer_company_id = up.supplier_id "
     "WHERE NOT r.supplier_company_id = ANY(up.path) AND up.depth < 6) "
     "SELECT c.name AS company, s.name AS upstream_supplier, min(up.depth) AS tier FROM up "
     "JOIN companies c ON c.company_id = up.customer_id JOIN companies s ON s.company_id = up.supplier_id GROUP BY c.name, s.name"),
    ("Risk Factor", "Risk exposure by category",
     "Number of disclosed risk factors per company and risk category (shared exposure when two companies cite the same category).",
     "SELECT c.name AS company, rc.name AS risk_category, count(*) AS risk_factors FROM risk_factors r "
     "JOIN companies c ON c.company_id = r.company_id JOIN risk_categories rc ON rc.risk_category_id = r.risk_category_id GROUP BY c.name, rc.name"),
    ("Risk Factor", "Counterparty-dependent risks",
     "Risk factors that name another company the filer depends on (mentioned_company_id).",
     "SELECT c.name AS company, m.name AS mentioned_company, r.title FROM risk_factors r JOIN companies c ON c.company_id = r.company_id "
     "JOIN companies m ON m.company_id = r.mentioned_company_id"),
    ("Legal Proceeding", "Pending legal proceedings",
     "Legal proceedings whose status is pending (open litigation, investigations, regulatory actions).",
     "SELECT c.name AS company, l.case_name, l.proceeding_type, l.forum FROM legal_proceedings l "
     "JOIN companies c ON c.company_id = l.company_id WHERE l.status = 'pending'"),
    ("Related Party Transaction", "Insider related-party transactions",
     "Related-party transactions with an individual insider (related_person_id) or an entity they control, with amounts in USD.",
     "SELECT c.name AS company, t.related_party_name, p.full_name AS insider, t.transaction_type, t.amount_usd, t.fiscal_year "
     "FROM footnote_related_parties t JOIN companies c ON c.company_id = t.company_id LEFT JOIN persons p ON p.person_id = t.related_person_id"),
]

TERM_PATCHES = {
    "Company": ("A business or government entity: the ten SEC issuers (is_sec_filer) plus counterparties named in their filings "
                "(suppliers, customers, JV partners, agencies).", ["issuer", "firm", "corporation", "registrant", "counterparty"]),
    "Institutional Holder": ("An investment manager that files Form 13F (e.g. Vanguard, BlackRock).",
                             ["institutional investor", "fund manager", "asset manager", "13F filer", "shareholder"]),
    "Company Executive": (None, ["director", "officer", "board member", "insider", "CEO", "CFO"]),
    "Supply Chain Relation": (None, ["supplier", "customer", "vendor", "manufacturing partner", "joint venture", "supply chain"]),
    "Financial Metric": (None, ["financials", "financial statement value", "XBRL fact", "reported figure"]),
    "Legal Proceeding": (None, ["lawsuit", "litigation", "case", "investigation", "class action"]),
    "Risk Factor": (None, ["risk", "risk disclosure", "Item 1A"]),
    "Related Party Transaction": (None, ["related-party deal", "insider transaction", "affiliate transaction"]),
}


def main() -> None:
    http = httpx.Client(base_url=API, timeout=120)
    terms = {t["name"]: t["id"] for t in http.get("/api/terms", params={"limit": 100}).json()["terms"]}
    created, failed = 0, []
    for name, (desc, syn) in TERM_PATCHES.items():
        # TermUpdate has no synonyms field, so synonyms go into the (certified) description.
        base = desc or http.get(f"/api/terms/{terms[name]}").json()["data"].get("description") or ""
        base = base.split(" Also called:")[0]
        body = {"description": f"{base} Also called: {', '.join(syn)}.", "description_certified": True}
        r = http.patch(f"/api/terms/{terms[name]}", json=body)
        if r.status_code >= 400:
            failed.append((name, r.status_code, r.text[:200]))
    attrs = [("Financial Metric", n, f"{d} Synonyms: {SYNONYMS.get(code, '')}. Category: {cat}. {PERIOD_NOTE}", metric_sql(code))
             for code, n, cat, _u, d, _g, _i in METRICS] + CALCULATIONS
    for term, name, desc, sql in attrs:
        v = http.post("/api/sql-attributes/validate", json={"expression": sql, "term_id": terms[term]})
        if v.status_code >= 400 or not v.json().get("data", {}).get("valid"):
            failed.append((name, v.status_code, v.text[:200]))
            continue
        r = http.post("/api/sql-attributes", json={"name": name, "description": desc, "expression": sql,
                                                   "term_id": terms[term], "source": "manual"})
        if r.status_code == 409:
            continue
        if r.status_code >= 400:
            failed.append((name, r.status_code, r.text[:200]))
        else:
            created += 1
    print(f"created {created} sql attributes; failures: {json.dumps(failed, indent=1)}")


if __name__ == "__main__":
    main()
