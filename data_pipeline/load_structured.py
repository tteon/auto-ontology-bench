"""Load the structured (non-LLM) part of finder_real from raw/edgar.

    uv run python -m data_pipeline.load_structured

Tables: companies, filings, metric_definitions, financial_metrics, persons,
company_executives, institutional_holders, institutional_holdings.

financial_metrics uses SEC's calendar "frame" on each XBRL fact (CY2024,
CY2024Q2, CY2024Q4I, ...) to pick one value per period: a fact is restated in
many later filings, and the frame is the SEC's own de-duplication. So
fiscal_year here is the calendar-aligned frame year, which for NVIDIA (fiscal
year ending late January) is one less than NVIDIA's own fiscal-year label.
"""

from __future__ import annotations

import json
import os
import re
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import date
from pathlib import Path

import psycopg
from dotenv import load_dotenv

from data_pipeline.edgar import RAW
from data_pipeline.fetch import COMPANIES

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

YEARS = range(2019, 2027)

# metric_code, name, category, unit, description, us-gaap tags, ifrs tags
METRICS: list[tuple[str, str, str, str, str, list[str], list[str]]] = [
    ("revenue", "Revenue", "Financials", "currency", "Total revenue recognized in the period.",
     ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"], ["Revenue"]),
    ("cost_of_revenue", "Cost of revenue", "Financials", "currency", "Cost of goods and services sold.",
     ["CostOfRevenue", "CostOfGoodsAndServicesSold"], ["CostOfSales"]),
    ("gross_profit", "Gross profit", "Financials", "currency", "Revenue minus cost of revenue, as reported.",
     ["GrossProfit"], ["GrossProfit"]),
    ("operating_income", "Operating income", "Financials", "currency", "Operating income (loss).",
     ["OperatingIncomeLoss"], ["ProfitLossFromOperatingActivities"]),
    ("net_income", "Net income", "Financials", "currency", "Net income (loss) attributable to the parent.",
     ["NetIncomeLoss"], ["ProfitLossAttributableToOwnersOfParent", "ProfitLoss"]),
    ("operating_cash_flow", "Operating cash flow", "Financials", "currency", "Net cash provided by operating activities.",
     ["NetCashProvidedByUsedInOperatingActivities"], ["CashFlowsFromUsedInOperatingActivities"]),
    ("capital_expenditure", "Capital expenditure", "Financials", "currency", "Payments to acquire property, plant and equipment.",
     ["PaymentsToAcquirePropertyPlantAndEquipment"], ["PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities"]),
    ("total_assets", "Total assets", "Financials", "currency", "Total assets at period end.",
     ["Assets"], ["Assets"]),
    ("total_liabilities", "Total liabilities", "Financials", "currency", "Total liabilities at period end.",
     ["Liabilities"], ["Liabilities"]),
    ("stockholders_equity", "Stockholders' equity", "Financials", "currency", "Equity attributable to the parent at period end.",
     ["StockholdersEquity"], ["EquityAttributableToOwnersOfParent"]),
    ("cash_and_equivalents", "Cash and cash equivalents", "Financials", "currency", "Cash and cash equivalents at period end.",
     ["CashAndCashEquivalentsAtCarryingValue"], ["CashAndCashEquivalents"]),
    ("long_term_debt", "Long-term debt", "Financials", "currency", "Long-term debt, non-current portion where reported.",
     ["LongTermDebtNoncurrent", "LongTermDebt"], ["NoncurrentPortionOfNoncurrentBorrowings", "LongtermBorrowings"]),
    ("inventory", "Inventory", "Financials", "currency", "Inventories, net, at period end.",
     ["InventoryNet"], ["Inventories"]),
    ("rd_expense", "Research and development expense", "Accounting", "currency", "Research and development expense.",
     ["ResearchAndDevelopmentExpense", "ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost"], ["ResearchAndDevelopmentExpense"]),
    ("share_based_compensation", "Share-based compensation", "Accounting", "currency", "Non-cash share-based compensation expense.",
     ["ShareBasedCompensation", "AllocatedShareBasedCompensationExpense"], ["AdjustmentsForSharebasedPayments"]),
    ("depreciation_amortization", "Depreciation and amortization", "Accounting", "currency", "Depreciation, depletion and amortization.",
     ["DepreciationDepletionAndAmortization", "DepreciationAmortizationAndAccretionNet"], ["DepreciationAndAmortisationExpense"]),
    ("income_tax_expense", "Income tax expense", "Accounting", "currency", "Income tax expense (benefit).",
     ["IncomeTaxExpenseBenefit"], ["IncomeTaxExpenseContinuingOperations"]),
    ("eps_diluted", "Diluted EPS", "Shareholder Return", "currency/share", "Diluted earnings per share.",
     ["EarningsPerShareDiluted"], ["DilutedEarningsLossPerShare"]),
    ("dividends_paid", "Dividends paid", "Shareholder Return", "currency", "Cash dividends paid to shareholders.",
     ["PaymentsOfDividendsCommonStock", "PaymentsOfDividends"], ["DividendsPaidClassifiedAsFinancingActivities", "DividendsPaid"]),
    ("share_repurchases", "Share repurchases", "Shareholder Return", "currency", "Cash paid to repurchase own shares.",
     ["PaymentsForRepurchaseOfCommonStock"], ["PaymentsToAcquireOrRedeemEntitysShares"]),
    ("dividends_per_share", "Dividends declared per share", "Shareholder Return", "currency/share", "Dividends declared per common share.",
     ["CommonStockDividendsPerShareDeclared"], []),
]

# Issuer name patterns used to pick our companies out of 13F information tables.
ISSUER_PATTERNS: dict[str, re.Pattern[str]] = {
    "TSLA": re.compile(r"^TESLA\b"),
    "SPACEX": re.compile(r"SPACE EXPLORATION|^SPACEX"),
    "RKLB": re.compile(r"^ROCKET LAB"),
    "LMT": re.compile(r"^LOCKHEED MARTIN"),
    "BA": re.compile(r"^BOEING"),
    "NVDA": re.compile(r"^NVIDIA"),
    "ALB": re.compile(r"^ALBEMARLE"),
    "CAT": re.compile(r"^CATERPILLAR"),
    "TSM": re.compile(r"^TAIWAN SEMICONDUCTOR"),
    "STM": re.compile(r"^STMICROELECTRONICS"),
}


def connect() -> psycopg.Connection:
    return psycopg.connect(
        host="127.0.0.1",
        port=int(os.environ["BENCH_PG_PORT"]),
        user="bench",
        password=os.environ["BENCH_PG_PASSWORD"],
        dbname=os.environ.get("BENCH_PG_DB", "finder_real"),
    )


def load_companies(cur: psycopg.Cursor, manifest: dict) -> dict[int, int]:
    """Insert our ten issuers; return cik -> company_id."""
    annual = {c["cik"]: c["annual"] for c in manifest["companies"]}
    ids: dict[int, int] = {}
    for key, cik in COMPANIES.items():
        sub = json.loads((RAW / f"submissions/{cik:010d}.json").read_text())
        tickers = sub.get("tickers") or []
        cur.execute(
            """INSERT INTO companies (cik, ticker, name, sic_code, sic_description, state_of_incorporation,
                   country, fiscal_year_end, is_sec_filer, annual_report_form)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,TRUE,%s) RETURNING company_id""",
            (
                cik,
                tickers[0] if tickers else None,
                sub["name"],
                sub.get("sic"),
                sub.get("sicDescription"),
                sub.get("stateOfIncorporation") or None,
                (sub.get("addresses", {}).get("business", {}) or {}).get("stateOrCountryDescription"),
                sub.get("fiscalYearEnd"),
                (annual.get(cik) or {}).get("form"),
            ),
        )
        ids[cik] = cur.fetchone()[0]
    return ids


class Filings:
    """Insert-once cache of filings rows keyed by accession number."""

    def __init__(self, cur: psycopg.Cursor) -> None:
        self.cur = cur
        self.ids: dict[str, int] = {}

    def get(self, accession: str, filer_cik: int, company_id: int | None, form: str,
            filing_date: str, report_date: str | None = None, doc: str | None = None) -> int:
        if accession not in self.ids:
            self.cur.execute(
                """INSERT INTO filings (accession_no, filer_cik, company_id, form_type, filing_date, report_date, primary_doc)
                   VALUES (%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (accession_no) DO UPDATE SET accession_no = EXCLUDED.accession_no
                   RETURNING filing_id""",
                (accession, filer_cik, company_id, form, filing_date, report_date or None, doc),
            )
            self.ids[accession] = self.cur.fetchone()[0]
        return self.ids[accession]


def frame_period(frame: str) -> tuple[int, str] | None:
    m = re.fullmatch(r"CY(\d{4})(Q([1-4]))?(I)?", frame)
    if not m:
        return None
    year, q, instant = int(m.group(1)), m.group(3), m.group(4)
    if instant:
        return year, "FY" if q == "4" else f"Q{q}"
    return (year, "FY") if q is None else (year, f"Q{q}")


def load_metrics(cur: psycopg.Cursor, ids: dict[int, int], filings: Filings) -> int:
    metric_ids: dict[str, int] = {}
    for code, name, cat, unit, desc, gaap, ifrs in METRICS:
        cur.execute(
            """INSERT INTO metric_definitions (metric_code, name, category, unit, description, us_gaap_tags, ifrs_tags)
               VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING metric_id""",
            (code, name, cat, unit, desc, gaap, ifrs),
        )
        metric_ids[code] = cur.fetchone()[0]

    # Filing dates for the accessions facts point at, from each issuer's submissions.
    rows = []
    for cik, company_id in ids.items():
        facts = json.loads((RAW / f"companyfacts/{cik:010d}.json").read_text()).get("facts", {})
        for code, _n, _c, _u, _d, gaap, ifrs in METRICS:
            taken: set[tuple[int, str, str]] = set()
            for taxonomy, tags in (("us-gaap", gaap), ("ifrs-full", ifrs)):
                for tag in tags:
                    units = facts.get(taxonomy, {}).get(tag, {}).get("units", {})
                    for unit, entries in units.items():
                        for e in entries:
                            fp = frame_period(e.get("frame", ""))
                            if not fp or fp[0] not in YEARS:
                                continue
                            key = (fp[0], fp[1], unit)
                            if key in taken:  # an earlier (preferred) tag already supplied it
                                continue
                            taken.add(key)
                            fid = filings.get(e["accn"], cik, company_id, e["form"], e["filed"])
                            rows.append((company_id, metric_ids[code], fp[0], fp[1], e.get("start"), e["end"],
                                         e["val"], unit, f"{taxonomy}:{tag}", fid))
    cur.executemany(
        """INSERT INTO financial_metrics (company_id, metric_id, fiscal_year, fiscal_period, period_start, period_end,
               value, unit, xbrl_tag, source_filing_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        rows,
    )
    return len(rows)


def _txt(el: ET.Element | None, path: str) -> str | None:
    if el is None:
        return None
    node = el.find(path)
    return node.text.strip() if node is not None and node.text else None


def _flag(el: ET.Element | None, path: str) -> bool:
    return (_txt(el, path) or "").lower() in ("1", "true")


def load_insiders(cur: psycopg.Cursor, ids: dict[int, int], filings: Filings) -> tuple[int, int, int]:
    """Forms 3/4 -> persons + company_executives. Only filings where our company is the issuer."""
    roles: dict[tuple[int, int], dict] = {}
    person_ids: dict[int, int] = {}
    skipped = 0
    for cik, company_id in ids.items():
        sub = json.loads((RAW / f"submissions/{cik:010d}.json").read_text())
        r = sub["filings"]["recent"]
        meta = {a: (f, d) for a, f, d in zip(r["accessionNumber"], r["form"], r["filingDate"])}
        for path in sorted((RAW / f"insider/{cik}").glob("*.xml")) if (RAW / f"insider/{cik}").exists() else []:
            acc = path.stem
            try:
                root = ET.fromstring(path.read_bytes())
            except ET.ParseError:
                skipped += 1
                continue
            if int(_txt(root, "issuer/issuerCik") or 0) != cik:
                skipped += 1  # our company is the reporting owner here, not the issuer
                continue
            form, fdate = meta.get(acc, ("4", None))
            if not fdate:
                continue
            fid = filings.get(acc, cik, company_id, form, fdate)
            for owner in root.findall("reportingOwner"):
                ocik = int(_txt(owner, "reportingOwnerId/rptOwnerCik") or 0)
                oname = _txt(owner, "reportingOwnerId/rptOwnerName")
                if not ocik or not oname:
                    continue
                if ocik not in person_ids:
                    cur.execute(
                        "INSERT INTO persons (owner_cik, full_name) VALUES (%s,%s) RETURNING person_id",
                        (ocik, oname),
                    )
                    person_ids[ocik] = cur.fetchone()[0]
                rel = owner.find("reportingOwnerRelationship")
                k = (company_id, person_ids[ocik])
                cur_role = roles.setdefault(k, {"dir": False, "off": False, "ten": False, "title": None,
                                                "first": fdate, "last": fdate, "fid": fid})
                cur_role["dir"] |= _flag(rel, "isDirector")
                cur_role["off"] |= _flag(rel, "isOfficer")
                cur_role["ten"] |= _flag(rel, "isTenPercentOwner")
                title = _txt(rel, "officerTitle")
                if fdate >= cur_role["last"]:
                    cur_role["last"], cur_role["fid"] = fdate, fid
                    cur_role["title"] = title or cur_role["title"]
                cur_role["first"] = min(cur_role["first"], fdate)
    cur.executemany(
        """INSERT INTO company_executives (company_id, person_id, is_director, is_officer, is_ten_percent_owner,
               officer_title, first_seen_date, last_seen_date, source_filing_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        [(c, p, v["dir"], v["off"], v["ten"], v["title"], v["first"], v["last"], v["fid"]) for (c, p), v in roles.items()],
    )
    return len(person_ids), len(roles), skipped


def load_13f(cur: psycopg.Cursor, manifest: dict, ids: dict[int, int], filings: Filings) -> int:
    by_key = {key: ids[cik] for key, cik in COMPANIES.items()}
    agg: dict[tuple[int, int, str], list] = defaultdict(lambda: [0, 0.0, None])
    for m in manifest["managers"]:
        cur.execute(
            "INSERT INTO institutional_holders (cik, name) VALUES (%s,%s) RETURNING institution_id",
            (m["cik"], m["edgar_name"]),
        )
        inst = cur.fetchone()[0]
        for f in m["filings"]:
            fid = filings.get(f["accessionNumber"], m["cik"], None, "13F-HR", f["filingDate"], f["reportDate"])
            root = ET.fromstring((RAW / f"13f/{m['cik']}/{f['accessionNumber']}.infotable.xml").read_bytes())
            for row in root.iter():
                if not row.tag.endswith("infoTable"):
                    continue
                vals = {c.tag.split("}")[-1]: c for c in row.iter()}
                if "putCall" in vals:
                    continue
                issuer = (vals["nameOfIssuer"].text or "").upper().strip()
                key = next((k for k, p in ISSUER_PATTERNS.items() if p.search(issuer)), None)
                if not key:
                    continue
                a = agg[(inst, by_key[key], f["reportDate"])]
                a[0] += int(float(vals["sshPrnamt"].text))
                a[1] += float(vals["value"].text)
                a[2] = fid
    cur.executemany(
        """INSERT INTO institutional_holdings (institution_id, company_id, report_period, shares, value_usd, source_filing_id)
           VALUES (%s,%s,%s,%s,%s,%s)""",
        [(i, c, p, v[0], v[1], v[2]) for (i, c, p), v in agg.items()],
    )
    return len(agg)


def main() -> None:
    manifest = json.loads((RAW / "manifest.json").read_text())
    with connect() as conn, conn.cursor() as cur:
        ids = load_companies(cur, manifest)
        filings = Filings(cur)
        for c in manifest["companies"]:
            a = c["annual"]
            if a:
                filings.get(a["accessionNumber"], c["cik"], ids[c["cik"]], a["form"], a["filingDate"],
                            a.get("reportDate"), a["primaryDocument"])
        print("financial_metrics:", load_metrics(cur, ids, filings))
        print("persons, company_executives, skipped(non-issuer/bad xml):", load_insiders(cur, ids, filings))
        print("institutional_holdings:", load_13f(cur, manifest, ids, filings))
        print("filings:", len(filings.ids))
    print("loaded", date.today())


if __name__ == "__main__":
    main()
