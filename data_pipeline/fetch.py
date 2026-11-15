"""Fetch the raw EDGAR inputs for finder_real into raw/edgar (append-only cache).

    uv run python -m data_pipeline.fetch [--insider-since 2024-01-01] [--insider-max 200]

Everything is cached on disk by URL-derived path, so re-running is cheap and the
dataset is reproducible from raw/ alone.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

from data_pipeline.edgar import RAW, companyfacts, get, get_json, submissions

COMPANIES: dict[str, int] = {
    "TSLA": 1318605,
    "SPACEX": 1181412,
    "RKLB": 1819994,
    "LMT": 936468,
    "BA": 12927,
    "NVDA": 1045810,
    "ALB": 915913,
    "CAT": 18230,
    "TSM": 1046179,
    "STM": 932787,
}

# Large 13F filers, chosen so that many of them hold several of our companies
# (shared-holder paths). Names are checked against EDGAR at fetch time.
MANAGERS: dict[str, int] = {
    "Vanguard Group": 102909,
    "BlackRock": 1364742,
    "State Street": 93751,
    "FMR": 315066,
    "Geode Capital Management": 1214717,
    "Morgan Stanley": 895421,
    "JPMorgan Chase": 19617,
    "Capital World Investors": 1422849,
    "T. Rowe Price Associates": 80255,
    "Northern Trust": 73124,
    "Goldman Sachs Group": 886982,
    "Bank of America": 70858,
    "Norges Bank": 1374170,
    "Wellington Management": 902219,
    "ARK Investment Management": 1697748,
    "Baillie Gifford": 1088875,
    "Invesco": 914208,
    "Renaissance Technologies": 1037389,
    "Citadel Advisors": 1423053,
    "Bridgewater Associates": 1350694,
}

ANNUAL_FORMS = ("10-K", "20-F", "S-1/A", "S-1")


def archive_url(cik: int, accession: str, doc: str) -> str:
    return f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession.replace('-', '')}/{doc}"


def recent(sub: dict) -> list[dict]:
    r = sub["filings"]["recent"]
    keys = ("accessionNumber", "form", "filingDate", "reportDate", "primaryDocument")
    return [dict(zip(keys, row)) for row in zip(*(r[k] for k in keys))]


def latest_annual(rows: list[dict]) -> dict | None:
    for form in ANNUAL_FORMS:
        for row in rows:  # EDGAR lists newest first
            if row["form"] == form:
                return row
    return None


def fetch_company(key: str, cik: int, insider_since: str, insider_max: int) -> dict:
    sub = submissions(cik)
    rows = recent(sub)
    companyfacts(cik)
    annual = latest_annual(rows)
    if annual:
        get(
            archive_url(cik, annual["accessionNumber"], annual["primaryDocument"]),
            f"docs/{cik}/{annual['accessionNumber']}.htm",
        )
    insider = [r for r in rows if r["form"] in ("3", "4") and r["filingDate"] >= insider_since][:insider_max]
    for r in insider:
        # primaryDocument is the XSL-rendered view ("xslF345X05/x.xml"); the raw XML drops the prefix.
        doc = re.sub(r"^xslF345X\d+/", "", r["primaryDocument"])
        try:
            get(archive_url(cik, r["accessionNumber"], doc), f"insider/{cik}/{r['accessionNumber']}.xml")
        except Exception as e:  # a missing exhibit should not stop the run
            print(f"  insider {key} {r['accessionNumber']}: {e}")
    return {"key": key, "cik": cik, "annual": annual, "insider_filings": len(insider)}


def fetch_manager(name: str, cik: int, quarters: int) -> dict:
    sub = submissions(cik)
    rows = [r for r in recent(sub) if r["form"] == "13F-HR"][:quarters]
    got = []
    for r in rows:
        acc = r["accessionNumber"]
        index = get_json(
            f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc.replace('-', '')}/index.json",
            f"13f/{cik}/{acc}.index.json",
        )
        # The information table is the XML file that is not the primary_doc cover page.
        xmls = [i["name"] for i in index["directory"]["item"] if i["name"].lower().endswith(".xml")]
        table = next((x for x in xmls if "primary_doc" not in x.lower()), None)
        if table:
            get(archive_url(cik, acc, table), f"13f/{cik}/{acc}.infotable.xml")
            got.append({**r, "infotable": table})
    return {"name": name, "edgar_name": sub["name"], "cik": cik, "filings": got}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--insider-since", default="2024-01-01")
    ap.add_argument("--insider-max", type=int, default=200)
    ap.add_argument("--quarters", type=int, default=2)
    a = ap.parse_args()

    manifest: dict = {"companies": [], "managers": []}
    for key, cik in COMPANIES.items():
        info = fetch_company(key, cik, a.insider_since, a.insider_max)
        print(f"{key}: annual={info['annual'] and info['annual']['form']} insider={info['insider_filings']}")
        manifest["companies"].append(info)
    for name, cik in MANAGERS.items():
        try:
            info = fetch_manager(name, cik, a.quarters)
        except Exception as e:
            print(f"manager {name} ({cik}): {e}")
            continue
        print(f"13F {name} -> {info['edgar_name']}: {len(info['filings'])} quarters")
        manifest["managers"].append(info)
    Path(RAW / "manifest.json").write_text(json.dumps(manifest, indent=1))


if __name__ == "__main__":
    main()
