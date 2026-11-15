"""Load LLM-extracted facts (raw/extract/extracted.json) into finder_real.

Company names are canonicalized so that "TSMC", "Taiwan Semiconductor
Manufacturing Company Limited" and "TSMC Arizona" land on one row. Names that
are not one of our ten issuers become non-SEC-filer rows (entity_type
'company' or 'government'), which is how counterparties like Panasonic, CATL,
NASA or xAI enter the graph.
"""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation

import psycopg

from data_pipeline.fetch import COMPANIES
from data_pipeline.load_structured import connect

_SUFFIX = re.compile(
    r"\b(incorporated|inc|corporation|corp|company|co|limited|ltd|llc|lp|plc|n v|nv|s a|sa|ag|se|holdings?|group|the)\b"
)

# normalized alias -> canonical display name
ALIASES: dict[str, str] = {
    "tesla": "Tesla, Inc.", "tesla motors": "Tesla, Inc.",
    "spacex": "SPACE EXPLORATION TECHNOLOGIES CORP", "space exploration technologies": "SPACE EXPLORATION TECHNOLOGIES CORP",
    "rocket lab": "Rocket Lab Corp", "rocket lab usa": "Rocket Lab Corp",
    "lockheed martin": "LOCKHEED MARTIN CORP", "lockheed": "LOCKHEED MARTIN CORP",
    "boeing": "BOEING CO",
    "nvidia": "NVIDIA CORP",
    "albemarle": "ALBEMARLE CORP",
    "caterpillar": "CATERPILLAR INC",
    "tsmc": "TAIWAN SEMICONDUCTOR MANUFACTURING CO LTD", "taiwan semiconductor manufacturing": "TAIWAN SEMICONDUCTOR MANUFACTURING CO LTD",
    "tsmc arizona": "TAIWAN SEMICONDUCTOR MANUFACTURING CO LTD",
    "stmicroelectronics": "STMicroelectronics N.V.", "st": "STMicroelectronics N.V.",
    "panasonic": "Panasonic", "panasonic energy": "Panasonic",
    "catl": "CATL", "contemporary amperex technology": "CATL",
    "lg energy solution": "LG Energy Solution", "lges": "LG Energy Solution",
    "samsung": "Samsung Electronics", "samsung electronics": "Samsung Electronics",
    "sk hynix": "SK hynix", "micron": "Micron Technology", "micron technology": "Micron Technology",
    "xai": "xAI", "x ai": "xAI", "x": "X Corp.", "twitter": "X Corp.",
    "the boring": "The Boring Company", "boring": "The Boring Company", "neuralink": "Neuralink",
    "united launch alliance": "United Launch Alliance", "ula": "United Launch Alliance",
    "nasa": "NASA", "national aeronautics and space administration": "NASA",
    "us government": "U.S. Government", "u s government": "U.S. Government", "united states government": "U.S. Government",
    "department of defense": "U.S. Department of Defense", "dod": "U.S. Department of Defense", "us department of defense": "U.S. Department of Defense",
    "u s department of defense": "U.S. Department of Defense", "department of war": "U.S. Department of Defense",
    "us air force": "U.S. Air Force", "u s air force": "U.S. Air Force", "space force": "U.S. Space Force", "us space force": "U.S. Space Force",
    "u s space force": "U.S. Space Force", "space development agency": "Space Development Agency",
    "united states department of war": "U.S. Department of Defense", "us department of war": "U.S. Department of Defense",
    "edb investment pte": "EDB Investments", "edb investments pte": "EDB Investments", "edb investments": "EDB Investments",
    "koninklijke philips": "Philips", "philips": "Philips", "nxp": "NXP Semiconductors", "nxp b v": "NXP Semiconductors",
    "nxp semiconductors": "NXP Semiconductors", "spirit": "Spirit AeroSystems", "spirit aerosystems": "Spirit AeroSystems",
    "spirit spirit acquisition": "Spirit AeroSystems", "hon hai precision industry": "Foxconn (Hon Hai)", "foxconn": "Foxconn (Hon Hai)",
}
_GOV = re.compile(r"\b(government|department|nasa|agency|air force|space force|army|navy|ministry|administration)\b", re.I)
_GENERIC = re.compile(r"^(we|us|our|the company|company|suppliers?|customers?|third part(y|ies)|various|unknown|none|n/?a|others?|undisclosed( related)? part(y|ies)|related part(y|ies))$", re.I)

RISK_TYPES = {"securities", "regulatory", "product_liability", "contract", "intellectual_property", "employment",
              "environmental", "government_investigation", "other"}
STATUSES = {"pending", "settled", "dismissed", "judgment", "appeal", "unknown"}
RELATION_TYPES = {"supplier", "manufacturing_partner", "joint_venture", "licensor", "launch_provider"}


def norm_name(name: str) -> str:
    s = re.sub(r"[^a-z0-9 ]", " ", name.lower().replace("&", " and "))
    s = _SUFFIX.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()


def money(v: object) -> Decimal | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        d = Decimal(str(v).replace(",", "").replace("$", "").strip())
    except InvalidOperation:
        return None
    return d if d >= 0 else None


def as_int(v: object) -> int | None:
    try:
        i = int(str(v)[:4])
    except (TypeError, ValueError):
        return None
    return i if 1950 <= i <= 2030 else None


class Resolver:
    def __init__(self, cur: psycopg.Cursor) -> None:
        self.cur = cur
        cur.execute("SELECT company_id, name FROM companies")
        self.by_canon = {name: cid for cid, name in cur.fetchall()}
        cur.execute("SELECT person_id, full_name FROM persons")
        self.persons = [(pid, set(re.findall(r"[a-z]{2,}", n.lower()))) for pid, n in cur.fetchall()]

    def canon(self, name: str) -> str | None:
        n = norm_name(name or "")
        if not n or _GENERIC.match(n) or len(n) < 2:
            return None
        if n in ALIASES:
            return ALIASES[n]
        for alias, canon in ALIASES.items():  # "panasonic energy co ltd" -> "panasonic"
            if len(alias) > 3 and n.startswith(alias + " "):
                return canon
        return name.strip()

    def company(self, name: str | None, *, create: bool) -> int | None:
        c = self.canon(name or "")
        if not c:
            return None
        if c in self.by_canon:
            return self.by_canon[c]
        # case-insensitive / normalized match against existing rows
        for existing, cid in self.by_canon.items():
            if norm_name(existing) == norm_name(c):
                return cid
        if not create:
            return None
        self.cur.execute(
            "INSERT INTO companies (name, is_sec_filer, entity_type) VALUES (%s, FALSE, %s) RETURNING company_id",
            (c, "government" if _GOV.search(c) else "company"),
        )
        cid = self.cur.fetchone()[0]
        self.by_canon[c] = cid
        return cid

    def person(self, name: str | None) -> int | None:
        toks = set(re.findall(r"[a-z]{2,}", (name or "").lower()))
        if len(toks) < 2:
            return None
        hits = [pid for pid, pt in self.persons if toks <= pt or pt <= toks]
        return hits[0] if len(hits) == 1 else None


def load(results: dict, manifest: dict, risk_categories: dict[str, str]) -> None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE risk_factors, legal_proceedings, footnote_related_parties, supply_chain_relations, risk_categories RESTART IDENTITY")
        cur.execute("DELETE FROM companies WHERE NOT is_sec_filer")
        cat_ids = {}
        for name, desc in risk_categories.items():
            cur.execute("INSERT INTO risk_categories (name, description) VALUES (%s,%s) RETURNING risk_category_id", (name, desc))
            cat_ids[name.lower()] = cur.fetchone()[0]
        res = Resolver(cur)
        filing = {}
        for c in manifest["companies"]:
            cur.execute("SELECT filing_id FROM filings WHERE accession_no = %s", (c["annual"]["accessionNumber"],))
            filing[c["key"]] = cur.fetchone()[0]
        cid_of = {key: _issuer_id(cur, cik) for key, cik in COMPANIES.items()}
        counts = {"risk": 0, "legal": 0, "related": 0, "supply": 0}
        for key, tasks in results.items():
            company_id, fid = cid_of[key], filing[key]
            seen: set[str] = set()
            for it in tasks.get("risk", []):
                title = str(it.get("title") or "").strip()
                k = norm_name(title)[:80]
                cat = cat_ids.get(str(it.get("category") or "").lower())
                if not title or not cat or k in seen:
                    continue
                seen.add(k)
                mentioned = res.company(it.get("mentioned_company"), create=True)
                if mentioned == company_id:
                    mentioned = None
                cur.execute(
                    """INSERT INTO risk_factors (company_id, risk_category_id, filing_id, title, summary, mentioned_company_id)
                       VALUES (%s,%s,%s,%s,%s,%s)""",
                    (company_id, cat, fid, title[:1000], str(it.get("summary") or "")[:2000], mentioned),
                )
                counts["risk"] += 1
            seen = set()
            for it in tasks.get("legal", []):
                name = str(it.get("case_name") or "").strip()
                k = norm_name(name)
                if not name or k in seen:
                    continue
                seen.add(k)
                ptype = str(it.get("proceeding_type") or "other").lower()
                status = str(it.get("status") or "unknown").lower()
                cp = res.company(it.get("counterparty"), create=False)
                cur.execute(
                    """INSERT INTO legal_proceedings (company_id, filing_id, case_name, proceeding_type, forum, counterparty,
                           counterparty_company_id, status, amount_usd, description) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (company_id, fid, name[:500], ptype if ptype in RISK_TYPES else "other", it.get("forum"),
                     it.get("counterparty"), cp if cp != company_id else None, status if status in STATUSES else "unknown",
                     money(it.get("amount_usd")), str(it.get("description") or "")[:2000]),
                )
                counts["legal"] += 1
            seen = set()
            for it in tasks.get("related", []):
                party = str(it.get("related_party_name") or "").strip()
                k = norm_name(party) + "|" + norm_name(str(it.get("transaction_type") or "")) + "|" + str(it.get("fiscal_year"))
                if not party or k in seen:
                    continue
                seen.add(k)
                person = res.person(it.get("related_person")) or res.person(party)
                rel_company = None if res.person(party) else res.company(party, create=True)
                cur.execute(
                    """INSERT INTO footnote_related_parties (company_id, filing_id, related_party_name, related_company_id,
                           related_person_id, relationship, transaction_type, amount_usd, fiscal_year, description)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (company_id, fid, party[:300], rel_company if rel_company != company_id else None, person,
                     str(it.get("relationship") or "unspecified")[:300], str(it.get("transaction_type") or "unspecified")[:200],
                     money(it.get("amount_usd")), as_int(it.get("fiscal_year")), str(it.get("description") or "")[:2000]),
                )
                counts["related"] += 1
            seen = set()
            for it in tasks.get("supply", []):
                rtype = str(it.get("relation_type") or "").lower()
                sup = res.company(it.get("supplier"), create=True)
                cus = res.company(it.get("customer"), create=True)
                prod = str(it.get("product_category") or "").strip().lower()
                k = f"{sup}|{cus}|{rtype}|{prod}"
                if not sup or not cus or sup == cus or rtype not in RELATION_TYPES or not prod or k in seen:
                    continue
                seen.add(k)
                cur.execute(
                    """INSERT INTO supply_chain_relations (supplier_company_id, customer_company_id, relation_type, product_category,
                           contract_value_usd, since_year, evidence, source_filing_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (sup, cus, rtype, prod[:200], money(it.get("contract_value_usd")), as_int(it.get("since_year")),
                     str(it.get("evidence"))[:1000], fid),
                )
                counts["supply"] += 1
        cur.execute("SELECT count(*) FROM companies WHERE NOT is_sec_filer")
        print("loaded:", counts, "non-filer entities:", cur.fetchone()[0])


def _issuer_id(cur: psycopg.Cursor, cik: int) -> int:
    cur.execute("SELECT company_id FROM companies WHERE cik = %s", (cik,))
    return cur.fetchone()[0]
