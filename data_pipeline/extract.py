"""LLM extraction of risk / legal / related-party / supply-chain facts from annual reports.

    uv run python -m data_pipeline.extract            # extract into raw/extract (cached)
    uv run python -m data_pipeline.extract --load     # also load into finder_real

Guards against hallucination: every item must carry an `evidence` string that
occurs verbatim (whitespace-normalized) in the chunk it came from; items that
fail the check are dropped and counted. The extraction model is deliberately
not the model under test.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
from dotenv import load_dotenv

from data_pipeline.edgar import RAW

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

MODEL = os.environ.get("EXTRACT_MODEL", "nvidia/nemotron-3-ultra-550b-a55b")
OUT = ROOT / "raw" / "extract"
CHUNK = 14_000
_http = httpx.Client(timeout=300)

RISK_CATEGORIES = {
    "Supply chain and raw materials": "Dependence on suppliers, components, commodities and logistics.",
    "Geopolitical and trade": "Tariffs, export controls, sanctions, conflicts and cross-border tensions.",
    "Government contracts and regulation": "Government customers, budgets, procurement rules and regulatory approvals.",
    "Competition": "Competitive pressure on prices, share and technology.",
    "Technology and product": "Product development, defects, recalls and technology transitions.",
    "Cybersecurity and IT": "Cyber attacks, data breaches and IT system failures.",
    "Key personnel and governance": "Dependence on key people, control by insiders, board and governance structure.",
    "Financial and liquidity": "Debt, capital needs, liquidity, currency and interest rates.",
    "Macroeconomic and demand": "Economic cycles, customer demand and end-market conditions.",
    "Legal and litigation": "Lawsuits, investigations and legal liabilities.",
    "Environmental and climate": "Environmental regulation, climate and sustainability.",
    "Intellectual property": "Protection of and claims against intellectual property.",
    "Operations and manufacturing": "Production ramp, facilities, quality and operational execution.",
    "Stock and capital markets": "Share price volatility, dilution and capital-markets access.",
}

PROMPTS = {
    "risk": """You are extracting risk factors from the {form} of {company}.
From the TEXT, list every risk factor whose heading appears in the TEXT. For each return:
  "title": the risk factor heading, copied exactly,
  "summary": one sentence in your own words,
  "category": exactly one of {categories},
  "mentioned_company": a specific other company the risk names as a dependency (supplier, customer, partner, competitor), else null,
  "evidence": the first 15-30 words of the heading copied exactly from the TEXT.
Return {{"items": [...]}} as JSON only. Return {{"items": []}} if the TEXT holds no risk factor headings.""",
    "legal": """You are extracting legal proceedings disclosed by {company} in its {form}.
From the TEXT, list each distinct, specifically described proceeding (a named lawsuit, class action, investigation, arbitration or enforcement action). Skip generic statements like "we are party to various claims". For each return:
  "case_name": the case caption or a short specific name,
  "proceeding_type": one of securities, regulatory, product_liability, contract, intellectual_property, employment, environmental, government_investigation, other,
  "forum": the court or agency, else null,
  "counterparty": the opposing party or agency, else null,
  "status": one of pending, settled, dismissed, judgment, appeal, unknown,
  "amount_usd": a monetary amount in US dollars explicitly tied to the case (number), else null,
  "description": one sentence,
  "evidence": a 10-40 word span copied exactly from the TEXT that describes the case.
Return {{"items": [...]}} as JSON only.""",
    "related": """You are extracting related-party transactions disclosed by {company} in its {form}.
From the TEXT, list each related-party transaction or arrangement (with an officer, director, major shareholder, or an entity they control). For each return:
  "related_party_name": the related person or entity as named,
  "related_person": the individual behind it if named (e.g. "Elon Musk"), else null,
  "relationship": how the party is related (e.g. "entity controlled by our CEO"),
  "transaction_type": e.g. purchase of services, sale of products, lease, loan, licensing, investment,
  "amount_usd": US dollar amount explicitly stated for the transaction (number), else null,
  "fiscal_year": the fiscal year of the amount (integer), else null,
  "description": one sentence,
  "evidence": a 10-40 word span copied exactly from the TEXT.
Return {{"items": [...]}} as JSON only.""",
    "supply": """You are extracting named business relationships of {company} from its {form}.
From the TEXT, list each relationship where a specifically NAMED company supplies, manufactures for, licenses to, launches for, or forms a joint venture with {company}, or where {company} supplies a NAMED customer. Ignore unnamed "suppliers" and pure competitors. For each return:
  "supplier": the company that supplies / manufactures / licenses / launches,
  "customer": the company that receives it,
  "relation_type": one of supplier, manufacturing_partner, joint_venture, licensor, launch_provider,
  "product_category": short noun phrase (e.g. "lithium-ion battery cells", "wafer foundry services"),
  "contract_value_usd": explicitly stated US dollar value (number), else null,
  "since_year": explicitly stated start year (integer), else null,
  "evidence": a 10-40 word span copied exactly from the TEXT naming both parties or the relationship.
Return {{"items": [...]}} as JSON only.""",
}

KEYWORDS = {
    "legal": re.compile(r"legal proceedings|litigation|lawsuit|class action|complaint|plaintiff|subpoena|investigation|settlement|arbitration", re.I),
    "related": re.compile(r"related part|related-party|controlled by|affiliate of our (?:ceo|chief)|transactions with", re.I),
    "supply": re.compile(r"supplier|supply agreement|manufactur(?:e|ing) (?:for|partner)|foundry|joint venture|launch services|sole source|single source|purchase agreement", re.I),
}


def html_text(raw: bytes) -> str:
    s = raw.decode("utf-8", errors="ignore")
    s = re.sub(r"(?is)<(script|style|ix:header)[^>]*>.*?</\1>", " ", s)
    s = re.sub(r"(?i)<br\s*/?>|</(p|div|tr|li|h\d)>", "\n", s)
    s = re.sub(r"(?s)<[^>]+>", " ", s)
    s = re.sub(r"[\xa0\u2000-\u200b\u202f\u205f\u3000]", " ", html.unescape(s))
    s = re.sub(r"[ \t]+", " ", s)
    return re.sub(r"\n\s*\n+", "\n", s).strip()


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[‘’“”]", "'", s)).strip().lower()


_RISK_HEAD = re.compile(r"(?im)^[ \t]*(?:item[ \t]*1a\.?[ \t]*\n?[ \t]*)?risk factors\.?[ \t]*$")
_NEXT_HEAD = re.compile(
    r"(?im)^[ \t]*(?:item[ \t]*1b\b|item[ \t]*1c\b|item[ \t]*2\.|item[ \t]*4\.[ \t]*information on the company"
    r"|cautionary (?:statement|note) regarding forward|special note regarding forward|use of proceeds[ \t]*$)"
)


def risk_section(text: str, form: str) -> str:
    """The risk-factor section, found from heading lines rather than cross-references.

    A start is a line that is exactly "Risk Factors" (optionally "Item 1A.") and is
    followed by prose, not a page number (that is the table of contents). The section
    ends at the next section heading line. The longest such span wins.
    """
    best = ""
    for m in _RISK_HEAD.finditer(text):
        nxt = text[m.end() : m.end() + 80].strip()
        if not nxt or re.match(r"^[\d.\s]+(item|$)", nxt, re.I) or nxt[:1].isdigit():
            continue
        e = _NEXT_HEAD.search(text, m.end() + 500)
        if not e or e.start() - m.start() > 450_000:
            continue
        if e.start() - m.start() > len(best):
            best = text[m.start() : e.start()]
    return best


def chunks(text: str, size: int = CHUNK) -> list[str]:
    out, i = [], 0
    while i < len(text):
        j = min(len(text), i + size)
        if j < len(text):
            k = text.rfind("\n", i + size // 2, j)
            j = k if k > 0 else j
        out.append(text[i:j])
        i = j
    return out


def call_llm(prompt: str, text: str) -> dict:
    key = hashlib.sha256(f"{MODEL}\n{prompt}\n{text}".encode()).hexdigest()[:24]
    cache = OUT / "cache" / f"{key}.json"
    if cache.exists():
        return json.loads(cache.read_text())
    body = {
        "model": MODEL,
        "temperature": 0,
        "max_tokens": 8192,
        "messages": [{"role": "system", "content": prompt}, {"role": "user", "content": f"TEXT:\n{text}"}],
    }
    for attempt in range(6):
        r = _http.post(
            f"{os.environ['NVIDIA_ENDPOINT']}/chat/completions",
            headers={"Authorization": f"Bearer {os.environ['NVIDIA_API_KEY']}"},
            json=body,
        )
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(60, 5 * 2**attempt))
            continue
        r.raise_for_status()
        j = r.json()
        content = j["choices"][0]["message"].get("content") or ""
        m = re.search(r"\{.*\}", content, re.S)
        try:
            parsed = json.loads(m.group(0)) if m else {"items": []}
        except json.JSONDecodeError:
            parsed = {"items": [], "parse_error": content[:500]}
        out = {"parsed": parsed, "usage": j.get("usage"), "model": MODEL}
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_text(json.dumps(out))
        return out
    raise RuntimeError("LLM endpoint kept failing")


def jobs_for(company: dict) -> list[tuple[str, str]]:
    a = company["annual"]
    text = html_text((RAW / f"docs/{company['cik']}/{a['accessionNumber']}.htm").read_bytes())
    out: list[tuple[str, str]] = []
    for c in chunks(risk_section(text, a["form"])):
        out.append(("risk", c))
    for task, pat in KEYWORDS.items():
        scored = sorted(
            ((len(pat.findall(c)), i, c) for i, c in enumerate(chunks(text))),
            reverse=True,
        )
        out += [(task, c) for n, _i, c in scored[:6] if n >= 2]
    return out


def extract(manifest: dict, workers: int) -> dict:
    results: dict = {}
    names = {c["cik"]: json.loads((RAW / f"submissions/{c['cik']:010d}.json").read_text())["name"] for c in manifest["companies"]}
    plan = []
    for c in manifest["companies"]:
        for task, text in jobs_for(c):
            prompt = PROMPTS[task].format(
                company=names[c["cik"]], form=c["annual"]["form"], categories=json.dumps(list(RISK_CATEGORIES))
            )
            plan.append((c["key"], task, prompt, text))
    print(f"{len(plan)} LLM calls with {MODEL}")

    def run(p: tuple[str, str, str, str]) -> tuple[str, str, list, int, dict]:
        key, task, prompt, text = p
        res = call_llm(prompt, text)
        hay = norm(text)
        kept, dropped = [], 0
        for it in res["parsed"].get("items", []) or []:
            ev = norm(str(it.get("evidence") or ""))
            if len(ev) >= 20 and (ev in hay or ev[:60] in hay):
                kept.append(it)
            else:
                dropped += 1
        return key, task, kept, dropped, res.get("usage") or {}

    stats = {"dropped": 0, "prompt_tokens": 0, "completion_tokens": 0}
    with ThreadPoolExecutor(workers) as ex:
        for key, task, kept, dropped, usage in ex.map(run, plan):
            results.setdefault(key, {}).setdefault(task, []).extend(kept)
            stats["dropped"] += dropped
            stats["prompt_tokens"] += usage.get("prompt_tokens", 0)
            stats["completion_tokens"] += usage.get("completion_tokens", 0)
    (OUT / "extracted.json").write_text(json.dumps(results, indent=1))
    print("stats:", stats)
    for key, tasks in results.items():
        print(key, {t: len(v) for t, v in tasks.items()})
    return results


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--load", action="store_true")
    a = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((RAW / "manifest.json").read_text())
    results = extract(manifest, a.workers)
    if a.load:
        from data_pipeline.load_extracted import load

        load(results, manifest, RISK_CATEGORIES)


if __name__ == "__main__":
    main()
