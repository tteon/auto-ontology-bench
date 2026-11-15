"""Build finder_scale: finder_real plus a seeded synthetic expansion (Stage B / runtime study only).

    uv run python -m data_pipeline.scale --companies 30000 --seed 7

The synthetic graph is shaped for multi-hop work, not realism of individual rows:
- a tiered supply network (tier 0 = real issuers and synthetic OEMs, tiers 1..6
  below them). Each company draws suppliers from the next tier with
  preferential attachment, so hubs appear and 4-6 hop fan-out explodes; a small
  share of skip-tier edges and back edges add shortcuts and cycles;
- board seats drawn Zipf-style from a shared person pool -> interlocks;
- overlapping 13F portfolios -> shared-holder paths;
- risk factors (with mentioned suppliers), litigation, FY metrics 2019-2025.

Accuracy evaluation and Auto-Ontology ingestion never use this database.
"""

from __future__ import annotations

import argparse
import io
import os
import time

import numpy as np
import psycopg

from data_pipeline.load_structured import ROOT

PRODUCTS = ["battery cells", "cathode materials", "lithium hydroxide", "power semiconductors", "wafer foundry services",
            "memory chips", "rocket engines", "avionics", "composite structures", "machined parts", "rare earth magnets",
            "logistics services", "satellite buses", "propellant", "sensors", "PCB assemblies", "software licenses",
            "steel castings", "hydraulic systems", "launch services"]
RELTYPES = ["supplier"] * 7 + ["manufacturing_partner", "licensor", "joint_venture", "launch_provider"]
LEGAL_TYPES = ["securities", "regulatory", "product_liability", "contract", "intellectual_property", "employment",
               "environmental", "government_investigation", "other"]
STATUSES = ["pending", "settled", "dismissed", "judgment", "appeal", "unknown"]
COUNTRIES = ["United States", "China", "Japan", "Korea", "Taiwan", "Germany", "Mexico", "France", "India", "Canada"]
TIER_SHARE = [0.0, 0.06, 0.12, 0.17, 0.20, 0.22, 0.23]  # tiers 0..6 (tier 0 = OEMs, sized separately)


def pg(db: str, autocommit: bool = False) -> psycopg.Connection:
    return psycopg.connect(host="127.0.0.1", port=int(os.environ["BENCH_PG_PORT"]), user="bench",
                           password=os.environ["BENCH_PG_PASSWORD"], dbname=db, autocommit=autocommit)


def copy_rows(cur: psycopg.Cursor, table: str, cols: list[str], rows: list[tuple]) -> None:
    with cur.copy(f"COPY {table} ({', '.join(cols)}) FROM STDIN") as cp:
        for r in rows:
            cp.write_row(r)


def clone_real(dst: psycopg.Connection) -> None:
    order = ["companies", "filings", "persons", "company_executives", "institutional_holders", "institutional_holdings",
             "supply_chain_relations", "metric_definitions", "financial_metrics", "risk_categories", "risk_factors",
             "legal_proceedings", "footnote_related_parties"]
    with pg("finder_real") as src, src.cursor() as s, dst.cursor() as d:
        for t in order:
            buf = io.BytesIO()
            with s.copy(f"COPY {t} TO STDOUT") as cp:
                for chunk in cp:
                    buf.write(chunk)
            buf.seek(0)
            with d.copy(f"COPY {t} FROM STDIN") as cp:
                cp.write(buf.read())


def next_id(cur: psycopg.Cursor, table: str, col: str) -> int:
    cur.execute(f"SELECT coalesce(max({col}), 0) + 1 FROM {table}")
    return cur.fetchone()[0]


def build(n_companies: int, seed: int, db: str = "finder_scale") -> dict[str, int]:
    rng = np.random.default_rng(seed)
    with pg("postgres", autocommit=True) as c:
        c.execute(f"DROP DATABASE IF EXISTS {db} WITH (FORCE)")
        c.execute(f"CREATE DATABASE {db}")
    stats: dict[str, int] = {}
    with pg(db) as conn, conn.cursor() as cur:
        cur.execute((ROOT / "data_pipeline" / "schema.sql").read_text())
        clone_real(conn)
        for t, col in [("companies", "company_id"), ("filings", "filing_id"), ("persons", "person_id"),
                       ("company_executives", "company_executive_id"), ("institutional_holders", "institution_id"),
                       ("institutional_holdings", "holding_id"), ("supply_chain_relations", "relation_id"),
                       ("financial_metrics", "financial_metric_id"), ("risk_factors", "risk_factor_id"),
                       ("legal_proceedings", "legal_proceeding_id"), ("footnote_related_parties", "related_party_id")]:
            cur.execute(f"SELECT setval(pg_get_serial_sequence('{t}', '{col}'), (SELECT coalesce(max({col}), 0) + 1 FROM {t}), false)")

        cur.execute("SELECT company_id FROM companies WHERE is_sec_filer")
        real_oems = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT company_id FROM companies WHERE NOT is_sec_filer AND entity_type = 'company'")
        real_cps = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT metric_id, metric_code FROM metric_definitions")
        metrics = cur.fetchall()
        cur.execute("SELECT risk_category_id FROM risk_categories")
        risk_cats = [r[0] for r in cur.fetchall()]

        # --- companies by tier ------------------------------------------------
        cid0 = next_id(cur, "companies", "company_id")
        n_oem = max(50, n_companies // 100)
        tiers: list[list[int]] = [real_oems + list(range(cid0, cid0 + n_oem))]
        rows, nid = [], cid0
        for i in range(n_oem):
            rows.append((nid, f"SynOEM {i:05d}", False, "company", str(rng.choice(COUNTRIES))))
            nid += 1
        for t in range(1, 7):
            n_t = int(n_companies * TIER_SHARE[t])
            ids = list(range(nid, nid + n_t))
            for i in range(n_t):
                rows.append((nid + i, f"SynCo T{t}-{i:06d}", False, "company", str(rng.choice(COUNTRIES))))
            nid += n_t
            tiers.append(ids)
        tiers[1] = real_cps + tiers[1]  # real counterparties (Panasonic, CATL, ...) sit at tier 1
        copy_rows(cur, "companies", ["company_id", "name", "is_sec_filer", "entity_type", "country"], rows)
        stats["companies"] = len(rows)
        all_syn = [c for t in tiers for c in t]  # every node in the supply network, real ones included
        syn_only = list(range(cid0, nid))        # synthetic companies: the only ones that get synthetic facts

        # one synthetic filing per synthetic company (provenance for synthetic facts)
        fid0 = next_id(cur, "filings", "filing_id")
        filing_of = {}
        frows = []
        for k, c in enumerate(all_syn):
            filing_of[c] = fid0 + k
            frows.append((fid0 + k, f"SYN-{c:08d}", 0, c, "SYN", "2025-12-31"))
        copy_rows(cur, "filings", ["filing_id", "accession_no", "filer_cik", "company_id", "form_type", "filing_date"], frows)

        # --- supply network ---------------------------------------------------
        srows, seen = [], set()
        for t in range(0, 6):
            below = np.array(tiers[t + 1])
            w = 1.0 / np.arange(1, len(below) + 1) ** 0.8  # preferential attachment: low ranks are hubs
            w /= w.sum()
            for c in tiers[t]:
                k = 1 + rng.poisson(3.0 if t else 6.0)
                for s in rng.choice(below, size=min(k, len(below)), replace=False, p=w):
                    seen.add((int(s), c))
                if t + 2 <= 6 and rng.random() < 0.05:  # skip-tier shortcut
                    seen.add((int(rng.choice(tiers[t + 2])), c))
        for _ in range(int(len(seen) * 0.005)):  # back edges -> cycles
            t = int(rng.integers(2, 7))
            seen.add((int(rng.choice(tiers[t - 2])), int(rng.choice(tiers[t]))))
        for s, c in seen:
            if s == c:
                continue
            srows.append((s, c, str(rng.choice(RELTYPES)), str(rng.choice(PRODUCTS)),
                          round(float(rng.lognormal(16, 1.5)), 2), int(rng.integers(2005, 2026)), "synthetic", filing_of.get(c)))
        copy_rows(cur, "supply_chain_relations", ["supplier_company_id", "customer_company_id", "relation_type", "product_category",
                                                  "contract_value_usd", "since_year", "evidence", "source_filing_id"], srows)
        stats["supply_chain_relations"] = len(srows)

        # --- people and board seats (interlocks) ------------------------------
        pid0 = next_id(cur, "persons", "person_id")
        n_p = int(len(all_syn) * 1.6)
        copy_rows(cur, "persons", ["person_id", "full_name"], [(pid0 + i, f"Person {i:07d}") for i in range(n_p)])
        pw = 1.0 / np.arange(1, n_p + 1) ** 0.6
        pw /= pw.sum()
        roles, rseen = [], set()
        for c in all_syn:
            for p in rng.choice(n_p, size=int(rng.integers(4, 10)), replace=False, p=pw):
                key = (c, int(pid0 + p))
                if key in rseen:
                    continue
                rseen.add(key)
                d = rng.random() < 0.6
                roles.append((c, key[1], d, not d, False, None if d else str(rng.choice(["CEO", "CFO", "COO", "CTO", "General Counsel"])),
                              "2020-01-01", "2025-12-31", filing_of[c]))
        copy_rows(cur, "company_executives", ["company_id", "person_id", "is_director", "is_officer", "is_ten_percent_owner",
                                              "officer_title", "first_seen_date", "last_seen_date", "source_filing_id"], roles)
        stats["company_executives"] = len(roles)

        # --- institutions and holdings ---------------------------------------
        iid0 = next_id(cur, "institutional_holders", "institution_id")
        n_i = max(100, n_companies // 75)  # 400 at 30k: holdings density stays constant across scale factors
        copy_rows(cur, "institutional_holders", ["institution_id", "cik", "name"],
                  [(iid0 + i, 900_000_000 + i, f"SynFund {i:04d}") for i in range(n_i)])
        cw = 1.0 / np.arange(1, len(all_syn) + 1) ** 0.7
        cw /= cw.sum()
        holds = []
        syn_arr = np.array(all_syn)
        for i in range(n_i):
            size = int(min(len(all_syn), rng.pareto(1.2) * 80 + 30))
            for c in rng.choice(syn_arr, size=size, replace=False, p=cw):
                for period in ("2025-06-30", "2025-09-30"):
                    sh = int(rng.lognormal(11, 1.5))
                    holds.append((iid0 + i, int(c), period, sh, round(sh * float(rng.lognormal(3.5, 1)), 2), filing_of[int(c)]))
        copy_rows(cur, "institutional_holdings", ["institution_id", "company_id", "report_period", "shares", "value_usd",
                                                  "source_filing_id"], holds)
        stats["institutional_holdings"] = len(holds)

        # --- risk, legal, metrics ---------------------------------------------
        sup_of: dict[int, list[int]] = {}
        for s, c in seen:
            sup_of.setdefault(c, []).append(s)
        risks, legal, fms = [], [], []
        for c in syn_only:
            for _ in range(int(rng.integers(4, 13))):
                cat = int(rng.choice(risk_cats))
                ment = int(rng.choice(sup_of[c])) if c in sup_of and rng.random() < 0.35 else None
                risks.append((c, cat, filing_of[c], f"Synthetic risk {cat}", "synthetic", ment))
            for _ in range(int(rng.poisson(1.2))):
                cp = int(rng.choice(syn_arr)) if rng.random() < 0.3 else None
                legal.append((c, filing_of[c], f"Case {c}-{rng.integers(1e6)}", str(rng.choice(LEGAL_TYPES)), None, None, cp,
                              str(rng.choice(STATUSES)), round(float(rng.lognormal(15, 2)), 2) if rng.random() < 0.4 else None, "synthetic"))
            scale = float(rng.lognormal(19, 1.6))
            for year in range(2019, 2026):
                g = scale * (1 + 0.08 * (year - 2019)) * float(rng.lognormal(0, 0.15))
                for mid, code in metrics:
                    if code in ("eps_diluted", "dividends_per_share"):
                        v = float(rng.normal(3, 2))
                    else:
                        v = g * float(rng.uniform(0.02, 1.2))
                    fms.append((c, mid, year, "FY", f"{year}-01-01", f"{year}-12-31", round(v, 4), "USD", "synthetic", filing_of[c]))
        copy_rows(cur, "risk_factors", ["company_id", "risk_category_id", "filing_id", "title", "summary", "mentioned_company_id"], risks)
        copy_rows(cur, "legal_proceedings", ["company_id", "filing_id", "case_name", "proceeding_type", "forum", "counterparty",
                                             "counterparty_company_id", "status", "amount_usd", "description"], legal)
        copy_rows(cur, "financial_metrics", ["company_id", "metric_id", "fiscal_year", "fiscal_period", "period_start", "period_end",
                                             "value", "unit", "xbrl_tag", "source_filing_id"], fms)
        stats.update(risk_factors=len(risks), legal_proceedings=len(legal), financial_metrics=len(fms))
        conn.commit()
        cur.execute("ANALYZE")
    return stats


def grant_ro(db: str = "finder_scale") -> None:
    with pg(db) as conn, conn.cursor() as cur:
        cur.execute(f"GRANT CONNECT ON DATABASE {db} TO bench_ro; GRANT USAGE ON SCHEMA public TO bench_ro;"
                    " GRANT SELECT ON ALL TABLES IN SCHEMA public TO bench_ro")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--companies", type=int, default=30_000)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--db", default="finder_scale")
    a = ap.parse_args()
    t = time.time()
    stats = build(a.companies, a.seed, a.db)
    grant_ro(a.db)
    print(stats, f"{time.time() - t:.0f}s")


if __name__ == "__main__":
    main()
