"""Private performance report for the second-round experiments (not exported: Neo4j AUP).

    uv run python -m load_test.analyze_perf --sfs s10k,s30k,s100k --out results/PERFORMANCE.md

Sections: scale sweep (server time vs data size, log-log scaling exponent), depth sweep (cost vs hop
depth per traversal strategy), mixed workload (policy comparison), tuning tracks, constraint A/B.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SIZE = {"s10k": 10_100, "s30k": 30_300, "s100k": 100_300}


def jl(p: Path) -> list[dict]:
    return [json.loads(x) for x in p.read_text().splitlines() if x.strip()] if p.exists() else []


def f(x, nd: int = 1) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "–"
    if isinstance(x, float):
        return f"{x:,.{nd}f}"
    return f"{x:,}" if isinstance(x, int) else str(x)


def slope(xs: list[float], ys: list[float]) -> float | None:
    pts = [(math.log(x), math.log(y)) for x, y in zip(xs, ys) if x and y and y > 0]
    if len(pts) < 2:
        return None
    mx = sum(p[0] for p in pts) / len(pts)
    my = sum(p[1] for p in pts) / len(pts)
    den = sum((p[0] - mx) ** 2 for p in pts)
    return sum((p[0] - mx) * (p[1] - my) for p in pts) / den if den else None


def scale_section(sfs: list[str], md: list[str]) -> None:
    cells = {sf: jl(ROOT / "results" / "runtime" / f"sweep-{sf}" / "cells.jsonl") for sf in sfs}
    sfs = [sf for sf in sfs if cells[sf]]
    if not sfs:
        return
    md.append("## Scale sweep — server time vs data size\n")
    md.append(f"Scale factors: {', '.join(f'{sf} ({SIZE.get(sf, 0):,} companies)' for sf in sfs)}. "
              "Exponent b in time ∝ size^b (log-log least squares over the scale factors): b≈0 constant, b≈1 linear.\n")
    for conc in (1, 16):
        md.append(f"\n### concurrency {conc}: server p50 (ms) per scale factor, exponent b" + (" and QPS" if conc == 16 else "") + "\n")
        md.append("| workload | engine-runtime | " + " | ".join(sfs) + " | b |" + (" QPS " + " / ".join(sfs) + " |" if conc == 16 else ""))
        md.append("|---|---|" + "---|" * (len(sfs) + 1) + ("---|" if conc == 16 else ""))
        keys = sorted({(c["workload"], c["engine"], c["runtime"]) for sf in sfs for c in cells[sf] if c["concurrency"] == conc})
        for w, e, rt in keys:
            vals, qps = [], []
            for sf in sfs:
                c = next((x for x in cells[sf] if (x["workload"], x["engine"], x["runtime"], x["concurrency"]) == (w, e, rt, conc)), None)
                v = c["server_ms"]["p50"] if c and c.get("ok") else None
                vals.append(v)
                qps.append(c["throughput_qps"] if c else None)
            b = slope([SIZE[s] for s in sfs], [v if v else 0 for v in vals])
            md.append(f"| {w} | {e}-{rt} | " + " | ".join(f(v) for v in vals) + f" | {f(b, 2)} |"
                      + (" " + " / ".join(f(q) for q in qps) + " |" if conc == 16 else ""))


def depth_section(sfs: list[str], md: list[str]) -> None:
    for sf in sfs:
        p = ROOT / "results" / "depth" / f"depth-{sf}" / "cells.json"
        if not p.exists():
            continue
        cells = json.loads(p.read_text())
        ks = sorted({c["k"] for c in cells})
        md.append(f"\n## Depth sweep ({sf}) — server p50 ms by hop depth k (result size of anchor 0 in brackets)\n")
        md.append("| variant | " + " | ".join(f"k={k}" for k in ks) + " |")
        md.append("|---|" + "---|" * len(ks))
        for key in sorted({(c["variant"], c["runtime"]) for c in cells}):
            row = []
            for k in ks:
                c = next((x for x in cells if (x["variant"], x["runtime"], x["k"]) == (*key, k)), None)
                if not c:
                    row.append("·")
                elif c["errors"]:
                    row.append(f"✗ {c['errors'][0]}")
                else:
                    row.append(f"{f(c['server_ms_p50'])} [{f(c['result_anchor0'])}]")
            md.append(f"| {key[0]}/{key[1]} | " + " | ".join(row) + " |")
        md.append("\nDB hits (Neo4j, anchor 0) / shared buffers hit (Postgres) at each k:\n")
        md.append("| variant | " + " | ".join(f"k={k}" for k in ks) + " |")
        md.append("|---|" + "---|" * len(ks))
        for key in sorted({(c["variant"], c["runtime"]) for c in cells}):
            row = []
            for k in ks:
                c = next((x for x in cells if (x["variant"], x["runtime"], x["k"]) == (*key, k)), None)
                row.append(f(c.get("db_hits") if c and c.get("db_hits") is not None else (c or {}).get("shared_hit")) if c else "·")
            md.append(f"| {key[0]}/{key[1]} | " + " | ".join(row) + " |")


def mixed_section(sfs: list[str], md: list[str]) -> None:
    for sf in sfs:
        rows = jl(ROOT / "results" / "mixed" / f"mixed-{sf}" / "policies.jsonl")
        if not rows:
            continue
        md.append(f"\n## Mixed workload ({sf}) — 24 light + 2 analytic + 4 LLM clients at once\n")
        md.append("| policy | light p50 / p99 ms | light QPS | analytic p50 ms | analytic QPS | LLM QPS | LLM errors | terminated | server conc mean/max | heap/tx p95 MB |")
        md.append("|---|---|---|---|---|---|---|---|---|---|")
        for r in rows:
            c = r["classes"]
            tx = r.get("tx") or {}
            sc = tx.get("server_concurrency") or {}
            heap = (tx.get("tx_heap_bytes") or {}).get("p95")
            md.append(f"| {r['policy']} | {f(c['light']['client_ms']['p50'], 0)} / {f(c['light']['client_ms']['p99'], 0)} | "
                      f"{f(c['light']['qps'], 0)} | {f(c['analytic']['client_ms']['p50'], 0)} | {f(c['analytic']['qps'], 2)} | "
                      f"{f(c['llm']['qps'], 1)} | {c['llm']['errors'] or ''} | {r.get('watchdog_terminated', '–')} | "
                      f"{f(sc.get('mean'))}/{f(sc.get('max'))} | {f(heap / 2**20 if heap else None)} |")


def tuning_section(sfs: list[str], md: list[str]) -> None:
    for sf in sfs:
        p = ROOT / "results" / "tuning" / f"tuning-{sf}" / "variants.json"
        if not p.exists():
            continue
        md.append(f"\n## Tuning tracks ({sf})\n")
        md.append("| workload | variant | equal to baseline | setup s | c=1 server ms | c=16 QPS | c=16 p99 ms | db hits / buffers | plan operators |")
        md.append("|---|---|---|---|---|---|---|---|---|")
        for r in json.loads(p.read_text()):
            md.append(f"| {r['workload']} | {r['variant']} | {r['equal_to_baseline']} | {r['setup_s']} | {f(r['c1_server_ms_median'])} | "
                      f"{f(r['c16_qps'])} | {f(r['c16_server_ms_p99'])} | {f(r.get('db_hits', r.get('shared_hit')))} | "
                      f"{', '.join(r.get('operators') or [])[:120]} |")


def constraint_section(md: list[str]) -> None:
    for p in sorted((ROOT / "results").glob("constraint_ab_*.json")):
        rows = json.loads(p.read_text())
        md.append(f"\n## Constraint A/B ({p.stem.removeprefix('constraint_ab_')})\n")
        md.append("| query | state | Neo4j ms | db hits | operators | Postgres ms |")
        md.append("|---|---|---|---|---|---|")
        for r in rows:
            md.append(f"| {r['query']} | {r['state']} | {r['neo_server_ms_median']} | {f(r['neo_db_hits'])} | "
                      f"{', '.join(r['neo_operators'] or [])} | {r['pg_ms_median']} |")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sfs", default="s10k,s30k,s100k")
    ap.add_argument("--out", default=str(ROOT / "results" / "PERFORMANCE.md"))
    a = ap.parse_args()
    sfs = a.sfs.split(",")
    md = ["# Performance results — second round (private: not for publication, Neo4j AUP)\n"]
    scale_section(sfs, md)
    depth_section(sfs, md)
    mixed_section(sfs, md)
    tuning_section(sfs, md)
    constraint_section(md)
    Path(a.out).write_text("\n".join(md) + "\n")
    print(f"wrote {a.out}")


if __name__ == "__main__":
    main()
