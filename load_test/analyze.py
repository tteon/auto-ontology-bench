"""Turn results/ into a Markdown report and CSV tables.

    uv run python -m load_test.analyze --stage-a TAG[,TAG...] [--stage-b TAG,...] [--runtime TAG] --out results/report.md

Stage A: accuracy by condition x tier with bootstrap 95% CIs, paired effects
(onto - no-onto per engine, neo - pg per arm) with exact McNemar p-values on
the per-question outcome, latency decomposition and token use.
Stage B / runtime study: throughput and server-time curves, Enterprise metrics.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


def load_jsonl(p: Path) -> list[dict]:
    return [json.loads(line) for line in p.read_text().splitlines() if line.strip()]


def boot_ci(vals: list[float], n: int = 2000, seed: int = 0) -> tuple[float, float]:
    if not vals:
        return (math.nan, math.nan)
    rng = random.Random(seed)
    means = sorted(sum(rng.choice(vals) for _ in vals) / len(vals) for _ in range(n))
    return means[int(n * 0.025)], means[int(n * 0.975)]


def mcnemar_exact(b: int, c: int) -> float:
    """Two-sided exact McNemar p-value from discordant counts b, c."""
    n = b + c
    if n == 0:
        return 1.0
    k = min(b, c)
    p = sum(math.comb(n, i) for i in range(0, k + 1)) / 2**n
    return min(1.0, 2 * p)


def pctl(v: list[float], p: float) -> float | None:
    v = sorted(x for x in v if x is not None)
    if not v:
        return None
    k = (len(v) - 1) * p
    lo = int(k)
    hi = min(lo + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (k - lo)


def f(x: Any, nd: int = 1) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "–"
    if isinstance(x, float):
        return f"{x:,.{nd}f}"
    return f"{x:,}" if isinstance(x, int) else str(x)


def stage_a(tags: list[str], md: list[str], csv_dir: Path) -> None:
    recs = [r for t in tags for r in load_jsonl(ROOT / "results" / "stage_a" / t / "records.jsonl")]
    th_path = ROOT / "results" / "threshold.json"
    tau = json.loads(th_path.read_text())["chosen"]["threshold"] if th_path.exists() else None
    if tau is not None:  # dev-calibrated check_answerable gate, applied to the ungated onto records
        gated = []
        for r in recs:
            if r["condition"].endswith("-onto") and "no-onto" not in r["condition"] and r.get("coverage") is not None:
                g = dict(r, condition=r["condition"] + "+gate")
                if r["coverage"] < tau:
                    ok = r["tier"] == 5
                    g["score"] = {"correct": ok, "strict": ok, "lenient": ok, "false_rejection": r["tier"] != 5, "rejected": True}
                gated.append(g)
        recs = recs + gated
        md.append(f"`+gate` rows apply the dev-calibrated check_answerable threshold τ = {tau} (results/threshold.json) to the "
                  "same onto records: coverage < τ becomes a rejection.\n")
    conds = sorted({r["condition"] for r in recs})
    tiers = sorted({r["tier"] for r in recs})
    md.append(f"## Stage A — accuracy (tags: {', '.join(tags)}; {len(recs)} requests)\n")
    md.append("Correct = lenient result-set match against gold (Tier 5: declined). 95% bootstrap CI in brackets.\n")
    md.append("| condition | " + " | ".join(f"Tier {t}" for t in tiers) + " | all |")
    md.append("|---|" + "---|" * (len(tiers) + 1))
    rows_csv = []
    for c in conds:
        cells = []
        for t in tiers + ["all"]:
            v = [float(r["score"]["correct"]) for r in recs if r["condition"] == c and (t == "all" or r["tier"] == t)]
            lo, hi = boot_ci(v)
            acc = sum(v) / len(v) if v else math.nan
            cells.append(f"{acc:.2f} [{lo:.2f}–{hi:.2f}] (n={len(v)})")
            rows_csv.append({"condition": c, "tier": t, "n": len(v), "accuracy": acc, "ci_lo": lo, "ci_hi": hi})
        md.append(f"| {c} | " + " | ".join(cells) + " |")
    with (csv_dir / "stage_a_accuracy.csv").open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows_csv[0]))
        w.writeheader()
        w.writerows(rows_csv)

    # paired effects on (cq_id, repeat)
    key = lambda r: (r["cq_id"], r.get("repeat", 0))  # noqa: E731
    by = defaultdict(dict)
    for r in recs:
        by[key(r)][r["condition"]] = r["score"]["correct"]
    md.append("\n### Paired effects (same question, same repeat)\n")
    md.append("| comparison | tier | n pairs | Δ accuracy | A-only correct | B-only correct | McNemar p |")
    md.append("|---|---|---|---|---|---|---|")
    comps = [("pg-onto", "pg-no-onto"), ("neo-onto", "neo-no-onto"), ("neo-no-onto", "pg-no-onto"), ("neo-onto", "pg-onto"),
             ("pg-onto+gate", "pg-no-onto"), ("neo-onto+gate", "neo-no-onto")]
    tier_of = {key(r): r["tier"] for r in recs}
    for a, b in comps:
        for t in tiers + ["all"]:
            pairs = [(v[a], v[b]) for k, v in by.items() if a in v and b in v and (t == "all" or tier_of[k] == t)]
            if not pairs:
                continue
            bo = sum(1 for x, y in pairs if x and not y)
            co = sum(1 for x, y in pairs if y and not x)
            d = (sum(x for x, _ in pairs) - sum(y for _, y in pairs)) / len(pairs)
            md.append(f"| {a} − {b} | {t} | {len(pairs)} | {d:+.3f} | {bo} | {co} | {mcnemar_exact(bo, co):.3g} |")

    md.append("\n### Latency and tokens per condition (medians; ms)\n")
    md.append("| condition | e2e p50 | e2e p90 | t_ctx p50 | LLM total p50 | TTFT p50 | DB server p50 | DB wall p50 | prompt tok | completion tok | reasoning tok | cached tok (sum) |")
    md.append("|---|" + "---|" * 11)
    for c in conds:
        rs = [r for r in recs if r["condition"] == c]
        L = [r.get("llm") or {} for r in rs]
        D = [r.get("db") or {} for r in rs]
        md.append(f"| {c} | {f(pctl([r.get('e2e_ms') for r in rs], .5), 0)} | {f(pctl([r.get('e2e_ms') for r in rs], .9), 0)} | "
                  f"{f(pctl([r.get('t_ctx_ms') for r in rs], .5), 0)} | {f(pctl([x.get('total_ms') for x in L], .5), 0)} | "
                  f"{f(pctl([x.get('ttft_ms') for x in L], .5), 0)} | {f(pctl([x.get('server_ms') for x in D], .5), 1)} | "
                  f"{f(pctl([x.get('client_wall_ms') for x in D], .5), 1)} | {f(pctl([x.get('prompt_tokens') for x in L], .5), 0)} | "
                  f"{f(pctl([x.get('completion_tokens') for x in L], .5), 0)} | {f(pctl([x.get('reasoning_tokens') for x in L], .5), 0)} | "
                  f"{sum((x.get('cached_tokens') or 0) for x in L):,} |")

    md.append("\n### Outcome breakdown\n")
    md.append("| condition | correct | wrong result | empty result | syntax error | other error | false rejection | Tier 5 declined |")
    md.append("|---|---|---|---|---|---|---|---|")
    for c in conds:
        rs = [r for r in recs if r["condition"] == c]
        ans = [r for r in rs if r["tier"] != 5]
        t5 = [r for r in rs if r["tier"] == 5]
        wrong = [r for r in ans if not r["score"]["correct"] and not r["score"]["rejected"] and not r.get("error_type")]
        md.append(f"| {c} | {sum(r['score']['correct'] for r in ans)}/{len(ans)} | "
                  f"{sum(1 for r in wrong if (r.get('db') or {}).get('row_count'))} | {sum(1 for r in wrong if not (r.get('db') or {}).get('row_count'))} | "
                  f"{sum(r.get('error_type') == 'SYNTAX_ERROR' for r in ans)} | "
                  f"{sum(bool(r.get('error_type')) and r.get('error_type') != 'SYNTAX_ERROR' for r in ans)} | "
                  f"{sum(r['score']['false_rejection'] for r in ans)} | {sum(r['score']['rejected'] for r in t5)}/{len(t5)} |")


def stage_b(tags: list[str], md: list[str]) -> None:
    for tag in tags:
        p = ROOT / "results" / "stage_b" / tag / "levels.jsonl"
        if not p.exists():
            continue
        rows = load_jsonl(p)
        md.append(f"\n## Stage B — DB-only replay ({tag})\n")
        md.append("Server time: Neo4j result_available_after + result_consumed_after; Postgres wall − loopback baseline.\n")
        md.append("| engine | conc | QPS | server p50 | server p99 | client p99 | errors | Neo4j server-side tx concurrency (max) | page-cache hit ratio (run) | PG CPU cores |")
        md.append("|---|---|---|---|---|---|---|---|---|---|")
        for r in rows:
            tx = r.get("tx", {})
            md.append(f"| {r['engine']} | {r['concurrency']} | {f(r['throughput_qps'])} | {f(r['server_ms']['p50'])} | {f(r['server_ms']['p99'])} | "
                      f"{f(r['client_latency_ms']['p99'])} | {r['errors'] or ''} | {f((tx.get('server_concurrency') or {}).get('max'))} | "
                      f"{f((r.get('jmx') or {}).get('page_cache.run_hit_ratio'), 4)} | {f(r.get('pg_cpu_cores_avg'), 2)} |")


def runtime(tag: str, md: list[str], csv_dir: Path) -> None:
    base = ROOT / "results" / "runtime" / tag
    if not base.exists():
        return
    md.append(f"\n## Runtime study ({tag})\n")
    corr = json.loads((base / "correctness.json").read_text()) if (base / "correctness.json").exists() else []
    if corr:
        bad = [c for c in corr if not all(v is True for v in c["checks"].values())]
        md.append(f"Correctness gate: {len(corr) - len(bad)}/{len(corr)} anchor checks equal across every engine-runtime"
                  + (f"; failures: {[(c['workload'], c['checks']) for c in bad]}" if bad else "") + "\n")
    plans = json.loads((base / "plans.json").read_text()) if (base / "plans.json").exists() else []
    if plans:
        md.append("### Plans (separate PROFILE / EXPLAIN ANALYZE pass)\n")
        md.append("| workload | engine-runtime | actual runtime / PG workers | pipelines (fused) | fused ops | partitioned | DB hits | page cache hits/misses | PG exec ms (EXPLAIN ANALYZE) |")
        md.append("|---|---|---|---|---|---|---|---|---|")
        for p in plans:
            if p["engine"] == "neo":
                pl = p.get("plan") or {}
                md.append(f"| {p['workload']} | neo-{p['runtime']} | {pl.get('actual_runtime', p.get('error', '–'))} | {pl.get('pipelines', '–')} ({pl.get('fused_pipelines', '–')}) | "
                          f"{pl.get('pipeline_fused_operators', '–')} | {pl.get('partitioned_scan', '–')} | {f(pl.get('db_hits'))} | "
                          f"{f(pl.get('page_cache_hits'))}/{f(pl.get('page_cache_misses'))} | |")
            else:
                md.append(f"| {p['workload']} | pg-{p['runtime']} | workers {p.get('workers_launched')} {p.get('parallel_nodes') or ''} | | | | | "
                          f"hit {f(p.get('shared_hit'))} / read {f(p.get('shared_read'))} | {f(p.get('execution_ms'), 2)} |")
    cells = load_jsonl(base / "cells.jsonl") if (base / "cells.jsonl").exists() else []
    if not cells:
        return
    with (csv_dir / f"runtime_{tag}.csv").open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["workload", "engine", "runtime", "concurrency", "qps", "server_p50", "server_p99", "client_p99", "errors"])
        for c in cells:
            w.writerow([c["workload"], c["engine"], c["runtime"], c["concurrency"], c["throughput_qps"], c["server_ms"]["p50"],
                        c["server_ms"]["p99"], c["client_latency_ms"]["p99"], json.dumps(c["errors"])])
    md.append("\n### Neo4j Enterprise metrics per cell (JMX metrics registry deltas + SHOW TRANSACTIONS sampling)\n")
    md.append("page-cache hits/query = JMX page_cache.hits delta ÷ executed queries; heap/query p95 = SHOW TRANSACTIONS "
              "estimatedUsedHeapMemory; server conc = transactions observed running at once (sampled); "
              "GC = young-gen GC ms during the cell; bolt queue = Bolt accumulated queue time per message.\n")
    md.append("| workload | runtime | conc | server conc mean/max | page-cache hits/query | hit ratio | heap/query p95 (MB) | GC young ms | bolt queue ms/msg | host load |")
    md.append("|---|---|---|---|---|---|---|---|---|---|")
    for c in sorted((c for c in cells if c["engine"] == "neo"), key=lambda c: (c["workload"], c["runtime"], c["concurrency"])):
        j, t = c.get("jmx") or {}, c.get("tx") or {}
        q = j.get("db.db.query.execution.latency.millis.count_delta") or 0
        sc = t.get("server_concurrency") or {}
        heap = (t.get("tx_heap_bytes") or {}).get("p95")
        bq = j.get("bolt.accumulated_queue_time.delta")
        bm = j.get("bolt.messages_received.delta")
        md.append(f"| {c['workload']} | {c['runtime']} | {c['concurrency']} | {f(sc.get('mean'))}/{f(sc.get('max'))} | "
                  f"{f(j.get('page_cache.hits.delta', 0) / q if q else None, 0)} | {f(j.get('page_cache.run_hit_ratio'), 3)} | "
                  f"{f(heap / 2**20 if heap else None, 1)} | {f(j.get('vm.gc.time.g1_young_generation.delta'))} | "
                  f"{f(bq / bm if bq is not None and bm else None, 2)} | {f(c.get('host_loadavg_1m'), 1)} |")
    md.append("\n### Postgres container per cell\n")
    md.append("| workload | runtime | conc | CPU cores used (of 8) | host load |")
    md.append("|---|---|---|---|---|")
    for c in sorted((c for c in cells if c["engine"] == "pg"), key=lambda c: (c["workload"], c["runtime"], c["concurrency"])):
        md.append(f"| {c['workload']} | {c['runtime']} | {c['concurrency']} | {f(c.get('pg_cpu_cores_avg'), 2)} | {f(c.get('host_loadavg_1m'), 1)} |")
    for wl in sorted({c["workload"] for c in cells}):
        md.append(f"\n### {wl}: server time p50 / p99 (ms) and QPS by concurrency\n")
        concs = sorted({c["concurrency"] for c in cells})
        md.append("| engine-runtime | " + " | ".join(f"c={k}" for k in concs) + " |")
        md.append("|---|" + "---|" * len(concs))
        for er in sorted({(c["engine"], c["runtime"]) for c in cells if c["workload"] == wl}):
            row = []
            for k in concs:
                c = next((x for x in cells if x["workload"] == wl and (x["engine"], x["runtime"]) == er and x["concurrency"] == k), None)
                if not c:
                    row.append("–")
                    continue
                err = sum(c["errors"].values()) if c["errors"] else 0
                row.append(f"{f(c['server_ms']['p50'])} / {f(c['server_ms']['p99'])} · {f(c['throughput_qps'])} qps" + (f" · {err} err" if err else ""))
            md.append(f"| {er[0]}-{er[1]} | " + " | ".join(row) + " |")
        best = {}
        for k in concs:
            cand = [c for c in cells if c["workload"] == wl and c["concurrency"] == k and c["ok"] and c["server_ms"]["p50"] is not None]
            if cand:
                b = min(cand, key=lambda c: c["server_ms"]["p50"])
                q = max(cand, key=lambda c: c["throughput_qps"])
                best[k] = f"lowest p50: {b['engine']}-{b['runtime']}; highest QPS: {q['engine']}-{q['runtime']}"
        md.append("\n" + "\n".join(f"- c={k}: {v}" for k, v in best.items()))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage-a", default="")
    ap.add_argument("--stage-b", default="")
    ap.add_argument("--runtime", default="")
    ap.add_argument("--out", default=str(ROOT / "results" / "report.md"))
    a = ap.parse_args()
    md = ["# Auto-Ontology × Postgres/Neo4j benchmark — results\n"]
    csv_dir = ROOT / "results" / "tables"
    csv_dir.mkdir(parents=True, exist_ok=True)
    if a.stage_a:
        stage_a(a.stage_a.split(","), md, csv_dir)
    if a.stage_b:
        stage_b(a.stage_b.split(","), md)
    for t in [x for x in a.runtime.split(",") if x]:
        runtime(t, md, csv_dir)
    Path(a.out).write_text("\n".join(md) + "\n")
    print(f"wrote {a.out}")


if __name__ == "__main__":
    main()
