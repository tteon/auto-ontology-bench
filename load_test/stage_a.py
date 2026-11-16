"""Stage A: end-to-end runs of the 2x2 conditions over the CQ set, with scoring.

    uv run python -m load_test.stage_a --split dev --conditions pg-no-onto,pg-onto,neo-no-onto,neo-onto \
        --concurrency 1 --repeats 1 [--limit-per-tier 2] [--threshold 0.25] [--tag smoke]

Writes results/stage_a/<tag>/records.jsonl (one line per request: timings,
tokens, coverage, query, verdict) and summary.json, plus the Enterprise metrics
captured for the run (JMX deltas, SHOW TRANSACTIONS sampling) and the
pg_stat_statements delta.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

from benchmarks.verifier import compare  # noqa: E402
from enterprise.conn import async_driver, driver  # noqa: E402
from enterprise.metrics import JmxSnapshot, TxSampler, connections, index_read_delta, index_reads  # noqa: E402
from gateway.core import Gateway  # noqa: E402
from gateway.executors import pg_stats_delta, pg_stats_snapshot  # noqa: E402
from gateway.tracing import flush  # noqa: E402
from load_test.loadgen import pin_cpus, run_load, summarize  # noqa: E402


def load_cq(split: str, limit_per_tier: int | None, tiers: set[int]) -> list[dict]:
    items = [json.loads(line) for line in (ROOT / "benchmarks" / "cq_dataset.jsonl").read_text().splitlines()]
    items = [it for it in items if (split == "all" or it["split"] == split) and it["tier"] in tiers]
    if limit_per_tier:
        out, seen = [], {}
        for it in items:
            if seen.get(it["tier"], 0) < limit_per_tier:
                out.append(it)
                seen[it["tier"]] = seen.get(it["tier"], 0) + 1
        items = out
    return items


def score(item: dict, res: dict) -> dict[str, Any]:
    rejected = bool(res.get("rejected"))
    if not item["answerable"]:
        return {"correct": rejected, "strict": rejected, "lenient": rejected, "false_rejection": False, "rejected": rejected}
    if rejected:
        return {"correct": False, "strict": False, "lenient": False, "false_rejection": True, "rejected": True}
    if res.get("error_type") or res.get("rows") is None:
        return {"correct": False, "strict": False, "lenient": False, "false_rejection": False, "rejected": False}
    v = compare(item["gold_rows"], res["rows"], item.get("ordered", False))
    return {"correct": v["lenient"], **v, "false_rejection": False, "rejected": False}


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--split", default="dev")
    ap.add_argument("--conditions", default="pg-no-onto,pg-onto,neo-no-onto,neo-onto")
    ap.add_argument("--concurrency", type=int, default=1)
    ap.add_argument("--repeats", type=int, default=1)
    ap.add_argument("--limit-per-tier", type=int, default=None)
    ap.add_argument("--tiers", default="1,2,3,4,5")
    ap.add_argument("--threshold", type=float, default=None, help="check_answerable coverage threshold for onto arms")
    ap.add_argument("--tag", default=time.strftime("run-%Y%m%d-%H%M%S"))
    ap.add_argument("--only-cached", action="store_true", help="only questions whose check_answerable result is cached")
    ap.add_argument("--exclude-tag", default="", help="skip cq_ids already present in these tags' records (comma list)")
    a = ap.parse_args()
    pin_cpus()
    items = load_cq(a.split, a.limit_per_tier, {int(t) for t in a.tiers.split(",")})
    if a.only_cached:
        cache_path = ROOT / "ontology" / "coverage_cache.json"
        cached = json.loads(cache_path.read_text()) if cache_path.exists() else {}
        items = [it for it in items if (cached.get(it["question"]) or {}).get("data") is not None]
    for t in [x for x in a.exclude_tag.split(",") if x]:
        done = {json.loads(line)["cq_id"] for line in (ROOT / "results" / "stage_a" / t / "records.jsonl").read_text().splitlines()}
        items = [it for it in items if it["cq_id"] not in done]
    print(f"{len(items)} questions", flush=True)
    conditions = a.conditions.split(",")
    work = [(it, c, r) for r in range(a.repeats) for it in items for c in conditions]
    out_dir = ROOT / "results" / "stage_a" / a.tag
    out_dir.mkdir(parents=True, exist_ok=True)

    gw = Gateway(pool_size=max(4, a.concurrency * 2), answerable_threshold=a.threshold)
    await gw.start()
    records: list[dict] = []

    async def request(w: tuple, rid: str) -> dict:
        it, cond, rep = w
        try:
            res = await gw.run(it["question"], cond, cq_id=it["cq_id"], request_id=rid, run_tag=a.tag)
        except Exception as e:  # noqa: BLE001 -- every request must leave a record
            res = {"cq_id": it["cq_id"], "condition": cond, "error_type": "UNHANDLED", "error": repr(e)[:300]}
        sc = score(it, res)
        rec = {"cq_id": it["cq_id"], "tier": it["tier"], "template": it["template"], "split": it["split"], "repeat": rep,
               **{k: v for k, v in res.items() if k != "rows"}, "score": sc,
               "pred_rows_sample": [list(map(str, r)) for r in (res.get("rows") or [])[:3]]}
        records.append(rec)
        return {"error_type": res.get("error_type")} if res.get("error_type") in ("RATE_LIMITED", "LLM_ERROR") else {}

    with driver() as d:
        jmx = JmxSnapshot()
        jmx.start(d)
        idx0 = index_reads(d)
        conn0 = connections(d)
    pg0 = await pg_stats_snapshot("finder_real")
    async with async_driver() as ad:
        sampler = TxSampler(ad, a.tag, interval_s=0.1)
        sampler.start()
        t0 = time.time()
        recs = await run_load(request, work, item_id=lambda w: f"{w[0]['cq_id']}|{w[1]}", concurrency=a.concurrency,
                              n_requests=len(work))
        wall = time.time() - t0
        await sampler.stop()
    pg1 = await pg_stats_snapshot("finder_real")
    with driver() as d:
        jmx.stop(d)
        idx1 = index_reads(d)
        conn1 = connections(d)
    await gw.close()
    flush()

    (out_dir / "records.jsonl").write_text("\n".join(json.dumps(r, default=str) for r in records) + "\n")
    summary: dict[str, Any] = {"tag": a.tag, "args": vars(a), "n_requests": len(work), "wall_s": round(wall, 1),
                               "load": summarize(recs), "conditions": {}}
    for c in conditions:
        rs = [r for r in records if r["condition"] == c]
        by_tier = {}
        for t in sorted({r["tier"] for r in rs}):
            tr = [r for r in rs if r["tier"] == t]
            by_tier[t] = {"n": len(tr), "accuracy": sum(r["score"]["correct"] for r in tr) / len(tr),
                          "strict": sum(r["score"]["strict"] for r in tr) / len(tr),
                          "false_rejections": sum(r["score"]["false_rejection"] for r in tr),
                          "errors": {e: sum(r.get("error_type") == e for r in tr) for e in {r.get("error_type") for r in tr} if e}}
        summary["conditions"][c] = {
            "accuracy": sum(r["score"]["correct"] for r in rs) / max(1, len(rs)),
            "by_tier": by_tier,
            "e2e_ms_p50": sorted(r.get("e2e_ms", 0) for r in rs)[len(rs) // 2] if rs else None,
            "t_ctx_ms_mean": (sum(r.get("t_ctx_ms", 0) for r in rs) / len(rs)) if rs and "onto" in c and "no-onto" not in c else None,
            "llm_total_ms_mean": sum((r.get("llm") or {}).get("total_ms") or 0 for r in rs) / max(1, len(rs)),
            "prompt_tokens_mean": sum((r.get("llm") or {}).get("prompt_tokens") or 0 for r in rs) / max(1, len(rs)),
            "completion_tokens_mean": sum((r.get("llm") or {}).get("completion_tokens") or 0 for r in rs) / max(1, len(rs)),
            "cached_tokens_sum": sum((r.get("llm") or {}).get("cached_tokens") or 0 for r in rs),
        }
    summary["neo4j_enterprise"] = {"jmx": jmx.summary(), "tx_sampler": sampler.summary(),
                                   "index_reads_delta": index_read_delta(idx0, idx1), "connections": {"before": conn0, "after": conn1}}
    summary["postgres"] = {"pg_stat_statements_delta": sorted(pg_stats_delta(pg0, pg1), key=lambda x: -x["calls"])[:30]}
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=1, default=str))
    print(json.dumps({c: {"acc": round(v["accuracy"], 3), "by_tier": {t: round(x["accuracy"], 2) for t, x in v["by_tier"].items()},
                          "e2e_p50_ms": v["e2e_ms_p50"], "t_ctx_ms": v["t_ctx_ms_mean"]} for c, v in summary["conditions"].items()}, indent=1))
    print("wall", summary["wall_s"], "s ->", out_dir)


if __name__ == "__main__":
    asyncio.run(main())
