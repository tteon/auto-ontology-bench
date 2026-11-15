"""Pre-compute check_answerable (POST /api/question-entity-coverage) once per question.

    uv run python -m ontology.cache_coverage [--split all]

Why: Auto-Ontology serializes this endpoint behind a process-wide lock
(auto_ontology/server/metadata/service.py `_run_lock`: "not safe to run in
parallel"), so under concurrent load every onto-arm request queues behind the
others (observed: 79/80 requests past a 180 s timeout at concurrency 8). The
coverage result depends only on the question, so it is computed once, serially,
with its unloaded latency recorded, and reused by every onto condition and
repeat. Resumable: questions already cached are skipped.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import httpx
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")
API = os.environ.get("AUTO_ONTOLOGY_API_URL", "http://127.0.0.1:3001")
CACHE = ROOT / "ontology" / "coverage_cache.json"


def load_cache() -> dict:
    return json.loads(CACHE.read_text()) if CACHE.exists() else {}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--split", default="all")
    ap.add_argument("--max-seconds", type=float, default=None, help="stop starting new questions after this budget")
    a = ap.parse_args()
    items = [json.loads(line) for line in (ROOT / "benchmarks" / "cq_dataset.jsonl").read_text().splitlines()]
    items = [it for it in items if a.split == "all" or it["split"] == a.split]
    items.sort(key=lambda it: it["split"] != "dev")  # dev first: needed for threshold calibration
    cache = load_cache()
    http = httpx.Client(timeout=600)
    t_start = time.monotonic()
    for n, it in enumerate(items):
        if it["question"] in cache:
            continue
        if a.max_seconds and time.monotonic() - t_start > a.max_seconds:
            print(f"budget reached; {sum(1 for x in items if x['question'] in cache)}/{len(items)} cached", flush=True)
            break
        t = time.perf_counter()
        r = http.post(f"{API}/api/question-entity-coverage", json={"question": it["question"]})
        ms = (time.perf_counter() - t) * 1000
        entry = {"cq_id": it["cq_id"], "status": r.status_code, "ms": ms,
                 "data": r.json().get("data") if r.status_code == 200 else None, "at": time.strftime("%H:%M:%S")}
        cache[it["question"]] = entry
        CACHE.write_text(json.dumps(cache, indent=1))
        print(f"[{n + 1}/{len(items)}] {it['cq_id']} {ms / 1000:.1f}s status={r.status_code} "
              f"coverage={(entry['data'] or {}).get('coverage')}", flush=True)


if __name__ == "__main__":
    main()
