"""Find the NVIDIA API concurrency ceiling for the generation model before Stage A.

    uv run python -m load_test.api_limit --levels 1,2,4,8,16 --per-level 16

Sends short streaming completions at increasing fixed concurrency and records
429s, other errors, TTFT and total latency per level.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

import httpx
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

from gateway import llm  # noqa: E402
from load_test.loadgen import run_load, summarize  # noqa: E402

PROMPT = [{"role": "system", "content": "Reply with one SQL query in a ```sql block."},
          {"role": "user", "content": "SCHEMA: CREATE TABLE t (id int, v numeric);\nQUESTION: total of v?"}]


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--levels", default="1,2,4,8,16")
    ap.add_argument("--per-level", type=int, default=16)
    a = ap.parse_args()
    out = {}
    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=64)) as http:
        async def req(_: int, rid: str) -> dict:
            try:
                r = await llm.complete(http, PROMPT, max_tokens=512)
                return {"ttft_ms": r["ttft_ms"], "total_ms": r["total_ms"]}
            except llm.RateLimited:
                return {"error_type": "RATE_LIMITED"}
            except httpx.HTTPError as e:
                return {"error_type": f"HTTP_{getattr(getattr(e, 'response', None), 'status_code', 'ERR')}"}

        for c in [int(x) for x in a.levels.split(",")]:
            recs = await run_load(req, list(range(a.per_level)), item_id=str, concurrency=c, n_requests=max(a.per_level, c * 2))
            s = summarize(recs)
            ttft = sorted(r.extra["ttft_ms"] for r in recs if r.ok and r.extra.get("ttft_ms"))
            s["ttft_p50"] = ttft[len(ttft) // 2] if ttft else None
            out[c] = s
            print(f"c={c:3d} n={s['n']} ok={s['ok']} rps={s['throughput_rps']:.2f} p50={s['p50']:.0f}ms p99={s['p99']:.0f}ms "
                  f"ttft_p50={s['ttft_p50']} errors={s['errors']}", flush=True)
            if s["errors"].get("RATE_LIMITED", 0) > s["n"] * 0.1:
                print("rate limit reached; stopping ladder")
                break
    (ROOT / "results" / "api_limit.json").write_text(json.dumps(out, indent=1))


if __name__ == "__main__":
    asyncio.run(main())
