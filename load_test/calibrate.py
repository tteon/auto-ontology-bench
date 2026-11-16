"""Choose the check_answerable coverage threshold for the onto arms on the dev split only.

    uv run python -m load_test.calibrate --tag dev-onto     -> results/threshold.json

Replays the recorded dev outcomes under each candidate threshold: a request
whose coverage is below the threshold becomes a rejection (correct for Tier 5,
a false rejection otherwise); otherwise its recorded outcome stands. Picks the
threshold with the highest mean accuracy over the onto conditions; ties go to
the lower threshold (fewer rejections). `None` (no gate) is a candidate.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", required=True)
    a = ap.parse_args()
    recs = [json.loads(line) for line in (ROOT / "results" / "stage_a" / a.tag / "records.jsonl").read_text().splitlines()]
    recs = [r for r in recs if r.get("coverage") is not None and r["split"] == "dev"]
    grid: list[float | None] = [None] + [round(x * 0.05, 2) for x in range(0, 21)]
    table = []
    for th in grid:
        correct = 0
        false_rej = 0
        for r in recs:
            if th is not None and r["coverage"] < th:
                ok = r["tier"] == 5
                false_rej += r["tier"] != 5
            else:
                ok = r["score"]["correct"]
            correct += ok
        table.append({"threshold": th, "accuracy": correct / len(recs), "false_rejections": false_rej})
    best = max(table, key=lambda x: (x["accuracy"], -(x["threshold"] or -1)))
    out = {"tag": a.tag, "n": len(recs), "chosen": best, "grid": table,
           "coverage_by_tier": {t: sorted(r["coverage"] for r in recs if r["tier"] == t) for t in sorted({r["tier"] for r in recs})}}
    (ROOT / "results" / "threshold.json").write_text(json.dumps(out, indent=1))
    for row in table:
        print(f"threshold={row['threshold']}: acc={row['accuracy']:.3f} false_rej={row['false_rejections']}")
    print("chosen:", best)


if __name__ == "__main__":
    main()
