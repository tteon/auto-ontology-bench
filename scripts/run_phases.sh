#!/usr/bin/env bash
# Measurements for one scale factor, phase by phase (each phase fits a 2-hour job limit).
#   scripts/run_phases.sh finder_s100k s100k load verify
#   scripts/run_phases.sh finder_s100k s100k sweep
#   scripts/run_phases.sh finder_s100k s100k depth mixed tuning
# Phases: load verify sweep depth mixed tuning. Neo4j must hold $DB for every phase after `load`.
set -euo pipefail
DB=$1; SF=$2; shift 2
PHASES=${*:-verify sweep depth mixed}
cd "$(dirname "$0")/.."
for p in $PHASES; do
  echo "=== $SF: $p ($(date +%H:%M:%S))"
  case $p in
    load)   uv run python -m graph_sync.load_fast --db "$DB" --wipe --mode server --batch 50000 --tx-rows 5000 ;;
    verify) uv run python -m graph_sync.load_fast --db "$DB" --verify-only ;;
    sweep)  uv run python -m load_test.runtime_study --db "$DB" --workloads W1,W2,W3,W4a,W4b --conc 1,16 --duration 20 --warmup 6 --tag "sweep-$SF" ;;
    depth)  uv run python -m load_test.depth_sweep --db "$DB" --kmax 8 --anchors 3 --reps 3 --tag "depth-$SF" ;;
    mixed)  uv run python -m load_test.mixed --db "$DB" --duration 45 --warmup 8 --tag "mixed-$SF" ;;
    tuning) uv run python -m load_test.tuning --db "$DB" --tag "tuning-$SF" ;;
    *) echo "unknown phase $p"; exit 2 ;;
  esac
done
echo "=== $SF done ($(date +%H:%M:%S))"
