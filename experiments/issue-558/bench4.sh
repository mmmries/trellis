#!/usr/bin/env bash
# Experiment 4 driver: the rel-churn grid once per ledger mode, through `bench`.
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(git -C "$here" rev-parse --show-toplevel)"
bench="${TRELLIS_MAIN_CHECKOUT:-/home/mike/code/trellis}/.claude/scripts/bench"
cd "$root"
mkdir -p "$here/logs"
modes=${MODES:-"off contrib membership"}
for mode in $modes; do
  export TRELLIS_EXP558_LEDGER=$mode
  echo "=== $(date +%T) mode=$mode rel-churn"
  TRELLIS_BENCH_LOG=warn "$bench" rel-churn --children 10,1000,100000 --parent-rate 100,1000 --child-rate 1000 --duration-secs 20 --grace-secs 300 \
    > "$here/logs/exp4-$mode-rel-churn.jsonl" 2> "$here/logs/exp4-$mode-rel-churn.log"
done
echo "=== $(date +%T) done"
