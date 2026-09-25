#!/usr/bin/env bash
# Experiment 3 driver: the fold-in ratio sweep and the #326 group-contention grid, once per
# ledger mode, through the `bench` script (exclusive lock; builds first). JSON lines land in
# logs/exp3-<mode>-<scenario>.jsonl next to this script; stderr verdicts in the .log files.
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(git -C "$here" rev-parse --show-toplevel)"
bench="${TRELLIS_MAIN_CHECKOUT:-/home/mike/code/trellis}/.claude/scripts/bench"  # scripts live only in the main checkout
cd "$root"
mkdir -p "$here/logs"
modes=${MODES:-"off contrib membership"}
for mode in $modes; do
  export TRELLIS_EXP558_LEDGER=$mode
  echo "=== $(date +%T) mode=$mode fold-in-ratio"
  "$bench" fold-in-ratio --ratios 1,10,100,1000 --duration-secs 20 --grace-secs 120 \
    > "$here/logs/exp3-$mode-fold-in-ratio.jsonl" 2> "$here/logs/exp3-$mode-fold-in-ratio.log"
  echo "=== $(date +%T) mode=$mode group-contention"
  "$bench" group-contention --groups 400,4000,40000 --threads 1,8 --duration-secs 20 --grace-secs 120 \
    > "$here/logs/exp3-$mode-group-contention.jsonl" 2> "$here/logs/exp3-$mode-group-contention.log"
done
echo "=== $(date +%T) done"
