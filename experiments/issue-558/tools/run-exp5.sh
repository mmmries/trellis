#!/usr/bin/env bash
# One experiment-5 build-under-load run, detached. Usage: run-exp5.sh <lane> <tag> [flags...]
# Env passthrough: TRELLIS_EXP558_LEDGER / TRELLIS_EXP558_BUILD; storage on the NVMe.
lane=$1; tag=$2; shift 2
L=/home/mike/code/trellis-lane-$lane/experiments/issue-558/logs/exp5
cd /home/mike/code/trellis-lane-$lane || exit 1
echo "=== $(date +%T) exp5 $tag (lane $lane, cargo procs: $(pgrep -x cargo | wc -l))"
TRELLIS_BENCH_DISK_DIR=/home/mike/exp558/tmpdisk \
TRELLIS_TESTKIT_PG_OPTIONS="${TRELLIS_TESTKIT_PG_OPTIONS:-shared_buffers=1GB checkpoint_timeout=1min max_wal_size=4GB}" \
TRELLIS_BENCH_LOG=warn /home/mike/code/trellis/.claude/scripts/bench --disk build-under-load "$@" \
  > "$L/$tag.jsonl" 2> "$L/$tag.log"
echo "=== $(date +%T) exp5 $tag exit $?"
