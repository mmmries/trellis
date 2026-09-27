#!/usr/bin/env bash
# Item 3, second half (#558 comment 2026-09-26 16:34, point 2): experiment 4's 100k-fan-out shape
# disk-backed on every lane: main control (lane d), contrib (lane c), factored (lane g).
# Waits for the earlier queues. The wait pattern is anchored so it can never match its own
# launcher's command line (the earlier queues deadlocked on exactly that for 2.5 h).
B=/home/mike/code/trellis/.claude/scripts/bench
L=/home/mike/code/trellis-lane-c/experiments/issue-558/logs/disk
export TRELLIS_BENCH_DISK_DIR=/home/mike/exp558/tmpdisk
mark() { echo "=== $(date +%T) $1 (cargo procs: $(pgrep -x cargo | wc -l))"; }
# (launched by queue-ctl.sh; no wait)

# Same-binary-as-yesterday control: lane c `off` fold-in on tmpfs, to separate "lane g's build" from "the box today".
cd /home/mike/code/trellis-lane-c || exit 1
mark "lane c off fold-in-ratio 100,1000 tmpfs (yesterday's binary, today)"
TRELLIS_EXP558_LEDGER=off "$B" fold-in-ratio --ratios 100,1000 --duration-secs 20 --grace-secs 120 \
  > "$L/../exp3-off-fold-in-ratio-today.jsonl" 2> "$L/../exp3-off-fold-in-ratio-today.log"

cd /home/mike/code/trellis-lane-d || exit 1
mark "558 main control rel-churn 100k disk (lane d)"
TRELLIS_BENCH_LOG=warn "$B" --disk rel-churn --children 100000 --parent-rate 100,1000 --child-rate 1000 --duration-secs 20 --grace-secs 300 \
  > "$L/exp4-main-rel-churn-100k.jsonl" 2> "$L/exp4-main-rel-churn-100k.log"

cd /home/mike/code/trellis-lane-c || exit 1
mark "558 contrib rel-churn 100k disk (lane c)"
TRELLIS_EXP558_LEDGER=contrib TRELLIS_BENCH_LOG=warn "$B" --disk rel-churn --children 100000 --parent-rate 100,1000 --child-rate 1000 --duration-secs 20 --grace-secs 300 \
  > "$L/exp4-contrib-rel-churn-100k.jsonl" 2> "$L/exp4-contrib-rel-churn-100k.log"

cd /home/mike/code/trellis-lane-g || exit 1
mark "558 factored rel-churn 100k disk (lane g)"
TRELLIS_EXP558_LEDGER=factored TRELLIS_BENCH_LOG=warn "$B" --disk rel-churn --children 100000 --parent-rate 100,1000 --child-rate 1000 --duration-secs 20 --grace-secs 300 \
  > "$L/exp4-factored-rel-churn-100k.jsonl" 2> "$L/exp4-factored-rel-churn-100k.log"
mark "all done"
