#!/usr/bin/env bash
# Same-session control for the factored plain-aggregate slowdown: off and contrib in lane g's
# build, tmpfs, right after the disk rerun. Then the 100k disk shapes (replaces queue-disk2.sh).
B=/home/mike/code/trellis/.claude/scripts/bench
L=/home/mike/code/trellis-lane-c/experiments/issue-558/logs
mark() { echo "=== $(date +%T) $1 (cargo procs: $(pgrep -x cargo | wc -l))"; }
while pgrep -f '^bash \./queue-rerun\.sh' > /dev/null; do sleep 30; done
cd /home/mike/code/trellis-lane-g || exit 1
for m in off contrib factored; do
  mark "lane g $m fold-in-ratio 100,1000 tmpfs (control)"
  TRELLIS_EXP558_LEDGER=$m "$B" fold-in-ratio --ratios 100,1000 --duration-secs 20 --grace-secs 120 \
    > "$L/exp3-$m-fold-in-ratio-laneg.jsonl" 2> "$L/exp3-$m-fold-in-ratio-laneg.log"
done
mark "controls done"
exec /home/mike/exp558/queue-disk2.sh
