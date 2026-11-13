#!/bin/bash
# X1 soak runner (#1917): reserve the box in the logbook (waiting), run the
# matrix, always finish the logbook entry.
set -u
cd /home/mike/code/trellis-lane-r/research/absolute-build/x1
LB=/home/mike/code/trellis/.claude/scripts/logbook
CMD="python3 soak.py matrix soak.jsonl --plan full --duration 20 --parallel 8"
echo "reserving at $(date -Is)"
ID=$($LB reserve --wait --kind soak --lane r --issue 1917 --expect 2h --cmd "x1 correctness-model soak: $CMD (research/absolute-build/x1)")
rc=$?
if [ $rc -ne 0 ] || [ -z "$ID" ]; then echo "reserve failed rc=$rc"; exit 1; fi
ID=$(echo "$ID" | tail -1 | awk '{print $1}')
echo "LOGBOOK_ID $ID at $(date -Is)"
rm -f soak.jsonl
outcome=aborted
trap 'ROWS=$(grep TOTAL_ROWS soak_matrix.log 2>/dev/null | awk "{print \$2}"); $LB finish "$ID" --rows "${ROWS:-0}" --outcome "$outcome" --note "X1 soak (estimate if aborted): $(python3 summarize.py soak.jsonl --oneline 2>/dev/null)"; echo "FINISHED $ID $outcome at $(date -Is)"' EXIT
$CMD > soak_matrix.log 2>&1
rc=$?
if [ $rc -eq 0 ]; then outcome=ok; else outcome=fail; fi
echo "matrix rc=$rc"
