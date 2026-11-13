#!/bin/bash
# X1b soak runner (#1917): reserve the box in the logbook (waiting), run the
# matrix, always finish the logbook entry. Start detached: nohup setsid ./run_soak_b.sh > run_soak_b.out 2>&1 &
set -u
cd /home/mike/code/trellis-lane-r/research/absolute-build/x1b
LB=/home/mike/code/trellis/.claude/scripts/logbook
CMD="python3 soak.py matrix soak_b.jsonl --plan x1b --duration 20 --parallel 8"
echo "reserving at $(date -Is)"
OUT=$($LB reserve --wait --kind soak --lane r --issue 1917 --expect 2h --cmd "x1b correctness-model soak (pre-enumeration, leaves, per-leaf B, resume): $CMD (research/absolute-build/x1b)")
rc=$?
echo "$OUT"
if [ $rc -ne 0 ] || [ -z "$OUT" ]; then echo "reserve failed rc=$rc"; exit 1; fi
ID=$(echo "$OUT" | tail -1 | awk '{print $1}')
echo "LOGBOOK_ID $ID at $(date -Is)"
echo "$ID" > logbook_id.txt
rm -f soak_b.jsonl
outcome=aborted
finish() {
  ROWS=$(grep TOTAL_ROWS soak_b_matrix.log 2>/dev/null | awk '{print $2}')
  NOTE="X1b soak, 20 s runs, appliers 2/4/8: $(python3 summarize_b.py soak_b.jsonl --oneline 2>/dev/null)"
  if [ -z "$ROWS" ]; then
    ROWS=$(python3 -c "import json;print(sum(json.loads(l).get('rows_written',0)+8 for l in open('soak_b.jsonl') if l.strip()))" 2>/dev/null || echo 0)
    NOTE="(rows from completed runs only) $NOTE"
  fi
  $LB finish "$ID" --rows "${ROWS:-0}" --outcome "$outcome" --note "$NOTE"
  echo "FINISHED $ID $outcome rows=$ROWS at $(date -Is)"
}
trap finish EXIT
$CMD > soak_b_matrix.log 2>&1
rc=$?
if [ $rc -eq 0 ]; then outcome=ok; else outcome=fail; fi
echo "matrix rc=$rc"
