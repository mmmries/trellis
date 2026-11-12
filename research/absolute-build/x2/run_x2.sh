#!/usr/bin/env bash
# X2 series under one logbook reservation (research #1917). Start detached: nohup setsid ./run_x2.sh > run_x2.out 2>&1 &
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); cd "$HERE"
export PATH=$HOME/.asdf/installs/postgres/18.6/bin:$PATH
S=$(cd ../../.. && pwd)/target/research-pg-x2; PORT=54872
SU="psql -X -q -At -h $S -p $PORT -U postgres"
LB=/home/mike/code/trellis/.claude/scripts/logbook
FRESH="plain a c1 c2 A0 A0.1 A1 A10 B0 B0.1 B1 B10"
ID=$($LB reserve --wait --kind bench --lane r --issue 1917 --expect 3h \
  --cmd "research/absolute-build/x2/run_x2.sh: X2 statement micro, fresh+rebuild, 1M rows (100k/1M groups, 3 reps) and 10M rows/10M groups (1 rep)" | grep -E '^[0-9]{8}T[0-9]{6}-' | tail -1)
[ -n "$ID" ] || { echo "no logbook id"; exit 1; }
echo "logbook id: $ID"; echo "$ID" > logbook_id.txt
OUTCOME=ok
finish() {
  python3 -I summarize.py results x2_1m x2_10m > results/summary-tables.md 2>&1
  $LB finish "$ID" --rows "${ROWS:-0}" --outcome "$OUTCOME" --note "X2 plain-aggregate build statements (today a, c1 key-ordered, c2 ff50, A write-once+stragglers, B gate+upsert, plain): fresh and rebuild at 1M rows/100k+1M groups x3 and 10M/10M x1; see research/absolute-build/x2/X2-results.md. rows = generated 11M posts + rebuild source changes (estimate)"
  echo "finished $ID outcome=$OUTCOME"
}
trap 'OUTCOME=aborted; finish; exit 1' INT TERM
step() { echo "[$(date +%T)] $*"; "$@" || { echo "step failed: $*"; OUTCOME=fail; }; }
ROWS=1000000
# ---------- 1M rows: 100k groups (auth) and 1M groups (hi), 3 reps
for r in 1 2 3; do
  for sh in auth hi; do
    step env KEEP_C=1 ./x2.sh src1m x2_1m $sh fresh $r $FRESH
    step ./x2.sh src1m x2_1m $sh rebuild $r plain a c1 c2 Au B
  done
done
ROWS=$((ROWS + 4 * 110000))
# ---------- 10M rows / 10M groups, 1 rep; generating the data is inside the reservation
{ echo "gen 10m"; uptime; echo "rustc=$(pgrep -c rustc) cargo=$(pgrep -c cargo)"; } 
$SU -c "drop database if exists src10m" postgres; $SU -c "create database src10m owner app" postgres
step psql -X -q -h $S -p $PORT -U app -v N=10000000 -f gen_x2.sql src10m
ROWS=$((ROWS + 10000000))
step env KEEP_C=1 ./x2.sh src10m x2_10m hi fresh 1 plain c1 c2 A0 A0.1 A1 A10 B0 B0.1 B1 B10
step ./x2.sh src10m x2_10m hi rebuild 1 plain c1 c2 Au B
ROWS=$((ROWS + 2 * 1100000))
step env TIMEOUT=30min ./x2.sh src10m x2_10m hi rebuild 1 a
finish
