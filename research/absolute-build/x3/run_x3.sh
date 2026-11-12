#!/usr/bin/env bash
# X3 end-to-end build series under one logbook reservation (research #1917).
# Start detached: nohup setsid ./run_x3.sh > run_x3.out 2>&1 &
# base = trellis-cli at 54cfddb2, proto = this branch's prototype (binaries copied to target/x3-bin).
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); cd "$HERE"; ROOT=$(cd ../../.. && pwd)
export PATH=$HOME/.asdf/installs/postgres/18.6/bin:$PATH
S=$ROOT/target/research-pg-x3; PORT=54873
SU="psql -X -q -At -h $S -p $PORT -U postgres"
LB=/home/mike/code/trellis/.claude/scripts/logbook
BASE=$ROOT/target/x3-bin/trellis-base; PROTO=$ROOT/target/x3-bin/trellis-proto
ID=$($LB reserve --wait --kind bench --lane r --issue 1917 --expect 2h \
  --cmd "research/absolute-build/x3/run_x3.sh: X3 end-to-end plain-aggregate build, base 54cfddb2 vs write-once prototype, E5 method, s2_agg+s2_agg_hi 1M x3, s2_agg_hi 10M x1 (proto only)" | grep -E '^[0-9]{8}T[0-9]{6}-' | tail -1)
[ -n "$ID" ] || { echo "no logbook id"; exit 1; }
echo "logbook id: $ID"; echo "$ID" > logbook_id.txt
OUTCOME=ok; ROWS=0
finish() {
  pkill -f "$ROOT/target/x3-bin/trellis" 2>/dev/null
  for l in base proto proto10m; do echo "## $l"; python3 -I summarize.py $l; done > results/summary-tables.md 2>&1
  pg_ctl -D "$S" stop -m fast >/dev/null 2>&1; rm -rf "$S"
  $LB finish "$ID" --rows "$ROWS" --outcome "$OUTCOME" --note "X3 end-to-end plain-aggregate fresh build (define to live), base 54cfddb2 vs candidate A prototype, E5 method: s2_agg/s2_agg_hi at 1M rows x3 each, s2_agg_hi 10M rows x1 proto only; see research/absolute-build/x3/X3-results.md. rows = generated posts+post_tags (1M: 2.9M, 10M: 29M), estimate"
  echo "finished $ID outcome=$OUTCOME"
}
trap 'OUTCOME=aborted; finish; exit 1' INT TERM
step() { echo "[$(date +%T)] $*"; "$@" || { echo "step failed: $*"; OUTCOME=fail; }; }
mkdir -p results
step ./setup_cluster.sh
$SU -c "create database src1m owner app" postgres
step psql -X -q -h $S -p $PORT -U app -v N=1000000 -f gen.sql src1m
ROWS=2900000
for r in 1 2 3; do
  for sh in s2_agg s2_agg_hi; do
    step env TR=$BASE ./run.sh src1m base $r $sh
    step env TR=$PROTO ./run.sh src1m proto $r $sh
  done
done
$SU -c "drop database src1m" postgres
{ echo "gen 10m"; uptime; echo "rustc=$(pgrep -c rustc) cargo=$(pgrep -c cargo)"; }
$SU -c "create database src10m owner app" postgres
step psql -X -q -h $S -p $PORT -U app -v N=10000000 -f gen.sql src10m
ROWS=$((ROWS + 29000000))
step env TR=$PROTO ./run.sh src10m proto10m 1 s2_agg_hi
finish
