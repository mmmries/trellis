#!/usr/bin/env bash
# X5 series under one logbook reservation (research #1917): fresh plain-aggregate build under concurrent writes,
# base 54cfddb2 vs candidate A prototype (b3b7856c). Start detached: nohup setsid ./run_x5.sh > run_x5.out 2>&1 &
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); cd "$HERE"; ROOT=$(cd ../../.. && pwd)
export PATH=$HOME/.asdf/installs/postgres/18.6/bin:$PATH
S=$ROOT/target/research-pg-x5; PORT=54875
SU="psql -X -q -At -h $S -p $PORT -U postgres"
LB=/home/mike/code/trellis/.claude/scripts/logbook
BASE=$ROOT/target/x5-bin/trellis-base; PROTO=$ROOT/target/x5-bin/trellis-proto
LOW=1000; HIGH=10000; export WORKERS=16
ID=$($LB reserve --wait --kind bench --lane r --issue 1917 --expect 2h \
  --cmd "research/absolute-build/x5/run_x5.sh: X5 plain-aggregate fresh build under concurrent writes (0/1k/10k row changes/s), base 54cfddb2 vs write-once prototype b3b7856c, s2_agg+s2_agg_hi 1M x2, no-index 1M, s2_agg_hi 10M proto" | grep -E '^[0-9]{8}T[0-9]{6}-' | tail -1)
[ -n "$ID" ] || { echo "no logbook id"; exit 1; }
echo "logbook id: $ID"; echo "$ID" > logbook_id.txt
OUTCOME=ok; GEN=0
finish() {
  pkill -f "$ROOT/target/x5-bin/trellis" 2>/dev/null; pkill -f "$HERE/loadgen.py" 2>/dev/null
  python3 analyze.py results/full/*/ results/full10m/*/ > results/analysis.tsv 2>&1
  WROWS=$(python3 -c "
import csv,sys
t=0
for r in csv.DictReader(open('results/analysis.tsv'), delimiter='\t'):
    try: t+=int(r.get('rows_changed') or 0)
    except: pass
print(t)")
  ROWS=$((GEN + WROWS))
  KEEP=$($SU -c "select count(*) from pg_database where datname like 'keep\_%'" postgres 2>/dev/null)
  if [ "${KEEP:-0}" = 0 ]; then
    pg_ctl -D "$S" stop -m fast >/dev/null 2>&1; rm -rf "$S"; echo "cluster deleted"
  else
    echo "KEEPING cluster $S: $KEEP mismatched database(s)"; pg_ctl -D "$S" stop -m fast >/dev/null 2>&1
  fi
  $LB finish "$ID" --rows "$ROWS" --outcome "$OUTCOME" --note "X5 fresh plain-aggregate build under concurrent writes, base 54cfddb2 vs candidate A prototype b3b7856c: s2_agg/s2_agg_hi 1M x {0,1k,10k rows/s} x2, no-index 1M, s2_agg_hi 10M proto {0,10k}; see research/absolute-build/x5/X5-results.md. rows = generated posts+post_tags ($GEN) + writer row changes ($WROWS)"
  echo "finished $ID outcome=$OUTCOME rows=$ROWS"
}
trap 'OUTCOME=aborted; finish; exit 1' INT TERM
step() { echo "[$(date +%T)] $*"; "$@" || { echo "step failed: $*"; OUTCOME=fail; }; }
boxload() { echo "== box load ($1): $(uptime) rustc=$(pgrep -c rustc) cargo=$(pgrep -c cargo)"; }
mkdir -p results
step ./setup_cluster.sh
$SU -c "create database src1m owner app" postgres
step psql -X -q -h $S -p $PORT -U app -v N=1000000 -f gen.sql src1m
GEN=2900000
boxload 1M
for r in 1 2; do
  for rate in 0 $LOW $HIGH; do
    for sh in s2_agg s2_agg_hi; do
      step env TR=$BASE ./run.sh src1m 1000000 full $r $sh $rate
      step env TR=$PROTO ./run.sh src1m 1000000 full $r $sh $rate
    done
  done
  step env TR=$PROTO ./run.sh src1m 1000000 full $r s2_agg_hi $HIGH noindex
done
$SU -c "drop database src1m" postgres
boxload 10M
$SU -c "create database src10m owner app" postgres
step psql -X -q -h $S -p $PORT -U app -v N=10000000 -f gen.sql src10m
GEN=$((GEN + 29000000))
boxload 10M-after-gen
step env WARM=15 TR=$PROTO ./run.sh src10m 10000000 full10m 1 s2_agg_hi 0
step env WARM=15 TR=$PROTO ./run.sh src10m 10000000 full10m 1 s2_agg_hi $HIGH
finish
