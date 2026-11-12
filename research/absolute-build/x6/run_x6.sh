#!/usr/bin/env bash
# #1917 X4 measurement + X6 plant sweep, each under its own logbook reservation.
# Start detached from the worktree root:
#   nohup setsid research/absolute-build/x6/run_x6.sh > research/absolute-build/x6/run_x6.out 2>&1 &
set -uo pipefail
ROOT=/home/mike/code/trellis-lane-r; cd "$ROOT"
export PATH=$HOME/.asdf/installs/postgres/18.6/bin:$PATH
LB=/home/mike/code/trellis/.claude/scripts/logbook
X4=$ROOT/research/absolute-build/x4; X6=$ROOT/research/absolute-build/x6
ts() { date +%T; }
reserve() { # kind expect cmd
  $LB reserve --wait --kind "$1" --lane r --issue 1917 --expect "$2" --cmd "$3" | grep -E '^[0-9]{8}T[0-9]{6}-' | tail -1
}

# ---------------------------------------------------------------- X4
ID4=$(reserve bench 1h "research/absolute-build/x4 (run_x6.sh part 1): X4 per-leaf fresh build, today vs candidate A per leaf, 1M rows/1M groups over 32 leaves and 1 leaf x2 reps, 10M/10M over 32 leaves x1")
[ -n "$ID4" ] || { echo "no logbook id for X4"; exit 1; }
echo "[$(ts)] X4 logbook id: $ID4"; echo "$ID4" > "$X4/logbook_id.txt"
OUT4=ok; ROWS4=0
finish4() {
  pkill -f "$ROOT/target/x4-bin/trellis" 2>/dev/null
  pg_ctl -D "$ROOT/target/research-pg-x4" stop -m fast >/dev/null 2>&1; rm -rf "$ROOT/target/research-pg-x4"
  $LB finish "$ID4" --rows "$ROWS4" --outcome "$OUT4" --note "X4: fresh build of a plain aggregate over a partitioned source, per leaf, today's three statements vs candidate A per leaf (TRELLIS_X4_LEAF_TODAY switch, one binary at the X4 commit); 1M/1M over 32 leaves and over 1 leaf x2 reps, 10M/10M over 32 leaves x1; see research/absolute-build/x4/X4-results.md. rows = seeded posts through capture (estimate)"
  echo "[$(ts)] X4 finished $ID4 outcome=$OUT4"
}
trap 'OUT4=aborted; finish4; exit 1' INT TERM
step() { echo "[$(ts)] $*"; "$@" || { echo "step failed: $*"; OUT4=fail; }; }
echo "[$(ts)] build cli"; cargo build --release -p trellis-cli 2>&1 | tail -2
mkdir -p target/x4-bin; cp target/release/trellis target/x4-bin/trellis
cd "$X4"; mkdir -p results
step ./setup_cluster.sh
step ./template.sh src1m32 1000000 32; ROWS4=$((ROWS4 + 1000000))
step ./template.sh src1m1 1000000 1; ROWS4=$((ROWS4 + 1000000))
for r in 1 2; do
  for t in src1m32 src1m1; do
    step ./run.sh $t $t $r today
    step ./run.sh $t $t $r a
  done
done
psql -X -q -h "$ROOT/target/research-pg-x4" -p 54874 -U postgres -c "drop database src1m32" -c "drop database src1m1" postgres
step ./template.sh src10m32 10000000 32; ROWS4=$((ROWS4 + 10000000))
step ./run.sh src10m32 src10m32 1 a
step ./run.sh src10m32 src10m32 1 today
cat results/*/*/summary.tsv > results/summary.tsv
cd "$ROOT"; finish4; trap - INT TERM

# ---------------------------------------------------------------- X6
PLANTS=ab_gc_ignores_build,ab_enum_no_hidden,ab_enum_no_target,ab_enum_no_ring,ab_stragglers_from_s1,gb_build_snapshot_before_locks,gb_early_zero_gc
ID6=$(reserve sweep 2h "research/absolute-build/x6/run_x6.sh part 2: GENERATIVE_PLANT_SEEDS=2 GENERATIVE_PLANT_CASES=6 GENERATIVE_PLANTS=$PLANTS planted_bugs_are_caught (concurrent_convergence) tiers mid_burst, steady_load (35 min cap each); then property_partitioned_hot_keys / property_a_partitioned_source PROPTEST_CASES=16 unplanted and TRELLIS_TEST_PLANT=ab_leaf_unrouted")
[ -n "$ID6" ] || { echo "no logbook id for X6"; exit 1; }
echo "[$(ts)] X6 logbook id: $ID6"; echo "$ID6" > "$X6/logbook_id.txt"
OUT6=ok
finish6() {
  local cases; cases=$(cat "$X6"/sweep-*.log 2>/dev/null | grep -c "plant-sweep	" || true)
  $LB finish "$ID6" --rows $((cases * 2000 + 64 * 500)) --outcome "$OUT6" --note "X6: candidate A prototype's plants (ab_*) + gb_build_snapshot_before_locks + gb_early_zero_gc, concurrent tier sweep mid_burst + steady_load at 2 seeds x 6 cases, plus the partitioned properties unplanted and with ab_leaf_unrouted; see research/absolute-build/x6/X6-results.md. rows estimate (2000/case)"
  echo "[$(ts)] X6 finished $ID6 outcome=$OUT6"
}
trap 'OUT6=aborted; finish6; exit 1' INT TERM
echo "[$(ts)] build generative"
cargo test -p generative --test concurrent_convergence --no-run 2>&1 | tail -2
for tier in mid_burst steady_load; do
  echo "[$(ts)] sweep $tier"
  timeout 35m env GENERATIVE_PLANTS=$PLANTS GENERATIVE_PLANT_TIER=$tier GENERATIVE_PLANT_SEEDS=2 \
    GENERATIVE_PLANT_CASES=6 cargo test -p generative --test concurrent_convergence \
    planted_bugs_are_caught -- --ignored --nocapture > "$X6/sweep-$tier.log" 2>&1
  rc=$?; echo "[$(ts)] sweep $tier rc=$rc"
  [ $rc = 0 ] || OUT6=miss
done
for plant in none ab_leaf_unrouted; do
  for prop in property_partitioned_hot_keys_converge_under_concurrent_drains \
              property_a_partitioned_source_converges_under_the_concurrent_backend; do
    echo "[$(ts)] $prop plant=$plant"
    p=$plant; [ "$p" = none ] && p=""
    timeout 12m env TRELLIS_TEST_PLANT=$p PROPTEST_CASES=16 cargo test -p generative \
      --test concurrent_convergence $prop -- --ignored --nocapture \
      > "$X6/prop-$plant-$prop.log" 2>&1
    echo "[$(ts)] $prop plant=$plant rc=$?"
  done
done
finish6
