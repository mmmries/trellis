#!/usr/bin/env bash
# X4: one fresh build of hi_totals over a partitioned template database, leaf path by $MODE
# (today = TRELLIS_X4_LEAF_TODAY set, a = unset). usage: run.sh <template> <label> <rep> <mode>
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); ROOT=$(cd "$HERE/../../.." && pwd)
export PATH=$HOME/.asdf/installs/postgres/18.6/bin:$PATH
S=$ROOT/target/research-pg-x4; PORT=54874; TR=$ROOT/target/x4-bin/trellis
SRC=$1; LABEL=$2; REP=$3; MODE=$4
SU="psql -X -q -At -h $S -p $PORT -U postgres"; APP="psql -X -q -At -h $S -p $PORT -U app"
now() { date +%s.%N; }
lsn() { $SU -c "select pg_current_wal_lsn()" postgres; }
OUT=$HERE/results/$LABEL/$MODE-r$REP; mkdir -p "$OUT"
DB=x4_${LABEL}_${MODE}_r$REP
$SU -c "drop database if exists $DB with (force)" postgres
$SU -c "create database $DB template $SRC owner app" postgres || exit 1
{ uptime; echo "rustc=$(pgrep -c rustc) cargo=$(pgrep -c cargo)"; } > "$OUT/box.txt"
# plain INSERT reference
$APP -c "create table plain_ins (grp_hi bigint primary key, n bigint, total_score bigint, total_likes bigint)" "$DB"
$SU -c checkpoint postgres; sleep 1
t0=$(now); $APP -c "insert into plain_ins select grp_hi, count(*), sum(score), sum(likes) from posts group by grp_hi" "$DB"; t1=$(now)
INS_S=$(echo "$t1 - $t0" | bc)
$SU -c checkpoint postgres; sleep 1
DBOID=$($SU -c "select oid from pg_database where datname='$DB'" postgres)
export TRELLIS_DATABASE_URL="postgres://app@127.0.0.1:$PORT/$DB"
if [ "$MODE" = today ]; then export TRELLIS_X4_LEAF_TODAY=1; else unset TRELLIS_X4_LEAF_TODAY; fi
$TR run > "$OUT/run.log" 2>&1 & RPID=$!
until grep -q "Ctrl-C" "$OUT/run.log" 2>/dev/null; do sleep 0.2; done
sleep 2
$SU -c "select pg_stat_statements_reset(0,$DBOID,0)" postgres >/dev/null
L0=$(lsn); t0=$(now)
$TR apply "TRANSFORM hi_totals FROM posts GROUP BY grp_hi SELECT grp_hi AS grp_hi, COUNT(*) AS n, SUM(score) AS total_score, SUM(likes) AS total_likes" > "$OUT/apply.log" 2>&1 || cat "$OUT/apply.log"
while :; do
  st=$($TR status 2>&1 | grep "target=public.hi_totals" | grep -m1 -o "status=[a-z_]*" | cut -d= -f2)
  case $st in live) break;; paused|failed|quarantined|halted) echo "ended in $st" >> "$OUT/apply.log"; break;; esac
  sleep 0.25
done
t1=$(now); L1=$(lsn); BUILD_S=$(echo "$t1 - $t0" | bc)
BUILD_W=$($SU -c "select pg_wal_lsn_diff('$L1','$L0')::bigint" postgres)
$TR status > "$OUT/status.txt" 2>&1
kill $RPID 2>/dev/null; wait $RPID 2>/dev/null
DIFF=$($APP -c "select (select count(*) from (select * from hi_totals except all select * from plain_ins) x) + (select count(*) from (select * from plain_ins except all select * from hi_totals) x)" "$DB" 2>&1)
$SU -c "select calls, round(total_exec_time::numeric,0) ms, rows, wal_bytes, wal_fpi, temp_blks_written*8/1024 temp_mb, regexp_replace(left(query,140),'\s+',' ','g') q from pg_stat_statements where dbid=$DBOID and query !~ 'pg_stat_statements|pg_current_wal' order by total_exec_time desc limit 15" postgres > "$OUT/pgss.txt"
$SU -c "select '-- calls='||calls||' ms='||round(total_exec_time::numeric)||' rows='||rows||' wal='||wal_bytes||' fpi='||wal_fpi||' temp_mb='||(temp_blks_written*8/1024)||E'\n'||query||E'\n' from pg_stat_statements where dbid=$DBOID and query !~ 'pg_stat_statements|pg_current_wal' order by total_exec_time desc limit 8" postgres > "$OUT/pgss-full.txt"
HID=$($SU -c "select pg_total_relation_size(format('trellis.agg_%s', id)) from trellis.transform_definitions where target_table = 'public.hi_totals'" "$DB")
printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "$LABEL" "$MODE" "$REP" "$INS_S" "$BUILD_S" "$BUILD_W" "$HID" "$DIFF" | tee "$OUT/summary.tsv"
$SU -c "drop database $DB with (force)" postgres
