#!/usr/bin/env bash
# X5: one fresh plain-aggregate build under concurrent writes (#1917).
# usage: run.sh <src_db> <N> <label> <rep> <shape> <rate> [noindex]
#   rate = target row changes/s (0 = no writers). noindex: drop the group-key index, define WITHOUT SOURCE INDEX.
# env: TR (trellis binary), PGDATA_DIR, PORT, WORKERS (default 8), POST_LIVE (20 s), WARM (8 s)
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); ROOT=$(cd "$HERE/../../.." && pwd)
export PATH=$HOME/.asdf/installs/postgres/18.6/bin:$PATH
S=${PGDATA_DIR:-$ROOT/target/research-pg-x5}; PORT=${PORT:-54875}
TR=${TR:?TR}; WORKERS=${WORKERS:-8}; POST_LIVE=${POST_LIVE:-20}; WARM=${WARM:-8}
SRC=$1; N=$2; LABEL=$3; REP=$4; SH=$5; RATE=$6; VAR=${7:-}
source "$HERE/shapes.sh"
SU="psql -X -q -At -h $S -p $PORT -U postgres"; APP="psql -X -q -At -h $S -p $PORT -U app"
now() { date +%s.%N; }
lsn() { $SU -c "select pg_current_wal_insert_lsn()" postgres; }
walb() { $SU -c "select pg_wal_lsn_diff('$2','$1')::bigint" postgres; }
TAG=$SH-n$N-rate$RATE${VAR:+-$VAR}-r$REP
BIN=$(basename "$TR"); BIN=${BIN#trellis-}
OUT=$HERE/results/$LABEL/$BIN/$TAG; rm -rf "$OUT"; mkdir -p "$OUT"
DB=x5_${LABEL}_r$REP; TT=${TARGET[$SH]}
case $SH in s2_agg) PK=author_id; GK=author_id; IDX=posts_author_idx;; s2_agg_hi) PK=grp_hi; GK=grp_hi; IDX=posts_grp_hi_idx;; esac
TRANS=${TRANSFORM[$SH]}
[ "$VAR" = noindex ] && TRANS=${TRANS/GROUP BY $GK/GROUP BY $GK WITHOUT SOURCE INDEX}
$SU -c "drop database if exists $DB with (force)" postgres
$SU -c "create database $DB template $SRC owner app" postgres || exit 1
[ "$VAR" = noindex ] && $APP -c "drop index $IDX" "$DB"
{ echo "tag=$TAG label=$LABEL tr=$TR"; $TR --version 2>&1; uptime; echo "rustc=$(pgrep -c rustc) cargo=$(pgrep -c cargo)"; echo "transform: $TRANS"; } > "$OUT/box.txt"
DBOID=$($SU -c "select oid from pg_database where datname='$DB'" postgres)
$APP -c "create table plain_ins as ${PLAIN[$SH]} limit 0; alter table plain_ins add primary key ($PK)" "$DB"
$SU -c checkpoint postgres
export TRELLIS_DATABASE_URL="postgres://app@127.0.0.1:$PORT/$DB"
T_RUN=$(now)
$TR run > "$OUT/run.log" 2>&1 & RPID=$!
until grep -q "Ctrl-C" "$OUT/run.log" 2>/dev/null; do sleep 0.2; done
python3 "$HERE/sampler.py" "postgresql://postgres@127.0.0.1:$PORT/postgres" "$DB" "$OUT/samples.tsv" 0.25 & SPID=$!
WPID=
if [ "$RATE" != 0 ]; then
  python3 "$HERE/loadgen.py" "postgresql://app@127.0.0.1:$PORT/$DB" "$N" "$RATE" "$WORKERS" "$OUT/writers" $REP 2> "$OUT/writers.err" & WPID=$!
fi
T_W0=$(now); sleep "$WARM"
# ---------- plain INSERT … SELECT on the same (loaded) database, into a scratch table
$SU -c "select pg_stat_statements_reset(0,$DBOID,0)" postgres >/dev/null
L0=$(lsn); t0=$(now)
$APP -c "insert into plain_ins ${PLAIN[$SH]}" "$DB"
t1=$(now); L1=$(lsn); INS_S=$(echo "$t1 - $t0" | bc); INS_W=$(walb $L0 $L1)
T_INS0=$t0; T_INS1=$t1
$APP -c "drop table plain_ins" "$DB"
sleep 3
# ---------- the build
$SU -c "select pg_stat_statements_reset(0,$DBOID,0)" postgres >/dev/null
L0=$(lsn); t0=$(now); T_APPLY=$t0
$TR apply "$TRANS" > "$OUT/apply.log" 2>&1 || { echo "APPLY FAILED"; cat "$OUT/apply.log"; }
ST=
while :; do
  st=$($TR status 2>&1 | grep -m1 "status=" | sed 's/.*status=\([a-z_]*\).*/\1/')
  case $st in live) ST=live; break;; paused|failed|quarantined|halted) ST=$st; echo "ended in $st" >> "$OUT/apply.log"; break;; esac
  sleep 0.25
done
t1=$(now); L1=$(lsn); T_LIVE=$t1; BUILD_S=$(echo "$t1 - $t0" | bc); BUILD_W=$(walb $L0 $L1)
$SU -c "select calls, round(total_exec_time::numeric,0) ms, rows, wal_bytes, wal_fpi, temp_blks_written*8/1024 temp_mb, regexp_replace(left(query,160),'\s+',' ','g') q from pg_stat_statements where dbid=$DBOID and query !~ 'pg_stat_statements|pg_current_wal' order by total_exec_time desc limit 25" postgres > "$OUT/pgss-build.txt"
$SU -c "select '-- calls='||calls||' ms='||round(total_exec_time::numeric)||' rows='||rows||' wal='||wal_bytes||' fpi='||wal_fpi||' temp_mb='||(temp_blks_written*8/1024)||E'\n'||query||E'\n' from pg_stat_statements where dbid=$DBOID and query !~ 'pg_stat_statements|pg_current_wal' and (total_exec_time > 50 or wal_bytes > 1000000 or query like 'with snap as materialized%') order by total_exec_time desc limit 40" postgres > "$OUT/pgss-build-full.txt"
[ "$ST" = live ] && sleep "$POST_LIVE"
# ---------- stop writers, wait for the drain to go idle
T_WSTOP=$(now)
[ -n "$WPID" ] && { kill -TERM $WPID; wait $WPID; }
SCH=$($SU -c "select n.nspname from pg_class c join pg_namespace n on n.oid=c.relnamespace where c.relname='segment_pointer' limit 1" $DB)
idle=0; tw=$(now)
DL=$(( $(date +%s) + 900 ))
while [ $(date +%s) -lt $DL ]; do
  # pending = sealed/draining segments + rows in the active slot
  slot=$($SU -c "select ring_slot from $SCH.segment_pointer" $DB)
  pend=$($SU -c "select (select count(*) from $SCH.segments where state in ('sealed','draining')) + (select count(*) from $SCH.seg_$slot)" $DB)
  if [ "$pend" = 0 ]; then idle=$((idle+1)); else idle=0; fi
  [ $idle -ge 8 ] && break
  sleep 0.25
done
T_IDLE=$(now); DRAIN_S=$(echo "$T_IDLE - $T_WSTOP" | bc)
kill -TERM $SPID; wait $SPID
$TR status > "$OUT/status.txt" 2>&1
# ---------- verify (writers stopped, drain idle): target vs the plain query, both ways
$APP -c "create table plain_ref as ${PLAIN[$SH]}" "$DB"
V_T=$($APP -c "select count(*) from $TT" "$DB")
V_P=$($APP -c "select count(*) from plain_ref" "$DB")
V_TP=$($APP -c "select count(*) from (select * from $TT except all select * from plain_ref) x" "$DB" 2>&1)
V_PT=$($APP -c "select count(*) from (select * from plain_ref except all select * from $TT) x" "$DB" 2>&1)
POISON=$($SU -c "select count(*) from $SCH.poison" $DB 2>&1)
echo -e "target_rows=$V_T\nplain_rows=$V_P\ntarget_minus_plain=$V_TP\nplain_minus_target=$V_PT\npoison=$POISON\nstatus=$ST" > "$OUT/verify.txt"
# ---------- the hidden table's bases: S1 (step 1), S2 (stragglers, step 2), NULL (made after the build)
HID=$($SU -c "select n.nspname||'.'||c.relname from pg_class c join pg_namespace n on n.oid=c.relnamespace where n.nspname='$SCH' and c.relname like 'agg\_%' and c.relkind='r' limit 1" $DB)
$SU -c "select coalesce(__basis::text,'NULL'), count(*) from $HID group by 1 order by 2 desc limit 10" $DB > "$OUT/bases.txt" 2>&1
kill $RPID 2>/dev/null; wait $RPID 2>/dev/null
$SU -c "select pg_size_pretty(pg_total_relation_size('$HID')), pg_size_pretty(pg_total_relation_size('$TT'))" $DB > "$OUT/sizes.txt"
GOOD=0; [ "$V_TP" = 0 ] && [ "$V_PT" = 0 ] && GOOD=1
printf "label\tshape\tN\trate\tvar\trep\tins_s\tins_wal\tbuild_s\tbuild_wal\tdrain_s\texact\tt_run\tt_w0\tt_ins0\tt_ins1\tt_apply\tt_live\tt_wstop\tt_idle\n" > "$OUT/summary.tsv"
printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "$LABEL" "$SH" "$N" "$RATE" "${VAR:--}" "$REP" "$INS_S" "$INS_W" "$BUILD_S" "$BUILD_W" "$DRAIN_S" "$GOOD" "$T_RUN" "$T_W0" "$T_INS0" "$T_INS1" "$T_APPLY" "$T_LIVE" "$T_WSTOP" "$T_IDLE" >> "$OUT/summary.tsv"
tail -1 "$OUT/summary.tsv"
if [ $GOOD = 1 ]; then
  $SU -c "drop database $DB with (force)" postgres
else
  echo "MISMATCH: keeping database $DB"; $SU -c "alter database $DB rename to keep_${DB}_${SH}_${RATE}" postgres
fi
gzip -f "$OUT/samples.tsv" 2>/dev/null
[ -d "$OUT/writers" ] && tar czf "$OUT/writers.tgz" -C "$OUT" writers && rm -rf "$OUT/writers"
exit 0
