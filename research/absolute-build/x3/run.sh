#!/usr/bin/env bash
# E5 build-ratio harness, copied from research/issue-1888-operator-model (research/operator-model/e5/run.sh)
# for #1917 X3. Changes: TR (binary) from the env, X3's cluster dir and port, x3_ database names.
# usage: run.sh <src_db> <label> <rep> <shape>...
# env: TR (trellis binary), PGDATA_DIR (default ../../../target/research-pg-x3), PORT (54873)
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); ROOT=$(cd "$HERE/../../.." && pwd)
export PATH=$HOME/.asdf/installs/postgres/18.6/bin:$PATH
S=${PGDATA_DIR:-$ROOT/target/research-pg-x3}; PORT=${PORT:-54873}
TR=${TR:-$ROOT/target/release/trellis}
SRC=$1; LABEL=$2; REP=$3; shift 3
source "$HERE/shapes.sh"
SU="psql -X -q -At -h $S -p $PORT -U postgres"; APP="psql -X -q -At -h $S -p $PORT -U app"
now() { date +%s.%N; }
lsn() { $SU -c "select pg_current_wal_lsn()" postgres; }
walb() { $SU -c "select pg_wal_lsn_diff('$2','$1')::bigint" postgres; }
for SH in "${@:-$SHAPES}"; do
  OUT=$HERE/results/$LABEL/$SH-r$REP; mkdir -p "$OUT"
  DB=x3_${LABEL}_${SH}_r$REP; TT=${TARGET[$SH]}
  $SU -c "drop database if exists $DB with (force)" postgres
  $SU -c "create database $DB template $SRC owner app" postgres || exit 1
  { echo "shape=$SH rep=$REP label=$LABEL"; uptime; echo "rustc=$(pgrep -c rustc)"; } > "$OUT/box.txt"
  # ---------- (i) plain query: INSERT into a table with the target's key, and CTAS
  $APP -c "create table plain_ctas as ${PLAIN[$SH]}" "$DB"  # warm + shape template
  case $SH in s2_agg) PK=author_id;; s2_agg_hi) PK=grp_hi;; s3_aggjoin) PK="author_id,tag";; *) PK=id;; esac
  $APP -c "drop table plain_ctas" "$DB"
  $SU -c checkpoint postgres; sleep 1
  L0=$(lsn); t0=$(now)
  $APP -c "create table plain_ctas as ${PLAIN[$SH]}" "$DB"
  t1=$(now); L1=$(lsn); CTAS_S=$(echo "$t1 - $t0" | bc); CTAS_W=$(walb $L0 $L1)
  $APP -c "create table plain_ins (like plain_ctas); alter table plain_ins add primary key ($PK)" "$DB"
  $SU -c checkpoint postgres; sleep 1
  L0=$(lsn); t0=$(now)
  $APP -c "insert into plain_ins ${PLAIN[$SH]}" "$DB"
  t1=$(now); L1=$(lsn); INS_S=$(echo "$t1 - $t0" | bc); INS_W=$(walb $L0 $L1)
  # ---------- (ii) Trellis build
  $SU -c checkpoint postgres; sleep 1
  DBOID=$($SU -c "select oid from pg_database where datname='$DB'" postgres)
  $SU -c "select pg_stat_statements_reset(0,$DBOID,0)" postgres >/dev/null
  LOGOFF=$(stat -c %s $S/server.log)
  export TRELLIS_DATABASE_URL="postgres://app@127.0.0.1:$PORT/$DB"
  $TR run > "$OUT/run.log" 2>&1 & RPID=$!
  until grep -q "Ctrl-C" "$OUT/run.log" 2>/dev/null; do sleep 0.2; done
  $SU -c "select pg_stat_statements_reset(0,$DBOID,0)" postgres >/dev/null
  L0=$(lsn); t0=$(now)
  $TR apply "${TRANSFORM[$SH]}" > "$OUT/apply.log" 2>&1 || { echo "APPLY FAILED"; cat "$OUT/apply.log"; }
  while :; do
    st=$($TR status 2>&1 | grep -m1 "status=" | sed 's/.*status=\([a-z_]*\).*/\1/')
    case $st in live) break;; paused|failed|quarantined|halted) echo "ended in $st" >> "$OUT/apply.log"; break;; esac
    sleep 0.25
  done
  t1=$(now); L1=$(lsn); BUILD_S=$(echo "$t1 - $t0" | bc); BUILD_W=$(walb $L0 $L1)
  $TR status > "$OUT/status.txt" 2>&1
  kill $RPID 2>/dev/null; wait $RPID 2>/dev/null
  tail -c +$((LOGOFF+1)) $S/server.log > "$OUT/server-log-slice.txt"
  # ---------- (iii) verify
  $APP -c "select count(*) from $TT" "$DB" > "$OUT/verify.txt"
  $APP -c "select count(*) from plain_ctas" "$DB" >> "$OUT/verify.txt"
  $APP -c "select count(*) from (select * from $TT except all select * from plain_ctas) x" "$DB" >> "$OUT/verify.txt" 2>&1
  $APP -c "select count(*) from (select * from plain_ctas except all select * from $TT) x" "$DB" >> "$OUT/verify.txt" 2>&1
  # ---------- (iv) per-statement
  $SU -c "select calls, round(total_exec_time::numeric,0) ms, rows, wal_bytes, wal_fpi, temp_blks_written*8/1024 temp_mb, regexp_replace(left(query,160),'\s+',' ','g') q from pg_stat_statements where dbid=$DBOID and query !~ 'pg_stat_statements|pg_current_wal' order by total_exec_time desc limit 15" postgres > "$OUT/pgss.txt"
  $SU -c "select '-- calls='||calls||' ms='||round(total_exec_time::numeric)||' rows='||rows||' wal='||wal_bytes||' fpi='||wal_fpi||' temp_mb='||(temp_blks_written*8/1024)||E'\n'||query||E'\n' from pg_stat_statements where dbid=$DBOID and query !~ 'pg_stat_statements|pg_current_wal' order by total_exec_time desc limit 10" postgres > "$OUT/pgss-full.txt"
  $SU -c "select '-- calls='||calls||' ms='||round(total_exec_time::numeric)||' rows='||rows||' wal='||wal_bytes||' fpi='||wal_fpi||' temp_mb='||(temp_blks_written*8/1024)||E'\n'||query||E'\n' from pg_stat_statements where dbid=$DBOID and query !~ 'pg_stat_statements|pg_current_wal' and (total_exec_time > 50 or wal_bytes > 1000000) order by total_exec_time desc limit 60" postgres > "$OUT/pgss-all.txt"
  # ---------- (v) sizes
  $SU -c "select n.nspname||'.'||c.relname, c.relkind, pg_total_relation_size(c.oid) from pg_class c join pg_namespace n on n.oid=c.relnamespace where c.relkind in ('r','m') and n.nspname not in ('pg_catalog','information_schema','pg_toast') and c.relname not in ('posts','post_tags','authors','regions','categories','plain_ins','plain_ctas') order by 3 desc limit 25" $DB > "$OUT/sizes.txt"
  STATE=$($SU -c "select coalesce(sum(pg_total_relation_size(c.oid)),0) from pg_class c join pg_namespace n on n.oid=c.relnamespace where c.relkind='r' and n.nspname not in ('pg_catalog','information_schema','pg_toast','public')" $DB)
  TGT=$($SU -c "select pg_total_relation_size('$TT')" $DB)
  PLN=$($SU -c "select pg_total_relation_size('plain_ins')" $DB)
  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "$LABEL" "$SH" "$REP" "$INS_S" "$INS_W" "$CTAS_S" "$CTAS_W" "$BUILD_S" "$BUILD_W" "$STATE" "$TGT" "$PLN" | tee "$OUT/summary.tsv"
  $SU -c "drop database $DB with (force)" postgres
done
