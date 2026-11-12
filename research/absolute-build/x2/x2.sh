#!/usr/bin/env bash
# X2 micro (research #1917): plain-aggregate build statements, today's vs candidates A, B, C.
# usage: x2.sh <src_db> <label> <shape: hi|auth> <mode: fresh|rebuild> <rep> <variant>...
#   fresh variants:   plain a c1 c2 A0 A0.1 A1 A10 B0 B0.1 B1 B10   (A<pct>/B<pct>: pct % of groups pre-exist as page rows)
#   rebuild variants: plain a c1 c2 Au B
# Env: FF2 (c2 fillfactor, default 50), TIMEOUT (statement_timeout, default 0), KEEP_BASES=1 keeps the base databases.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); ROOT=$(cd "$HERE/../../.." && pwd)
export PATH=$HOME/.asdf/installs/postgres/18.6/bin:$PATH
S=$ROOT/target/research-pg-x2; PORT=54872
SRC=$1; LABEL=$2; SHAPE=$3; MODE=$4; REP=$5; shift 5
FF2=${FF2:-50}; TIMEOUT=${TIMEOUT:-0}
case $SHAPE in hi) G=grp_hi; TGT=hi_totals;; auth) G=author_id; TGT=author_totals;; *) echo "bad shape"; exit 2;; esac
ST=$HERE/st-$SHAPE
RECOMP=$(grep -o 'trellis_group_recompute_[a-z_0-9]*' $ST/rebuild.sql | head -1)
SIDES=$(grep -o 'trellis_group_sides_[a-z_0-9]*' $ST/lock.sql | head -1)
OUT=$HERE/results/$LABEL/$SHAPE-$MODE-r$REP; mkdir -p "$OUT"
SU="psql -X -q -At -h $S -p $PORT -U postgres"
APP="psql -X -q -At -h $S -p $PORT -U app -v ON_ERROR_STOP=1"
lsn() { $SU -c "select pg_current_wal_insert_lsn()" postgres; }
log() { echo "[$(date +%T)] $*" | tee -a "$OUT/run.log"; }
{ echo "label=$LABEL shape=$SHAPE mode=$MODE rep=$REP"; uptime; echo "rustc=$(pgrep -c rustc) cargo=$(pgrep -c cargo)"; } >> "$OUT/box.txt"

ddl() { # $1 db, $2 fillfactor
$APP $1 <<SQL
create schema trellis;
create table trellis.segments (seg_seq bigint);
create table trellis.seg_0 (src_table text, old_image jsonb, transition_images jsonb);
create table trellis.seg_1 (like trellis.seg_0); create table trellis.seg_2 (like trellis.seg_0); create table trellis.seg_3 (like trellis.seg_0);
create table trellis.agg_1 (
    $G integer, __n bigint DEFAULT 0 NOT NULL, __count_1 bigint DEFAULT 0 NOT NULL, __sum_1 numeric DEFAULT 0 NOT NULL,
    __scales_1 jsonb, __count_2 bigint DEFAULT 0 NOT NULL, __sum_2 numeric DEFAULT 0 NOT NULL, __basis pg_snapshot, __stamp bigint,
    __zero boolean GENERATED ALWAYS AS (((__n = 0) AND (__count_1 = 0) AND (__sum_1 = (0)::numeric) AND (__scales_1 IS NULL) AND (__count_2 = 0) AND (__sum_2 = (0)::numeric))) STORED NOT NULL)
    with (fillfactor = $2);
alter table trellis.agg_1 add constraint agg_1_author_id_key unique nulls not distinct ($G);
create index agg_1_author_id_idx on trellis.agg_1 ($G) where __zero;
create table public.$TGT ($G integer, n bigint, total_score numeric, total_likes bigint);
alter table public.$TGT add constraint author_totals_author_id_key unique nulls not distinct ($G);
SQL
}
clone() { $SU -c "drop database if exists $2 with (force)" postgres; $SU -c "create database $2 template $1 owner app strategy file_copy" postgres; }

# fresh base: empty hidden/target (fillfactor $1), plus page-created rows for pct $2 of the groups (uniform: key % k = 0)
fresh_base() { local ff=$1 pct=$2 B=x2fb_${LABEL}_${SHAPE}_ff$1_p${2/./_}
  if [ -z "$($SU -c "select 1 from pg_database where datname='$B'" postgres)" ]; then
    clone $SRC $B; ddl $B $ff
    if [ "$pct" != 0 ]; then local k; k=$(python3 -c "print(round(100/$pct))")
      # a page applied one source row's insert: partials of that row, NULL basis, and its visible target row
      $APP $B -c "insert into trellis.agg_1 ($G, __n, __count_1, __sum_1, __scales_1, __count_2, __sum_2)
          select distinct on ($G) $G, 1, 1, score, null, 1, likes::numeric from posts where $G % $k = 0 order by $G, id" \
        -c "insert into public.$TGT select $G, 1, score, likes from (select distinct on ($G) $G, score, likes from posts where $G % $k = 0 order by $G, id) x"
    fi
    # hidden/target left un-analyzed (reltuples -1), as after define and as in E5
  fi >> "$OUT/run.log" 2>&1; echo $B; }

# rebuild base: the c1 (ff 100) or c2 (ff FF2) fresh build kept, then source changes, then vacuum
SRCROWS_CHANGED=0
rebuild_change() { # $1 db
  local n; n=$($APP $1 -c "select count(*) from posts")
  if [ $SHAPE = hi ]; then
    $APP $1 -c "update posts set likes = likes + 1, score = score + 1 where id % 10 = 3" \
            -c "insert into posts select $n + g, 1, 1, 1, $n + g - 1, g % 1000, (g % 10000)::numeric / 100, 'new '||g from generate_series(1, $n / 100) g"
  else
    local na; na=$($APP $1 -c "select max(author_id) from posts")
    $APP $1 -c "update posts set likes = likes + 1, score = score + 1 where id % 10 = 3" \
            -c "insert into posts select $n + g, $na + 1 + (g - 1) / 10, 1, 1, $n + g - 1, g % 1000, (g % 10000)::numeric / 100, 'new '||g from generate_series(1, $n / 100) g"
  fi
  $APP $1 -c "vacuum (analyze) posts, trellis.agg_1, public.$TGT"
}

# each statement: a marker, the WAL position before it, then the timed EXPLAIN ANALYZE (it executes the statement)
mark() { printf '%s\n' "\\echo STMT $1" "select 'LSN', pg_current_wal_insert_lsn();" "\\timing on"; }
stmt() { mark $1; echo "explain (analyze, wal, buffers, timing off) $(sed 's/;$//' $ST/$1.sql);"; echo "\\timing off"; }
body() { # variant -> SQL between begin/commit
  local V=$1
  case $V in
    plain) echo "set search_path to public;"; stmt plain;;
    a) stmt fill; stmt lock; stmt rebuild;;
    c1|c2) stmt fill; stmt lock; echo "set local enable_hashjoin = off;"; stmt c1_update;;
    A*|Au) stmt A1
       echo "select count(*) as nstrag, count(*) > 0 as has_strag from pg_temp.x2_strag \\gset"
       echo "\\echo nstrag=:nstrag"
       echo "\\if :has_strag"
       echo "set local enable_hashjoin = off;"
       if [ $V = Au ]; then stmt A2_scan; else stmt A2_probe; fi
       echo "\\endif";;
    B*) mark gate; echo "select pg_advisory_xact_lock(1917);"; echo "\\timing off"; stmt B;;
  esac
  echo "\\echo STMT end"; echo "select 'LSN', pg_current_wal_insert_lsn();"
}

run_variant() { # $1 base, $2 variant, $3 keep-as (optional db name to keep)
  local BASE=$1 V=$2 DB=x2v_${LABEL}_$$ F=$OUT/$2.explain.txt
  [ -n "${3:-}" ] && DB=$3
  clone $BASE $DB >> "$OUT/run.log" 2>&1
  [ $V = plain ] && $APP $DB -c "create table plain_ins ($G int primary key, n bigint, total_score numeric, total_likes bigint)"
  $SU -c checkpoint postgres; sleep 1
  $SU -c "select pg_stat_reset_shared('wal')" postgres >/dev/null
  local L0; L0=$(lsn); local T0; T0=$(date +%s.%N)
  log "variant $V on $BASE"
  $APP $DB > "$F" 2>&1 <<SQL
set search_path to trellis, public;
set statement_timeout = '$TIMEOUT';
begin;
create temp table $SIDES ($G int);
create temp table $RECOMP ($G int);
create temp table x2_strag ($G int);
$(body $V)
commit;
select pg_stat_force_next_flush();
SQL
  local rc=$?; local T1; T1=$(date +%s.%N); local L1; L1=$(lsn)
  { echo "rc=$rc wall_s=$(echo "$T1 - $T0" | bc)"
    echo "wal_total_bytes=$($SU -c "select pg_wal_lsn_diff('$L1','$L0')::bigint" postgres)"
    sleep 1
    echo "pg_stat_wal=$($SU -c "select wal_records||' '||wal_fpi||' '||wal_bytes from pg_stat_wal" postgres)"
    echo "temp=$($SU $DB -c "select temp_files||' '||temp_bytes from pg_stat_database where datname = current_database()")"
    echo "stats=$($SU $DB -c "select n_tup_ins||' '||n_tup_upd||' '||n_tup_hot_upd||' '||n_dead_tup from pg_stat_user_tables where relid='trellis.agg_1'::regclass")"
    echo "sizes hidden=$($SU $DB -c "select pg_total_relation_size('trellis.agg_1')") hidden_heap=$($SU $DB -c "select pg_relation_size('trellis.agg_1')") target=$($SU $DB -c "select pg_total_relation_size('public.$TGT')")"
    echo "hidden_md5=$($APP $DB -c "\\copy (select $G,__n,__count_1,__sum_1,__scales_1,__count_2,__sum_2 from trellis.agg_1 order by 1) to stdout" | md5sum | cut -c1-32) rows=$($APP $DB -c "select count(*) from trellis.agg_1")"
    echo "target_md5=$($APP $DB -c "\\copy (select * from public.$TGT order by 1) to stdout" | md5sum | cut -c1-32) rows=$($APP $DB -c "select count(*) from public.$TGT")"
    [ $V = plain ] && echo "plain_md5=$($APP $DB -c "\\copy (select * from plain_ins order by 1) to stdout" | md5sum | cut -c1-32)"
  } >> "$F" 2>&1
  log "variant $V rc=$rc $(grep -h 'wall_s\|_md5\|stats=' $F | tr '\n' ' ')"
  [ -z "${3:-}" ] && $SU -c "drop database $DB with (force)" postgres
  return 0
}

for V in "$@"; do
  if [ $MODE = fresh ]; then
    case $V in
      plain|a|c1) run_variant $(fresh_base 100 0) $V $( [ $V = c1 ] && [ -n "${KEEP_C:-}" ] && echo x2keep_${LABEL}_${SHAPE}_c1 );;
      c2) run_variant $(fresh_base $FF2 0) $V $( [ -n "${KEEP_C:-}" ] && echo x2keep_${LABEL}_${SHAPE}_c2 );;
      A*|B*) p=${V:1}; B=$(fresh_base 100 $p); run_variant $B $V;;
    esac
  else
    case $V in
      c2) RB=x2keep_${LABEL}_${SHAPE}_c2;; *) RB=x2keep_${LABEL}_${SHAPE}_c1;;
    esac
    if [ -z "$($SU -c "select 1 from pg_database where datname='${RB}_rb'" postgres)" ]; then
      log "rebuild base ${RB}_rb from $RB"
      [ -n "$($SU -c "select 1 from pg_database where datname='$RB'" postgres)" ] || { log "missing $RB (run fresh with KEEP_C=1)"; exit 3; }
      clone $RB ${RB}_rb >> "$OUT/run.log" 2>&1; rebuild_change ${RB}_rb >> "$OUT/run.log" 2>&1
    fi
    run_variant ${RB}_rb $V
  fi
done
if [ -z "${KEEP_BASES:-}" ]; then
  for B in $($SU -c "select datname from pg_database where datname like 'x2fb_${LABEL}_${SHAPE}_%'" postgres); do $SU -c "drop database $B with (force)" postgres; done
fi
