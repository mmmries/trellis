#!/usr/bin/env bash
# E5 micro: plain-aggregate build, emitted lock+rebuild (a) vs hand-written candidates (b: sorted hidden upsert
# only; c: b + target write with hash join).  usage: micro.sh <src_db> <label> <stmts_dir>
# <stmts_dir> holds lock.sql / rebuild.sql from extract_stmts.py on a real build's log (same source shape).
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); ROOT=$(cd "$HERE/../../.." && pwd)
export PATH=$HOME/.asdf/installs/postgres/18.6/bin:$PATH
S=${PGDATA_DIR:-$ROOT/target/research-pg-e5}; PORT=${PORT:-54895}
SRC=$1; LABEL=$2; STM=$3; OUT=$HERE/results/micro-$LABEL; mkdir -p "$OUT"
SU="psql -X -q -At -h $S -p $PORT -U postgres"
export GCOL=${GCOL:-author_id}; TGT=${TGT:-author_totals}
python3 -I "$HERE/make_candidates.py" "$STM"
BASE=e5m_base_$LABEL
$SU -c "drop database if exists $BASE with (force)" postgres
$SU -c "create database $BASE template $SRC owner app" postgres
psql -X -q -h $S -p $PORT -U app -v ON_ERROR_STOP=1 $BASE <<SQL
create schema trellis;
create table trellis.segments (seg_seq bigint);
create table trellis.seg_0 (src_table text, old_image jsonb, transition_images jsonb);
create table trellis.seg_1 (like trellis.seg_0); create table trellis.seg_2 (like trellis.seg_0); create table trellis.seg_3 (like trellis.seg_0);
create table trellis.agg_1 (
    $GCOL integer, __n bigint DEFAULT 0 NOT NULL, __count_1 bigint DEFAULT 0 NOT NULL, __sum_1 numeric DEFAULT 0 NOT NULL,
    __scales_1 jsonb, __count_2 bigint DEFAULT 0 NOT NULL, __sum_2 numeric DEFAULT 0 NOT NULL, __basis pg_snapshot, __stamp bigint,
    __zero boolean GENERATED ALWAYS AS (((__n = 0) AND (__count_1 = 0) AND (__sum_1 = (0)::numeric) AND (__scales_1 IS NULL) AND (__count_2 = 0) AND (__sum_2 = (0)::numeric))) STORED NOT NULL);
alter table trellis.agg_1 add constraint agg_1_author_id_key unique nulls not distinct ($GCOL);
create index agg_1_author_id_idx on trellis.agg_1 ($GCOL) where __zero;
create table public.$TGT ($GCOL integer, n bigint, total_score numeric, total_likes bigint);
alter table public.$TGT add constraint author_totals_author_id_key unique nulls not distinct ($GCOL);
SQL
SIDES=$(grep -o 'trellis_group_sides_[a-z_0-9]*' $STM/lock.sql | head -1); RECOMP=$(grep -o 'trellis_group_recompute_[a-z_0-9]*' $STM/lock.sql | head -1)
for V in ${VARIANTS:-a b c}; do
  DB=e5m_${LABEL}_$V
  $SU -c "drop database if exists $DB with (force)" postgres
  $SU -c "create database $DB template $BASE owner app" postgres
  case $V in
    a) if [ -f $STM/recompute_fill.sql ]; then FILL="$(cat $STM/recompute_fill.sql | sed 's/;$//')"; else FILL="insert into $RECOMP select distinct $GCOL from public.posts"; fi
       BODY="explain (analyze, wal, buffers, timing off) $FILL;
explain (analyze, wal, buffers, timing off) $(cat $STM/lock.sql | sed 's/;$//');
explain (analyze, wal, buffers, timing off) $(cat $STM/rebuild.sql | sed 's/;$//');";;
    b) BODY="explain (analyze, wal, buffers, timing off) $(cat $STM/cand_b.sql | sed 's/;$//');";;
    d) BODY="explain (analyze, wal, buffers, timing off) $(cat $STM/cand_d.sql | sed 's/;$//');";;
    c) BODY="explain (analyze, wal, buffers, timing off) $(cat $STM/cand_c.sql | sed 's/;$//');";;
  esac
  # the emitted statements read pg_temp recompute set; fresh build: every source group
  $SU -c checkpoint postgres; sleep 2
  L0=$($SU -c "select pg_current_wal_lsn()" postgres)
  psql -X -q -h $S -p $PORT -U app $DB > "$OUT/$V.explain.txt" 2>&1 <<SQL
set search_path to trellis;
begin;
create temp table $SIDES ($GCOL int);
create temp table $RECOMP ($GCOL int);
$( [ $V != a ] && echo "insert into $RECOMP select distinct $GCOL from public.posts; analyze $RECOMP;" )
\timing on
$BODY
\timing off
commit;
SQL
  L1=$($SU -c "select pg_current_wal_lsn()" postgres)
  echo "variant $V wal_total_bytes=$($SU -c "select pg_wal_lsn_diff('$L1','$L0')::bigint" postgres)" >> "$OUT/$V.explain.txt"
  psql -X -q -At -h $S -p $PORT -U app $DB -c "\copy (select $GCOL,__n,__count_1,__sum_1,__scales_1,__count_2,__sum_2 from trellis.agg_1 order by 1) to '$OUT/$V.hidden.csv' csv" \
     -c "\copy (select * from public.$TGT order by 1) to '$OUT/$V.target.csv' csv"
  echo "sizes $V: hidden=$($SU -c "select pg_total_relation_size('trellis.agg_1')" $DB) target=$($SU -c "select pg_total_relation_size('public.$TGT')" $DB)" >> "$OUT/$V.explain.txt"
  $SU -c "drop database $DB with (force)" postgres
done
for f in hidden target; do for V in b c d; do [ -f $OUT/$V.$f.csv ] || continue; cmp -s $OUT/a.$f.csv $OUT/$V.$f.csv && echo "$f a==$V" || echo "$f a!=$V ($(wc -l < $OUT/a.$f.csv) vs $(wc -l < $OUT/$V.$f.csv) rows)"; done; done | tee "$OUT/diff.txt"
$SU -c "drop database $BASE with (force)" postgres
