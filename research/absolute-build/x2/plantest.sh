#!/usr/bin/env bash
# plantest.sh <base> <shape> <stmt-file> [pre-sql]: runs fill+lock then EXPLAIN ANALYZE of <stmt-file> in a throwaway clone, rolled back
export PATH=$HOME/.asdf/installs/postgres/18.6/bin:$PATH; D=/home/mike/code/trellis-lane-r/target/research-pg-x2
B=$1; SH=$2; F=$3; PRE=${4:-}; ST=st-$SH; G=$( [ $SH = hi ] && echo grp_hi || echo author_id )
R=$(grep -o 'trellis_group_recompute_[a-z_0-9]*' $ST/rebuild.sql|head -1); SI=$(grep -o 'trellis_group_sides_[a-z_0-9]*' $ST/lock.sql|head -1)
psql -X -q -h $D -p 54872 -U postgres -c "drop database if exists x2plan" -c "create database x2plan template $B owner app strategy file_copy" postgres
psql -X -q -h $D -p 54872 -U app x2plan <<SQL 2>&1 | grep -v "^ *SubPlan\|InitPlan\|__u\.\|__sk\|Function Scan\|Aggregate  (cost=[0-9.]*\.\.[0-9.]* rows=1 \|Result  (cost=0\|(never executed)\|jsonb_each_text\|Filter: (sum(1)"
set search_path to trellis, public;
begin;
create temp table $SI ($G int); create temp table $R ($G int); create temp table x2_strag ($G int);
$( [ "${NOLOCK:-}" ] || echo "$(sed 's/;$//' $ST/fill.sql); $(sed 's/;$//' $ST/lock.sql);" )
$PRE
\timing on
explain (analyze, buffers, wal, timing off) $(sed 's/;$//' $F);
rollback;
SQL
psql -X -q -h $D -p 54872 -U postgres -c "drop database x2plan" postgres
