#!/usr/bin/env bash
# X4: template database <name> with N rows over L leaves, captured by a cheap definition
# (warm, GROUP BY grp_hi, COUNT only) built over the empty source, the rows then drained through it.
# usage: template.sh <name> <N> <L>
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); ROOT=$(cd "$HERE/../../.." && pwd)
export PATH=$HOME/.asdf/installs/postgres/18.6/bin:$PATH
S=$ROOT/target/research-pg-x4; PORT=54874; TR=$ROOT/target/x4-bin/trellis
NAME=$1; N=$2; L=$3
SU="psql -X -q -At -h $S -p $PORT -U postgres"; APP="psql -X -q -At -h $S -p $PORT -U app"
$SU -c "drop database if exists $NAME with (force)" postgres
$SU -c "create database $NAME owner app" postgres
$APP -v N=$N -v L=$L -f "$HERE/gen.sql" "$NAME"
export TRELLIS_DATABASE_URL="postgres://app@127.0.0.1:$PORT/$NAME"
LOG=$HERE/results/template-$NAME.log; mkdir -p "$HERE/results"
$TR run > "$LOG" 2>&1 & RPID=$!
until grep -q "Ctrl-C" "$LOG" 2>/dev/null; do sleep 0.2; done
$TR apply "TRANSFORM warm FROM posts GROUP BY grp_hi SELECT grp_hi AS grp_hi, COUNT(*) AS n"
until $TR status 2>&1 | grep "target=public.warm" | grep -q "status=live"; do sleep 0.5; done
# the capture pass records the tree
until [ "$($SU -c "select count(*) from trellis.partition_sources" "$NAME" 2>/dev/null || echo 0)" -gt 0 ]; do sleep 0.5; done
$APP -c "insert into posts select i, i, (i % 10)::int, ((i * 7) % 1000)::int, ((i * 13) % 100)::int from generate_series(0, $N - 1) i" "$NAME"
# drained when warm counts every row
until [ "$($APP -c "select coalesce(sum(n), 0) from warm" "$NAME")" = "$N" ]; do sleep 1; done
sleep 3
kill $RPID; wait $RPID 2>/dev/null || true
$SU -c "vacuum analyze" "$NAME"
echo "template $NAME ready"
