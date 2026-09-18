#!/bin/bash
# Ad hoc / manual runner for the issue-102-settled-state-v2.sql model against
# a hand-started Postgres instance (e.g. one left running by a debugging
# session). For CI and everyday `cargo test`, this campaign is wired into
# `trellis/tests/spike_102_settled_state.rs` instead -- see that file for the
# fast (every PR) / deep (nightly, `.github/workflows/nightly.yml`) split.
# This script remains as a quick way to point psql at an arbitrary running
# cluster without going through the Rust harness or testkit.
#
# Connection info defaults match a typical `testkit`-style throwaway
# instance; override via SPIKE102_HOST/SPIKE102_PORT/SPIKE102_DB as needed.
HOST="${SPIKE102_HOST:-/tmp/spike102/sock}"
PORT="${SPIKE102_PORT:-5599}"
DB="${SPIKE102_DB:-spike}"
RUNS="${SPIKE_102_CAMPAIGN_RUNS:-3000}"
OPS="${SPIKE102_OPS:-50}"
SEED="${SPIKE102_SEED:-900000}"

PSQL="psql -h $HOST -p $PORT -d $DB -q -t -A -F| "
for D in D0 D1 D3 D5 D5-a D5-b D5-c D5-d; do
  $PSQL -c "set search_path to m, public; delete from stats;" \
        -c "select * from fuzz('$D',$RUNS,$OPS,$SEED);" \
        -c "select 'stats|'||coalesce(string_agg(name||'='||v,' ' order by name),'-') from stats where name like 'd5%' or name like 'HARNESS%' or name='out_of_order';"
done
