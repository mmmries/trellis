#!/usr/bin/env bash
# Every INTERVAL s until the cluster goes away: slot WAL retention, pg_wal size, chunk progress,
# segments, the oldest client transactions with what they wait on. Usage: pgstate.sh <out> [interval]
out=$1; IV=${2:-300}; DISK=${DISK:-/home/mike/exp558/tmpdisk}
while s=$(ls -d "$DISK"/trellis-testkit-*/sock 2>/dev/null | head -1) && [ -n "$s" ]; do
  db=$(psql -U postgres -h "$s" -p 5432 -d postgres -XAtc "select datname from pg_database where datname not in ('postgres','template0','template1') order by oid desc limit 1" 2>/dev/null) || break
  [ -n "$db" ] || break
  {
    echo "=== $(date '+%F %T')  pg_wal $(du -sh "$(dirname "$s")/data/pg_wal" 2>/dev/null | cut -f1)"
    timeout 60 psql -U postgres -h "$s" -p 5432 -d "$db" -X \
      -c "select slot_name, pg_size_pretty(pg_current_wal_lsn()-restart_lsn) behind_restart, pg_size_pretty(pg_current_wal_lsn()-confirmed_flush_lsn) behind_confirmed from pg_replication_slots" \
      -c "select (select status from trellis.transform_definitions limit 1) status, count(*) filter (where done) chunks_done, count(*) chunks from trellis.backfill_chunks" \
      -c "select seg_seq, ring_slot, state from trellis.segments order by seg_seq" \
      -c "select pid, wait_event_type, wait_event, now()-xact_start xact_age, pg_blocking_pids(pid) blockers, left(regexp_replace(query,'\s+',' ','g'),70) q from pg_stat_activity where backend_type='client backend' and xact_start is not null order by xact_start limit 4" 2>&1
  } >> "$out"
  sleep "$IV"
done
echo "=== $(date '+%F %T') cluster gone" >> "$out"
