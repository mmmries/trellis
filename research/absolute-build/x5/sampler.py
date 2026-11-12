#!/usr/bin/env python3
"""X5 sampler: every INTERVAL s, the Trellis sessions' activity and lock waits, and the ring backlog.

usage: sampler.py SU_DSN DB OUT.tsv [INTERVAL]
Connects as a superuser (sees every query). Runs until SIGTERM. One line per sample per session of DB
that is not this sampler or an x5 writer, plus one 'ring' line per sample:
  act  t  pid  state  wait_type  wait_event  query_age_s  xact_age_s  blockers  query(80)
  ring t  pending_segs  pending_rows_est  active_rows  oldest_pending_sealed_age_s  active_seq
"""
import signal, sys, time
import psycopg

dsn, db, out = sys.argv[1], sys.argv[2], sys.argv[3]
iv = float(sys.argv[4]) if len(sys.argv) > 4 else 0.25
stop = False


def _stop(*_):
    global stop
    stop = True


signal.signal(signal.SIGTERM, _stop)
signal.signal(signal.SIGINT, _stop)
conn = psycopg.connect(dsn, autocommit=True, application_name="x5-sampler")
cconn = psycopg.connect(dsn.rsplit("/", 1)[0] + "/" + db, autocommit=True, application_name="x5-sampler")
cconn.execute("set statement_timeout = '200ms'")
ACT = """
select extract(epoch from clock_timestamp()), pid, coalesce(state,''), coalesce(wait_event_type,''),
       coalesce(wait_event,''),
       coalesce(extract(epoch from clock_timestamp() - query_start),0),
       coalesce(extract(epoch from clock_timestamp() - xact_start),-1),
       array_to_string(pg_blocking_pids(pid), ','),
       regexp_replace(left(query, 80), '\\s+', ' ', 'g'), coalesce(application_name,'')
from pg_stat_activity
where datname = %s and pid <> pg_backend_pid() and backend_type = 'client backend'
  and state <> 'idle' and (coalesce(application_name,'') not like 'x5-%%' or wait_event_type = 'Lock')
"""
schema = None
f = open(out, "w", buffering=1 << 16)
while not stop:
    t0 = time.monotonic()
    try:
        for r in conn.execute(ACT, (db,)).fetchall():
            f.write("act\t" + "\t".join(str(x) for x in r) + "\n")
        if schema is None:
            r = cconn.execute("select n.nspname from pg_class c join pg_namespace n on n.oid=c.relnamespace "
                              "where c.relname='segment_pointer' limit 1").fetchone()
            schema = r[0] if r else None
        if schema:
            r = cconn.execute(f"""
              select extract(epoch from clock_timestamp()),
                     count(*) filter (where state in ('sealed','draining')),
                     coalesce(sum(row_count * (1 - bit_count(drained_mask::bit(64))::float8 / bucket_count))
                              filter (where state in ('sealed','draining')),0)::bigint,
                     coalesce(extract(epoch from clock_timestamp() - min(sealed_at)
                              filter (where state in ('sealed','draining'))),0),
                     (select ring_slot from {schema}.segment_pointer),
                     (select active_seq from {schema}.segment_pointer)
              from {schema}.segments""").fetchone()
            slot = r[4]
            try:
                act = cconn.execute(f"select count(*) from {schema}.seg_{slot}").fetchone()[0]
            except Exception:
                act = -1
            f.write(f"ring\t{r[0]}\t{r[1]}\t{r[2]}\t{act}\t{r[3]}\t{r[5]}\n")
    except Exception as e:
        f.write(f"err\t{time.time()}\t{type(e).__name__}: {str(e)[:100]}\n")
        try:
            cconn.rollback()
        except Exception:
            pass
    d = iv - (time.monotonic() - t0)
    if d > 0:
        time.sleep(d)
f.close()
