"""#565 E7: join/drop locking on a hot table.

16 pgbench writers at a paced 1000 commits/s total (1 row/commit) on public.gen, plus one
application transaction that inserts and then stays open for 30 s. Five seconds in, the join
runs. Measured from pgbench's per-transaction log: how long writers queued (max latency, and
transaction-seconds spent above 100 ms), and when the join committed.

Joins compared:
  publication   ALTER PUBLICATION ... ADD TABLE          (today, SHARE UPDATE EXCLUSIVE)
  trigger       CREATE TRIGGER x3 in one transaction      (SHARE ROW EXCLUSIVE)
  trigger_lt    same, lock_timeout 50 ms, retried every 200 ms until it lands
Drops: `publication_drop` (ALTER PUBLICATION DROP TABLE), `drop_trigger`, `drop_trigger_lt`.
"""
import glob, json, os, shutil, signal, subprocess, sys, threading, time
import psycopg
sys.path.insert(0, os.path.dirname(__file__))
import capture_sql

BASE, PORT = "/tmp/tc-565/e7", 5569
WINDOW, JOIN_AT, HOLD = 45, 5, 30


def main(mode):
    shutil.rmtree(BASE, ignore_errors=True); os.makedirs(f"{BASE}/pblog")
    subprocess.run(["initdb", "-D", f"{BASE}/data", "-U", "postgres", "-A", "trust"], check=True, capture_output=True)
    pg = subprocess.Popen(["postgres", "-D", f"{BASE}/data", "-h", "", "-k", BASE, "-p", str(PORT),
                           "-c", "wal_level=logical", "-c", "logging_collector=off", "-c", "dynamic_shared_memory_type=mmap",
                           "-c", "max_connections=100"], stdout=open(f"{BASE}/pg.log", "w"), stderr=subprocess.STDOUT)
    time.sleep(1.5)
    dsn = f"host={BASE} port={PORT} user=postgres dbname=postgres"
    db = psycopg.connect(dsn, autocommit=True)
    try:
        db.execute("create table public.gen (id bigint primary key, val numeric)")
        db.execute("create schema trellis")
        db.execute("create table trellis.segment_pointer (id bool primary key default true, ring_slot smallint not null)")
        db.execute("insert into trellis.segment_pointer values (true, 0)")
        db.execute("create sequence trellis.staging_change_id_seq")
        for i in range(4):
            db.execute(f"create table trellis.seg_{i} (src_table text, key text, op text, lsn pg_lsn, old_image jsonb, "
                       f"new_image jsonb, origin_lsn pg_lsn, src_changed timestamptz, "
                       f"change_id bigint default nextval('trellis.staging_change_id_seq'))")
        db.execute("create publication trellis_pub")
        ddl = capture_sql.stmt_trigger("gen", ["id", "val"], ["id"])
        # each chunk is "CREATE OR REPLACE FUNCTION ... $f$;  CREATE TRIGGER ... ;": the function is
        # set up beforehand, the CREATE TRIGGER statements are the join being measured
        chunks = [c for c in ddl.split("\nCREATE OR REPLACE FUNCTION") if "CREATE TRIGGER" in c]
        func_stmts = ["CREATE OR REPLACE FUNCTION" + c.split("CREATE TRIGGER")[0].split("CREATE OR REPLACE FUNCTION")[-1] for c in chunks]
        trig_stmts = ["CREATE TRIGGER " + c.split("CREATE TRIGGER")[1].strip().rstrip(";") for c in chunks]
        for f in func_stmts:
            db.execute(f)
        if mode.startswith("publication_drop"):
            db.execute("alter publication trellis_pub add table public.gen")
        if mode.startswith("drop_trigger"):
            for t in trig_stmts:
                db.execute(t)
        script = f"{BASE}/load.sql"
        open(script, "w").write("\\set id random(1, 1000000000000)\ninsert into public.gen values (:id, 1) on conflict do nothing;\n")
        pb = subprocess.Popen(["pgbench", "-h", BASE, "-p", str(PORT), "-U", "postgres", "-d", "postgres", "-n",
                               "-c", "16", "-j", "4", "-R", "1000", "-T", str(WINDOW), "-f", script,
                               "-l", f"--log-prefix={BASE}/pblog/l"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        holder = psycopg.connect(dsn)
        holder.execute("insert into public.gen values (-1, 0)")      # an application writer, open for HOLD s
        threading.Timer(HOLD, lambda: holder.commit()).start()
        t_start = time.time()
        time.sleep(JOIN_AT)
        t_join = time.time()
        attempts = 0
        if mode == "publication":
            stmts = ["alter publication trellis_pub add table public.gen"]
        elif mode == "publication_drop":
            stmts = ["alter publication trellis_pub drop table public.gen"]
        elif mode.startswith("trigger"):
            stmts = trig_stmts
        else:
            stmts = [f"drop trigger trellis_capture_{e} on public.gen" for e in ("insert", "update", "delete")]
        lt = mode.endswith("_lt")
        while True:
            attempts += 1
            try:
                with db.transaction():
                    if lt:
                        db.execute("set local lock_timeout = '50ms'")
                    for s in stmts:
                        db.execute(s)
                break
            except psycopg.errors.LockNotAvailable:
                time.sleep(0.2)
        t_done = time.time()
        pb.wait()
        lat = []
        for fn in glob.glob(f"{BASE}/pblog/l*"):
            for line in open(fn):
                p = line.split()
                lat.append((int(p[4]) + int(p[5]) / 1e6, int(p[2]) / 1e6))  # (end epoch s, latency s)
        slow = [l for _, l in lat if l > 0.1]
        res = dict(mode=mode, join_committed_after_s=round(t_done - t_join, 2), attempts=attempts,
                   txns=len(lat), max_latency_s=round(max(l for _, l in lat), 3),
                   txns_over_100ms=len(slow), writer_seconds_over_100ms=round(sum(slow), 1),
                   p99_ms=round(sorted(l for _, l in lat)[int(0.99 * len(lat))] * 1000, 2))
        print(json.dumps(res), flush=True)
        with open(os.path.expanduser("~/tc-565/results/e7.jsonl"), "a") as f:
            f.write(json.dumps(res) + "\n")
    finally:
        db.close()
        pg.send_signal(signal.SIGINT); pg.wait(); shutil.rmtree(BASE, ignore_errors=True)


if __name__ == "__main__":
    for m in sys.argv[1:]:
        main(m)
