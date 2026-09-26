"""#565 E8: idle cost. One transform, no writes, 60 s. `staging` is today's `trellis run` (intake +
2 drain threads); `no_staging` is `trellis run --no-staging` (2 drain threads, no intake, no
walsender): the closest thing to trigger capture's idle footprint without the spike."""
import json, os, shutil, signal, subprocess, sys, time
import psycopg
sys.path.insert(0, os.path.dirname(__file__))
from e1 import pg_cpu, proc_cpu

TRELLIS = "/home/mike/code/trellis-565/target/release/trellis"
BASE, PORT, WINDOW = "/tmp/tc-565/e8", 5570, 60


def main(mode):
    shutil.rmtree(BASE, ignore_errors=True); os.makedirs(BASE)
    subprocess.run(["initdb", "-D", f"{BASE}/data", "-U", "postgres", "-A", "trust"], check=True, capture_output=True)
    pg = subprocess.Popen(["postgres", "-D", f"{BASE}/data", "-h", "", "-k", BASE, "-p", str(PORT),
                           "-c", "wal_level=logical", "-c", "logging_collector=off", "-c", "dynamic_shared_memory_type=mmap"],
                          stdout=open(f"{BASE}/pg.log", "w"), stderr=subprocess.STDOUT)
    time.sleep(1.5)
    url = f"postgresql://postgres@/postgres?host={BASE}&port={PORT}"
    db = psycopg.connect(f"host={BASE} port={PORT} user=postgres dbname=postgres", autocommit=True)
    tr = None
    try:
        db.execute("create table public.gen (id bigint primary key, val numeric)")
        subprocess.run([TRELLIS, "-d", url, "apply", "TRANSFORM gen_out FROM gen SELECT val AS val"], check=True, capture_output=True)
        args = [TRELLIS, "-d", url, "run", "--drain-threads", "2"] + (["--no-staging"] if mode == "no_staging" else [])
        tr = subprocess.Popen(args, stdout=open(f"{BASE}/trellis.log", "w"), stderr=subprocess.STDOUT)
        time.sleep(15)   # let startup, publication and the capture marker settle
        q = "select xact_commit + xact_rollback from pg_stat_database where datname = 'postgres'"
        x0, l0, c0, t0 = db.execute(q).fetchone()[0], db.execute("select pg_current_wal_lsn()").fetchone()[0], pg_cpu(pg.pid), proc_cpu(tr.pid)
        time.sleep(WINDOW)
        x1, l1, c1, t1 = db.execute(q).fetchone()[0], db.execute("select pg_current_wal_lsn()").fetchone()[0], pg_cpu(pg.pid), proc_cpu(tr.pid)
        wal = db.execute("select pg_wal_lsn_diff(%s::pg_lsn, %s::pg_lsn)", (l1, l0)).fetchone()[0]
        res = dict(mode=mode, txn_per_s=round((x1 - x0 - 3) / WINDOW, 1), wal_bytes_per_s=round(float(wal) / WINDOW),
                   pg_cpu_pct=round((c1 - c0) / WINDOW * 100, 2), trellis_cpu_pct=round((t1 - t0) / WINDOW * 100, 2))
        print(json.dumps(res), flush=True)
        with open(os.path.expanduser("~/tc-565/results/e8.jsonl"), "a") as f:
            f.write(json.dumps(res) + "\n")
    finally:
        if tr: tr.terminate(); tr.wait()
        db.close(); pg.send_signal(signal.SIGINT); pg.wait(); shutil.rmtree(BASE, ignore_errors=True)


if __name__ == "__main__":
    for m in sys.argv[1:]:
        main(m)
