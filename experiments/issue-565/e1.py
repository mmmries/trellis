"""#565 E1 (write-path tax) and E2 (capture ceiling) harness.

One fresh tmpfs cluster per run, Trellis installed with one transform on public.gen so every
variant has the identical schema. Variants:
  none   no capture (control)
  slot   today's path: `trellis run --staging --drain-threads 0` (walsender + intake, no drain)
  row    PL/pgSQL AFTER ... FOR EACH ROW capture trigger appending to the active ring slot
  stmt   statement-level capture triggers with transition tables
  idx_btree  no capture, one extra btree index on val (a comparison point, not a capture)
  idx_regex  no capture, one extra expression index on regexp_count(val::text, '[13579]')

Writers are pgbench clients inserting `rpc` rows per commit. Reported per run: writer tx/s and
rows/s, commit latency p50/p99 (pgbench per-transaction log), WAL bytes per source row (including,
for `slot`, the ring WAL intake writes once it has caught up), primary CPU (every postgres process,
reaped backends included via the postmaster's cutime/cstime), and for `slot` the Trellis process's
CPU and how long intake took to stage everything after the writers stopped.

Holds /tmp/trellis-bench.lock exclusively for the measurement, like .claude/scripts/bench.
"""
import argparse, fcntl, glob, json, os, shutil, statistics, subprocess, sys, time
import psycopg
import capture_sql

TRELLIS = "/home/mike/code/trellis-565/target/release/trellis"
BASE = os.environ.get("TC565_BASE", "/tmp/tc-565/e1")
UPDATE_TABLE_ROWS = 1_000_000
PORT = int(os.environ.get("TC565_PORT", "5566"))
TICK = os.sysconf("SC_CLK_TCK")


def sh(*a, **kw):
    return subprocess.run(a, check=True, capture_output=True, text=True, **kw)


def proc_cpu(pid, children_too=False):
    with open(f"/proc/{pid}/stat") as f:
        parts = f.read().rsplit(")", 1)[1].split()
    # fields after the comm: state=0 ... utime=11 stime=12 cutime=13 cstime=14
    t = int(parts[11]) + int(parts[12])
    if children_too:
        t += int(parts[13]) + int(parts[14])
    return t / TICK


def pg_cpu(postmaster):
    """Every postgres process's CPU: live children plus reaped ones (postmaster's cutime)."""
    total = proc_cpu(postmaster, children_too=True)
    for child in open(f"/proc/{postmaster}/task/{postmaster}/children").read().split():
        try:
            total += proc_cpu(int(child))
        except (FileNotFoundError, ProcessLookupError):
            pass   # a backend that exits mid-scan is counted in the postmaster's cutime instead
    return total


class Cluster:
    def __init__(self, extra=()):
        shutil.rmtree(BASE, ignore_errors=True)
        os.makedirs(BASE)
        sh("initdb", "-D", f"{BASE}/data", "-U", "postgres", "-A", "trust")
        self.log = open(f"{BASE}/pg.log", "w")
        args = ["postgres", "-D", f"{BASE}/data", "-h", "", "-k", BASE, "-p", str(PORT),
                "-c", "wal_level=logical", "-c", "max_replication_slots=50", "-c", "max_wal_senders=50",
                "-c", "dynamic_shared_memory_type=mmap", "-c", "logging_collector=off",
                "-c", "max_connections=200", "-c", "max_wal_size=2GB", *extra]
        self.proc = subprocess.Popen(args, stdout=self.log, stderr=self.log)
        for _ in range(200):
            if subprocess.run(["pg_isready", "-h", BASE, "-p", str(PORT), "-q"]).returncode == 0:
                break
            time.sleep(0.05)
        self.dsn = f"host={BASE} port={PORT} user=postgres dbname=postgres"
        self.url = f"postgresql://postgres@/postgres?host={BASE}&port={PORT}"

    def conn(self):
        return psycopg.connect(self.dsn, autocommit=True)

    def stop(self):
        self.proc.terminate()
        try:
            self.proc.wait(10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
        shutil.rmtree(BASE, ignore_errors=True)


def pctl(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p / 100 * len(xs)))] if xs else float("nan")


def sample_waits(dsn, stop, counts):
    """Sample what active client backends wait on, every 10 ms, until `stop` is set."""
    with psycopg.connect(dsn, autocommit=True) as s:
        while not stop.is_set():
            for et, ev, n in s.execute(
                    "select coalesce(wait_event_type,'CPU'), coalesce(wait_event,'-'), count(*) "
                    "from pg_stat_activity where backend_type = 'client backend' and state = 'active' "
                    "and pid <> pg_backend_pid() group by 1, 2"):
                counts[f"{et}:{ev}"] = counts.get(f"{et}:{ev}", 0) + n
            time.sleep(0.01)


def run(variant, rpc, clients, duration, copy_rows=None, wide=False, lsn_mode="insert_lsn", seq_cache=1, ptr="table",
        enc="coltext", pin=False, workload="insert", shape="", pg=()):
    c = Cluster(pg)
    trellis = None
    try:
        db = c.conn()
        if wide:
            cols = ["id", "val", "a", "b", "c", "d", "ts", "ok"]
            db.execute("create table public.gen (id bigint primary key, val numeric, a text, b text, c text, d text, ts timestamptz, ok bool)")
            vals = "g, g, md5(g::text), md5((g+1)::text), md5((g+2)::text), md5((g+3)::text), now(), g % 2 = 0"
        elif workload in ("update", "update_other"):
            # `other` is written by the load but read by no transform, so the trigger doesn't image it
            cols = ["id", "val"]
            db.execute("create table public.gen (id bigint primary key, val numeric, other int not null default 0)")
            vals = "g, g"
        else:
            cols = ["id", "val"]
            db.execute("create table public.gen (id bigint primary key, val numeric)")
            vals = "g, g"
        sh(TRELLIS, "-d", c.url, "apply", "TRANSFORM gen_out FROM gen SELECT val AS val")
        # Familiar comparison points for application engineers: one extra index on the table.
        if variant == "idx_btree":
            db.execute("create index gen_val_idx on public.gen (val)")
        elif variant == "idx_regex":
            db.execute("create index gen_val_regex_idx on public.gen (regexp_count(val::text, '[13579]'))")
        if seq_cache != 1:
            db.execute(f"alter sequence trellis.staging_change_id_seq cache {seq_cache}")
        capture_sql.PTR["mode"] = ptr
        capture_sql.ENC.update(mode=enc, pin=pin)
        capture_sql.SHAPE.update(new_only="new_only" in shape, skip_noop="skip_noop" in shape, reread="reread" in shape)
        if workload in ("update", "update_other"):
            db.execute(f"insert into public.gen (id, val) select g, g from generate_series(1, {UPDATE_TABLE_ROWS}) g")
            db.execute("vacuum analyze public.gen")
        if enc == "hstore":
            db.execute("create extension hstore")
        if workload == "toast_update":
            # 2,000 rows each carrying a ~100 KB, mostly incompressible text column; the load then
            # updates only `val` on `rpc` rows per commit. pgoutput omits the unchanged TOAST value
            # from the new image; a trigger has to detoast it to build one.
            db.execute("alter table public.gen add column doc text")
            db.execute("insert into public.gen (id, val, doc) select g, g, (select string_agg(md5(g::text || i::text || random()::text), '') "
                       "from generate_series(1, 3200) i) from generate_series(1, 2000) g")
            db.execute("alter table public.gen replica identity full")
            cols = cols + ["doc"]
        if ptr == "seq":
            db.execute("create sequence trellis.active_slot_seq minvalue 0 maxvalue 3 start 0")
            db.execute("select setval('trellis.active_slot_seq', (select ring_slot from trellis.segment_pointer), true)")
        trig_cols = [c for c in cols if c != "doc"] if enc == "coltext_narrow" else cols
        capture_sql.ENC["mode"] = "coltext" if enc == "coltext_narrow" else enc
        if variant == "row":
            db.execute(capture_sql.row_trigger("gen", trig_cols, ["id"], lsn_mode))
        elif variant == "stmt":
            db.execute(capture_sql.stmt_trigger("gen", trig_cols, ["id"], lsn_mode))
        elif variant == "slot":
            trellis = subprocess.Popen([TRELLIS, "-d", c.url, "run", "--staging", "--drain-threads", "0"],
                                       stdout=open(f"{BASE}/trellis.log", "w"), stderr=subprocess.STDOUT)
            deadline = time.time() + 60
            while time.time() < deadline:
                pub = db.execute("select count(*) from pg_publication_tables where tablename = 'gen'").fetchone()[0]
                pend = db.execute("select count(*) from trellis.pending_backfill").fetchone()[0]
                slot = db.execute("select count(*) from pg_replication_slots where active").fetchone()[0]
                if pub and not pend and slot:
                    break
                time.sleep(0.2)
            else:
                raise RuntimeError("slot path never became ready: " + open(f"{BASE}/trellis.log").read()[-2000:])
            time.sleep(1)
        db.execute("checkpoint")
        postmaster = c.proc.pid
        lsn0 = db.execute("select pg_current_wal_lsn()").fetchone()[0]
        cpu0 = pg_cpu(postmaster)
        tcpu0 = proc_cpu(trellis.pid) if trellis else 0.0
        lat = []
        t0 = time.time()
        if copy_rows:
            path = f"{BASE}/copy.csv"
            with open(path, "w") as f:
                if wide:
                    raise SystemExit("wide copy not implemented")
                f.writelines(f"{i},{i}\n" for i in range(1, copy_rows + 1))
            cpu0 = pg_cpu(postmaster)
            t0 = time.time()
            db.execute(f"copy public.gen from '{path}' (format csv)")
            elapsed = time.time() - t0
            txns, rows = 1, copy_rows
        else:
            # Fixed row count, not fixed time: the slot path retains WAL until intake catches up,
            # and tmpfs is shared with other lanes.
            target = duration if duration > 1000 else (400_000 if rpc == 1 else 4_000_000)
            if workload == "toast_update":
                target = 2_000
            elif workload in ("update", "update_other"):
                target = min(target, 2_000_000)
            per_client = max(1, target // (clients * rpc))
            script = f"{BASE}/load.sql"
            with open(script, "w") as f:
                if workload in ("update", "update_other"):
                    col = "val = val + 1" if workload == "update" else "other = other + 1"
                    f.write(f"\\set lo random(1, {UPDATE_TABLE_ROWS - rpc + 1})\n")
                    f.write(f"update public.gen set {col} where id between :lo and :lo + {rpc - 1};\n")
                elif workload == "toast_update":
                    f.write(f"\\set lo random(1, {2000 - rpc + 1})\n")
                    f.write(f"update public.gen set val = val + 1 where id between :lo and :lo + {rpc - 1};\n")
                else:
                  f.write(f"\\set next :next + {rpc}\n")
                  f.write(f"insert into public.gen ({', '.join(cols)}) select {vals} from generate_series("
                        f":client_id::bigint * 1000000000 + :next, :client_id::bigint * 1000000000 + :next + {rpc - 1}) g;\n")
            os.makedirs(f"{BASE}/pblog", exist_ok=True)
            import threading
            stop, waits = threading.Event(), {}
            sampler = threading.Thread(target=sample_waits, args=(c.dsn, stop, waits)); sampler.start()
            out = subprocess.run(["pgbench", "-h", BASE, "-p", str(PORT), "-U", "postgres", "-d", "postgres", "-n",
                                  "-c", str(clients), "-j", str(clients), "-t", str(per_client), "-D", "next=0",
                                  "-f", script, "-l", f"--log-prefix={BASE}/pblog/l"],
                                 capture_output=True, text=True)
            elapsed = time.time() - t0
            stop.set(); sampler.join()
            if out.returncode != 0:
                raise RuntimeError(out.stdout + out.stderr)
            for fn in glob.glob(f"{BASE}/pblog/l*"):
                with open(fn) as f:
                    lat.extend(int(line.split()[2]) for line in f)
            txns = len(lat)
            rows = txns * rpc
        lsn_w = db.execute("select pg_current_wal_lsn()").fetchone()[0]
        cpu_w = pg_cpu(postmaster)
        catch_up = 0.0
        catch_up_error = None
        if trellis:
            tc = time.time()
            while True:
                conf = db.execute("select max(confirmed_lsn) >= %s::pg_lsn from trellis.replication_progress", (lsn_w,)).fetchone()[0]
                if conf:
                    break
                if time.time() - tc > 900 or trellis.poll() is not None:
                    catch_up_error = open(f"{BASE}/trellis.log").read()[-1500:]
                    break
                time.sleep(0.2)
            catch_up = time.time() - tc
        lsn1 = db.execute("select pg_current_wal_lsn()").fetchone()[0]
        wal = db.execute("select pg_wal_lsn_diff(%s::pg_lsn, %s::pg_lsn)", (lsn1, lsn0)).fetchone()[0]
        cpu1 = pg_cpu(postmaster)
        tcpu1 = proc_cpu(trellis.pid) if trellis else 0.0
        ring = db.execute("select (select count(*) from trellis.seg_0)+(select count(*) from trellis.seg_1)"
                          "+(select count(*) from trellis.seg_2)+(select count(*) from trellis.seg_3)").fetchone()[0]
        src = db.execute("select count(*) from public.gen").fetchone()[0]
        tot = sum(waits.values()) if not copy_rows else 0
        top_waits = {k: round(v / tot, 3) for k, v in sorted(waits.items(), key=lambda kv: -kv[1])[:6]} if tot else {}
        res = dict(variant=variant, shape=shape, pg=list(pg), storage=BASE, enc=enc, pin=pin, workload=workload, seq_cache=seq_cache, ptr=ptr, top_waits=top_waits, rpc=rpc, clients=clients, wide=wide, copy_rows=copy_rows, lsn_mode=lsn_mode,
                   secs=round(elapsed, 2), txns=txns, rows=rows, src_rows=src, ring_rows=ring,
                   tps=round(txns / elapsed, 1), rows_per_s=round(rows / elapsed),
                   lat_p50_ms=round(pctl(lat, 50) / 1000, 3) if lat else None,
                   lat_p99_ms=round(pctl(lat, 99) / 1000, 3) if lat else None,
                   wal_bytes_per_row=round(float(wal) / rows, 1),
                   pg_cpu_s_writers=round(cpu_w - cpu0, 2), pg_cpu_s_total=round(cpu1 - cpu0, 2),
                   pg_cpu_us_per_row=round((cpu1 - cpu0) / rows * 1e6, 2),
                   trellis_cpu_s=round(tcpu1 - tcpu0, 2),
                   trellis_cpu_us_per_row=round((tcpu1 - tcpu0) / rows * 1e6, 2),
                   intake_catch_up_s=round(catch_up, 2),
                   staged_rows_per_s=round(rows / (elapsed + catch_up)),
                   intake_error=catch_up_error)
        return res
    finally:
        if trellis:
            trellis.terminate()
            try:
                trellis.wait(10)
            except subprocess.TimeoutExpired:
                trellis.kill()
        c.stop()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--variants", default="none,slot,row,stmt")
    ap.add_argument("--rpc", default="1,100,1000")
    ap.add_argument("--clients", default="1,4,16")
    ap.add_argument("--duration", type=int, default=0, help="total rows per run if > 1000; default 400k at 1 row/commit, else 4M")
    ap.add_argument("--copy", type=int, default=0)
    ap.add_argument("--wide", action="store_true")
    ap.add_argument("--lsn-mode", default="insert_lsn")
    ap.add_argument("--seq-cache", type=int, default=1)
    ap.add_argument("--ptr", default="table")
    ap.add_argument("--enc", default="coltext")
    ap.add_argument("--pin", action="store_true")
    ap.add_argument("--workload", default="insert")
    ap.add_argument("--shape", default="", help="comma list of capture_sql.SHAPE flags: new_only, skip_noop, reread")
    ap.add_argument("--pg", default="", help="extra postgres settings, e.g. checkpoint_timeout=30s;max_wal_size=1GB")
    ap.add_argument("--no-lock", action="store_true", help="functional smoke only; numbers are not measurements")
    ap.add_argument("--out", default=os.path.expanduser("~/tc-565/results/e1.jsonl"))
    a = ap.parse_args()
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    lock = open("/tmp/trellis-bench.lock", "w")
    try:
        if a.no_lock:
            raise StopIteration
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except StopIteration:
        pass
    except BlockingIOError:
        print("waiting for the bench lock ...", file=sys.stderr)
        fcntl.flock(lock, fcntl.LOCK_EX)
    for variant in a.variants.split(","):
        shapes = [(None, None)] if a.copy else [(int(r), int(n)) for r in a.rpc.split(",") for n in a.clients.split(",")]
        for rpc, clients in shapes:
            others = subprocess.run("pgrep -ax cargo; pgrep -ax rustc; pgrep -ax clippy-driver",
                                    shell=True, capture_output=True, text=True).stdout.strip()
            res = run(variant, rpc, clients, a.duration, a.copy or None, a.wide, a.lsn_mode, a.seq_cache, a.ptr, a.enc, a.pin, a.workload,
                      a.shape, tuple(x for kv in a.pg.split(";") if kv for x in ("-c", kv)))
            res["contaminated"] = bool(others) or a.no_lock
            res["at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
            line = json.dumps(res)
            print(line, flush=True)
            with open(a.out, "a") as f:
                f.write(line + "\n")


if __name__ == "__main__":
    main()
