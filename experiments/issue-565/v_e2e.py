"""#565 phase-2 validations on the pseudo-spike: capture triggers + Trellis's real seal/drain.

`spike/565-trigger-capture` adds TRELLIS_SPIKE_TRIGGER_CAPTURE: `trellis run --staging` then
runs the maintenance loop (seal, retire, reclaim) but no publication, slot or intake. Definitions
are applied to empty sources and flipped to `live` by hand (no backfill, since the ring has every
change from the first write), then the generated statement triggers are installed.

  correctness  a mixed concurrent workload (upserts, range updates, regroups, deletes, key moves,
               multi-statement transactions touching one key twice, savepoint rollbacks, MERGE)
               at one isolation level, pgbench retrying serialization failures and deadlocks.
               Then directed fence probes: a REPEATABLE READ transaction whose snapshot predates
               several seals writes last; a prepared transaction held across seals. Pass = the 1-1
               target and a GROUP BY target equal the source recomputed from scratch.
  backpressure paced inserts at a fixed offered rate while the drain runs; ring bytes sampled
               every 5 s; reports capture rate, apply rate, peak ring size, drain time after.
"""
import argparse, json, os, subprocess, sys, threading, time, fcntl
import psycopg
sys.path.insert(0, os.path.dirname(__file__))
import capture_sql
import e1
from e1 import Cluster, pg_cpu, proc_cpu

TRELLIS = "/home/mike/code/trellis-565/target/release/trellis"
MOVE_BASE, MERGE_BASE = 10_000_000, 50_000_000


def setup(c, aggregate=True, shape="", capture="trigger"):
    db = c.conn()
    db.execute("create table public.gen (id bigint primary key, grp int not null, val numeric, other int not null default 0)")
    db.execute("alter table public.gen replica identity full")   # admission check for the aggregate's old image
    e1.sh(TRELLIS, "-d", c.url, "apply", "TRANSFORM gen_out FROM gen SELECT grp AS grp, val AS val")
    if aggregate:
        e1.sh(TRELLIS, "-d", c.url, "apply", "TRANSFORM gen_sum FROM gen GROUP BY grp SELECT sum(val) AS total, count(*) AS n")
    if capture == "slot":
        return db
    db.execute("update trellis.transform_definitions set status = 'live'")
    capture_sql.PTR["mode"] = "mirror"
    capture_sql.ENC.update(mode="format", pin=True)
    capture_sql.SHAPE.update(new_only="new_only" in shape, skip_noop="skip_noop" in shape, reread="reread" in shape)
    db.execute(capture_sql.stmt_trigger("gen", ["id", "grp", "val"], ["id"]))
    return db


def start_trellis(c, drain_threads, capture="trigger"):
    env = dict(os.environ)
    if capture == "trigger":
        env["TRELLIS_SPIKE_TRIGGER_CAPTURE"] = "1"
    return subprocess.Popen([TRELLIS, "-d", c.url, "run", "--staging", "--drain-threads", str(drain_threads)],
                            stdout=open(f"{e1.BASE}/trellis.log", "w"), stderr=subprocess.STDOUT, env=env)


def diff(db, aggregate=True):
    one = db.execute("select (select count(*) from (select id, grp, val from gen except select id, grp, val from gen_out) a) "
                     "+ (select count(*) from (select id, grp, val from gen_out except select id, grp, val from gen) b)").fetchone()[0]
    agg = 0
    if aggregate:
        agg = db.execute("""select count(*) from (select grp, sum(val) total, count(*) n from gen group by grp) s
                            full join gen_sum t using (grp)
                            where s.grp is null or t.grp is null or s.total is distinct from t.total or s.n <> t.n""").fetchone()[0]
    return one, agg


def wait_converged(db, timeout, aggregate=True):
    t0 = time.time()
    while True:
        one, agg = diff(db, aggregate)
        if one == 0 and agg == 0:
            return time.time() - t0, one, agg
        if time.time() - t0 > timeout:
            return None, one, agg
        time.sleep(2)


SCRIPTS = {
    "upsert": ("\\set id random(1, 200000)\n\\set g random(1, 50)\n\\set v random(1, 1000)\n"
               "insert into gen (id, grp, val) values (:id, :g, :v) on conflict (id) do update set val = excluded.val, grp = excluded.grp;\n"),
    "range_update": "\\set id random(1, 200000)\nupdate gen set val = val + 1 where id between :id and :id + 49;\n",
    "regroup": "\\set id random(1, 200000)\n\\set g random(1, 50)\nupdate gen set grp = :g where id = :id;\n",
    "other_only": "\\set id random(1, 200000)\nupdate gen set other = other + 1 where id between :id and :id + 9;\n",
    "delete": "\\set id random(1, 200000)\ndelete from gen where id = :id;\n",
    "move": ("\\set next :next + 1\n\\set id random(1, 200000)\n"
             f"update gen set id = {MOVE_BASE} + :client_id * 1000000 + :next where id = :id;\n"),
    "twice": ("\\set id random(1, 200000)\n\\set id2 random(1, 200000)\nbegin;\n"
              "update gen set val = val + 1 where id = :id;\nupdate gen set val = val * 2, grp = grp % 50 + 1 where id = :id;\n"
              "delete from gen where id = :id2;\ninsert into gen (id, grp, val) values (:id2, 7, 7) on conflict (id) do nothing;\ncommit;\n"),
    "savepoint": ("\\set id random(1, 200000)\nbegin;\nupdate gen set val = -1 where id = :id;\nsavepoint a;\n"
                  "update gen set val = -2, grp = 1 where id = :id;\nrollback to savepoint a;\nupdate gen set grp = grp % 50 + 1 where id = :id;\ncommit;\n"),
    "merge": (f"\\set id random(1, 1000)\n\\set v random(1, 1000)\n"
              f"merge into gen t using (select {MERGE_BASE} + :client_id * 1000000 + :id as id, :v::numeric as val) s on t.id = s.id "
              "when matched and t.val > 800 then delete when matched then update set val = s.val "
              "when not matched then insert (id, grp, val) values (s.id, 3, s.val);\n"),
}


def correctness(a):
    c = Cluster(("-c", "max_prepared_transactions=10"))
    tr = None
    capture = a.capture.split(",")[0]
    res = dict(mode="correctness", capture=capture, scripts=a.scripts, iso=a.iso, shape=a.shape, storage=e1.BASE, clients=a.clients, secs=a.secs)
    try:
        db = setup(c, shape=a.shape, capture=capture)
        tr = start_trellis(c, a.drain_threads, capture)
        if capture == "slot":   # the control: today's slot + intake, definitions go live the normal way
            deadline = time.time() + 120
            while time.time() < deadline and db.execute(
                    "select count(*) from trellis.transform_definitions where status <> 'live'").fetchone()[0]:
                time.sleep(0.5)
        db.execute("insert into gen (id, grp, val) select g, g % 50 + 1, g % 997 from generate_series(1, 200000) g")
        os.makedirs(f"{e1.BASE}/pb", exist_ok=True)
        args = ["pgbench", "-h", e1.BASE, "-p", str(e1.PORT), "-U", "postgres", "-d", "postgres", "-n",
                "-c", str(a.clients), "-j", str(a.clients), "-T", str(a.secs), "-D", "next=0", "--max-tries", "50"]
        for name, body in SCRIPTS.items():
            if a.scripts and name not in a.scripts.split(","):
                continue
            path = f"{e1.BASE}/pb/{name}.sql"
            open(path, "w").write(body)
            args += ["-f", f"{path}@{3 if name in ('upsert', 'range_update') else 1}"]
        env = dict(os.environ, PGOPTIONS=f"-c default_transaction_isolation={a.iso.replace('_', chr(92) + ' ')}")
        out = subprocess.run(args, capture_output=True, text=True, env=env)
        summary = [l for l in out.stdout.splitlines() if any(k in l for k in ("processed", "failed", "retried", "tps", "aborted"))]
        res["pgbench"] = summary
        res["pgbench_rc"] = out.returncode
        if out.returncode != 0:
            res["pgbench_err"] = out.stderr[-1500:]

        # Directed fence probe 1: a REPEATABLE READ transaction whose snapshot predates several seals.
        rr = psycopg.connect(c.dsn)
        rr.execute("set transaction isolation level repeatable read")
        rr.execute("select count(*) from gen")                       # snapshot taken, no xid yet
        seg0 = db.execute("select active_seq from trellis.segment_pointer").fetchone()[0]
        db.execute("insert into gen (id, grp, val) values (90000001, 1, 1)")
        deadline = time.time() + 60
        while db.execute("select active_seq from trellis.segment_pointer").fetchone()[0] < seg0 + 3 and time.time() < deadline:
            db.execute("update gen set val = val + 1 where id = 90000001")
            time.sleep(0.3)
        res["rr_probe_seals_crossed"] = db.execute("select active_seq from trellis.segment_pointer").fetchone()[0] - seg0
        rr.execute("insert into gen (id, grp, val) values (90000002, 2, 2)")
        rr.commit(); rr.close()
        # Directed fence probe 2: a prepared transaction held across seals, then committed.
        p = psycopg.connect(c.dsn)
        p.execute("insert into gen (id, grp, val) values (90000003, 3, 3)")
        p.execute("prepare transaction 'tc565'")
        p.close()
        seg1 = db.execute("select active_seq from trellis.segment_pointer").fetchone()[0]
        for _ in range(20):
            db.execute("update gen set val = val + 1 where id = 90000001")
            time.sleep(0.3)
        res["prepared_probe_seals_crossed"] = db.execute("select active_seq from trellis.segment_pointer").fetchone()[0] - seg1
        db.execute("commit prepared 'tc565'")

        conv, one, agg = wait_converged(db, a.timeout)
        res.update(converged_after_s=None if conv is None else round(conv, 1), one_to_one_diff_rows=one, aggregate_diff_groups=agg,
                   src_rows=db.execute("select count(*) from gen").fetchone()[0],
                   segments_sealed=db.execute("select max(seg_seq) from trellis.segments").fetchone()[0],
                   poison=db.execute("select count(*) from trellis.poison").fetchone()[0],
                   ok=conv is not None)
        if conv is None:
            res["trellis_log_tail"] = open(f"{e1.BASE}/trellis.log").read()[-1500:]
    finally:
        if tr:
            tr.terminate(); tr.wait()
        c.stop()
    return res


def ring_bytes(db):
    return db.execute("select " + " + ".join(f"pg_total_relation_size('trellis.seg_{i}')" for i in range(4))).fetchone()[0]


def backpressure(a, rate):
    c = Cluster(tuple(x for kv in a.pg.split(";") if kv for x in ("-c", kv)))
    tr = None
    res = dict(mode="backpressure", offered_rows_per_s=rate, rpc=a.rpc, clients=a.clients, drain_threads=a.drain_threads,
               aggregate=a.aggregate, shape=a.shape, storage=e1.BASE, secs=a.secs, pg=a.pg)
    try:
        db = setup(c, aggregate=a.aggregate, shape=a.shape)
        tr = start_trellis(c, a.drain_threads)
        time.sleep(3)
        script = f"{e1.BASE}/load.sql"
        open(script, "w").write(f"\\set next :next + {a.rpc}\n\\set g random(1, 50)\n"
                                f"insert into gen (id, grp, val) select g, :g, g % 997 from generate_series("
                                f":client_id::bigint * 1000000000 + :next, :client_id::bigint * 1000000000 + :next + {a.rpc - 1}) g;\n")
        tps = max(1, rate // a.rpc)
        samples, stop = [], threading.Event()

        def sampler():
            with psycopg.connect(c.dsn, autocommit=True) as s:
                t0 = time.time()
                while not stop.is_set():
                    src, tgt = s.execute("select (select count(*) from gen), (select count(*) from gen_out)").fetchone()
                    undrained = s.execute("select count(*) from trellis.segments where state <> 'drained'").fetchone()[0]
                    samples.append(dict(t=round(time.time() - t0, 1), src=src, tgt=tgt, ring_bytes=ring_bytes(s), undrained_segments=undrained))
                    stop.wait(5)
        th = threading.Thread(target=sampler); th.start()
        cpu0, tcpu0, lsn0 = pg_cpu(c.proc.pid), proc_cpu(tr.pid), db.execute("select pg_current_wal_lsn()").fetchone()[0]
        t0 = time.time()
        out = subprocess.run(["pgbench", "-h", e1.BASE, "-p", str(e1.PORT), "-U", "postgres", "-d", "postgres", "-n",
                              "-c", str(a.clients), "-j", str(a.clients), "-T", str(a.secs), "-R", str(tps), "-D", "next=0",
                              "-f", script], capture_output=True, text=True)
        load_s = time.time() - t0
        lsn1 = db.execute("select pg_current_wal_lsn()").fetchone()[0]
        src, tgt_at_stop = db.execute("select (select count(*) from gen), (select count(*) from gen_out)").fetchone()
        rb_at_stop = ring_bytes(db)
        conv, one, agg = wait_converged(db, a.timeout, a.aggregate)
        stop.set(); th.join()
        wal = float(db.execute("select pg_wal_lsn_diff(%s::pg_lsn, %s::pg_lsn)", (lsn1, lsn0)).fetchone()[0])
        res.update(pgbench_rc=out.returncode, captured_rows=src, capture_rows_per_s=round(src / load_s),
                   applied_rows_during_load=tgt_at_stop, apply_rows_per_s_during_load=round(tgt_at_stop / load_s),
                   backlog_rows_at_stop=src - tgt_at_stop, ring_bytes_at_stop=rb_at_stop,
                   peak_ring_bytes=max([s["ring_bytes"] for s in samples] + [rb_at_stop]),
                   drain_after_stop_s=None if conv is None else round(conv, 1),
                   overall_apply_rows_per_s=None if conv is None else round(src / (load_s + conv)),
                   wal_mb_per_s_load=round(wal / load_s / 1e6, 1),
                   pg_cpu_s=round(pg_cpu(c.proc.pid) - cpu0, 1), trellis_cpu_s=round(proc_cpu(tr.pid) - tcpu0, 1),
                   diff_one=one, diff_agg=agg, samples=samples[::max(1, len(samples) // 30)])
        if out.returncode != 0:
            res["pgbench_err"] = (out.stdout + out.stderr)[-1000:]
    finally:
        if tr:
            tr.terminate(); tr.wait()
        c.stop()
    return res


def down(a, capture):
    """Trellis down for `secs` at a paced insert rate: what accumulates on disk. `slot`: the slot
    exists (one `trellis run` created it) but nothing consumes it, so WAL is retained. `trigger`:
    triggers keep appending to the active segment and nothing seals or drains it."""
    c = Cluster(tuple(x for kv in a.pg.split(";") if kv for x in ("-c", kv)))
    tr = None
    rate = int(a.rates.split(",")[0])
    res = dict(mode="down", capture=capture, offered_rows_per_s=rate, rpc=a.rpc, clients=a.clients, secs=a.secs,
               storage=e1.BASE, pg=a.pg)
    try:
        if capture == "trigger":
            db = setup(c, aggregate=False)
        else:
            db = c.conn()
            db.execute("create table public.gen (id bigint primary key, grp int not null, val numeric, other int not null default 0)")
            e1.sh(TRELLIS, "-d", c.url, "apply", "TRANSFORM gen_out FROM gen SELECT grp AS grp, val AS val")
            tr = subprocess.Popen([TRELLIS, "-d", c.url, "run", "--staging", "--drain-threads", "0"],
                                  stdout=open(f"{e1.BASE}/trellis.log", "w"), stderr=subprocess.STDOUT)
            deadline = time.time() + 60
            while time.time() < deadline and not db.execute("select count(*) from pg_replication_slots where active").fetchone()[0]:
                time.sleep(0.2)
            time.sleep(2)
            tr.terminate(); tr.wait(); tr = None
        db.execute("checkpoint")
        script = f"{e1.BASE}/load.sql"
        open(script, "w").write(f"\\set next :next + {a.rpc}\n\\set g random(1, 50)\n"
                                f"insert into gen (id, grp, val) select g, :g, g % 997 from generate_series("
                                f":client_id::bigint * 1000000000 + :next, :client_id::bigint * 1000000000 + :next + {a.rpc - 1}) g;\n")
        samples, stop = [], threading.Event()

        def sampler():
            with psycopg.connect(c.dsn, autocommit=True) as s:
                t0 = time.time()
                while not stop.is_set():
                    wal = s.execute("select coalesce(sum(size), 0) from pg_ls_waldir()").fetchone()[0]
                    src = s.execute("select pg_total_relation_size('public.gen')").fetchone()[0]
                    samples.append(dict(t=round(time.time() - t0), pg_wal_bytes=int(wal), ring_bytes=int(ring_bytes(s)), source_bytes=int(src)))
                    stop.wait(10)
        th = threading.Thread(target=sampler); th.start()
        out = subprocess.run(["pgbench", "-h", e1.BASE, "-p", str(e1.PORT), "-U", "postgres", "-d", "postgres", "-n",
                              "-c", str(a.clients), "-j", str(a.clients), "-T", str(a.secs), "-R", str(max(1, rate // a.rpc)),
                              "-D", "next=0", "-f", script], capture_output=True, text=True)
        stop.set(); th.join()
        rows = db.execute("select count(*) from gen").fetchone()[0]
        last = samples[-1]
        res.update(pgbench_rc=out.returncode, rows=rows,
                   pg_wal_bytes_end=last["pg_wal_bytes"], ring_bytes_end=last["ring_bytes"], source_bytes_end=last["source_bytes"],
                   retained_bytes_per_row=round((last["pg_wal_bytes"] if capture == "slot" else last["ring_bytes"]) / rows, 1),
                   samples=samples)
    finally:
        if tr:
            tr.terminate(); tr.wait()
        c.stop()
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["correctness", "backpressure", "down"])
    ap.add_argument("--capture", default="trigger,slot")
    ap.add_argument("--iso", default="read_committed")
    ap.add_argument("--clients", type=int, default=16)
    ap.add_argument("--secs", type=int, default=120)
    ap.add_argument("--drain-threads", type=int, default=4)
    ap.add_argument("--timeout", type=int, default=900)
    ap.add_argument("--rates", default="50000")
    ap.add_argument("--rpc", type=int, default=100)
    ap.add_argument("--aggregate", action="store_true")
    ap.add_argument("--shape", default="")
    ap.add_argument("--scripts", default="", help="subset of SCRIPTS (default all)")
    ap.add_argument("--pg", default="")
    ap.add_argument("--no-lock", action="store_true")
    ap.add_argument("--out", default=os.path.expanduser("~/tc-565/results/v_e2e.jsonl"))
    a = ap.parse_args()
    lock = open("/tmp/trellis-bench.lock", "w")
    if not a.no_lock:
        print("taking the bench lock ...", file=sys.stderr, flush=True)
        fcntl.flock(lock, fcntl.LOCK_EX)
    if a.mode == "correctness":
        runs = [lambda: correctness(a)]
    elif a.mode == "down":
        runs = [lambda cap=cap: down(a, cap) for cap in a.capture.split(",")]
    else:
        runs = [lambda r=int(r): backpressure(a, r) for r in a.rates.split(",")]
    for run in runs:
        res = run()
        res["contaminated"] = a.no_lock
        res["at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        line = json.dumps(res)
        print(line, flush=True)
        with open(a.out, "a") as f:
            f.write(line + "\n")


if __name__ == "__main__":
    main()
