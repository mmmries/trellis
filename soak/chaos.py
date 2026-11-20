#!/usr/bin/env python3
"""Chaos and soak harness: Trellis workers on a scratch Postgres, load, named faults, an oracle.

  soak/chaos.py setup [--accounts N --orders N] [--volume-mb MB] [--bin PATH]
  soak/chaos.py run (--scenario NAME | --step ACTION ...) [load and threshold flags]
  soak/chaos.py stop        stop the workers, the load and Postgres, by pid file
  soak/chaos.py actions     list the fault actions and the scenarios

`setup` builds the cluster, seeds it, defines the six transforms in defs.txt, waits for
them to go live and snapshots the result. Each `run` restores that snapshot, starts the
workers and the load, runs its steps in order, waits for the load to end, and then waits
for oracle.sql to report no differences. It reserves and finishes its own logbook entry,
and exits 0 when the oracle converged and every threshold held, 1 when not, 2 when the
harness itself failed and 64 on a usage error. See soak/README.md.
"""

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.request

import psycopg2

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
ADMIN = os.environ.get("USER", "postgres")  # the cluster's superuser: harness plumbing only
ROLE = "trellis"  # the role Trellis runs as: not a superuser
APP = "app"  # the role exhaust-conns holds connections as
DB = "shop"
METRICS_BASE = 59500


class UsageError(Exception):
    pass


class Harness:
    """Paths, processes and the event log for one cluster directory."""

    def __init__(self, root, port, bin_path=None, run_dir=None):
        self.root = root
        self.port = port
        self.sock = os.path.join(root, "s")
        self.pids = os.path.join(root, "pids")
        self.cfg_path = os.path.join(root, "setup.json")
        self.cfg = json.load(open(self.cfg_path)) if os.path.exists(self.cfg_path) else {}
        self.bin = os.path.abspath(
            bin_path or self.cfg.get("bin") or os.path.join(REPO, "target", "release", "trellis")
        )
        self.run_dir = run_dir or os.path.join(root, "logs")
        self.workers = {}  # name -> {"proc", "bin", "expected_down"}
        self.lock = threading.Lock()
        self.failures = []
        self.cleanups = []  # callables run once, after Postgres stops
        for d in (self.sock, self.pids, self.run_dir, os.path.join(root, "tmp")):
            os.makedirs(d, exist_ok=True)

    # -- paths -----------------------------------------------------------
    @property
    def data(self):
        return os.path.join(self.root, "vol" if self.cfg.get("volume_mb") else "", "data")

    @property
    def wal(self):
        return os.path.join(self.root, "wal")

    def dsn(self, user=ROLE, db=DB, tcp=False):
        host = "127.0.0.1" if tcp else self.sock
        return f"host={host} port={self.port} user={user} dbname={db}"

    # -- logging ---------------------------------------------------------
    def event(self, msg):
        line = f"{time.strftime('%T')} {msg}"
        print(line, flush=True)
        with self.lock, open(os.path.join(self.run_dir, "events.log"), "a") as f:
            f.write(line + "\n")

    def fail(self, msg):
        self.event(f"FAIL {msg}")
        with self.lock:
            self.failures.append(msg)

    # -- commands --------------------------------------------------------
    def env(self):
        return dict(os.environ, PGHOST=self.sock, PGPORT=str(self.port), TMPDIR=os.path.join(self.root, "tmp"))

    def sh(self, cmd, check=True, **kw):
        return subprocess.run(cmd, check=check, env=self.env(), text=True, **kw)

    def psql(self, sql=None, user=ADMIN, db=DB, file=None, check=True, variables=()):
        cmd = ["psql", "-XAtq", "-v", "ON_ERROR_STOP=1", "-U", user, "-d", db]
        for v in variables:
            cmd += ["-v", v]
        cmd += ["-f", file] if file else ["-c", sql]
        r = self.sh(cmd, check=check, capture_output=True)
        return r.stdout.strip()

    def sudo(self, *cmd):
        r = subprocess.run(["sudo", "-n", *cmd], text=True, capture_output=True)
        if r.returncode:
            raise RuntimeError(f"sudo -n {' '.join(cmd)} failed: {r.stderr.strip()}")

    # -- Postgres --------------------------------------------------------
    def pg_running(self):
        return self.sh(["pg_ctl", "-D", self.data, "status"], check=False, capture_output=True).returncode == 0

    def pg_start(self):
        self.sh(
            ["pg_ctl", "-D", self.data, "-w", "-t", "120", "-l", os.path.join(self.root, "pg_ctl.log"), "start"],
            capture_output=True,
        )

    def pg_stop(self, mode="fast"):
        if self.pg_running():
            self.sh(["pg_ctl", "-D", self.data, "-w", "-t", "120", "-m", mode, "stop"], capture_output=True)

    # -- pid files -------------------------------------------------------
    def pidfile(self, name):
        return os.path.join(self.pids, f"{name}.pid")

    def write_pid(self, name, pid):
        with open(self.pidfile(name), "w") as f:
            f.write(str(pid))

    def live_pid(self, name, needle):
        """The pid in <name>.pid if that process is alive and its command line has `needle`.

        Processes are only ever signalled through this: `pkill -f` patterns match the
        harness's own command line, and a recycled pid must not be killed."""
        try:
            pid = int(open(self.pidfile(name)).read().strip())
            cmdline = open(f"/proc/{pid}/cmdline").read().replace("\0", " ")
        except (OSError, ValueError):
            return None
        return pid if needle in cmdline else None

    def signal_pidfile(self, name, needle, sig, wait=10.0):
        pid = self.live_pid(name, needle)
        if pid is None:
            return False
        os.kill(pid, sig)
        deadline = time.time() + wait
        while time.time() < deadline and self.live_pid(name, needle) == pid:
            proc = self.workers.get(name, {}).get("proc")
            if proc is not None:
                proc.poll()  # reap our own child so it leaves /proc
            time.sleep(0.1)
        if self.live_pid(name, needle) == pid:
            os.kill(pid, signal.SIGKILL)
        return True

    # -- workers ---------------------------------------------------------
    def worker_names(self):
        return sorted(self.workers, key=lambda n: int(n[1:]))

    def start_worker(self, name, bin_path=None):
        bin_path = bin_path or self.workers.get(name, {}).get("bin") or self.bin
        idx = int(name[1:])
        # w1 connects over TCP so the blackhole action reaches it; the others use the socket.
        dsn = self.dsn(tcp=(idx == 1))
        cmd = [
            "systemd-run",
            "--user",
            "--scope",
            "-q",
            "-p",
            f"MemoryMax={self.memory_max}",
            bin_path,
            "run",
            "--drain-threads",
            str(self.drain_threads),
            "--prometheus-bind",
            f"127.0.0.1:{METRICS_BASE + idx}",
            "-d",
            dsn,
        ]
        env = dict(self.env(), RUST_LOG=os.environ.get("RUST_LOG", "info"))
        out = open(os.path.join(self.run_dir, f"{name}.log"), "a")
        proc = subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT, env=env, start_new_session=True)
        self.write_pid(name, proc.pid)  # systemd-run --scope execs the command: same pid
        with self.lock:
            self.workers[name] = {"proc": proc, "bin": bin_path, "expected_down": False}
        self.event(f"start {name} pid {proc.pid} ({bin_path})")

    def stop_worker(self, name, sig=signal.SIGINT):
        with self.lock:
            if name in self.workers:
                self.workers[name]["expected_down"] = True
        bin_path = self.workers.get(name, {}).get("bin", self.bin)
        if self.signal_pidfile(name, bin_path, sig):
            self.event(f"{'kill -9' if sig == signal.SIGKILL else 'stop'} {name}")
        proc = self.workers.get(name, {}).get("proc")
        if proc is not None:
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                pass

    def check_workers(self):
        """Fail on a worker that exited when no action stopped it (a crash, an OOM kill)."""
        for name in self.worker_names():
            w = self.workers[name]
            code = w["proc"].poll()
            if code is not None and not w["expected_down"]:
                w["expected_down"] = True
                self.fail(f"{name} exited unexpectedly with {code}; see {name}.log")

    def metrics(self, name):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{METRICS_BASE + int(name[1:])}/metrics", timeout=2) as r:
                return r.read().decode()
        except Exception:
            return ""

    # -- teardown --------------------------------------------------------
    def stop_all(self):
        """Stop the load and every worker by pid file, then Postgres, then run cleanups.

        Workers stop before Postgres and before any restore, or they keep writing into
        (or reconnecting to) the cluster being replaced."""
        self.signal_pidfile("loadgen", "loadgen.py", signal.SIGTERM, wait=5)
        names = set(self.workers) | {f[:-4] for f in os.listdir(self.pids) if f.startswith("w") and f.endswith(".pid")}
        for name in sorted(names):
            if name in self.workers:
                self.stop_worker(name)
            else:  # a worker from an earlier, interrupted invocation
                self.signal_pidfile(name, " run ", signal.SIGINT)
        self.pg_stop()
        for fn in self.cleanups:
            try:
                fn()
            except Exception as exc:
                self.event(f"cleanup failed: {exc}")
        self.cleanups.clear()
        tbs = os.path.join(self.root, "tbs")
        if os.path.ismount(tbs):
            self.sudo("umount", tbs)

    def restore(self):
        self.stop_all()
        for name in os.listdir(self.pids):
            if name.startswith("w") and self.live_pid(name[:-4], " run "):
                raise RuntimeError(f"worker {name[:-4]} is still running; not restoring under it")
        replace_tree(os.path.join(self.root, "data.base"), self.data)
        if self.cfg.get("volume_mb"):
            replace_tree(os.path.join(self.root, "wal.base"), self.wal)
        self.event("restored the base snapshot")


def replace_tree(src, dst):
    if os.path.lexists(dst):
        shutil.rmtree(dst)
    subprocess.run(["cp", "-a", "--reflink=auto", src, dst], check=True)


# -- the monitor -------------------------------------------------------------

MON_COLUMNS = [
    "ts",
    "workers",
    "worker_rss_kb",
    "ring_rows",
    "ring_bytes",
    "oldest_age_s",
    "live",
    "paused",
    "backfilling",
    "catching_up",
    "quarantined",
    "drain_failures",
    "held_keys",
    "changes_applied",
    "trellis_conns",
    "trellis_schema_mb",
    "target_mb",
]


def metric_max(text, name, label=""):
    """The largest sample of `name` (whose labels contain `label`), or None."""
    best = None
    for line in text.splitlines():
        if line.startswith("#") or not (line.startswith(name + "{") or line.startswith(name + " ")):
            continue
        if label and label not in line:
            continue
        try:
            v = float(line.rsplit(None, 1)[1])
        except (IndexError, ValueError):
            continue
        best = v if best is None else max(best, v)
    return best


def monitor(h, out_path, stop, interval=5.0):
    """One TSV row every `interval` s; tracks the peaks the thresholds read."""
    h.peak_rss_kb = 0
    h.peak_age = 0.0
    sizes = (
        "select (select round(sum(pg_total_relation_size(c.oid))/1048576.0) from pg_class c join pg_namespace n "
        "on n.oid = c.relnamespace where n.nspname = 'trellis' and c.relkind = 'r'), "
        "(select round(sum(pg_total_relation_size(c.oid))/1048576.0) from pg_class c where c.oid = any(array["
        "'public.acct_view','public.region_totals','public.acct_order_totals','public.region_order_totals',"
        "'public.order_view','public.big_accts']::regclass[])), "
        f"(select count(*) from pg_stat_activity where usename = '{ROLE}')"
    )
    with open(out_path, "w") as f:
        f.write("\t".join(MON_COLUMNS) + "\n")
        while not stop.is_set():
            h.check_workers()
            alive = [n for n in h.worker_names() if h.workers[n]["proc"].poll() is None]
            rss = 0
            for n in alive:
                try:
                    rss += int(
                        next(
                            line.split()[1]
                            for line in open(f"/proc/{h.workers[n]['proc'].pid}/status")
                            if line.startswith("VmRSS")
                        )
                    )
                except (OSError, StopIteration):
                    pass
            h.peak_rss_kb = max(h.peak_rss_kb, rss)
            text = "\n".join(h.metrics(n) for n in alive)

            def g(name, label="", text=text):
                v = metric_max(text, name, label)
                return "NA" if v is None else (str(int(v)) if v.is_integer() else f"{v:.2f}")

            age = metric_max(text, "trellis_ring_oldest_undrained_age_seconds")
            if age is not None:
                h.peak_age = max(h.peak_age, age)
            try:
                schema_mb, target_mb, conns = h.psql(sizes, check=True).split("|")
            except Exception:
                schema_mb = target_mb = conns = "NA"
            row = [
                time.strftime("%T"),
                str(len(alive)),
                str(rss),
                g("trellis_ring_rows"),
                g("trellis_ring_bytes"),
                g("trellis_ring_oldest_undrained_age_seconds"),
                *(
                    g("trellis_definitions", f'status="{s}"')
                    for s in ("live", "paused", "backfilling", "catching_up", "quarantined")
                ),
                g("trellis_drain_failures"),
                g("trellis_held_keys"),
                g("trellis_changes_applied_total"),
                conns,
                schema_mb,
                target_mb,
            ]
            f.write("\t".join(row) + "\n")
            f.flush()
            stop.wait(interval)


# -- the oracle --------------------------------------------------------------


def parse_oracle(text):
    """'target|count' lines -> {target: count}."""
    out = {}
    for line in text.splitlines():
        name, sep, n = line.rpartition("|")
        if sep and n.strip().isdigit():
            out[name.strip()] = int(n)
    return out


def oracle(h):
    try:
        return parse_oracle(h.psql(file=os.path.join(HERE, "oracle.sql"), user=ROLE))
    except subprocess.CalledProcessError as exc:
        h.event(f"oracle query failed: {(exc.stderr or '').strip()[:200]}")
        return None


def definition_states(h):
    rows = h.psql("select target_table || '=' || status from trellis.transform_definitions order by 1", user=ROLE)
    return dict(r.split("=", 1) for r in rows.splitlines() if "=" in r)


# -- fault actions -------------------------------------------------------------
# Each takes the harness and its string arguments. Arguments are positional, after the
# name and separated by ':'. Steps joined with '+' run at the same time.

ACTIONS = {}


def action(usage):
    def deco(fn):
        ACTIONS[fn.__name__.replace("_", "-")] = (fn, usage, (fn.__doc__ or "").strip())
        return fn

    return deco


def secs(arg):
    v = float(arg)
    if v < 0:
        raise ValueError(f"negative duration {arg}")
    return v


def pick_worker(h, which):
    names = [n for n in h.worker_names() if h.workers[n]["proc"].poll() is None]
    if which in ("", "any"):
        if not names:
            raise RuntimeError("no running worker to pick")
        return names[int.from_bytes(os.urandom(2), "little") % len(names)]
    if which not in h.workers:
        raise UsageError(f"no worker {which}; workers are {', '.join(h.worker_names())}")
    return which


@action("sleep:SECS")
def sleep(h, n):
    """Do nothing for SECS seconds."""
    time.sleep(secs(n))


@action("kill9:[WORKER|any][:RESTART_AFTER_SECS=2]")
def kill9(h, which="any", after="2"):
    """kill -9 one worker, then start it again."""
    name = pick_worker(h, which)
    h.stop_worker(name, signal.SIGKILL)
    time.sleep(secs(after))
    h.start_worker(name)


@action("kill9-storm:SECS")
def kill9_storm(h, n):
    """For SECS: every 5-45 s kill -9 a random worker; one time in four, three fast start/kill cycles first."""
    rnd = __import__("random")
    end = time.time() + secs(n)
    while time.time() < end:
        time.sleep(min(5 + rnd.random() * 40, max(0, end - time.time())))
        if time.time() >= end:
            break
        name = pick_worker(h, "any")
        h.stop_worker(name, signal.SIGKILL)
        if rnd.random() < 0.25:
            for _ in range(3):
                h.start_worker(name)
                time.sleep(1 + rnd.random() * 2)
                h.stop_worker(name, signal.SIGKILL)
        time.sleep(rnd.random() * 5)
        h.start_worker(name)


@action("pg-restart:[fast|immediate]")
def pg_restart(h, mode="fast"):
    """Restart Postgres; `immediate` crashes it and runs recovery."""
    if mode not in ("fast", "immediate"):
        raise UsageError(f"pg-restart mode {mode!r}")
    h.event(f"pg restart -m {mode}")
    h.sh(
        ["pg_ctl", "-D", h.data, "-w", "-t", "300", "-m", mode, "-l", os.path.join(h.root, "pg_ctl.log"), "restart"],
        capture_output=True,
    )
    h.event("pg back")


@action("pg-outage:SECS[:fast|immediate]")
def pg_outage(h, n, mode="fast"):
    """Stop Postgres, keep it down for SECS, start it."""
    h.event(f"pg stop -m {mode}")
    h.pg_stop(mode)
    time.sleep(secs(n))
    h.pg_start()
    h.event("pg back")


@action("terminate-backends:[TIMES=5]:[INTERVAL_SECS=10]")
def terminate_backends(h, times="5", interval="10"):
    """pg_terminate_backend every Trellis client backend, TIMES times."""
    for i in range(int(times)):
        n = h.psql(
            f"select count(pg_terminate_backend(pid)) from pg_stat_activity where usename = '{ROLE}' "
            "and backend_type = 'client backend' and pid <> pg_backend_pid()",
            db="postgres",
        )
        h.event(f"terminated {n} backends")
        if i + 1 < int(times):
            time.sleep(secs(interval))


@action("blackhole:SECS")
def blackhole(h, n):
    """Drop TCP to Postgres's port on lo for SECS (sudo nft). Only w1 connects over TCP."""
    table = f"trellis_soak_{h.port}"
    h.sudo("nft", "add", "table", "inet", table)

    def undo():
        subprocess.run(["sudo", "-n", "nft", "delete", "table", "inet", table], capture_output=True)

    h.cleanups.append(undo)
    try:
        h.sudo("nft", "add", "chain", "inet", table, "input", "{ type filter hook input priority -10; }")
        for d in ("dport", "sport"):
            h.sudo("nft", "add", "rule", "inet", table, "input", "iifname", "lo", "tcp", d, str(h.port), "drop")
        h.event(f"blackhole on for {n}s")
        time.sleep(secs(n))
    finally:
        undo()
        h.cleanups.remove(undo)
        h.event("blackhole lifted")


@action("exhaust-conns:SECS")
def exhaust_conns(h, n):
    """Hold every non-reserved connection slot (as role app) for SECS."""
    conns = []
    try:
        while True:
            try:
                conns.append(psycopg2.connect(h.dsn(user=APP, db="postgres"), connect_timeout=5))
            except psycopg2.Error as exc:
                h.event(f"holding {len(conns)} connections ({str(exc).strip().splitlines()[-1][:100]})")
                break
        time.sleep(secs(n))
    finally:
        for c in conns:
            c.close()
        h.event("connections released")


@action("worker-outage:SECS")
def worker_outage(h, n):
    """Stop every worker (SIGINT), keep them down for SECS, start them."""
    names = [w for w in h.worker_names() if h.workers[w]["proc"].poll() is None]
    for w in names:
        h.stop_worker(w)
    time.sleep(secs(n))
    for w in names:
        h.start_worker(w)


@action("wait-caught-up:[MAX_SECS=900]:[AGE_SECS=10]")
def wait_caught_up(h, max_secs="900", age="10"):
    """Wait until the ring's oldest undrained change is younger than AGE_SECS; fail after MAX_SECS."""
    t0 = time.time()
    time.sleep(min(30, secs(max_secs)))  # let a freshly started worker publish its gauges
    while time.time() - t0 < secs(max_secs):
        ages = [metric_max(h.metrics(w), "trellis_ring_oldest_undrained_age_seconds") for w in h.worker_names()]
        ages = [a for a in ages if a is not None]
        if ages and max(ages) < secs(age):
            h.event(f"caught up after {time.time() - t0:.0f}s")
            return
        time.sleep(5)
    h.fail(f"not caught up (oldest undrained age >= {age}s) within {max_secs}s")


@action("long-txn:SECS")
def long_txn(h, n):
    """Hold an application transaction that wrote a row open for SECS, then commit."""
    conn = psycopg2.connect(h.dsn())
    try:
        with conn.cursor() as cur:
            cur.execute("update accounts set balance = balance where id = 1")
        h.event(f"long transaction open for {n}s")
        time.sleep(secs(n))
        conn.commit()
        h.event("long transaction committed")
    finally:
        conn.close()


@action("pg-dump:SECS")
def pg_dump(h, n):
    """Run pg_dump into a reader that stalls for SECS, so it holds ACCESS SHARE on every table."""
    p = subprocess.Popen(["pg_dump", "-U", ADMIN, DB], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=h.env())
    h.event(f"pg_dump started, stalled for {n}s")
    time.sleep(secs(n))
    shutil.copyfileobj(p.stdout, open(os.devnull, "wb"))
    h.event(f"pg_dump finished ({p.wait()})")


def fill_and_hold(h, tablespace, n, rows_per_insert):
    """Fill `tablespace` with a ballast table until a write fails, hold SECS, drop it."""
    clause = f" tablespace {tablespace}" if tablespace else ""
    h.psql(f"drop table if exists soak_ballast; create table soak_ballast (x text){clause}")
    err = ""
    for _ in range(100000):
        r = h.sh(
            [
                "psql",
                "-XAtq",
                "-U",
                ADMIN,
                "-d",
                DB,
                "-c",
                "insert into soak_ballast select repeat(md5(g::text), 60)"
                f" from generate_series(1, {rows_per_insert}) g",
            ],
            check=False,
            capture_output=True,
        )
        if r.returncode:
            lines = r.stderr.strip().splitlines() or ["?"]
            err = next((ln for ln in lines if ln.startswith("ERROR")), lines[-1])
            break
    h.event(f"full: {err[:160]}")
    try:
        time.sleep(secs(n))
    finally:
        h.psql("drop table soak_ballast")
        h.event("ballast dropped")


@action("tablespace-full:SECS[:TABLE=order_view][:MB=auto]")
def tablespace_full(h, n, table="order_view", mb="auto"):
    """Move a target onto a small tmpfs tablespace (sudo mount), fill it for SECS, free it.

    `auto` sizes the tablespace at 1.3x the table plus 32 MB."""
    if mb == "auto":
        size = int(h.psql(f"select pg_total_relation_size('public.{table}'::regclass) / 1048576"))
        mb = str(int(size * 1.3) + 32)
    tbs = os.path.join(h.root, "tbs")
    os.makedirs(tbs, exist_ok=True)
    if not os.path.ismount(tbs):
        h.sudo(
            "mount", "-t", "tmpfs", "-o", f"size={int(mb)}m,uid={os.getuid()},gid={os.getgid()},mode=0700", "tmpfs", tbs
        )
    h.psql(f"create tablespace soak_small location '{tbs}'")
    h.psql(f"alter table public.{table} set tablespace soak_small")
    h.event(f"{table} moved to a {mb} MB tablespace")
    fill_and_hold(h, "soak_small", n, 2000)


@action("volume-full:SECS")
def volume_full(h, n):
    """Fill the data volume for SECS (needs `setup --volume-mb`; WAL lives elsewhere)."""
    if not h.cfg.get("volume_mb"):
        raise UsageError("volume-full needs a cluster set up with --volume-mb")
    fill_and_hold(h, "", n, 20000)


@action("handover:NEW_BIN[:MAX_SECS=300]")
def handover(h, new_bin, max_secs="300"):
    """Start a worker from NEW_BIN beside the running ones, kill -9 them, wait for it to take over."""
    old = [w for w in h.worker_names() if h.workers[w]["proc"].poll() is None]
    name = f"w{max(int(w[1:]) for w in h.workers) + 1}"
    h.start_worker(name, os.path.abspath(new_bin))
    time.sleep(30)
    for w in old:
        h.stop_worker(w, signal.SIGKILL)
    t0, log = time.time(), os.path.join(h.run_dir, f"{name}.log")
    while time.time() - t0 < secs(max_secs):
        if "taking over" in open(log, errors="replace").read():
            h.event(f"{name} took over after {time.time() - t0:.1f}s")
            return
        time.sleep(0.5)
    h.fail(f"{name} did not take over within {max_secs}s")


def parse_step(text):
    """'kill9:w1:3+long-txn:60' -> [(fn, ['w1', '3']), (fn, ['60'])]; raises UsageError."""
    parts = []
    for piece in text.split("+"):
        name, *args = piece.strip().split(":")
        if name not in ACTIONS:
            raise UsageError(f"unknown action {name!r} in step {text!r}; run `chaos.py actions`")
        fn = ACTIONS[name][0]
        nparams = fn.__code__.co_argcount - 1
        nrequired = nparams - len(fn.__defaults__ or ())
        if not nrequired <= len(args) <= nparams:
            raise UsageError(f"step {piece!r}: usage {ACTIONS[name][1]}")
        parts.append((fn, args))
    return parts


def run_step(h, text):
    parts = parse_step(text)
    h.event(f"step {text}")
    errors = []

    def one(fn, args):
        try:
            fn(h, *args)
        except Exception as exc:
            errors.append(f"{fn.__name__}: {exc}")

    threads = [threading.Thread(target=one, args=p) for p in parts]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise RuntimeError(f"step {text} failed: {'; '.join(errors)}")


# -- scenarios ---------------------------------------------------------------
# Load flags a scenario sets unless the command line does, then its steps.

SCENARIOS = {
    "smoke": ({"load_secs": 120, "tps": 1000}, ["sleep:60", "pg-restart:immediate"]),
    "kill-storm": ({"load_secs": 1200, "tps": 1500}, ["kill9-storm:1150"]),
    "pg-faults": (
        {"load_secs": 1500, "tps": 1000},
        [
            "sleep:120",
            "pg-restart:immediate",
            "sleep:180",
            "terminate-backends:5:10",
            "sleep:120",
            "blackhole:90",
            "sleep:150",
            "exhaust-conns:120",
            "sleep:120",
            "pg-outage:60",
        ],
    ),
    "outages": (
        {"load_secs": 2100, "tps": 1000, "workers": 1},
        ["sleep:120", "worker-outage:900", "wait-caught-up:1500", "sleep:60", "long-txn:300", "wait-caught-up:900"],
    ),
    "backup": ({"load_secs": 540, "tps": 150, "threads": 3, "workers": 1}, ["sleep:60", "pg-dump:240"]),
    "tablespace-full": (
        {"load_secs": 420, "tps": 200, "threads": 4, "workers": 1},
        ["sleep:30", "tablespace-full:120"],
    ),
    "soak": ({"load_secs": 10800, "tps": 1000}, []),
}


def logbook_path():
    """The main checkout's logbook script (worktrees share its git dir)."""
    if os.environ.get("TRELLIS_LOGBOOK_SCRIPT"):
        return os.environ["TRELLIS_LOGBOOK_SCRIPT"]
    common = subprocess.run(
        ["git", "-C", REPO, "rev-parse", "--path-format=absolute", "--git-common-dir"], capture_output=True, text=True
    ).stdout.strip()
    return os.path.join(os.path.dirname(common), ".claude", "scripts", "logbook")


# -- commands ----------------------------------------------------------------


def build_cli():
    subprocess.run(["cargo", "build", "--release", "-p", "trellis-cli", "--bin", "trellis"], cwd=REPO, check=True)


def cmd_setup(args):
    h = Harness(args.dir, args.port, args.bin)
    if h.cfg and not args.force:
        raise UsageError(f"{args.dir} is already set up; pass --force to rebuild it")
    if not args.bin:
        build_cli()
    h.stop_all()
    for d in ("data", "data.base", "wal", "wal.base", "vol/data"):
        p = os.path.join(args.dir, d)
        if os.path.lexists(p):
            shutil.rmtree(p)
    vol = os.path.join(args.dir, "vol")
    if args.volume_mb:
        os.makedirs(vol, exist_ok=True)
        if not os.path.ismount(vol):
            h.sudo(
                "mount",
                "-t",
                "tmpfs",
                "-o",
                f"size={args.volume_mb}m,uid={os.getuid()},gid={os.getgid()},mode=0700",
                "tmpfs",
                vol,
            )
    h.cfg = {
        "accounts": args.accounts,
        "orders": args.orders,
        "volume_mb": args.volume_mb,
        "bin": os.path.abspath(h.bin),
        "port": args.port,
    }
    initdb = ["initdb", "-U", ADMIN, "-A", "trust", "-D", h.data]
    if args.volume_mb:
        initdb += [f"--waldir={os.path.join(h.wal, 'pg_wal')}"]
    h.sh(initdb, capture_output=True)
    with open(os.path.join(h.data, "postgresql.conf"), "a") as f:
        f.write(f"""
port = {args.port}
unix_socket_directories = '{h.sock}'
listen_addresses = '127.0.0.1'
max_connections = 120
shared_buffers = {"64MB" if args.volume_mb else "512MB"}
max_wal_size = 4GB
logging_collector = on
log_directory = '{os.path.join(args.dir, "pglog")}'
log_filename = 'pg.log'
log_line_prefix = '%m [%p] %u %a '
log_min_duration_statement = 5000
log_lock_waits = on
""")
    h.pg_start()
    h.psql(f"create role {ROLE} login nosuperuser", db="postgres")
    h.psql(f"create role {APP} login nosuperuser", db="postgres")
    h.psql(f"create database {DB} owner {ROLE}", db="postgres")
    h.psql(
        file=os.path.join(HERE, "schema.sql"),
        user=ROLE,
        variables=(f"accounts={args.accounts}", f"orders={args.orders}"),
    )
    print(f"seeded {args.accounts} accounts and {args.orders} orders", flush=True)
    h.memory_max, h.drain_threads = args.memory_max, 4
    h.start_worker("w1")
    defs = [d for d in open(os.path.join(HERE, "defs.txt")).read().splitlines() if d.strip()]
    # The last definition reads acct_order_totals' target, so it waits for the rest to go live.
    for batch in (defs[:-1], defs[-1:]):
        for d in batch:
            h.sh([h.bin, "apply", "-d", h.dsn(), d], capture_output=True)
        deadline = time.time() + args.build_secs
        while True:
            states = definition_states(h)
            if len(states) >= len(batch) and all(s == "live" for s in states.values()):
                break
            if time.time() > deadline:
                raise RuntimeError(f"definitions not live after {args.build_secs}s: {states}")
            time.sleep(2)
    for _ in range(30):  # live can be reported a moment before the last page lands
        bad = {k: v for k, v in (oracle(h) or {"oracle": -1}).items() if v}
        if not bad:
            break
        time.sleep(2)
    else:
        raise RuntimeError(f"oracle differs right after setup: {bad}")
    h.stop_all()
    replace_tree(h.data, os.path.join(args.dir, "data.base"))
    if args.volume_mb:
        replace_tree(h.wal, os.path.join(args.dir, "wal.base"))
    json.dump(h.cfg, open(h.cfg_path, "w"), indent=1)
    print(f"base snapshot ready in {args.dir}", flush=True)


def cmd_stop(args):
    Harness(args.dir, args.port).stop_all()


def cmd_actions(_args):
    for _name, (_, usage, doc) in ACTIONS.items():
        print(f"  {usage:48} {doc.splitlines()[0] if doc else ''}")
    print("\nscenarios:")
    for name, (load, steps) in SCENARIOS.items():
        print(f"  {name:16} {' '.join(f'{k}={v}' for k, v in load.items())}: {' '.join(steps) or '(load only)'}")


def cmd_run(args):
    if bool(args.scenario) == bool(args.step):
        raise UsageError("give --scenario or --step, not both or neither")
    load, steps = SCENARIOS[args.scenario] if args.scenario else ({}, args.step)
    for k, v in load.items():
        if getattr(args, k) is None:
            setattr(args, k, v)
    for k, v in {"load_secs": 120, "tps": 1000, "threads": 12, "workers": 2}.items():
        if getattr(args, k) is None:
            setattr(args, k, v)
    for s in steps:
        parse_step(s)  # usage errors before anything starts
    if not os.path.exists(os.path.join(args.dir, "setup.json")):
        raise UsageError(f"{args.dir} isn't set up; run `chaos.py setup` first")
    if not args.bin:
        build_cli()

    label = args.scenario or "custom"
    run_dir = os.path.join(args.dir, "logs", time.strftime("%Y%m%dT%H%M%S") + "-" + label)
    h = Harness(args.dir, args.port, args.bin, run_dir)
    h.memory_max, h.drain_threads = args.memory_max, args.drain_threads
    cmdline = "soak/chaos.py run " + (
        f"--scenario {label}" if args.scenario else " ".join(f"--step {s}" for s in steps)
    )
    cmdline += f" load={args.load_secs}s@{args.tps:g}tps workers={args.workers}"
    entry = None
    if not args.no_logbook:
        expect = int(args.load_secs + args.converge_secs / 3 + 300)
        r = subprocess.run(
            [
                logbook_path(),
                "reserve",
                "--kind",
                args.kind,
                "--lane",
                args.lane,
                "--issue",
                str(args.issue),
                "--expect",
                f"{expect}s",
                "--cmd",
                cmdline,
                "--wait",
            ],
            capture_output=True,
            text=True,
        )
        if r.returncode:
            raise RuntimeError(f"logbook reserve failed: {r.stderr.strip()}")
        entry = r.stdout.strip().splitlines()[-1]
        h.event(f"logbook entry {entry}")
    h.event(f"run {cmdline}; logs in {run_dir}")
    outcome, rows, note = "aborted", 0, "harness error"
    stop_mon = threading.Event()
    try:
        h.restore()
        h.pg_start()
        for i in range(1, args.workers + 1):
            h.start_worker(f"w{i}")
        mon = threading.Thread(target=monitor, args=(h, os.path.join(run_dir, "monitor.tsv"), stop_mon), daemon=True)
        mon.start()
        time.sleep(5)
        load_out = os.path.join(run_dir, "load.log")
        lg = subprocess.Popen(
            [
                sys.executable,
                os.path.join(HERE, "loadgen.py"),
                "--dsn",
                h.dsn(),
                "--threads",
                str(args.threads),
                "--tps",
                str(args.tps),
                "--secs",
                str(args.load_secs),
                "--accounts",
                str(h.cfg["accounts"]),
                "--orders",
                str(h.cfg["orders"]),
                "--mix",
                args.mix,
            ],
            stdout=open(load_out, "w"),
            stderr=subprocess.STDOUT,
        )
        h.write_pid("loadgen", lg.pid)
        h.event(f"load: {args.threads} threads at {args.tps:g} tps for {args.load_secs}s")
        for s in steps:
            run_step(h, s)
        lg.wait()
        last = open(load_out).read().strip().splitlines()[-1:] or [""]
        try:
            totals = json.loads(last[0])
        except ValueError:
            raise RuntimeError(f"the load generator didn't report totals; see {load_out}") from None
        rows = totals["rows"]
        h.event(f"load done: {totals['ok']} statements, {totals['err']} errors, {rows} rows")

        t0, diffs = time.time(), None
        while True:
            h.check_workers()
            diffs = oracle(h)
            if diffs is not None and not any(diffs.values()):
                break
            if time.time() - t0 > args.converge_secs:
                break
            time.sleep(10)
        converged = diffs is not None and not any(diffs.values())
        h.event(f"oracle {'converged' if converged else 'did NOT converge'} after {time.time() - t0:.0f}s: {diffs}")
        if not converged:
            h.fail(f"oracle differs after {args.converge_secs}s: {diffs}")
        states = definition_states(h)
        not_live = {k: v for k, v in states.items() if v != "live"}
        if not_live:
            h.fail(f"definitions not live at the end: {not_live}")
        if args.max_rss_mb and h.peak_rss_kb / 1024 > args.max_rss_mb:
            h.fail(f"peak worker RSS {h.peak_rss_kb / 1024:.0f} MB > {args.max_rss_mb} MB")
        if args.max_age_secs and h.peak_age > args.max_age_secs:
            h.fail(f"peak oldest-undrained age {h.peak_age:.0f}s > {args.max_age_secs}s")
        status = h.sh([h.bin, "status", "-d", h.dsn()], check=False, capture_output=True)
        open(os.path.join(run_dir, "status.txt"), "w").write(status.stdout + status.stderr)
        oracle_failed = not converged
        outcome = "ok" if not h.failures else ("fail" if oracle_failed else "miss")
        note = (
            f"{label}: {'oracle 0 diffs' if converged else 'ORACLE DIFF'} after {time.time() - t0:.0f}s; "
            f"peak RSS {h.peak_rss_kb / 1024:.0f} MB, peak age {h.peak_age:.0f}s; load err {totals['err']}"
            + (f"; failures: {'; '.join(h.failures)}" if h.failures else "")
        )
    except UsageError:
        raise
    except BaseException as exc:
        h.event(f"aborted: {type(exc).__name__}: {exc}")
        note = f"{label}: aborted: {exc}"
        raise
    finally:
        stop_mon.set()
        try:
            h.stop_all()
        finally:
            if entry:
                subprocess.run(
                    [logbook_path(), "finish", entry, "--rows", str(rows), "--outcome", outcome, "--note", note[:400]]
                )
            h.event(f"outcome {outcome}: {note}")
    return 0 if outcome == "ok" else 1


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--dir",
        default=os.environ.get("TRELLIS_SOAK_DIR", os.path.join(REPO, "target", "soak")),
        help="cluster directory (default target/soak, or $TRELLIS_SOAK_DIR)",
    )
    ap.add_argument("--port", type=int, default=56701, help="Postgres port (default 56701)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("setup", help="build, seed and snapshot the cluster")
    s.add_argument("--accounts", type=int, default=100000)
    s.add_argument("--orders", type=int, default=1000000)
    s.add_argument(
        "--volume-mb", type=int, default=0, help="put the data directory on a tmpfs this size (for volume-full)"
    )
    s.add_argument("--bin", help="trellis binary (default: build target/release/trellis)")
    s.add_argument("--memory-max", default="6G")
    s.add_argument("--build-secs", type=int, default=900, help="how long the definitions may take to go live")
    s.add_argument("--force", action="store_true", help="rebuild an existing cluster")

    r = sub.add_parser("run", help="run a scenario against the base snapshot")
    r.add_argument("--scenario", choices=sorted(SCENARIOS))
    r.add_argument("--step", action="append", default=[], help="ACTION[:ARG...][+ACTION...], repeatable, run in order")
    r.add_argument("--load-secs", type=int)
    r.add_argument("--tps", type=float)
    r.add_argument("--threads", type=int)
    r.add_argument("--mix", default="", help="loadgen op weights, e.g. insert_order=50,delete_account=0")
    r.add_argument("--workers", type=int)
    r.add_argument("--drain-threads", type=int, default=4)
    r.add_argument("--memory-max", default="6G", help="systemd-run MemoryMax per worker")
    r.add_argument("--bin", help="trellis binary for the workers (default: build target/release/trellis)")
    r.add_argument("--converge-secs", type=int, default=900, help="how long the oracle may take to reach 0 diffs")
    r.add_argument("--max-rss-mb", type=float, help="fail if the workers' summed RSS ever exceeds this")
    r.add_argument("--max-age-secs", type=float, help="fail if the oldest undrained change ever gets this old")
    r.add_argument("--no-logbook", action="store_true", help="don't reserve a logbook entry (short trial runs only)")
    r.add_argument("--kind", default="soak", choices=["soak", "repro", "acceptance"])
    r.add_argument("--lane", default=os.environ.get("TRELLIS_LANE", "x"))
    r.add_argument("--issue", type=int, default=0)

    sub.add_parser("stop", help="stop the workers, the load and Postgres")
    sub.add_parser("actions", help="list fault actions and scenarios")
    args = ap.parse_args(argv)
    args.dir = os.path.abspath(args.dir)
    try:
        return {"setup": cmd_setup, "run": cmd_run, "stop": cmd_stop, "actions": cmd_actions}[args.cmd](args) or 0
    except UsageError as exc:
        print(f"chaos.py: {exc}", file=sys.stderr)
        return 64
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"chaos.py: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
