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
import datetime
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.request

import psycopg2

import loadgen

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
ADMIN = os.environ.get("USER", "postgres")  # the cluster's superuser: harness plumbing only
# The role Trellis runs as: not a superuser, and not named after Trellis's schema (`trellis`),
# which the default search_path would resolve first. It inherits the application role's
# ownership of the database and the sources, as docs/recommendations.md sets it up.
ROLE = "trellis_runner"
APP = "app"  # owns the database and the sources; the load runs as it
DB = "shop"
METRICS_BASE = 59500


class UsageError(Exception):
    pass


class Aborted(Exception):
    """The run is being torn down: an action must stop and start nothing new."""


class Harness:
    """Paths, processes and the event log for one cluster directory."""

    def __init__(self, root, port, bin_path=None, run_dir=None):
        self.root = root
        self.sock = os.path.join(root, "s")
        self.pids = os.path.join(root, "pids")
        self.cfg_path = os.path.join(root, "setup.json")
        self.cfg = {}
        if os.path.exists(self.cfg_path):
            with open(self.cfg_path) as f:
                self.cfg = json.load(f)
        self.port = self.cfg.get("port", port)  # a set-up cluster listens where setup put it
        self.bin = os.path.abspath(
            bin_path or self.cfg.get("bin") or os.path.join(REPO, "target", "release", "trellis")
        )
        self.run_dir = run_dir or os.path.join(root, "logs")
        self.workers = {}  # name -> {"proc", "bin", "expected_down"}
        self.lock = threading.Lock()
        self.failures = []
        self.cleanups = []  # callables run once, after Postgres stops
        self.aborting = threading.Event()
        self.timeline = None  # (start epoch, loadgen segments) once the load starts
        self.samples = []  # monitor rows as dicts, with "epoch"
        self.probes = []  # {"epoch", "segment", "tag", "latency", "error"}
        self.step_log = []  # (epoch, step text)
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

    @property
    def needle(self):
        """In the command line of every worker and load generator this harness starts, and
        specific to its port: a pid is signalled only if its command line contains it."""
        return f" port={self.port} user="

    def sleep(self, n):
        """Sleep for an action; raises Aborted as soon as the run is torn down."""
        if self.aborting.wait(n):
            raise Aborted("the run is being torn down")

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

        Processes are only ever signalled through this, with `needle` = `self.needle`:
        `pkill -f` patterns match the harness's own command line, and a recycled pid (a
        stale pid file's) must not be killed."""
        try:
            with open(self.pidfile(name)) as f:
                pid = int(f.read().strip())
            with open(f"/proc/{pid}/cmdline") as f:
                cmdline = f.read().replace("\0", " ")
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
        if self.aborting.is_set():
            raise Aborted(f"not starting {name}: the run is being torn down")
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
        if self.signal_pidfile(name, self.needle, sig):
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

    def segment_at(self, t):
        """(index, profile) of the load schedule's segment at epoch t, or None outside it."""
        if not self.timeline:
            return None
        start, segments = self.timeline
        for i, (profile, secs, _) in enumerate(segments):
            if start <= t < start + secs:
                return i, profile
            start += secs
        return None

    def metrics(self, name):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{METRICS_BASE + int(name[1:])}/metrics", timeout=2) as r:
                return r.read().decode()
        except Exception:
            return ""

    # -- teardown --------------------------------------------------------
    def nft_table(self):
        return f"trellis_soak_{self.port}"

    def stop_all(self):
        """Stop the load and every worker by pid file, then Postgres, then undo host changes.

        Workers stop before Postgres and before any restore, or they keep writing into
        (or reconnecting to) the cluster being replaced. The host changes (the blackhole's
        nft table, the tablespace mount) are undone even when a step before fails, and
        even when an earlier invocation was killed before it could undo them."""
        self.aborting.set()
        errors = []

        def attempt(what, fn):
            try:
                fn()
            except Exception as exc:
                errors.append(f"{what}: {exc}")
                self.event(f"teardown: {what} failed: {exc}")

        attempt("stop the load", lambda: self.signal_pidfile("loadgen", self.needle, signal.SIGTERM, wait=5))
        names = set(self.workers) | {f[:-4] for f in os.listdir(self.pids) if f.startswith("w") and f.endswith(".pid")}
        for name in sorted(names, key=lambda n: int(n[1:]) if n[1:].isdigit() else 0):
            attempt(f"stop {name}", lambda name=name: self.stop_worker(name))
        attempt("stop Postgres", self.pg_stop)
        for fn in self.cleanups:
            attempt("cleanup", fn)
        self.cleanups.clear()
        if (
            shutil.which("nft")
            and subprocess.run(
                ["sudo", "-n", "nft", "list", "table", "inet", self.nft_table()], capture_output=True
            ).returncode
            == 0
        ):
            attempt(
                "delete the blackhole's nft table",
                lambda: self.sudo("nft", "delete", "table", "inet", self.nft_table()),
            )
        tbs = os.path.join(self.root, "tbs")
        if os.path.ismount(tbs):
            attempt("unmount the tablespace", lambda: self.sudo("umount", tbs))
        if errors:
            raise RuntimeError("teardown incomplete: " + "; ".join(errors))

    def restore(self):
        self.stop_all()
        for name in os.listdir(self.pids):
            if name.startswith("w") and self.live_pid(name[:-4], self.needle):
                raise RuntimeError(f"worker {name[:-4]} is still running; not restoring under it")
        replace_tree(os.path.join(self.root, "data.base"), self.data)
        if self.cfg.get("volume_mb"):
            replace_tree(os.path.join(self.root, "wal.base"), self.wal)
        self.event("restored the base snapshot")
        self.aborting.clear()


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
    "wal_bytes",
    "xmin_age",
    "xmin_hold_s",
    "idle_in_txn",
    "profile",
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
        f"(select count(*) from pg_stat_activity where usename = '{ROLE}'), "
        "pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint, "
        # How far Trellis's sessions hold back the vacuum horizon, in xids and in seconds.
        f"(select coalesce(max(age(backend_xmin)), 0) from pg_stat_activity where usename = '{ROLE}'), "
        "(select coalesce(round(extract(epoch from max(now() - xact_start))), 0)::bigint from pg_stat_activity "
        f"where usename = '{ROLE}' and backend_xmin is not null), "
        "(select count(*) from pg_stat_activity "
        f"where usename = '{ROLE}' and state like 'idle in transaction%')"
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
            now = time.time()
            try:
                schema_mb, target_mb, conns, wal, xmin_age, xmin_hold, idle_txn = h.psql(sizes, check=True).split("|")
            except Exception:
                schema_mb = target_mb = conns = wal = xmin_age = xmin_hold = idle_txn = "NA"
            seg = h.segment_at(now)
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
                wal,
                xmin_age,
                xmin_hold,
                idle_txn,
                seg[1] if seg else "-",
            ]
            f.write("\t".join(row) + "\n")
            f.flush()
            with h.lock:
                h.samples.append(dict(zip(MON_COLUMNS, row, strict=True), epoch=now, rss_kb=rss))
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


# -- write probes --------------------------------------------------------------
# A probe upserts one account outside the load's id range with a fresh name and times how
# long the name takes to reach acct_view after the commit. The prober probes every
# --probe-secs during steady and spike segments, every 10x that during a trickle, and once
# a second before each idle segment ends: the first write after the idle.


def probe_id(h):
    return 2 * h.cfg["accounts"] + 1  # loadgen's ids stop at 1.2x the seed


def probe(h, tag, timeout=300.0):
    t0 = time.time()
    seg = h.segment_at(t0)
    rec = {"epoch": t0, "segment": seg[0] if seg else None, "tag": tag, "latency": None, "error": None}
    name = f"probe {t0:.3f}"
    try:
        deadline = t0 + timeout
        while True:  # Postgres may be restarting: retry the write until the deadline
            try:
                conn = psycopg2.connect(h.dsn(user=APP), connect_timeout=5, options="-c statement_timeout=30s")
                break
            except psycopg2.OperationalError:
                if time.time() > deadline:
                    raise
                h.sleep(1)
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    "insert into accounts(id, region, name, balance) values (%s, 1, %s, 0)"
                    " on conflict (id) do update set name = excluded.name, active = true",
                    (probe_id(h), name),
                )
        finally:
            conn.close()
        committed = time.time()
        reader = psycopg2.connect(h.dsn(user=ADMIN), connect_timeout=5, options="-c statement_timeout=30s")
        try:
            reader.autocommit = True
            with reader.cursor() as cur:
                while True:
                    cur.execute("select 1 from public.acct_view where id = %s and uname = %s", (probe_id(h), name))
                    if cur.fetchone():
                        rec["latency"] = time.time() - committed
                        break
                    if time.time() > deadline:
                        raise TimeoutError(f"not in acct_view after {timeout:.0f}s")
                    h.sleep(0.05)
        finally:
            reader.close()
    except Aborted:
        raise
    except Exception as exc:
        rec["error"] = f"{type(exc).__name__}: {str(exc).strip()[:120]}"
        h.event(f"probe ({tag}) failed: {rec['error']}")
    with h.lock:
        h.probes.append(rec)
    return rec


def prober(h, every):
    """Probes along the schedule in h.timeline until it ends or the run is torn down."""
    start, segments = h.timeline
    try:
        for profile, secs, _ in segments:
            end = start + secs
            if profile == "idle":
                h.sleep(max(0.0, end - 1 - time.time()))
                rec = probe(h, "after-idle")
                if rec["latency"] is not None:
                    h.event(f"first write after {secs:.0f}s idle: {rec['latency']:.3f}s")
            else:
                interval = every * (10 if profile == "trickle" else 1)
                t = start + min(5.0, secs / 2)
                while t < end - 1:
                    h.sleep(max(0.0, t - time.time()))
                    probe(h, profile)
                    t = max(t + interval, time.time())
            start = end
    except Aborted:
        pass


# -- operational metrics ---------------------------------------------------------
# Recorded for step 10b's thresholds; nothing here fails a run yet.

LOG_LINE = re.compile(r"^(\d{4}-\d\d-\d\dT[\d:.]+Z)\s+(WARN|ERROR)\s+(.*)$")
LOG_FIELD = re.compile(r"(\w+)=(\"[^\"]*\"|\S+)")


LOG_KEEP_FIELDS = {"error", "err", "reason", "cause"}  # what tells two failures apart


def log_template(text):
    """A log message with numbers and most field values blanked, to group repeats. Error
    fields and T-codes stay, so failures that differ only in their cause stay apart."""
    text = LOG_FIELD.sub(lambda m: m.group(0) if m.group(1) in LOG_KEEP_FIELDS else f"{m.group(1)}=…", text)
    return re.sub(r"\b(T\d{4})\b|\d+", lambda m: m.group(1) or "N", text).strip()[:400]


def parse_log_levels(lines):
    """Tracing lines -> [(epoch, level, template)] for WARN and ERROR."""
    out = []
    for line in lines:
        m = LOG_LINE.match(line)
        if m:
            ts = datetime.datetime.fromisoformat(m.group(1).replace("Z", "+00:00")).timestamp()
            out.append((ts, m.group(2), log_template(m.group(3))))
    return out


def percentile(values, q):
    v = sorted(values)
    return v[min(len(v) - 1, int(q * len(v)))] if v else None


def num(sample, key):
    try:
        return float(sample[key])
    except (KeyError, TypeError, ValueError):
        return None


def summarize(start, segments, loadgen_segments, samples, probes, log_lines, steps):
    """Per-segment operational metrics, plus first-write latency after idle against steady."""
    out = {"segments": [], "after_idle": []}
    steady = [p["latency"] for p in probes if p["tag"] == "steady" and p["latency"] is not None]
    out["steady_latency_p50"] = percentile(steady, 0.5)
    out["steady_latency_p95"] = percentile(steady, 0.95)
    t = start
    for i, (profile, secs, tps) in enumerate(segments):
        s, e = t, t + secs
        t = e
        within = [x for x in samples if s <= x["epoch"] < e]
        seg = {"segment": i + 1, "profile": profile, "start_offset": s - start, "secs": secs, "tps": tps}
        if i < len(loadgen_segments):
            seg["ok"], seg["err"] = loadgen_segments[i][1], loadgen_segments[i][2]
        seg["steps"] = [text for at, text in steps if s <= at < e]
        for key, col in (("xmin_age_max", "xmin_age"), ("xmin_hold_s_max", "xmin_hold_s")):
            vals = [v for v in (num(x, col) for x in within) if v is not None]
            seg[key] = max(vals) if vals else None
        settle = s + min(120.0, secs / 4)  # the drain of what the load before left, and autovacuum after it
        vals = [v for v in (num(x, "xmin_hold_s") for x in within if x["epoch"] >= settle) if v is not None]
        seg["xmin_hold_s_settled_max"] = max(vals) if vals else None
        vals = [v for v in (num(x, "idle_in_txn") for x in within) if v is not None]
        seg["idle_in_txn_max"] = max(vals) if vals else None
        if within:
            seg["rss_mb_end"] = round(within[-1]["rss_kb"] / 1024)
            seg["trellis_conns_end"] = num(within[-1], "trellis_conns")
        # WAL: the whole segment, and its rate after a settling time (the drain of what the
        # load before it left, and the autovacuum after it).
        wal = [(x["epoch"], num(x, "wal_bytes")) for x in within if num(x, "wal_bytes") is not None]
        if len(wal) >= 2:
            seg["wal_bytes"] = wal[-1][1] - wal[0][1]
            settled = [w for w in wal if settle <= w[0] <= e - 2]
            if len(settled) >= 2 and settled[-1][0] - settled[0][0] >= 30:
                span = settled[-1][0] - settled[0][0]
                seg["wal_bytes_per_hour_settled"] = round((settled[-1][1] - settled[0][1]) / span * 3600)
        logs = [(lvl, tpl) for at, lvl, tpl in log_lines if s <= at < e]
        for lvl in ("WARN", "ERROR"):
            counts = {}
            for level, tpl in logs:
                if level == lvl:
                    counts[tpl] = counts.get(tpl, 0) + 1
            seg[lvl.lower()] = dict(sorted(counts.items(), key=lambda kv: -kv[1]))
        if profile == "idle":
            seg["warn_per_hour"] = round(sum(seg["warn"].values()) / secs * 3600, 1)
        lat = [p["latency"] for p in probes if p["segment"] == i and p["latency"] is not None and p["tag"] == profile]
        if lat:
            seg["probe_latency_p50"], seg["probe_latency_max"] = percentile(lat, 0.5), max(lat)
        out["segments"].append(seg)
        for p in probes:
            if p["segment"] == i and p["tag"] == "after-idle":
                ratio = None
                if p["latency"] is not None and out["steady_latency_p50"]:
                    ratio = round(p["latency"] / out["steady_latency_p50"], 1)
                out["after_idle"].append(
                    {"segment": i + 1, "idle_secs": secs, "latency": p["latency"], "ratio": ratio, "error": p["error"]}
                )
    return out


def metrics_lines(m):
    """A few human lines for the event log, and a compact one for the logbook note."""

    def fmt(v, unit="s"):
        return "NA" if v is None else f"{v:.3f}{unit}"

    lines = [f"steady write->target latency p50 {fmt(m['steady_latency_p50'])}, p95 {fmt(m['steady_latency_p95'])}"]
    for a in m["after_idle"]:
        lines.append(
            f"first write after {a['idle_secs']:.0f}s idle (segment {a['segment']}): {fmt(a['latency'])}"
            + (f" = {a['ratio']}x steady" if a["ratio"] is not None else "")
            + (f" ({a['error']})" if a["error"] else "")
        )
    for seg in m["segments"]:
        bits = [f"segment {seg['segment']} {seg['profile']} {seg['secs']:.0f}s"]
        if "ok" in seg:
            bits.append(f"{seg['ok']} writes, {seg['ok'] / seg['secs']:.4g}/s (planned {seg['tps']:.4g}/s)")
        if "probe_latency_p50" in seg:
            bits.append(f"latency p50 {fmt(seg['probe_latency_p50'])}")
        if "wal_bytes_per_hour_settled" in seg:
            bits.append(f"WAL {seg['wal_bytes_per_hour_settled'] / 1048576:.2f} MB/h settled")
        bits.append(f"xmin hold max {seg['xmin_hold_s_max']}s / {seg['xmin_age_max']} xids")
        if seg["profile"] == "idle":
            bits.append(f"WARN {seg['warn_per_hour']}/h")
        if seg["steps"]:
            bits.append(f"steps {', '.join(seg['steps'])}")
        lines.append("; ".join(bits))
        for tpl, n in list(seg["warn"].items())[:5] + list(seg["error"].items())[:5]:
            lines.append(f"    {n} x {tpl}")
    idle = [s for s in m["segments"] if s["profile"] == "idle"]
    wal = [s["wal_bytes_per_hour_settled"] for s in idle if "wal_bytes_per_hour_settled" in s]
    ratios = [a["ratio"] for a in m["after_idle"] if a["ratio"] is not None]
    holds = [s["xmin_hold_s_max"] for s in m["segments"] if s["xmin_hold_s_max"] is not None]
    short = (
        f"steady p50 {fmt(m['steady_latency_p50'])}"
        + (f"; after-idle max {max(ratios)}x" if ratios else "")
        + (f"; idle WAL max {max(wal) / 1048576:.2f} MB/h" if wal else "")
        + (f"; xmin hold max {max(holds):.0f}s" if holds else "")
        + (f"; idle WARN max {max(s['warn_per_hour'] for s in idle)}/h" if idle else "")
    )
    return lines, short


# -- profile gates (#2189, 10b) -------------------------------------------------
# Thresholds signed off on the issue (2026-11-21) at about 2x the 10a baselines.

GATE_AFTER_IDLE_RATIO = 2.0  # first write after an idle, as a multiple of steady p50
GATE_IDLE_WAL_MB_PER_HOUR = 6.5  # settled WAL rate of an idle segment
GATE_XMIN_HOLD_SECS = 2.0  # settled backend_xmin hold of Trellis's sessions
# Steps that hold the vacuum horizon back on purpose; a segment with one is exempt from the hold gate.
HOLDS_XMIN_STEPS = ("long-txn", "pg-dump")


def check_gates(m):
    """Breaches of the profile thresholds in a metrics dict, as messages naming the metric and
    segment. Empty for a run with no idle segment: those aren't profile runs. WARN lines in an
    idle segment that had a fault injected aren't gated."""
    segs = m["segments"]
    if not any(s["profile"] == "idle" for s in segs):
        return []
    out = []
    for a in m["after_idle"]:
        if a["ratio"] is not None and a["ratio"] > GATE_AFTER_IDLE_RATIO:
            out.append(
                f"first write after idle (segment {a['segment']}) took {a['ratio']}x steady p50,"
                f" over {GATE_AFTER_IDLE_RATIO}x"
            )
        elif a["latency"] is None:
            out.append(f"first write after idle (segment {a['segment']}) never reached its target: {a['error']}")
    for seg in segs:
        n, profile = seg["segment"], seg["profile"]
        wal = seg.get("wal_bytes_per_hour_settled")
        if profile == "idle" and wal is not None and wal / 1048576 > GATE_IDLE_WAL_MB_PER_HOUR:
            out.append(
                f"idle WAL (segment {n}) {wal / 1048576:.2f} MB/h settled, over {GATE_IDLE_WAL_MB_PER_HOUR} MB/h"
            )
        hold = seg.get("xmin_hold_s_settled_max")
        deliberate = any(step.startswith(HOLDS_XMIN_STEPS) for step in seg["steps"])
        if hold is not None and hold > GATE_XMIN_HOLD_SECS and not deliberate:
            out.append(
                f"backend_xmin hold ({profile} segment {n}) {hold:.0f}s settled, over {GATE_XMIN_HOLD_SECS:.0f}s"
            )
        if profile == "idle" and not seg["steps"] and seg["warn"]:
            first = next(iter(seg["warn"]))
            out.append(f"WARN in a quiet idle (segment {n}): {sum(seg['warn'].values())} lines, e.g. {first}")
    return out


def record_metrics(h, start, segments, loadgen_segments):
    """Write metrics.json and log the summary. Returns (the logbook's short form, gate breaches).
    A failure to compute them is logged and doesn't fail the run."""
    try:
        log_lines = []
        for name in h.worker_names():
            with open(os.path.join(h.run_dir, f"{name}.log"), errors="replace") as f:
                log_lines += parse_log_levels(f)
        steps = [(at, t) for at, t in h.step_log if not t.startswith(("until:", "sleep:"))]
        m = summarize(start, segments, loadgen_segments, h.samples, h.probes, log_lines, steps)
        with open(os.path.join(h.run_dir, "metrics.json"), "w") as f:
            json.dump(m, f, indent=1)
        lines, short = metrics_lines(m)
        for line in lines:
            h.event(f"metric: {line}")
        return short, check_gates(m)
    except Exception as exc:
        h.event(f"metrics not recorded: {type(exc).__name__}: {exc}")
        return "metrics not recorded", []


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
    h.sleep(secs(n))


@action("until:SEGMENT[:OFFSET_SECS=0]")
def until(h, segment, offset="0"):
    """Wait until OFFSET s into load segment SEGMENT (from 1); a negative OFFSET counts back from its end."""
    if not h.timeline:
        raise RuntimeError("until: the load hasn't started")
    start, segments = h.timeline
    i, off = int(segment), float(offset)
    if not 1 <= i <= len(segments):
        raise UsageError(f"until: segment {segment} of {len(segments)}")
    at = start + sum(secs for _, secs, _ in segments[: i - 1]) + (off if off >= 0 else segments[i - 1][1] + off)
    if time.time() > at:
        h.event(f"until:{segment}:{offset} is already past by {time.time() - at:.0f}s")
    h.sleep(max(0.0, at - time.time()))


@action("kill9:[WORKER|any][:RESTART_AFTER_SECS=2]")
def kill9(h, which="any", after="2"):
    """kill -9 one worker, then start it again."""
    name = pick_worker(h, which)
    h.stop_worker(name, signal.SIGKILL)
    h.sleep(secs(after))
    h.start_worker(name)


@action("kill9-storm:SECS")
def kill9_storm(h, n):
    """For SECS: every 5-45 s kill -9 a random worker; one time in four, three fast start/kill cycles first."""
    rnd = __import__("random")
    end = time.time() + secs(n)
    while time.time() < end:
        h.sleep(min(5 + rnd.random() * 40, max(0, end - time.time())))
        if time.time() >= end:
            break
        name = pick_worker(h, "any")
        h.stop_worker(name, signal.SIGKILL)
        if rnd.random() < 0.25:
            for _ in range(3):
                h.start_worker(name)
                h.sleep(1 + rnd.random() * 2)
                h.stop_worker(name, signal.SIGKILL)
        h.sleep(rnd.random() * 5)
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
    h.sleep(secs(n))
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
            h.sleep(secs(interval))


@action("blackhole:SECS")
def blackhole(h, n):
    """Drop TCP to Postgres's port on lo for SECS (sudo nft). Only w1 connects over TCP."""
    table = h.nft_table()
    h.sudo("nft", "add", "table", "inet", table)

    def undo():
        subprocess.run(["sudo", "-n", "nft", "delete", "table", "inet", table], capture_output=True)

    h.cleanups.append(undo)
    try:
        h.sudo("nft", "add", "chain", "inet", table, "input", "{ type filter hook input priority -10; }")
        for d in ("dport", "sport"):
            h.sudo("nft", "add", "rule", "inet", table, "input", "iifname", "lo", "tcp", d, str(h.port), "drop")
        h.event(f"blackhole on for {n}s")
        h.sleep(secs(n))
    finally:
        undo()
        if undo in h.cleanups:  # stop_all may have run it already
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
        h.sleep(secs(n))
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
    h.sleep(secs(n))
    for w in names:
        h.start_worker(w)


@action("wait-caught-up:[MAX_SECS=900]:[AGE_SECS=10]")
def wait_caught_up(h, max_secs="900", age="10"):
    """Wait until the ring's oldest undrained change is younger than AGE_SECS; fail after MAX_SECS."""
    t0 = time.time()
    h.sleep(min(30, secs(max_secs)))  # let a freshly started worker publish its gauges
    while time.time() - t0 < secs(max_secs):
        ages = [metric_max(h.metrics(w), "trellis_ring_oldest_undrained_age_seconds") for w in h.worker_names()]
        ages = [a for a in ages if a is not None]
        if ages and max(ages) < secs(age):
            h.event(f"caught up after {time.time() - t0:.0f}s")
            return
        h.sleep(5)
    h.fail(f"not caught up (oldest undrained age >= {age}s) within {max_secs}s")


@action("long-txn:SECS")
def long_txn(h, n):
    """Hold an application transaction that wrote a row open for SECS, then commit."""
    conn = psycopg2.connect(h.dsn(user=APP))
    try:
        with conn.cursor() as cur:
            cur.execute("update accounts set balance = balance where id = 1")
        h.event(f"long transaction open for {n}s")
        h.sleep(secs(n))
        conn.commit()
        h.event("long transaction committed")
    finally:
        conn.close()


@action("pg-dump:SECS")
def pg_dump(h, n):
    """Run pg_dump into a reader that stalls for SECS, so it holds ACCESS SHARE on every table."""
    p = subprocess.Popen(["pg_dump", "-U", ADMIN, DB], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=h.env())
    h.event(f"pg_dump started, stalled for {n}s")
    h.sleep(secs(n))
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
        h.sleep(secs(n))
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
    h.sleep(30)
    for w in old:
        h.stop_worker(w, signal.SIGKILL)
    t0, log = time.time(), os.path.join(h.run_dir, f"{name}.log")
    while time.time() - t0 < secs(max_secs):
        if "taking over" in open(log, errors="replace").read():
            h.event(f"{name} took over after {time.time() - t0:.1f}s")
            return
        h.sleep(0.5)
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
    h.step_log.append((time.time(), text))
    errors = []

    def one(fn, args):
        try:
            fn(h, *args)
        except Exception as exc:
            errors.append(f"{fn.__name__}: {exc}")

    # Daemon threads: after Ctrl-C, stop_all sets h.aborting and the actions stop; nothing
    # may keep the interpreter alive starting workers after the teardown.
    threads = [threading.Thread(target=one, args=p, daemon=True) for p in parts]
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
    # Load profiles (#2189). Steps at an idle point address it with until:SEGMENT:OFFSET.
    "profiles-short": (
        {"schedule": "steady:90,trickle:120,idle:240,spike:60:x10,idle:60", "tps": 200, "threads": 16},
        ["until:3:60", "worker-outage:5", "until:3:150", "pg-restart:fast"],
    ),
    "daily": (
        {"schedule": "idle:600,trickle:600,steady:900,spike:60:x10,steady:840,trickle:600", "tps": 100},
        [],
    ),
    # The baselines for #2189's thresholds: a clean 2 h idle, the silence after a 100x spike,
    # a trickle, then an idle hour with a worker restart and a Postgres restart in it.
    "idle-baseline": (
        {
            "schedule": "steady:1200,idle:7200,spike:60:x100,idle:1800,trickle:3600,idle:3600,steady:600",
            "tps": 50,
            "threads": 64,  # 16 threads reached 1739 of 2000 tps in profiles-short's spike
        },
        ["until:6:1200", "worker-outage:5", "until:6:2400", "pg-restart:fast"],
    ),
}


def logbook_path():
    """The main checkout's logbook script (worktrees share its git dir)."""
    if os.environ.get("TRELLIS_LOGBOOK_SCRIPT"):
        return os.environ["TRELLIS_LOGBOOK_SCRIPT"]
    common = subprocess.run(
        ["git", "-C", REPO, "rev-parse", "--path-format=absolute", "--git-common-dir"], capture_output=True, text=True
    ).stdout.strip()
    return os.path.join(os.path.dirname(common), ".claude", "scripts", "logbook")


def logbook_reserve(h, args, cmdline, expect_secs):
    """Reserve the box (waiting while another run holds it); the entry id, or None."""
    if args.no_logbook:
        return None
    cmd = [logbook_path(), "reserve", "--kind", args.kind, "--lane", args.lane, "--issue", str(args.issue)]
    cmd += ["--expect", f"{int(expect_secs)}s", "--cmd", cmdline, "--wait"]
    r = subprocess.run(cmd, stdout=subprocess.PIPE, text=True)  # stderr shows who holds the box
    if r.returncode:
        raise RuntimeError(f"logbook reserve failed ({r.returncode})")
    entry = r.stdout.strip().splitlines()[-1]
    h.event(f"logbook entry {entry}")
    return entry


def logbook_finish(h, entry, rows, outcome, note):
    if entry:
        r = subprocess.run(
            [logbook_path(), "finish", entry, "--rows", str(rows), "--outcome", outcome, "--note", note[:400]]
        )
        if r.returncode:
            h.event(f"logbook finish {entry} FAILED ({r.returncode}): finish it by hand")
    h.event(f"outcome {outcome}: {note}")


def no_interrupts():
    """From here the teardown must complete: ignore Ctrl-C, SIGTERM and SIGHUP."""
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, signal.SIG_IGN)


# -- commands ----------------------------------------------------------------


def build_cli():
    subprocess.run(["cargo", "build", "--release", "-p", "trellis-cli", "--bin", "trellis"], cwd=REPO, check=True)


def cmd_setup(args):
    h = Harness(args.dir, args.port, args.bin)
    if h.cfg and not args.force:
        raise UsageError(f"{args.dir} is already set up; pass --force to rebuild it")
    if not args.bin:
        build_cli()
    cmdline = f"soak/chaos.py setup accounts={args.accounts} orders={args.orders} volume_mb={args.volume_mb}"
    entry = logbook_reserve(h, args, cmdline, args.build_secs + 600)
    outcome, note = "aborted", "setup: harness error"
    try:
        setup_cluster(h, args)
        outcome, note = "ok", f"setup: {args.accounts} accounts, {args.orders} orders, 6 transforms live, oracle 0"
    except BaseException as exc:
        note = f"setup: aborted: {type(exc).__name__}: {exc}"
        raise
    finally:
        no_interrupts()
        try:
            h.stop_all()
        finally:
            logbook_finish(h, entry, args.accounts + args.orders + 50, outcome, note)


def setup_cluster(h, args):
    h.stop_all()
    h.aborting.clear()
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
    h.port = args.port  # stop_all above used the old cluster's port
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
    # As docs/recommendations.md sets it up: the application role owns the database and the
    # sources, and Trellis's role inherits that ownership.
    h.psql(f"create role {APP} login nosuperuser", db="postgres")
    h.psql(f"create role {ROLE} login nosuperuser in role {APP}", db="postgres")
    h.psql(f"create database {DB} owner {APP}", db="postgres")
    h.psql(
        file=os.path.join(HERE, "schema.sql"),
        user=APP,
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
    targets = [d.split()[1] for d in defs]
    counts = h.psql(" union all ".join(f"select '{t}' || '|' || count(*) from public.{t}" for t in targets))
    empty = [t for t, n in parse_oracle(counts).items() if n == 0]
    if empty:  # an empty target makes its oracle line pass vacuously
        raise RuntimeError(f"targets empty after setup: {', '.join(empty)}; adjust the seed or defs.txt")
    h.stop_all()  # the cmd_setup teardown runs it again: it is idempotent
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
    if args.schedule and args.load_secs:
        raise UsageError("give --schedule or --load-secs, not both")
    explicit_secs = args.load_secs is not None
    for k, v in load.items():
        if getattr(args, k) is None and not (k == "schedule" and explicit_secs):
            setattr(args, k, v)
    if args.schedule:
        args.load_secs = None
    for k, v in {"tps": 1000, "threads": 12, "workers": 2}.items():
        if getattr(args, k) is None:
            setattr(args, k, v)
    if not args.schedule:
        args.schedule = f"steady:{args.load_secs or 120}"
    try:
        segments = loadgen.parse_schedule(args.schedule, args.tps)
    except ValueError as exc:
        raise UsageError(str(exc)) from None
    args.load_secs = int(sum(secs for _, secs, _ in segments))
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
    cmdline += f" load={args.schedule}@{args.tps:g}tps workers={args.workers}"
    h.event(f"run {cmdline}; logs in {run_dir}")
    outcome, rows, note = "aborted", 0, "harness error"
    stop_mon = threading.Event()
    entry = logbook_reserve(h, args, cmdline, args.load_secs + args.converge_secs / 3 + 300)
    try:
        h.restore()
        h.pg_start()
        for i in range(1, args.workers + 1):
            h.start_worker(f"w{i}")
        mon = threading.Thread(target=monitor, args=(h, os.path.join(run_dir, "monitor.tsv"), stop_mon), daemon=True)
        mon.start()
        time.sleep(5)
        start_at = time.time() + 2
        h.timeline = (start_at, segments)
        load_out = os.path.join(run_dir, "load.log")
        lg = subprocess.Popen(
            [
                sys.executable,
                os.path.join(HERE, "loadgen.py"),
                "--dsn",
                h.dsn(user=APP),
                "--threads",
                str(args.threads),
                "--tps",
                str(args.tps),
                "--schedule",
                args.schedule,
                "--start-at",
                repr(start_at),
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
        h.event(f"load: {args.threads} threads, {args.schedule} at {args.tps:g} tps ({args.load_secs}s)")
        probes = None
        if args.probe_secs > 0:
            probes = threading.Thread(target=prober, args=(h, args.probe_secs), daemon=True)
            probes.start()
        for s in steps:
            run_step(h, s)
        lg.wait()
        if probes is not None:
            probes.join()
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
        stop_mon.set()
        mon.join()
        h.check_workers()  # the monitor's last look may predate a crash
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
        short, breaches = record_metrics(h, start_at, segments, totals.get("segments", []))
        for b in breaches:
            h.fail(f"gate: {b}")
        oracle_failed = not converged
        outcome = "ok" if not h.failures else ("fail" if oracle_failed or breaches else "miss")
        note = (
            f"{label}: {'oracle 0 diffs' if converged else 'ORACLE DIFF'} after {time.time() - t0:.0f}s; "
            f"peak RSS {h.peak_rss_kb / 1024:.0f} MB, peak age {h.peak_age:.0f}s; load err {totals['err']}"
            + (f"; failures: {'; '.join(h.failures)}" if h.failures else "")
            + f"; {short}"
        )
    except BaseException as exc:
        h.event(f"aborted: {type(exc).__name__}: {exc}")
        note = f"{label}: aborted: {type(exc).__name__}: {exc}"
        raise
    finally:
        no_interrupts()
        stop_mon.set()
        try:
            h.stop_all()
        finally:
            logbook_finish(h, entry, rows, outcome, note)
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
    book = argparse.ArgumentParser(add_help=False)
    book.add_argument("--no-logbook", action="store_true", help="don't reserve a logbook entry (short trials only)")
    book.add_argument("--kind", default="soak", choices=["soak", "repro", "acceptance"])
    book.add_argument("--lane", default=os.environ.get("TRELLIS_LANE", "x"))
    book.add_argument("--issue", type=int, default=0)

    s = sub.add_parser("setup", parents=[book], help="build, seed and snapshot the cluster")
    s.add_argument("--accounts", type=int, default=100000)
    s.add_argument("--orders", type=int, default=1000000)
    s.add_argument(
        "--volume-mb", type=int, default=0, help="put the data directory on a tmpfs this size (for volume-full)"
    )
    s.add_argument("--bin", help="trellis binary (default: build target/release/trellis)")
    s.add_argument("--memory-max", default="6G")
    s.add_argument("--build-secs", type=int, default=900, help="how long the definitions may take to go live")
    s.add_argument("--force", action="store_true", help="rebuild an existing cluster")

    r = sub.add_parser("run", parents=[book], help="run a scenario against the base snapshot")
    r.add_argument("--scenario", choices=sorted(SCENARIOS))
    r.add_argument("--step", action="append", default=[], help="ACTION[:ARG...][+ACTION...], repeatable, run in order")
    r.add_argument("--load-secs", type=int, help="shorthand for --schedule steady:SECS (default 120)")
    r.add_argument(
        "--schedule",
        help="load profile segments PROFILE:SECS[:RATE],...; profiles idle, trickle, steady, spike (see loadgen.py)",
    )
    r.add_argument(
        "--probe-secs", type=float, default=30, help="write-to-target latency probe interval while loaded; 0 disables"
    )
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

    sub.add_parser("stop", help="stop the workers, the load and Postgres")
    sub.add_parser("actions", help="list fault actions and scenarios")
    args = ap.parse_args(argv)
    args.dir = os.path.abspath(args.dir)

    def interrupted(signum, _frame):
        raise KeyboardInterrupt(signal.Signals(signum).name)

    # SIGTERM and SIGHUP tear down like Ctrl-C: stop everything, undo host changes, finish
    # the logbook entry. Their default action would skip every `finally`.
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGHUP, interrupted)
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
