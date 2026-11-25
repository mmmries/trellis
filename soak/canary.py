#!/usr/bin/env python3
"""The standing canary (#2191): one Trellis instance that is never reset.

  soak/canary.py provision --bin PATH   build the cluster once: seed, define, check
  soak/canary.py run                    the daemon: rotating load, a check at every quiet point
  soak/canary.py upgrade --bin PATH     hand over to another release at the next quiet point
  soak/canary.py clear                  after a failure has been looked at: let `run` resume
  soak/canary.py status                 print status.json

It reuses chaos.py's cluster, workers, load, monitor, oracle and profile gates. A failed
check stops the load and the workers, keeps the database, writes a report, sets status.json
to `failed` and holds until `clear`. See soak/README.md, "The canary".
"""

import argparse
import datetime
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import traceback

import chaos
import loadgen

PORT = 56801  # beside chaos.py's 56701
METRICS_BASE = 59600
WORKERS = 2
DRAIN_THREADS = 2
ACCOUNTS, ORDERS = 20000, 200000
TPS, THREADS = 20, 2  # steady is 20 writes/s, spike 200
# One round per hour, weighted to idle and trickle; each has a steady segment so the
# after-idle gate has a baseline. Rounds rotate.
ROUNDS = [
    "trickle:900,steady:300,idle:1800,trickle:600",
    "steady:120,idle:2700,trickle:780",
    "trickle:1200,steady:600,idle:1800",
    "steady:300,spike:60,trickle:1200,idle:2040",
]
CONVERGE_SECS = 900  # for the definitions to be live and the oracle to report no differences
MAX_RSS_MB = 1024  # the workers' summed RSS
POLL_SECS = 30  # how often the daemon looks for a bench reservation and beats
CHURN_EVERY = 86400
SUMMARY_EVERY = 7 * 86400
HANDOVER_SECS = 300
MIN_FREE_GB = 20
# The probe's target: a 1-1 of accounts every release can define (acct_view needs WHERE, 0.4.0).
PROBE_TARGET = "acct_names"
PROBE_DEF = f"TRANSFORM {PROBE_TARGET} FROM accounts SELECT name AS uname"
PROBE_ORACLE = (
    f"select '{PROBE_TARGET}' t, count(*) from ((select id, name from accounts except all select id, uname"
    f" from public.{PROBE_TARGET}) union all (select id, uname from public.{PROBE_TARGET} except all"
    " select id, name from accounts)) x"
)
# Definitions nothing else reads (acct_order_totals feeds big_accts): paused, dropped and redefined.
CHURNABLE = ["acct_view", "region_totals", "region_order_totals", "order_view", "big_accts"]
# Routine source migrations, one a day in this order; the cycle is idempotent and none should
# pause a definition (docs/operations.md, "Rewrites that rebuild by themselves").
MIGRATIONS = [
    ("add-column", "alter table orders add column if not exists canary_note text"),
    ("create-index", "create index if not exists canary_orders_status on orders (status)"),
    ("vacuum-full", "vacuum full orders"),
    ("drop-column", "alter table orders drop column if exists canary_note"),
    ("cluster", "cluster accounts using accounts_pkey"),
    ("drop-index", "drop index if exists canary_orders_status"),
    ("reindex", "reindex table concurrently accounts"),
]
HISTORY_COLUMNS = [
    "at",
    "kind",
    "detail",
    "version",
    "rows",
    "peak_rss_mb",
    "schema_mb",
    "target_mb",
    "ring_seq",
    "converge_s",
    "outcome",
]


class UsageError(Exception):
    pass


class Cleared(Exception):
    """`clear` ran while this daemon held: it exits non-zero, so systemd restarts it."""


def now_iso(t=None):
    return datetime.datetime.fromtimestamp(time.time() if t is None else t, datetime.timezone.utc).isoformat(
        timespec="seconds"
    )


def write_json_atomic(path, obj):
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=1)
        f.write("\n")
    os.replace(tmp, path)


def read_json(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def check_home(home, free_bytes):
    """Why `home` can't hold the canary, or None: never under /tmp or a cargo target/ (lane
    cleanups delete those), and with room to age."""
    real = os.path.realpath(home) + "/"
    if real.startswith("/tmp/") or "/target/" in real:
        return f"{home} is under /tmp or a target/ directory, which cleanups delete"
    if free_bytes < MIN_FREE_GB * 2**30:
        return f"only {free_bytes / 2**30:.0f} GB free under {home}; the canary wants {MIN_FREE_GB} GB"
    return None


def pick_round(n):
    return ROUNDS[n % len(ROUNDS)]


def churn_plan(n, targets):
    """The nth day's churn: a definition to pause and resume, one to drop and redefine (of
    the CHURNABLE ones among `targets`), and a migration."""
    pool = [t for t in CHURNABLE if t in targets]
    return {
        "pause": pool[n % len(pool)],
        "redefine": pool[(n + 1) % len(pool)],
        "migration": MIGRATIONS[n % len(MIGRATIONS)],
    }


def due(last, every, now):
    return last is None or now - last >= every


def status_doc(state, phase, checked_at, summary, report, version, weekly=None, now=None):
    """status.json, the contract the phone alert polls (soak/README.md, "The canary")."""
    return {
        "state": state,  # ok | failed
        "phase": phase,  # running | yielding | upgrading | held | stopped
        "heartbeat_at": now_iso(now),
        "checked_at": checked_at,
        "summary": summary,
        "report": report,
        "version": version,
        "weekly": weekly,
    }


def read_history(path):
    try:
        with open(path) as f:
            lines = f.read().splitlines()
    except OSError:
        return []
    return [dict(zip(HISTORY_COLUMNS, line.split("\t"), strict=False)) for line in lines[1:] if line]


def fnum(row, key):
    try:
        return float(row[key])
    except (KeyError, TypeError, ValueError):
        return None


def weekly_summary(rows, now, provisioned_at, started_at, version):
    """The weekly summary's Markdown: per day, quiet points, rows, peak RSS, sizes and ring laps."""
    since = now - 7 * 86400
    week = [r for r in rows if (fnum(r, "at") or 0) >= since]
    out = [
        f"# Canary week to {now_iso(now)[:10]}",
        "",
        f"- Running {version}; provisioned {(now - provisioned_at) / 86400:.1f} days ago,"
        f" this daemon up {(now - started_at) / 3600:.1f} h.",
        f"- {len(week)} quiet points, {sum(1 for r in week if r.get('outcome') != 'ok')} flagged,"
        f" {sum(int(fnum(r, 'rows') or 0) for r in week)} rows written.",
    ]
    seqs = [fnum(r, "ring_seq") for r in rows if fnum(r, "ring_seq") is not None]
    if seqs:
        week_seqs = [fnum(r, "ring_seq") for r in week if fnum(r, "ring_seq") is not None]
        laps_week = (max(week_seqs) - min(week_seqs)) / 4 if week_seqs else 0
        out.append(f"- Ring laps: {laps_week:.0f} this week, {(max(seqs) - 1) / 4:.0f} since provisioning.")
    kinds = sorted({r["kind"] for r in week if r.get("kind") not in (None, "round")})
    if kinds:
        out.append("- Other events: " + ", ".join(f"{k} x{sum(1 for r in week if r['kind'] == k)}" for k in kinds))
    out += [
        "",
        "| day | quiet points | rows | peak RSS MB | trellis schema MB | targets MB |",
        "|---|---|---|---|---|---|",
    ]
    days = {}
    for r in week:
        days.setdefault(now_iso(fnum(r, "at"))[:10], []).append(r)
    for day, rs in sorted(days.items()):
        rss = [v for v in (fnum(r, "peak_rss_mb") for r in rs) if v is not None]
        last = rs[-1]
        out.append(
            f"| {day} | {len(rs)} | {sum(int(fnum(r, 'rows') or 0) for r in rs)} | {max(rss) if rss else 'NA'}"
            f" | {last.get('schema_mb', 'NA')} | {last.get('target_mb', 'NA')} |"
        )
    flagged = [r for r in week if r.get("outcome") != "ok"]
    if flagged:
        out += ["", "Flagged:"] + [f"- {now_iso(fnum(r, 'at'))} {r['kind']} {r['detail']}" for r in flagged]
    return "\n".join(out) + "\n"


def bench_active():
    """Whether a logbook reservation of kind `bench` holds the box."""
    script = chaos.logbook_path()
    if not os.path.exists(script):
        return False
    return subprocess.run([script, "busy", "--kind", "bench"], capture_output=True).returncode == 0


class Canary:
    """Paths and persistent state under the canary's home (default ~/canary)."""

    def __init__(self, home, port=PORT):
        self.home = home
        self.port = port
        self.cluster = os.path.join(home, "cluster")
        self.status_path = os.path.join(home, "status.json")
        self.state_path = os.path.join(home, "state.json")
        self.history_path = os.path.join(home, "history.tsv")
        self.reports = os.path.join(home, "reports")
        self.summaries = os.path.join(home, "summaries")
        self.upgrade_path = os.path.join(home, "upgrade-request.json")
        self.state = read_json(self.state_path, {})
        self.status = read_json(self.status_path, None) or status_doc("ok", "stopped", None, "", None, None)
        self.started_at = time.time()
        self.converge_secs = CONVERGE_SECS
        self.versions = {}
        self.h = None

    def provisioned(self):
        return os.path.exists(os.path.join(self.cluster, "setup.json"))

    def harness(self, bin_path=None, run_dir=None):
        h = chaos.Harness(self.cluster, self.port, bin_path, run_dir)
        h.metrics_base, h.scope = METRICS_BASE, False  # the service's cgroup caps everything
        h.probe_target = PROBE_TARGET
        h.memory_max, h.drain_threads = "unused", DRAIN_THREADS
        self.h = h
        return h

    def targets(self):
        """The definitions this canary keeps, in definition order."""
        return self.state.get("targets") or list(all_defs())

    def save_state(self):
        write_json_atomic(self.state_path, self.state)

    def version(self, bin_path):
        if bin_path not in self.versions:
            r = subprocess.run([bin_path, "--version"], capture_output=True, text=True)
            self.versions[bin_path] = (r.stdout.strip() or os.path.basename(os.path.dirname(bin_path))).split()[-1]
        return self.versions[bin_path]

    def beat(self, phase, **changes):
        """Rewrite status.json with a fresh heartbeat; `changes` set its other fields."""
        if self.status.get("state") == "failed" and "state" not in changes:
            disk = read_json(self.status_path, {})
            if disk.get("state") == "ok":  # `clear` ran while this daemon held: it wins
                self.status = disk
        s = dict(self.status, **changes)
        self.status = status_doc(
            s["state"], phase, s["checked_at"], s["summary"], s["report"], s.get("version"), s.get("weekly")
        )
        write_json_atomic(self.status_path, self.status)

    def history(self, kind, detail, rows, peak_rss_mb, sizes, converge_s, outcome):
        new = not os.path.exists(self.history_path)
        row = [f"{time.time():.0f}", kind, detail, self.version(self.h.bin), str(rows), peak_rss_mb, *sizes]
        row += [f"{converge_s:.0f}", outcome]
        with open(self.history_path, "a") as f:
            if new:
                f.write("\t".join(HISTORY_COLUMNS) + "\n")
            f.write("\t".join(str(v).replace("\t", " ") for v in row) + "\n")

    # -- the logbook: one background entry per release the canary runs ------------------
    def reserve(self, rows=0):
        script = chaos.logbook_path()
        cmd = [script, "reserve", "--kind", "soak", "--lane", "canary", "--issue", "2191", "--expect", "2160h"]
        cmd += ["--cmd", f"soak/canary.py run ({self.version(self.h.bin)})", "--background"]
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode:
            self.h.event(f"logbook reserve failed: {r.stderr.strip()}")
            return
        self.state["entry"], self.state["entry_rows"] = r.stdout.strip().splitlines()[-1], rows
        self.save_state()
        self.h.event(f"logbook background entry {self.state['entry']}")

    def finish(self, outcome, note):
        entry = self.state.get("entry")
        if entry:
            rows = str(self.state.get("entry_rows", 0))
            r = subprocess.run(
                [chaos.logbook_path(), "finish", entry, "--rows", rows, "--outcome", outcome, "--note", note[:400]]
            )
            if r.returncode:
                self.h.event(f"logbook finish {entry} FAILED: finish it by hand")
        self.state["entry"], self.state["entry_rows"] = None, 0
        self.save_state()

    def add_rows(self, n):
        self.state["entry_rows"] = self.state.get("entry_rows", 0) + n
        self.state["rows_total"] = self.state.get("rows_total", 0) + n
        self.save_state()


# -- checks -------------------------------------------------------------------


def sizes(h):
    """[trellis schema MB, targets MB, the ring's active segment sequence] or NAs."""
    out = []
    for sql in (
        "select round(sum(pg_total_relation_size(c.oid))/1048576.0) from pg_class c join pg_namespace n"
        " on n.oid = c.relnamespace where n.nspname = 'trellis' and c.relkind = 'r'",
        "select round(sum(pg_total_relation_size(format('public.%I', t)::regclass))/1048576.0)"
        " from unnest(array['acct_view','region_totals','acct_order_totals','region_order_totals',"
        "'order_view','big_accts','acct_names']) t where to_regclass(format('public.%I', t)) is not null",
        "select active_seq from trellis.segment_pointer",
    ):
        try:
            out.append(h.psql(sql) or "NA")
        except subprocess.CalledProcessError:
            out.append("NA")
    return out


def converge(c, h, secs=None):
    """Wait for every definition to be live and the oracle to report no differences.
    Returns (failures, seconds waited)."""
    secs = secs or c.converge_secs
    t0 = time.time()
    while True:
        c.beat(c.status["phase"])
        h.check_workers()
        try:
            # target_table is schema-qualified (public.x) from 0.3 on; the targets are bare names.
            states = {k.removeprefix("public."): v for k, v in chaos.definition_states(h).items()}
        except subprocess.CalledProcessError as exc:
            states = {"(catalog)": (exc.stderr or "").strip()[:200]}
        diffs = chaos.oracle(h, oracle_parts(c.targets())) if all(s == "live" for s in states.values()) else None
        if diffs is not None and not any(diffs.values()) and set(states) == set(c.targets()):
            return [], time.time() - t0
        if time.time() - t0 > secs:
            not_live = {k: v for k, v in states.items() if v != "live"}
            out = []
            if not_live or set(states) != set(c.targets()):
                out.append(f"definitions not live after {secs}s: {not_live or states}")
            if diffs is not None:
                out.append(f"oracle differs after {secs}s: {diffs}")
            if not out:
                out.append(f"the oracle didn't run after {secs}s; see events.log")
            return out, time.time() - t0
        time.sleep(10)


def all_defs():
    """target -> its definition: the probe's, then defs.txt's in order."""
    lines = [d for d in open(os.path.join(chaos.HERE, "defs.txt")).read().splitlines() if d.strip()]
    return {PROBE_TARGET: PROBE_DEF, **{d.split()[1]: d for d in lines}}


def oracle_parts(targets):
    parts = dict(chaos.oracle_parts(), **{PROBE_TARGET: PROBE_ORACLE})
    return [parts[t] for t in targets]


def rss_mb(h):
    """The live workers' summed RSS now, in MB."""
    kb = 0
    for name in h.worker_names():
        proc = h.workers[name]["proc"]
        try:
            with open(f"/proc/{proc.pid}/status") as f:
                kb += int(next(line.split()[1] for line in f if line.startswith("VmRSS")))
        except (OSError, StopIteration, ValueError):
            pass
    return kb / 1024


def quiet_point(c, h, kind, detail, rows=0, extra_failures=(), gate=None):
    """Check at a quiet point: every definition live, the oracle at no differences, no worker
    lost, and what `gate()` (after the oracle) returns as (failures, metric lines). Records it;
    on failure stops, reports and holds, and never returns."""
    c.add_rows(rows)
    failures, waited = converge(c, h)
    gated, lines = gate() if gate else ([], [])
    peak = h.peak_rss_kb / 1024 if gate else rss_mb(h)
    if peak > MAX_RSS_MB:
        gated.append(f"worker RSS {peak:.0f} MB > {MAX_RSS_MB} MB")
    failures = list(extra_failures) + gated + list(h.failures) + failures
    h.failures.clear()
    c.history(kind, detail, rows, f"{peak:.0f}", sizes(h), waited, "fail" if failures else "ok")
    if failures:
        fail(c, h, kind, detail, failures, lines)
    msg = f"{kind} {detail}: oracle 0 diffs after {waited:.0f}s, {rows} rows, RSS {peak:.0f} MB"
    h.event(f"quiet point ok: {msg}")
    c.beat(c.status["phase"], state="ok", checked_at=now_iso(), summary=msg, report=None, version=c.version(h.bin))


def fail(c, h, kind, detail, failures, metric_lines=()):
    """Stop the load and the workers, keep the database, report, mark failed and hold."""
    for f in failures:
        h.event(f"FAIL {f}")
    h.signal_pidfile("loadgen", h.needle, signal.SIGTERM, wait=5)
    for name in h.worker_names():
        h.stop_worker(name)
    os.makedirs(c.reports, exist_ok=True)
    report = os.path.join(c.reports, time.strftime("%Y%m%dT%H%M%S") + f"-{kind}.md")
    status = h.sh([h.bin, "status", "-d", h.dsn()], check=False, capture_output=True)
    try:
        states = chaos.definition_states(h)
    except subprocess.CalledProcessError:
        states = "unreadable"
    with open(report, "w") as f:
        f.write(f"# Canary failure: {kind} {detail}\n\n")
        f.write(f"- When: {now_iso()}\n- Release: {c.version(h.bin)} ({h.bin})\n")
        f.write(f"- Cluster: {c.cluster} (port {h.port}; Postgres left running, workers stopped)\n")
        f.write(f"- Logs: {h.run_dir} (events.log, wN.log, round-*/)\n- Definitions: {states}\n\n## Failures\n\n")
        f.write("".join(f"- {x}\n" for x in failures))
        if metric_lines:
            f.write("\n## Round metrics\n\n```\n" + "\n".join(metric_lines) + "\n```\n")
        f.write(f"\n## trellis status\n\n```\n{status.stdout}{status.stderr}```\n")
    summary = f"{kind} {detail} failed: {'; '.join(failures)}"[:500]
    c.beat("held", state="failed", summary=summary, report=report)
    c.finish("fail", summary)
    hold(c, h)


def hold(c, h):
    """Keep Postgres up for inspection and beat until stopped or `clear` runs (then exit, and
    systemd's restart resumes)."""
    h.event(f"holding: {c.status['summary']}; report {c.status['report']}")
    while True:
        c.beat("held")
        if c.status["state"] != "failed":
            h.event("cleared: exiting for a restart")
            raise Cleared()
        time.sleep(60)


# -- the daemon's work ----------------------------------------------------------


def run_round(c, h, n, schedule):
    """One round of load on `schedule`, then its quiet point. Yields to a bench reservation."""
    segments = loadgen.parse_schedule(schedule, TPS)
    round_dir = os.path.join(h.run_dir, f"round-{n:05d}")
    os.makedirs(round_dir, exist_ok=True)
    h.samples, h.probes, h.step_log = [], [], []
    offsets = {w: os.path.getsize(os.path.join(h.run_dir, f"{w}.log")) for w in h.worker_names()}
    stop_mon = threading.Event()
    mon = threading.Thread(
        target=chaos.monitor, args=(h, os.path.join(round_dir, "monitor.tsv"), stop_mon), daemon=True
    )
    mon.start()
    start_at = time.time() + 2
    h.timeline = (start_at, segments)
    load_out = os.path.join(round_dir, "load.log")
    lg = chaos.start_loadgen(h, schedule, TPS, THREADS, "", start_at, load_out)
    prober = threading.Thread(target=chaos.prober, args=(h, 30), daemon=True)
    prober.start()
    h.event(f"round {n}: {schedule} at {TPS} tps")
    yielded = False
    while True:
        c.beat("running")
        try:
            lg.wait(timeout=POLL_SECS)
            break
        except subprocess.TimeoutExpired:
            pass
        if bench_active():
            h.event("a bench reservation holds the box: stopping the load")
            yielded = True
            h.aborting.set()  # stops the prober
            h.signal_pidfile("loadgen", h.needle, signal.SIGTERM, wait=5)
            lg.wait()
            break
    prober.join()
    h.aborting.clear()
    totals = {"rows": 0, "segments": []}
    if yielded:
        yield_to_bench(c, h)
    else:
        totals = chaos.load_totals(load_out)

    def gate():
        """Stop the monitor once the oracle has converged, then apply the profile gates."""
        stop_mon.set()
        mon.join()
        if yielded:
            return [], []
        _, breaches = chaos.record_metrics(h, start_at, segments, totals["segments"], round_dir, offsets)
        try:
            with open(os.path.join(round_dir, "metrics.json")) as f:
                lines = chaos.metrics_lines(json.load(f))[0]
        except (OSError, ValueError, KeyError):
            lines = []
        return [f"gate: {b}" for b in breaches], lines

    detail = f"{n} {schedule}" + (" (yielded to a bench)" if yielded else "")
    quiet_point(c, h, "round", detail, totals["rows"], gate=gate)


def yield_to_bench(c, h):
    while bench_active():
        c.beat("yielding")
        time.sleep(60)
    h.event("the bench reservation ended: resuming")
    c.beat("running")


def apply(h, statement):
    r = h.sh([h.bin, "apply", "-d", h.dsn(), statement], check=False, capture_output=True)
    if r.returncode:
        return f"`{statement[:80]}` refused ({r.returncode}): {(r.stderr or r.stdout).strip()[:300]}"
    h.event(f"applied {statement[:80]}")
    return None


def churn(c, h):
    """The day's churn: pause one definition under a minute of load and resume it, drop and
    redefine another, and run one migration; then a quiet point."""
    n = c.state.get("churns", 0)
    plan = churn_plan(n, c.targets())
    name, sql = plan["migration"]
    h.event(f"churn {n}: pause {plan['pause']}, redefine {plan['redefine']}, migration {name}")
    failures = [apply(h, f"PAUSE TRANSFORM {plan['pause']}")]
    load_out = os.path.join(h.run_dir, f"churn-{n:04d}-load.log")
    chaos.start_loadgen(h, "steady:60", TPS, THREADS, "", time.time() + 1, load_out).wait()
    rows = chaos.load_totals(load_out)["rows"]
    failures.append(apply(h, f"RESUME TRANSFORM {plan['pause']}"))
    failures.append(apply(h, f"PAUSE TRANSFORM {plan['redefine']}"))  # DROP takes a paused definition
    failures.append(apply(h, f"DROP TRANSFORM {plan['redefine']}"))
    failures.append(apply(h, all_defs()[plan["redefine"]]))
    try:
        h.psql(sql, user=chaos.APP)
        h.event(f"migration {name}: {sql}")
    except subprocess.CalledProcessError as exc:
        failures.append(f"migration {name} failed: {(exc.stderr or '').strip()[:300]}")
    c.state["churns"], c.state["last_churn"] = n + 1, time.time()
    c.save_state()
    detail = f"{n} pause {plan['pause']}, redefine {plan['redefine']}, {name}"
    quiet_point(c, h, "churn", detail, rows, [f for f in failures if f])


def upgrade(c, h):
    """The documented handover (docs/upgrading.md): start the new release beside the old,
    stop the old cleanly, wait for the new to take over; then a quiet point."""
    new = os.path.abspath(read_json(c.upgrade_path, {}).get("bin", ""))
    os.remove(c.upgrade_path)
    if not os.access(new, os.X_OK):
        h.event(f"upgrade request ignored: {new} isn't an executable")
        c.beat("running", summary=f"upgrade request ignored: {new} isn't an executable")
        return
    old_version, new_version = c.version(h.bin), c.version(new)
    c.beat("upgrading", summary=f"upgrading {old_version} -> {new_version}")
    old = [w for w in h.worker_names() if h.workers[w]["proc"].poll() is None]
    first = max(int(w[1:]) for w in h.workers) + 1
    new_names = [f"w{first + i}" for i in range(WORKERS)]
    for name in new_names:
        h.start_worker(name, new)
    time.sleep(30)
    for w in old:
        h.stop_worker(w)
    t0 = time.time()
    took = False
    while time.time() - t0 < HANDOVER_SECS and not took:
        took = any(
            "taking over" in open(os.path.join(h.run_dir, f"{w}.log"), errors="replace").read() for w in new_names
        )
        c.beat("upgrading")
        time.sleep(1)
    h.bin = h.cfg["bin"] = new
    write_json_atomic(h.cfg_path, h.cfg)
    days = (time.time() - c.state.get("provisioned_at", time.time())) / 86400
    c.finish("ok", f"canary on {old_version}: {days:.1f} days old, handed over to {new_version}")
    c.reserve()
    c.state.setdefault("upgrades", []).append([now_iso(), old_version, new_version])
    c.save_state()
    extra = [] if took else [f"{new_version} did not take over from {old_version} within {HANDOVER_SECS}s"]
    quiet_point(c, h, "upgrade", f"{old_version} -> {new_version}", 0, extra)
    adopt(c, h)


def adopt(c, h):
    """Define what an earlier release couldn't (--without at provision), if this one can."""
    missing = [t for t in all_defs() if t not in c.targets()]
    added = [t for t in missing if apply(h, all_defs()[t]) is None]
    if not added:
        return
    c.state["targets"] = [t for t in all_defs() if t in c.targets() or t in added]
    c.save_state()
    quiet_point(c, h, "adopt", ", ".join(added))


def weekly(c, h):
    os.makedirs(c.summaries, exist_ok=True)
    path = os.path.join(c.summaries, time.strftime("%Y-%m-%d") + ".md")
    text = weekly_summary(
        read_history(c.history_path),
        time.time(),
        c.state.get("provisioned_at", time.time()),
        c.started_at,
        c.version(h.bin),
    )
    with open(path, "w") as f:
        f.write(text)
    c.state["last_summary"] = time.time()
    c.save_state()
    h.event(f"weekly summary: {path}")
    c.beat("running", weekly=path)


# -- commands -------------------------------------------------------------------


def cmd_provision(args):
    c = Canary(args.home, args.port)
    if c.provisioned() or os.path.lexists(c.cluster):
        raise UsageError(f"{c.cluster} exists; the canary is never reset (provisioning would delete it)")
    os.makedirs(args.home, exist_ok=True)
    why = check_home(args.home, shutil.disk_usage(args.home).free)
    if why:
        raise UsageError(why)
    if not os.access(args.bin, os.X_OK):
        raise UsageError(f"{args.bin} isn't an executable")
    h = c.harness(os.path.abspath(args.bin), os.path.join(c.cluster, "logs", "provision"))
    ns = argparse.Namespace(
        dir=c.cluster,
        port=args.port,
        accounts=args.accounts,
        orders=args.orders,
        volume_mb=0,
        build_secs=900,
        memory_max="unused",
    )
    skip = set(filter(None, args.without.split(",")))
    if PROBE_TARGET in skip or skip - set(all_defs()):
        raise UsageError(f"--without takes targets from defs.txt; {PROBE_TARGET} is the probe's")
    targets = [t for t in all_defs() if t not in skip]
    defs = [all_defs()[t] for t in targets]
    try:
        chaos.setup_cluster(h, ns, snapshot=False, defs=defs, parts=oracle_parts(targets))
    finally:
        chaos.no_interrupts()
        h.stop_all()
    c.state = {"provisioned_at": time.time(), "churns": 0, "round": 0, "rows_total": 0, "last_churn": time.time()}
    c.state["targets"] = targets
    c.save_state()
    c.reserve(rows=args.accounts + args.orders)
    c.beat("stopped", state="ok", checked_at=now_iso(), summary="provisioned", version=c.version(h.bin))
    print(f"canary provisioned in {c.cluster} on {c.version(h.bin)}")


def cmd_run(args):
    c = Canary(args.home, args.port)
    if not c.provisioned():
        raise UsageError(f"{c.cluster} isn't provisioned; run `canary.py provision --bin PATH` first")
    c.converge_secs = args.converge_secs
    h = c.harness(run_dir=os.path.join(c.cluster, "logs", time.strftime("%Y%m%dT%H%M%S")))
    try:
        h.stop_all()  # whatever an earlier daemon left
        h.aborting.clear()
        h.pg_start()
        h.event(f"canary daemon on {c.version(h.bin)}; logs in {h.run_dir}")
        if c.status.get("state") == "failed":
            hold(c, h)
        if not c.state.get("entry"):
            c.reserve()
        for i in range(1, WORKERS + 1):
            h.start_worker(f"w{i}")
        c.beat("running", version=c.version(h.bin))
        time.sleep(5)
        quiet_point(c, h, "start", c.version(h.bin))
        done = 0
        while not args.rounds or done < args.rounds:
            if bench_active():
                yield_to_bench(c, h)
            if os.path.exists(c.upgrade_path):
                upgrade(c, h)
            if due(c.state.get("last_churn"), CHURN_EVERY, time.time()):
                churn(c, h)
            if due(c.state.get("last_summary", c.state.get("provisioned_at")), SUMMARY_EVERY, time.time()):
                weekly(c, h)
            n = c.state.get("round", 0)
            run_round(c, h, n, args.round or pick_round(n))
            c.state["round"] = n + 1
            c.save_state()
            done += 1
    except Cleared:
        raise
    except Exception as exc:  # a harness error fails like a check, so a restart can't resume load over it
        h.event(f"harness error:\n{traceback.format_exc()}")
        fail(c, h, "error", "harness", [f"harness error: {type(exc).__name__}: {exc}"])
    finally:
        chaos.no_interrupts()
        try:
            h.stop_all()
        finally:
            c.beat("stopped")
    return 0


def cmd_upgrade(args):
    c = Canary(args.home, args.port)
    new = os.path.abspath(args.bin)
    if not os.access(new, os.X_OK):
        raise UsageError(f"{new} isn't an executable")
    write_json_atomic(c.upgrade_path, {"bin": new, "requested_at": now_iso()})
    print(f"requested; the daemon hands over to {new} at its next quiet point (within about an hour)")
    deadline = time.time() + args.wait_secs
    while os.path.exists(c.upgrade_path) and time.time() < deadline:
        time.sleep(10)
    if os.path.exists(c.upgrade_path):
        print("the daemon hasn't taken the request yet; it stays queued")
        return 1
    while time.time() < deadline:
        st = read_json(c.status_path, {})
        if st.get("phase") != "upgrading" and st.get("version") == c.version(new) or st.get("state") == "failed":
            print(json.dumps(st, indent=1))
            return 0 if st.get("state") == "ok" else 1
        time.sleep(10)
    print("the handover hasn't finished; watch status.json")
    return 1


def cmd_clear(args):
    c = Canary(args.home, args.port)
    was = c.status.get("report")
    c.beat("stopped", state="ok", summary=f"cleared by hand after {was}", report=None)
    print("cleared; a running daemon restarts itself within about two minutes")


def cmd_status(args):
    print(json.dumps(Canary(args.home, args.port).status, indent=1))


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--home",
        default=os.environ.get("TRELLIS_CANARY_HOME", os.path.expanduser("~/canary")),
        help="the canary's directory (default ~/canary, or $TRELLIS_CANARY_HOME)",
    )
    ap.add_argument("--port", type=int, default=PORT, help=f"Postgres port (default {PORT})")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("provision", help="build the cluster once")
    p.add_argument("--bin", required=True, help="the trellis CLI to run")
    p.add_argument("--accounts", type=int, default=ACCOUNTS)
    p.add_argument("--orders", type=int, default=ORDERS)
    p.add_argument(
        "--without", default="", help="defs.txt targets the release can't define, comma-separated (0.3.x: no WHERE)"
    )
    r = sub.add_parser("run", help="the daemon")
    r.add_argument("--rounds", type=int, default=0, help="stop after N rounds (default: run until stopped)")
    r.add_argument("--round", help="a schedule for every round instead of the rotation (smoke runs)")
    r.add_argument(
        "--converge-secs", type=int, default=CONVERGE_SECS, help="how long a quiet point waits for live and the oracle"
    )
    u = sub.add_parser("upgrade", help="hand over to another release at the next quiet point")
    u.add_argument("--bin", required=True)
    u.add_argument("--wait-secs", type=int, default=7200)
    sub.add_parser("clear", help="mark a failure as looked at so `run` resumes")
    sub.add_parser("status", help="print status.json")
    args = ap.parse_args(argv)
    args.home = os.path.abspath(args.home)
    if getattr(args, "round", None):
        try:
            loadgen.parse_schedule(args.round, TPS)
        except ValueError as exc:
            print(f"canary.py: {exc}", file=sys.stderr)
            return 64

    def interrupted(signum, _frame):
        raise KeyboardInterrupt(signal.Signals(signum).name)

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGHUP, interrupted)
    cmds = {"provision": cmd_provision, "run": cmd_run, "upgrade": cmd_upgrade, "clear": cmd_clear}
    cmds["status"] = cmd_status
    try:
        return cmds[args.cmd](args) or 0
    except UsageError as exc:
        print(f"canary.py: {exc}", file=sys.stderr)
        return 64
    except Cleared:
        return 75
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"canary.py: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
