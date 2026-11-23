"""Unit tests for the harness's pure parts.

Run: PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s soak -p 'test_*.py'
"""

import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest

sys.dont_write_bytecode = True
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import chaos  # noqa: E402
import loadgen  # noqa: E402


class ParseStep(unittest.TestCase):
    def test_sequential_and_parallel(self):
        parts = chaos.parse_step("kill9:w1:3+long-txn:60")
        self.assertEqual([(fn.__name__, args) for fn, args in parts], [("kill9", ["w1", "3"]), ("long_txn", ["60"])])

    def test_optional_args(self):
        self.assertEqual(chaos.parse_step("pg-restart")[0][1], [])

    def test_unknown_action(self):
        with self.assertRaisesRegex(chaos.UsageError, "unknown action"):
            chaos.parse_step("reboot:5")

    def test_arity(self):
        with self.assertRaisesRegex(chaos.UsageError, "usage sleep:SECS"):
            chaos.parse_step("sleep")
        with self.assertRaises(chaos.UsageError):
            chaos.parse_step("sleep:1:2")

    def test_every_scenario_parses(self):
        for name, (_, steps) in chaos.SCENARIOS.items():
            for s in steps:
                with self.subTest(scenario=name, step=s):
                    chaos.parse_step(s)


class PidFiles(unittest.TestCase):
    """A pid is signalled only if its command line names this harness's port."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.h = chaos.Harness(self.tmp.name, 56999)
        self.procs = []

    def tearDown(self):
        for p in self.procs:
            p.kill()
            p.wait()
        self.tmp.cleanup()

    def spawn(self, name, *argv):
        p = subprocess.Popen(["sh", "-c", "sleep 30; :", "sh", *argv])
        self.procs.append(p)
        self.h.write_pid(name, p.pid)
        for _ in range(100):  # until the child has exec'd: before that /proc shows our cmdline
            with open(f"/proc/{p.pid}/cmdline") as f:
                if f.read().startswith("sh"):
                    break
            time.sleep(0.01)
        return p

    def test_signals_only_its_own_port(self):
        mine = self.spawn("w1", "host=/x port=56999 user=trellis_runner")
        other = self.spawn("w2", "host=/x port=56998 user=trellis_runner")
        self.assertIsNone(self.h.live_pid("w2", self.h.needle))
        self.assertFalse(self.h.signal_pidfile("w2", self.h.needle, signal.SIGTERM))
        self.assertTrue(self.h.signal_pidfile("w1", self.h.needle, signal.SIGTERM, wait=5))
        self.assertEqual(mine.wait(5), -signal.SIGTERM)
        self.assertIsNone(other.poll())

    def test_missing_or_garbled_pid_file(self):
        self.assertIsNone(self.h.live_pid("w9", self.h.needle))
        with open(self.h.pidfile("w8"), "w") as f:
            f.write("not a pid")
        self.assertIsNone(self.h.live_pid("w8", self.h.needle))

    def test_abort_stops_sleeps_and_starts(self):
        self.h.aborting.set()
        with self.assertRaises(chaos.Aborted):
            self.h.sleep(30)
        with self.assertRaises(chaos.Aborted):
            self.h.start_worker("w1")


class Oracle(unittest.TestCase):
    def test_parse(self):
        self.assertEqual(chaos.parse_oracle("acct_view|0\norder_view|12\n\nnoise"), {"acct_view": 0, "order_view": 12})

    def test_one_line_per_definition(self):
        with open(os.path.join(chaos.HERE, "oracle.sql")) as f:
            sql = f.read()
        with open(os.path.join(chaos.HERE, "defs.txt")) as f:
            defs = [d.split()[1] for d in f if d.strip()]
        for target in defs:
            self.assertIn(f"'{target}'", sql)


class Metrics(unittest.TestCase):
    TEXT = """# HELP x
trellis_definitions{trellis_instance="shop/trellis",status="live"} 6
trellis_definitions{trellis_instance="shop/trellis",status="paused"} 0
trellis_ring_rows{trellis_instance="shop/trellis"} 12
trellis_ring_rows_extra 99
"""

    def test_label_filter(self):
        self.assertEqual(chaos.metric_max(self.TEXT, "trellis_definitions", 'status="live"'), 6)
        self.assertEqual(chaos.metric_max(self.TEXT, "trellis_definitions", 'status="paused"'), 0)

    def test_name_is_exact(self):
        self.assertEqual(chaos.metric_max(self.TEXT, "trellis_ring_rows"), 12)
        self.assertIsNone(chaos.metric_max(self.TEXT, "trellis_ring"))


class Mix(unittest.TestCase):
    def test_overrides(self):
        w = loadgen.parse_mix("insert_order=50,delete_account=0")
        self.assertEqual((w["insert_order"], w["delete_account"], w["update_order"]), (50, 0, 25))

    def test_rejects_unknown(self):
        with self.assertRaises(ValueError):
            loadgen.parse_mix("truncate=1")


class Schedule(unittest.TestCase):
    def test_rates(self):
        self.assertEqual(loadgen.parse_rate("250", 100), 250)
        self.assertEqual(loadgen.parse_rate("6/m", 100), 0.1)
        self.assertEqual(loadgen.parse_rate("x100", 50), 5000)
        for bad in ("fast", "x", "/m", "-1", "x-2", "nan", "inf", "1/s"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                loadgen.parse_rate(bad, 100)

    def test_defaults_and_overrides(self):
        segs = loadgen.parse_schedule("steady:60,trickle:120,idle:3600,spike:60,spike:30:x100", 50)
        self.assertEqual(
            segs,
            [("steady", 60, 50), ("trickle", 120, 1 / 60), ("idle", 3600, 0), ("spike", 60, 500), ("spike", 30, 5000)],
        )

    def test_rejects(self):
        bad_segments = (
            "",
            "burst:60",
            "idle",
            "idle:60:5",
            "steady:0",
            "steady:-5",
            "idle:nan",
            "steady:x",
            "steady:60:1:2",
        )
        for bad in bad_segments:
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                loadgen.parse_schedule(bad, 100)


class PacerTest(unittest.TestCase):
    def slots(self, schedule, base=10, now_of=lambda t: t):
        p = loadgen.Pacer(loadgen.parse_schedule(schedule, base), 1000.0)
        out = []
        while (slot := p.take(now_of(p.next))) is not None:
            out.append(slot)
        return out

    def test_steady_rate(self):
        out = self.slots("steady:10:2")
        self.assertEqual(len(out), 20)
        self.assertEqual(out[1][0] - out[0][0], 0.5)

    def test_idle_hands_out_nothing_and_next_segment_starts_on_time(self):
        out = self.slots("steady:1:1,idle:100,steady:1:1")
        self.assertEqual(out, [(1000.0, 0), (1101.0, 2)])

    def test_trickle_does_not_delay_the_spike(self):
        out = self.slots("trickle:30,spike:1:4")  # one a minute: one slot, then the spike at 1030
        self.assertEqual([t for t, _ in out], [1000.0, 1030.0, 1030.25, 1030.5, 1030.75])
        self.assertEqual([i for _, i in out], [0, 1, 1, 1, 1])

    def test_no_burst_after_falling_behind(self):
        p = loadgen.Pacer(loadgen.parse_schedule("steady:100:10", 1), 1000.0)
        p.take(1000.0)
        self.assertEqual(p.take(1060.0), (1060.0, 0))

    def test_a_backlog_does_not_carry_into_the_next_segment(self):
        # A spike 3 s behind at its end: no spike slot in the idle, the next one on time.
        p = loadgen.Pacer(loadgen.parse_schedule("spike:10:100,idle:10,steady:10:1", 1), 1000.0)
        p.next = 1007.0
        self.assertEqual(p.take(1011.0), (1020.0, 2))
        p = loadgen.Pacer(loadgen.parse_schedule("spike:10:100,idle:10", 1), 1000.0)
        p.next = 1007.0
        self.assertIsNone(p.take(1011.0))

    def test_nothing_after_the_end(self):
        p = loadgen.Pacer(loadgen.parse_schedule("steady:10:100", 1), 1000.0)
        p.next = 1008.0
        self.assertIsNone(p.take(1010.0))


class Until(unittest.TestCase):
    def test_offsets(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = chaos.Harness(tmp, 56999)
            h.event = lambda msg: None
            waits = []
            h.sleep = waits.append
            now = time.time()
            h.timeline = (now + 100, loadgen.parse_schedule("steady:50,idle:200", 1))
            chaos.until(h, "2", "30")
            chaos.until(h, "2", "-10")
            self.assertAlmostEqual(waits[0], 180, delta=1)
            self.assertAlmostEqual(waits[1], 340, delta=1)
            self.assertEqual(h.segment_at(now + 160), (1, "idle"))
            self.assertIsNone(h.segment_at(now + 400))
            with self.assertRaises(chaos.UsageError):
                chaos.until(h, "3")


class OperationalMetrics(unittest.TestCase):
    LOG = [
        "2026-11-20T10:00:10.000000Z  WARN deadpool.postgres: Connection error: connection closed",
        "2026-11-20T10:00:20.000000Z  WARN trellis::client: LISTEN closed trellis_instance=shop/x retry_in=250ms",
        "2026-11-20T10:00:30.000000Z  WARN trellis::client: LISTEN closed trellis_instance=shop/x retry_in=500ms",
        "2026-11-20T10:00:40.000000Z  INFO trellis: fine",
        "2026-11-20T10:01:50.000000Z ERROR trellis: boom 42",
        "trellis running: staging=on",
    ]

    def test_log_templates_group_repeats(self):
        lines = chaos.parse_log_levels(self.LOG)
        self.assertEqual(len(lines), 4)
        self.assertEqual(lines[1][2], lines[2][2])
        self.assertEqual(lines[1][2], "trellis::client: LISTEN closed trellis_instance=… retry_in=…")
        self.assertEqual(lines[3][1:], ("ERROR", "trellis: boom N"))

    def test_log_templates_keep_the_cause(self):
        prefix = (
            "2026-11-20T10:00:10.000000Z  WARN trellis::client: maintenance step failed; retrying"
            ' trellis_instance=shop/trellis step="self_check" error=T0224: failed to acquire a connection: Error'
            " occurred while creating a new object: db error: FATAL: "
        )
        a, b, c = chaos.parse_log_levels(
            [
                prefix + "the database system is shutting down failures=1",
                prefix + "the database system is shutting down failures=12",
                prefix + "too many connections for role failures=1",
            ]
        )
        self.assertEqual(a[2], b[2])
        self.assertNotEqual(a[2], c[2])
        self.assertIn("error=T0224: failed", a[2])
        self.assertIn('error="cause N"', chaos.log_template('x error="cause 7"'))

    def test_summarize(self):
        start = chaos.parse_log_levels(self.LOG[:1])[0][0] - 10  # 10:00:00
        segments = loadgen.parse_schedule("steady:60,idle:3600", 10)
        samples = [
            {"epoch": start + t, "wal_bytes": str(w), "xmin_age": "3", "xmin_hold_s": str(hold), "idle_in_txn": "0",
             "trellis_conns": "4", "rss_kb": 2048}
            for t, w, hold in ((5, 0, 1), (55, 5000, 2), (60, 6000, 0), (180, 7000, 0), (3600, 7000 + 3420, 0))
        ]  # fmt: skip
        probes = [
            {"epoch": start + 10, "segment": 0, "tag": "steady", "latency": 0.2, "error": None},
            {"epoch": start + 30, "segment": 0, "tag": "steady", "latency": 0.4, "error": None},
            {"epoch": start + 3659, "segment": 1, "tag": "after-idle", "latency": 1.2, "error": None},
        ]
        log = chaos.parse_log_levels(self.LOG)
        m = chaos.summarize(start, segments, [["steady", 600, 1], ["idle", 0, 0]], samples, probes, log, [])
        self.assertEqual(m["steady_latency_p50"], 0.4)
        after = {"segment": 2, "idle_secs": 3600, "latency": 1.2, "ratio": 3.0, "error": None}
        self.assertEqual(m["after_idle"], [after])
        steady, idle = m["segments"]
        self.assertEqual((steady["ok"], steady["xmin_hold_s_max"], len(steady["warn"])), (600, 2, 2))
        self.assertEqual(idle["wal_bytes"], 4420)
        self.assertEqual(idle["wal_bytes_per_hour_settled"], 3600)  # 3420 bytes over 3420 s after settling
        self.assertEqual(idle["error"], {"trellis: boom N": 1})
        self.assertEqual(idle["warn_per_hour"], 0)
        lines, short = chaos.metrics_lines(m)
        self.assertIn("3.0x steady", lines[1])
        self.assertIn("after-idle max 3.0x", short)


class Gates(unittest.TestCase):
    """check_gates on synthetic metrics.json content."""

    @staticmethod
    def seg(n, profile, steps=(), **kw):
        base = {"segment": n, "profile": profile, "steps": list(steps), "warn": {}, "error": {}}
        return {**base, **kw}

    def metrics(self, **over):
        m = {
            "steady_latency_p50": 0.25,
            "segments": [
                self.seg(1, "steady", xmin_hold_s_settled_max=0),
                self.seg(2, "idle", wal_bytes_per_hour_settled=3 * 1048576, xmin_hold_s_settled_max=0),
                self.seg(3, "idle", steps=["pg-restart:fast"], warn={"closed": 41}, xmin_hold_s_settled_max=0),
            ],
            "after_idle": [
                {"segment": 2, "idle_secs": 7200, "latency": 0.1, "ratio": 0.4, "error": None},
                {"segment": 3, "idle_secs": 3600, "latency": 0.26, "ratio": 1.0, "error": None},
            ],
        }
        m.update(over)
        return m

    def test_baseline_passes(self):
        # Faulted idle WARNs are not gated.
        self.assertEqual(chaos.check_gates(self.metrics()), [])

    def test_no_idle_segment_is_not_gated(self):
        m = self.metrics(segments=[self.seg(1, "steady", xmin_hold_s_settled_max=99)], after_idle=[])
        self.assertEqual(chaos.check_gates(m), [])

    def test_first_write_ratio(self):
        m = self.metrics()
        m["after_idle"][1].update(latency=0.51, ratio=2.0)  # 2.04x: summarize's rounding must not pass it
        (msg,) = chaos.check_gates(m)
        self.assertIn("first write after idle (segment 3) took 2.04x", msg)
        m["after_idle"][1]["latency"] = 0.5  # the bound itself holds
        self.assertEqual(chaos.check_gates(m), [])

    def test_first_write_never_arrived(self):
        m = self.metrics()
        m["after_idle"][0].update(latency=None, ratio=None, error="timeout")
        (msg,) = chaos.check_gates(m)
        self.assertIn("segment 2", msg)
        self.assertIn("timeout", msg)

    def test_idle_wal(self):
        m = self.metrics()
        m["segments"][1]["wal_bytes_per_hour_settled"] = int(6.6 * 1048576)
        (msg,) = chaos.check_gates(m)
        self.assertIn("idle WAL (segment 2)", msg)
        m["segments"][1]["wal_bytes_per_hour_settled"] = int(6.5 * 1048576)  # the bound itself holds
        self.assertEqual(chaos.check_gates(m), [])

    def test_non_idle_wal_is_not_gated(self):
        m = self.metrics()
        m["segments"][0]["wal_bytes_per_hour_settled"] = 5 * 1024**3
        self.assertEqual(chaos.check_gates(m), [])

    def test_xmin_hold(self):
        m = self.metrics()
        m["segments"][0]["xmin_hold_s_settled_max"] = 3
        (msg,) = chaos.check_gates(m)
        self.assertIn("backend_xmin hold (steady segment 1)", msg)
        m["segments"][0]["xmin_hold_s_settled_max"] = 2  # the bound itself holds
        self.assertEqual(chaos.check_gates(m), [])

    def test_xmin_hold_deliberate_step_is_exempt(self):
        m = self.metrics()
        m["segments"][2].update(steps=["long-txn:60"], xmin_hold_s_settled_max=60)
        self.assertEqual(chaos.check_gates(m), [])

    def test_warn_in_quiet_idle(self):
        m = self.metrics()
        m["segments"][1]["warn"] = {"deadpool: Connection error": 2}
        (msg,) = chaos.check_gates(m)
        self.assertIn("WARN in a quiet idle (segment 2)", msg)

    def test_error_in_quiet_idle(self):
        m = self.metrics()
        m["segments"][1]["error"] = {"trellis: apply failed": 3}
        (msg,) = chaos.check_gates(m)
        self.assertIn("ERROR in a quiet idle (segment 2): 3 lines, e.g. trellis: apply failed", msg)
        self.assertNotIn("WARN", msg)

    def test_error_in_faulted_idle_is_not_gated(self):
        m = self.metrics()
        m["segments"][2]["error"] = {"trellis: apply failed": 9}
        self.assertEqual(chaos.check_gates(m), [])

    def test_every_breach_is_reported(self):
        m = self.metrics()
        m["segments"][1].update(wal_bytes_per_hour_settled=10 * 1048576, warn={"x": 1})
        m["after_idle"][0].update(latency=1.25, ratio=5.0)
        self.assertEqual(len(chaos.check_gates(m)), 3)

    def test_summarize_settled_hold_skips_the_drain(self):
        start = 1000.0
        segments = loadgen.parse_schedule("idle:400", 10)
        samples = [
            {"epoch": start + t, "xmin_hold_s": str(h), "xmin_age": "0", "idle_in_txn": "0", "rss_kb": 1024}
            for t, h in ((10, 30), (100, 0), (300, 1))
        ]
        (seg,) = chaos.summarize(start, segments, [], samples, [], [], [])["segments"]
        self.assertEqual((seg["xmin_hold_s_max"], seg["xmin_hold_s_settled_max"]), (30, 1))  # settle = 100 s


class LoadgenRevive(unittest.TestCase):
    class Conn:
        def __init__(self, broken):
            self.broken, self.closed = broken, False

        def cursor(self):
            if self.broken:
                raise loadgen.psycopg2.OperationalError("server closed the connection unexpectedly")
            return self

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql):
            self.sql = sql

        def close(self):
            self.closed = True

    def test_live_connection_is_kept(self):
        c = self.Conn(False)
        self.assertIs(loadgen.revive(c), c)
        self.assertEqual(c.sql, "select 1")

    def test_broken_connection_is_dropped(self):
        c = self.Conn(True)
        self.assertIsNone(loadgen.revive(c))
        self.assertTrue(c.closed)


class LoadgenWorker(unittest.TestCase):
    """worker() against fake connections: a failed write counts once and is never retried."""

    class Conn:
        def __init__(self, log, writes=(False,), probe_fails=False):
            self.log, self.writes, self.probe_fails = log, list(writes), probe_fails  # writes: fails?
            self.rowcount = 1

        def cursor(self):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql, params=None):
            probe = sql == "select 1"
            self.log.append("probe" if probe else "write")
            if self.probe_fails if probe else self.writes.pop(0):
                raise loadgen.psycopg2.OperationalError("server closed the connection unexpectedly")

        def close(self):
            pass

    class Pacer:
        def __init__(self, n):
            self.n = n

        def take(self, now):
            if self.n == 0:
                return None
            self.n -= 1
            return now, 0

    def run_worker(self, conns, slots):
        """Runs worker() over `slots` statements with every connection treated as idle."""
        args = type("A", (), {"dsn": "x", "accounts": 10, "orders": 10})()
        counters, pending = loadgen.Counters(), list(conns)
        saved = (loadgen.psycopg2.connect, loadgen.IDLE_PING_SECS, loadgen.time.sleep)
        loadgen.psycopg2.connect = lambda *a, **k: pending.pop(0)
        loadgen.IDLE_PING_SECS, loadgen.time.sleep = -1, lambda s: None
        try:
            loadgen.worker(args, loadgen.parse_mix(""), self.Pacer(slots), counters)
        finally:
            loadgen.psycopg2.connect, loadgen.IDLE_PING_SECS, loadgen.time.sleep = saved
        return (counters.ok, counters.err), pending

    def test_failed_write_counts_and_is_not_retried(self):
        log = []
        counts, pending = self.run_worker([self.Conn(log, [True]), self.Conn(log)], slots=1)
        self.assertEqual((counts, log, len(pending)), ((0, 1), ["write"], 1))

    def test_probe_does_not_hide_a_failing_write(self):
        log = []
        counts, _ = self.run_worker([self.Conn(log, [False, True])], slots=2)
        self.assertEqual((counts, log), ((1, 1), ["write", "probe", "write"]))

    def test_dead_idle_connection_is_replaced_before_the_write(self):
        log = []
        counts, pending = self.run_worker([self.Conn(log, probe_fails=True), self.Conn(log)], slots=2)
        self.assertEqual((counts, log, pending), ((2, 0), ["write", "probe", "write"], []))


class RunUsage(unittest.TestCase):
    def test_schedule_errors_are_usage_errors(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = ["--dir", tmp, "run", "--no-logbook", "--step", "sleep:1"]
            self.assertEqual(chaos.main(base + ["--schedule", "steady:5", "--load-secs", "5"]), 64)
            self.assertEqual(chaos.main(base + ["--schedule", "burst:5"]), 64)
            self.assertEqual(chaos.main(base + ["--schedule", "steady:5"]), 64)  # not set up


if __name__ == "__main__":
    unittest.main()
