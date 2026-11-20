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


if __name__ == "__main__":
    unittest.main()
