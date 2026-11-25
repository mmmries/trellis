"""Unit tests for the canary's pure parts.

Run: PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s soak -p 'test_*.py'
"""

import argparse
import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from unittest import mock

sys.dont_write_bytecode = True
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import canary  # noqa: E402
import loadgen  # noqa: E402

DAY = 86400


class Rounds(unittest.TestCase):
    def test_every_round_parses_with_an_idle_and_a_steady(self):
        for r in canary.ROUNDS:
            profiles = [p for p, _, _ in loadgen.parse_schedule(r, canary.TPS)]
            self.assertIn("idle", profiles, r)
            self.assertIn("steady", profiles, r)

    def test_weighted_to_idle_and_trickle(self):
        quiet = total = 0
        for r in canary.ROUNDS:
            for profile, secs, _ in loadgen.parse_schedule(r, canary.TPS):
                total += secs
                quiet += secs if profile in ("idle", "trickle") else 0
        self.assertGreater(quiet / total, 0.8)

    def test_rotation(self):
        self.assertEqual(canary.pick_round(0), canary.ROUNDS[0])
        self.assertEqual(canary.pick_round(len(canary.ROUNDS) + 1), canary.ROUNDS[1])


class Churn(unittest.TestCase):
    def test_pause_and_redefine_differ_and_exist(self):
        for targets in (list(canary.all_defs()), ["acct_names", "region_totals", "order_view", "region_order_totals"]):
            seen = set()
            for n in range(20):
                plan = canary.churn_plan(n, targets)
                self.assertNotEqual(plan["pause"], plan["redefine"])
                self.assertIn(plan["pause"], targets)
                self.assertIn(plan["redefine"], targets)
                seen |= {plan["pause"], plan["redefine"]}
            self.assertEqual(seen, {t for t in targets if t in canary.CHURNABLE})

    def test_nothing_reads_a_churned_target(self):
        defs = canary.all_defs()
        for target in canary.CHURNABLE:
            readers = [t for t, d in defs.items() if t != target and f" {target} " in f" {d} "]
            self.assertEqual(readers, [], target)

    def test_migrations_cycle(self):
        names = [canary.churn_plan(n, canary.CHURNABLE)["migration"][0] for n in range(len(canary.MIGRATIONS) * 2)]
        self.assertEqual(names[: len(canary.MIGRATIONS)], names[len(canary.MIGRATIONS) :])
        self.assertEqual(len(set(names)), len(canary.MIGRATIONS))

    def test_due(self):
        self.assertTrue(canary.due(None, DAY, 5))
        self.assertFalse(canary.due(100, DAY, 100 + DAY - 1))
        self.assertTrue(canary.due(100, DAY, 100 + DAY))


class Oracle(unittest.TestCase):
    def test_parts_cover_every_definition(self):
        targets = list(canary.all_defs())
        parts = canary.oracle_parts(targets)
        self.assertEqual([p.split("'")[1] for p in parts], targets)
        self.assertTrue(all(p.startswith("select '") and not p.endswith(";") for p in parts))
        self.assertEqual(targets[0], canary.PROBE_TARGET)


class Home(unittest.TestCase):
    def test_refuses_paths_cleanups_delete(self):
        self.assertIn("/tmp", canary.check_home("/tmp/canary", 10**12))
        self.assertIn("target/", canary.check_home("/home/x/code/trellis/target/canary", 10**12))

    def test_refuses_a_full_disk(self):
        self.assertIn("GB free", canary.check_home(os.path.expanduser("~/canary"), 2**30))

    def test_accepts(self):
        self.assertIsNone(canary.check_home(os.path.expanduser("~/canary"), 10**12))


class Status(unittest.TestCase):
    def test_contract(self):
        doc = canary.status_doc("ok", "running", "2026-11-25T00:00:00+00:00", "s", None, "0.3.1", now=0)
        self.assertEqual(
            set(doc), {"state", "phase", "heartbeat_at", "checked_at", "summary", "report", "version", "weekly"}
        )
        self.assertEqual(doc["heartbeat_at"], "1970-01-01T00:00:00+00:00")

    def test_beat_keeps_fields_and_writes_atomically(self):
        with tempfile.TemporaryDirectory() as d:
            c = canary.Canary(d)
            c.beat("running", state="failed", summary="boom", report="/r.md", checked_at="t", version="0.3.1")
            c.beat("held")
            with open(os.path.join(d, "status.json")) as f:
                st = json.load(f)
            self.assertEqual(
                (st["state"], st["phase"], st["summary"], st["report"]), ("failed", "held", "boom", "/r.md")
            )
            self.assertFalse(os.path.exists(os.path.join(d, "status.json.tmp")))
            self.assertEqual(canary.Canary(d).status["state"], "failed")

    def test_clear_wins_over_a_holding_daemon(self):
        with tempfile.TemporaryDirectory() as d:
            daemon = canary.Canary(d)
            daemon.beat("held", state="failed", summary="boom", report="/r.md")
            with contextlib.redirect_stdout(io.StringIO()):
                canary.cmd_clear(argparse.Namespace(home=d, port=canary.PORT))
            h = mock.Mock()
            with (
                mock.patch.object(canary.time, "sleep", side_effect=AssertionError("still holding")),
                self.assertRaises(canary.Cleared),
            ):
                canary.hold(daemon, h)
            daemon.beat("stopped")  # cmd_run's teardown
            st = canary.Canary(d).status
            self.assertEqual((st["state"], st["phase"], st["report"]), ("ok", "stopped", None))


class Daemon(unittest.TestCase):
    def test_a_harness_error_fails_and_holds(self):
        with tempfile.TemporaryDirectory() as d:
            os.makedirs(os.path.join(d, "cluster"))
            open(os.path.join(d, "cluster", "setup.json"), "w").close()
            h = mock.Mock()
            h.pg_start.side_effect = RuntimeError("no postgres")
            failed = []

            def fail(c, h, kind, detail, failures, metric_lines=()):
                failed.append((kind, failures))
                raise KeyboardInterrupt  # fail() holds forever; stop it as SIGTERM would

            args = argparse.Namespace(home=d, port=canary.PORT, converge_secs=1, rounds=1, round=None)
            with (
                mock.patch.object(canary.Canary, "harness", return_value=h),
                mock.patch.object(canary, "fail", fail),
                mock.patch.object(canary.chaos, "no_interrupts"),
                self.assertRaises(KeyboardInterrupt),
            ):
                canary.cmd_run(args)
            self.assertEqual(failed, [("error", ["harness error: RuntimeError: no postgres"])])

    def test_provision_never_touches_an_existing_cluster(self):
        with tempfile.TemporaryDirectory() as d:
            os.makedirs(os.path.join(d, "cluster", "data"))  # a provision that never wrote setup.json
            args = argparse.Namespace(home=d, port=canary.PORT, bin="/bin/true", without="", accounts=1, orders=1)
            with self.assertRaisesRegex(canary.UsageError, "never reset"):
                canary.cmd_provision(args)
            self.assertTrue(os.path.isdir(os.path.join(d, "cluster", "data")))


class Weekly(unittest.TestCase):
    def rows(self, now):
        out = []
        for i in range(10):
            at = now - (9 - i) * DAY - 60  # i = 3..9 fall in the last week
            out.append(
                {
                    "at": str(at),
                    "kind": "churn" if i == 5 else "round",
                    "detail": f"{i}",
                    "version": "0.3.1",
                    "rows": "100",
                    "peak_rss_mb": str(50 + i),
                    "schema_mb": "12",
                    "target_mb": "40",
                    "ring_seq": str(1 + 40 * i),
                    "converge_s": "3",
                    "outcome": "fail" if i == 9 else "ok",
                }
            )
        return out

    def test_summary(self):
        now = 1_800_000_000
        text = canary.weekly_summary(self.rows(now), now, now - 30 * DAY, now - 3600, "0.3.1")
        self.assertIn("7 quiet points, 1 flagged, 700 rows", text)
        self.assertIn("Ring laps: 60 this week, 90 since provisioning.", text)
        self.assertIn("churn x1", text)
        self.assertIn("provisioned 30.0 days ago", text)
        self.assertIn("Flagged:", text)
        self.assertEqual(text.count("\n| 20"), 7)

    def test_history_round_trip(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "history.tsv")
            with open(path, "w") as f:
                f.write("\t".join(canary.HISTORY_COLUMNS) + "\n")
                f.write("\t".join(str(i) for i in range(len(canary.HISTORY_COLUMNS))) + "\n")
            rows = canary.read_history(path)
            self.assertEqual(rows[0]["outcome"], str(len(canary.HISTORY_COLUMNS) - 1))
            self.assertEqual(canary.read_history(os.path.join(d, "missing")), [])


if __name__ == "__main__":
    unittest.main()
