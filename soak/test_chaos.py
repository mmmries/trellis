"""Unit tests for the harness's pure parts.

Run: PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s soak -p 'test_*.py'
"""

import os
import sys
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


class Oracle(unittest.TestCase):
    def test_parse(self):
        self.assertEqual(chaos.parse_oracle("acct_view|0\norder_view|12\n\nnoise"), {"acct_view": 0, "order_view": 12})

    def test_one_line_per_definition(self):
        sql = open(os.path.join(chaos.HERE, "oracle.sql")).read()
        defs = [d.split()[1] for d in open(os.path.join(chaos.HERE, "defs.txt")) if d.strip()]
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
