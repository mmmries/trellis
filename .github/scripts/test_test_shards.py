"""Unit tests for `test-shards.py check`.

Run: PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s .github/scripts -p 'test_*.py'
"""
import contextlib
import importlib.machinery
import importlib.util
import io
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.dont_write_bytecode = True
_loader = importlib.machinery.SourceFileLoader("test_shards", os.path.join(HERE, "test-shards.py"))
_spec = importlib.util.spec_from_loader("test_shards", _loader)
shards = importlib.util.module_from_spec(_spec)
_loader.exec_module(shards)


def run_check(units, weights):
    """Runs `check` against the given units and weights; returns (exit code or None, output)."""
    shards.units = lambda: units
    shards.read_weights = lambda: weights
    shards.shard_count = lambda: 2
    out = io.StringIO()
    code = None
    with contextlib.redirect_stdout(out):
        try:
            shards.cmd_check()
        except SystemExit as e:
            code = e.code
    return code, out.getvalue()


# Excerpt of CI run 43618059444, job "test (shard 2)": stderr `Running` lines and
# stdout `test result` lines are out of step (a result can follow the next
# binary's `Running` line).
ESC = "\x1b"
INTERLEAVED_LOG = f"""\
2026-11-14T16:54:35.6185204Z {ESC}[1m{ESC}[92m     Running{ESC}[0m unittests src/bin/engine_subprocess.rs (target/debug/deps/engine_subprocess-56b8b41aeb09f714)
2026-11-14T16:54:35.6198894Z test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
2026-11-14T16:54:35.6199856Z {ESC}[1m{ESC}[92m     Running{ESC}[0m unittests src/main.rs (target/debug/deps/generative-bc2945292d2f49d8)
2026-11-14T16:54:35.6217508Z {ESC}[1m{ESC}[92m     Running{ESC}[0m tests/backup_restore.rs (target/debug/deps/backup_restore-5091450e91f38355)
2026-11-14T16:54:35.6218468Z test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
2026-11-14T16:54:55.0459506Z test result: ok. 4 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 19.42s
2026-11-14T16:55:00.0000000Z test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 4.00s
2026-11-14T16:54:55.0477292Z {ESC}[1m{ESC}[92m     Running{ESC}[0m tests/db_admin.rs (target/debug/deps/db_admin-2bb754e0dd59f482)
2026-11-14T16:55:11.8205386Z {ESC}[1m{ESC}[92m     Running{ESC}[0m unittests src/bin/trellis-testkit.rs (target/debug/deps/trellis_testkit-fb0a04fe493f385f)
2026-11-14T16:55:11.8224219Z {ESC}[1m{ESC}[92m     Running{ESC}[0m tests/capture_metrics/main.rs (target/debug/deps/capture_metrics-98319b436171f77a)
2026-11-14T16:55:11.8225468Z test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
2026-11-14T16:55:30.0000000Z test result: ok. 14 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 17.90s
2026-11-14T16:55:30.0000001Z    Doc-tests trellis
2026-11-14T16:55:31.0000000Z test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.50s
"""


def run_weights(log, units):
    """Runs `weights` on a log text; returns {unit: seconds} from the output."""
    import tempfile

    shards.units = lambda: units
    with tempfile.NamedTemporaryFile("w", suffix=".log", delete=False) as f:
        f.write(log)
    out = io.StringIO()
    try:
        with contextlib.redirect_stdout(out):
            shards.cmd_weights([f.name])
    finally:
        os.unlink(f.name)
    got = {}
    for line in out.getvalue().splitlines():
        if line.startswith("#") or "not in the log" in line:
            continue
        name, secs = line.split()
        got[name] = float(secs)
    return got


class Weights(unittest.TestCase):
    UNITS = [
        "doc", "lib", "bin:engine_subprocess", "bin:generative", "bin:trellis-testkit",
        "test:backup_restore", "test:db_admin", "test:capture_metrics",
    ]

    def test_pairs_results_with_running_lines_when_streams_interleave(self):
        got = run_weights(INTERLEAVED_LOG, self.UNITS)
        self.assertEqual(got["test:backup_restore"], 19.4)
        self.assertEqual(got["test:capture_metrics"], 17.9)
        self.assertEqual(got["bin:generative"], 0.0)
        self.assertEqual(got["bin:trellis-testkit"], 0.0)
        self.assertEqual(got["test:db_admin"], 4.0)

    def test_doc_tests_are_not_attributed_to_a_binary(self):
        got = run_weights(INTERLEAVED_LOG, self.UNITS)
        self.assertEqual(got["doc"], shards.DOC_DEFAULT_SECONDS)
        self.assertNotIn(0.5, got.values())
        self.assertNotIn(None, got)


class Check(unittest.TestCase):
    UNITS = ["doc", "lib", "test:a", "test:b"]

    def test_accepts_matching_weights(self):
        code, out = run_check(self.UNITS, {"doc": 1.0, "lib": 2.0, "test:a": 3.0, "test:b": 4.0})
        self.assertIsNone(code)
        self.assertIn("ok: 4 units", out)

    def test_warns_but_passes_for_an_unweighted_unit(self):
        code, out = run_check(self.UNITS, {"lib": 2.0, "test:a": 3.0})
        self.assertIsNone(code)
        self.assertIn("test:b", out)

    def test_fails_on_a_stale_entry(self):
        code, out = run_check(self.UNITS, {"lib": 2.0, "test:a": 3.0, "test:gone": 9.0})
        self.assertEqual(code, 1)
        self.assertIn("no longer exist", out)
        self.assertIn("test:gone", out)


if __name__ == "__main__":
    unittest.main()
