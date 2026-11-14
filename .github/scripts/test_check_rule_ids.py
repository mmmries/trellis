"""Unit tests for check-rule-ids.py.

Run: PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s .github/scripts -p 'test_*.py'
"""
import importlib.machinery
import importlib.util
import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.dont_write_bytecode = True
_loader = importlib.machinery.SourceFileLoader("check_rule_ids", os.path.join(HERE, "check-rule-ids.py"))
_spec = importlib.util.spec_from_loader("check_rule_ids", _loader)
rules = importlib.util.module_from_spec(_spec)
_loader.exec_module(rules)

INDEX = rules.INDEX
LIB = "trellis/src/lib.rs"


def index(*ids, prose=""):
    """An index doc whose rule tables list `ids`, one linked or plain row each."""
    rows = "".join(
        f"| [{i}](../decisions/0002-async-data-flow.md#x) | rule | doc | fn | - | - |\n"
        if n % 2
        else f"| {i} | rule | doc | fn | - | - |\n"
        for n, i in enumerate(ids)
    )
    return (
        "# Invariants\n\n| Essential | Postgres |\n|---|---|\n"
        f"| holds (I7) under a lock | {prose} |\n\n## Rule index\n\n"
        "| Id | Rule | Owning section | Enforced by | Pin | Plant |\n|---|---|---|---|---|---|\n" + rows
    )


def run(source, ids=("I1", "R7"), doc=None, path=LIB):
    """Writes the index and one source file under a temp dir and returns check()'s problems."""
    with tempfile.TemporaryDirectory() as root:
        for rel, text in ((INDEX, doc if doc is not None else index(*ids)), (path, source)):
            os.makedirs(os.path.dirname(os.path.join(root, rel)), exist_ok=True)
            with open(os.path.join(root, rel), "w", encoding="utf-8") as f:
                f.write(text)
        return rules.check(root, [path])


def cited(source):
    return [rule for _, rule in rules.citations(source)]


class Citations(unittest.TestCase):
    def test_every_comment_form(self):
        src = "//! I0 here\n/// I1 and R7\n// Rx3, JR2 and W1\nlet x = 1; // I8 trailing\n"
        self.assertEqual(cited(src), ["I0", "I1", "R7", "Rx3", "JR2", "W1", "I8"])

    def test_ids_in_code_are_not_citations(self):
        self.assertEqual(cited("const I2: ValueType = ValueType::Integer(IntWidth::Int2);\nlet R1 = 1;\n"), [])

    def test_ids_inside_words_are_not_citations(self):
        src = "// R10 AR1 R1x Rx1b _I2 I2_ JR10 WI1 Rx9 R0 I9 W4 R19 xI1\n"
        self.assertEqual(cited(src), [])

    def test_punctuation_is_a_boundary(self):
        src = "// (I1), R7. I2: Rx4; [W3] I5-I7 R1/R2 `JR1`\n"
        self.assertEqual(cited(src), ["I1", "R7", "I2", "Rx4", "W3", "I5", "I7", "R1", "R2", "JR1"])

    def test_a_slashes_inside_a_string_start_no_comment(self):
        self.assertEqual(cited('let u = "http://x/I1"; let v = 2;\n'), [])
        self.assertEqual(cited('let u = "http://x"; // I1 after\n'), ["I1"])

    def test_escaped_quote_and_char_literals(self):
        self.assertEqual(cited('let s = "a\\"// I1"; let c = \'"\'; // R7\n'), ["R7"])

    def test_the_line_number_is_reported(self):
        self.assertEqual(list(rules.citations("fn f() {}\n\n// I4\n")), [(3, "I4")])


class Index(unittest.TestCase):
    def test_first_cells_only(self):
        doc = index("I1", "R7", "Rx5", prose="R9")
        self.assertEqual(rules.index_ids(doc), {"I1", "R7", "Rx5"})

    def test_rows_above_the_heading_and_rule_text_do_not_count(self):
        doc = index("I1") + "| I5 is mentioned in a first cell sentence | x |\n"
        self.assertEqual(rules.index_ids(doc), {"I1"})

    def test_no_heading_is_no_ids(self):
        self.assertEqual(rules.index_ids("| I1 | x |\n"), set())


class Check(unittest.TestCase):
    def test_present_ids_pass(self):
        self.assertEqual(run("/// I1 and R7 (ADR-0002)\nfn f() {}\n"), [])

    def test_a_cited_id_missing_from_the_index_fails(self):
        problems = run("fn f() {}\n// R7 ok\n// Rx5 not listed\n")
        self.assertEqual(len(problems), 1)
        self.assertIn(f"{LIB}:3: rule id Rx5", problems[0])

    def test_removing_a_row_fails(self):
        src = "// I1\n// R7\n"
        self.assertEqual(run(src, ids=("I1", "R7")), [])
        self.assertEqual(len(run(src, ids=("I1",))), 1)

    def test_a_longer_id_does_not_satisfy_a_shorter_one(self):
        self.assertEqual(len(run("// R1\n", ids=("R10", "Rx1", "JR1"))), 1)

    def test_lookalikes_never_need_a_row(self):
        self.assertEqual(run("// R10 AR1 Rx1b\nconst I2: u8 = 2;\n", ids=("I1",)), [])

    def test_only_trellis_src_is_scanned(self):
        self.assertEqual(run("// R3 local label\n", path="trellis/tests/x.rs"), [])
        self.assertEqual(run("// R3 local label\n", path="trellis/src/x.md"), [])

    def test_a_missing_or_empty_index_fails(self):
        self.assertEqual(len(run("// I1\n", doc="# no rule index here\n")), 1)

    def test_a_missing_index_file_fails(self):
        with tempfile.TemporaryDirectory() as root:
            self.assertIn("missing", rules.check(root, [LIB])[0])


class CurrentTree(unittest.TestCase):
    def test_the_repository_passes(self):
        files = [
            os.path.relpath(os.path.join(d, f), rules.ROOT)
            for d, _, names in os.walk(os.path.join(rules.ROOT, rules.SCANNED))
            for f in names
        ]
        self.assertEqual(rules.check(rules.ROOT, files), [])


if __name__ == "__main__":
    unittest.main()
