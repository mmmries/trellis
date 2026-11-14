"""Unit tests for check-doc-links.py.

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
_loader = importlib.machinery.SourceFileLoader("check_doc_links", os.path.join(HERE, "check-doc-links.py"))
_spec = importlib.util.spec_from_loader("check_doc_links", _loader)
links = importlib.util.module_from_spec(_spec)
_loader.exec_module(links)


def run(tree):
    """Writes {path: text} under a temp dir and returns check()'s problems for all of it."""
    with tempfile.TemporaryDirectory() as root:
        for rel, text in tree.items():
            os.makedirs(os.path.dirname(os.path.join(root, rel)), exist_ok=True)
            with open(os.path.join(root, rel), "w", encoding="utf-8") as f:
                f.write(text)
        return links.check(root, sorted(tree))


class Anchors(unittest.TestCase):
    def test_github_slugs(self):
        text = (
            "# Plain heading\n## `code` and *emphasis*, [a link](x.md)!\n"
            "### Snake_case & (punctuation): v1.2\n## Trailing hashes ##\n"
        )
        self.assertEqual(
            links.anchors(text),
            {"plain-heading", "code-and-emphasis-a-link", "snake_case--punctuation-v12", "trailing-hashes"},
        )

    def test_code_spans_render_verbatim_and_entities_decode(self):
        self.assertEqual(
            links.anchors("# `Vec<T>`\n# `__init__` and _em_\n# a &amp; b\n"),
            {"vect", "__init__-and-em", "a--b"},
        )

    def test_duplicates_get_numeric_suffixes(self):
        self.assertEqual(links.anchors("# Dup\n# Dup\n# Dup\n"), {"dup", "dup-1", "dup-2"})

    def test_fenced_headings_are_ignored(self):
        text = "# Real\n```sh\n# comment\n```\n~~~\n# also comment\n~~~\n"
        self.assertEqual(links.anchors(text), {"real"})

    def test_explicit_anchors(self):
        self.assertIn("mine", links.anchors('| <a id="mine"></a>row |\n<a name=\'other\'></a>\n'))


class Check(unittest.TestCase):
    def test_good_links_pass(self):
        tree = {
            "docs/a.md": "# Top\nSee [b](b.md#sub-part), [self](#top), [dir](../src), `[x](gone.md)`,\n"
            "[web](https://example.com/x.md#y) and [r][ref].\n\n[ref]: b.md\n",
            "docs/b.md": "## Sub part\n",
            "src/lib.rs": "",
        }
        self.assertEqual(run(tree), [])

    def test_broken_file(self):
        self.assertEqual(
            run({"docs/a.md": "x\n[b](missing.md)\n"}),
            ["docs/a.md:2: broken link: missing.md does not exist"],
        )

    def test_broken_reference_definition(self):
        self.assertEqual(len(run({"a.md": "[r][x]\n\n[x]: nope.md\n"})), 1)

    def test_broken_anchor(self):
        problems = run({"docs/a.md": "[b](b.md#nope)\n[s](#nope2)\n", "docs/b.md": "# Real\n"})
        self.assertEqual(
            problems,
            [
                "docs/a.md:1: broken anchor: no heading '#nope' in docs/b.md",
                "docs/a.md:2: broken anchor: no heading '#nope2' in docs/a.md",
            ],
        )

    def test_fragments_are_case_sensitive(self):
        self.assertEqual(len(run({"a.md": "# Top\n[x](#Top)\n"})), 1)

    def test_footnotes_and_parenthesized_paths(self):
        tree = {"a.md": "x[^1] [p](b(1).md) [q](c(1).md)\n\n[^1]: See the doc\n", "b(1).md": ""}
        self.assertEqual(run(tree), ["a.md:1: broken link: c(1).md does not exist"])

    def test_link_to_a_fenced_heading_is_broken(self):
        self.assertEqual(len(run({"a.md": "[x](#comment)\n```\n# comment\n```\n"})), 1)

    def test_links_in_fences_are_ignored(self):
        self.assertEqual(run({"a.md": "```\n[x](missing.md)\n```\n"}), [])

    def test_source_reference(self):
        tree = {
            "docs/d.md": "# Heading\n",
            "src/a.rs": "//! see `docs/d.md#heading`, [ADR](../docs/d.md#heading) and docs/d.md#gone.\n",
            "src/b.rs": "/// docs/missing.md#x\n",
        }
        self.assertEqual(
            run(tree),
            [
                "src/a.rs:1: broken anchor: no heading '#gone' in docs/d.md",
                "src/b.rs:1: broken link: docs/missing.md does not exist",
            ],
        )

    def test_source_reference_without_anchor_is_not_checked(self):
        self.assertEqual(run({"a.rs": "// docs/missing.md\n"}), [])


if __name__ == "__main__":
    unittest.main()
