#!/usr/bin/env python3
"""Fails when a `trellis/src` comment cites a rule id the rule index doesn't list.

The index is the tables under "Rule index" in
docs/staging-and-claiming/08-invariants-and-pins.md: a row's first cell is the id.
The rules are defined in the ADRs (ADR-0002, 0019, 0028); the index points at them.

The id vocabulary is `I0`-`I8`, `R1`-`R9`, `Rx1`-`Rx8`, `JR1`-`JR3` and `W1`-`W3`. A
citation is one of those tokens in comment text (`//`, `///`, `//!`, whole-line or
trailing) with no letter, digit or underscore either side, so `R10`, `AR1`, `Rx1b`
and `I2_000` are not citations. A row's first cell may be a Markdown link.

Only `trellis/src` is scanned: tests keep local scenario labels (`R1`-`R19` in the
star-join families) that are not these rules.

Run: python3 .github/scripts/check-rule-ids.py   (tests: test_check_rule_ids.py)
"""
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
INDEX = "docs/staging-and-claiming/08-invariants-and-pins.md"
INDEX_HEADING = "## Rule index"
SCANNED = "trellis/src"

ID = r"(?:I[0-8]|R[1-9]|Rx[1-8]|JR[1-3]|W[1-3])"
CITATION = re.compile(r"(?<![A-Za-z0-9_])(" + ID + r")(?![A-Za-z0-9_])")
CELL_ID = re.compile(ID)
LINK = re.compile(r"\[([^\]]*)\]\([^)]*\)")


def comment_text(line):
    """The text of `line`'s `//` comment, or "" when it has none.

    Quotes are tracked so a `//` inside a string literal (a URL) starts no comment.
    Char literals (`'"'`) are skipped; lifetimes (`'a`) need no skipping.
    """
    i, n, in_str = 0, len(line), False
    while i < n:
        c = line[i]
        if in_str:
            if c == "\\":
                i += 1
            elif c == '"':
                in_str = False
        elif c == '"':
            in_str = True
        elif c == "'":
            m = re.match(r"'(?:\\.[^']*|[^\\'])'", line[i:])
            if m:
                i += m.end() - 1
        elif line.startswith("//", i):
            return line[i:]
        i += 1
    return ""


def index_ids(text):
    """The ids whose rows the index tables list: the first cell of each table row below the heading."""
    start = text.find("\n" + INDEX_HEADING + "\n")
    if start < 0:
        return set()
    ids = set()
    for line in text[start:].split("\n"):
        if not line.startswith("|"):
            continue
        first = line.split("|")[1].strip() if line.count("|") > 1 else ""
        first = LINK.sub(r"\1", first)
        if CELL_ID.fullmatch(first):
            ids.add(first)
    return ids


def citations(text):
    """Yields (line number, id) for every rule id cited in a comment of `text`."""
    for n, line in enumerate(text.split("\n"), 1):
        if "//" not in line:
            continue
        for m in CITATION.finditer(comment_text(line)):
            yield n, m.group(1)


def check(root, files):
    """The problems found in `files` (paths under `root`) against the index under `root`."""
    try:
        with open(os.path.join(root, INDEX), encoding="utf-8") as f:
            listed = index_ids(f.read())
    except FileNotFoundError:
        return [f"{INDEX}: the rule index is missing"]
    if not listed:
        return [f"{INDEX}: no rule ids found under '{INDEX_HEADING}'"]
    problems = []
    for rel in files:
        if not rel.endswith(".rs") or not rel.startswith(SCANNED + "/"):
            continue
        with open(os.path.join(root, rel), encoding="utf-8") as f:
            text = f.read()
        for n, rule in citations(text):
            if rule not in listed:
                problems.append(f"{rel}:{n}: rule id {rule} is cited but has no row in {INDEX}")
    return problems


def main():
    files = subprocess.check_output(["git", "ls-files", SCANNED], cwd=ROOT, text=True).split()
    problems = check(ROOT, files)
    for p in problems:
        print(p)
    if problems:
        print(f"{len(problems)} cited rule id(s) missing from the index")
        return 1
    print(f"ok: every rule id cited in {SCANNED} comments is in the index")
    return 0


if __name__ == "__main__":
    sys.exit(main())
