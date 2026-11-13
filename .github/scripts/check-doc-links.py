#!/usr/bin/env python3
"""Fails when a Markdown link or a `docs/....md#anchor` reference in source points nowhere.

Checks, over tracked files only:
  * `.md`: every `[text](target)` and `[label]: target` whose target is a relative path. The
    file or directory must exist, and a `#fragment` into a `.md` file must match one of its
    heading anchors (GitHub's slug rules) or an explicit `<a id=...>`.
  * `.rs`/`.rb`/`.ex`/`.exs`: every `path.md#anchor` mention, the path resolved from the repo
    root (or from the file's directory when it starts with `.`).

Run: python3 .github/scripts/check-doc-links.py   (tests: test_check_doc_links.py)
"""
import os
import re
import subprocess
import sys
from urllib.parse import unquote

SOURCE_EXTS = (".rs", ".rb", ".ex", ".exs")
FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})")
HEADING = re.compile(r"^ {0,3}#{1,6}[ \t]+(.*?)(?:[ \t]+#+)?[ \t]*$")
CODE_SPAN = re.compile(r"(`+)(.+?)\1")
INLINE_LINK = re.compile(r"\]\(\s*(<[^>]*>|[^)\s]*(?:\([^)\s]*\)[^)\s]*)*)")
REF_DEF = re.compile(r"^ {0,3}\[[^\]]+\]:\s*(<[^>]*>|\S+)")
EXPLICIT_ANCHOR = re.compile(r"""<a\s[^>]*?\b(?:id|name)=["']([^"']+)["']""")
SCHEME = re.compile(r"^(?:[a-zA-Z][a-zA-Z0-9+.-]*:|//)")
SOURCE_REF = re.compile(r"(?<![\w/.~-])((?:\.\.?/)*(?:[\w.-]+/)*[\w.-]+\.md)#([\w%-]+)")


def lines_outside_fences(text):
    """Yields (line number, line) for lines not inside a fenced code block."""
    fence = None
    for n, line in enumerate(text.split("\n"), 1):
        m = FENCE.match(line)
        if fence:
            if m and m.group(1)[0] == fence[0] and len(m.group(1)) >= len(fence) and not line.strip(" `~"):
                fence = None
            continue
        if m:
            fence = m.group(1)
            continue
        yield n, line


def slug(heading):
    """GitHub's anchor for heading text, before the duplicate suffix."""
    t = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", heading)  # links, images
    t = re.sub(r"\[([^\]]*)\]\[[^\]]*\]", r"\1", t)  # reference links
    t = CODE_SPAN.sub(lambda m: m.group(2).strip(), t)
    t = re.sub(r"<[^>]+>", "", t)  # inline HTML
    t = re.sub(r"(?<![\w*])_+([^_]+?)_+(?!\w)", r"\1", t)  # _emphasis_ (underscores in words stay)
    t = re.sub(r"[^\w\- ]", "", t.lower())
    return t.replace(" ", "-")


def anchors(text):
    """The set of fragments a link into this Markdown text can use."""
    found, seen = set(), {}
    for _, line in lines_outside_fences(text):
        found.update(a.lower() for a in EXPLICIT_ANCHOR.findall(line))
        m = HEADING.match(line)
        if m:
            s = slug(m.group(1))
            k = seen.get(s, 0)
            seen[s] = k + 1
            found.add(s if k == 0 else f"{s}-{k}")
    return found


def split_target(raw):
    raw = raw.strip()
    if raw.startswith("<") and raw.endswith(">"):
        raw = raw[1:-1]
    path, _, frag = raw.partition("#")
    return unquote(path.split("?")[0]), unquote(frag).lower()


def md_targets(text):
    """Yields (line number, raw target) for each link and reference definition in Markdown."""
    for n, line in lines_outside_fences(text):
        m = REF_DEF.match(line)
        if m:
            yield n, m.group(1)
        stripped = CODE_SPAN.sub(lambda c: " " * len(c.group(0)), line)
        for m in INLINE_LINK.finditer(stripped):
            yield n, m.group(1)


def check(root, files):
    """Returns `file:line: message` problems for the given repo-relative files under root."""
    problems, cache = [], {}

    def anchors_of(rel):
        if rel not in cache:
            with open(os.path.join(root, rel), encoding="utf-8") as f:
                cache[rel] = anchors(f.read())
        return cache[rel]

    def resolve(src, path, frag, n, shown):
        if path.startswith("/"):
            rel = os.path.normpath(path.lstrip("/"))
        else:
            rel = os.path.normpath(os.path.join(os.path.dirname(src), path)) if path else src
        full = os.path.join(root, rel)
        if rel.startswith("..") or not os.path.exists(full):
            problems.append(f"{src}:{n}: broken link: {shown} does not exist")
        elif frag and rel.endswith(".md") and os.path.isfile(full) and frag not in anchors_of(rel):
            problems.append(f"{src}:{n}: broken anchor: no heading '#{frag}' in {rel}")

    for src in files:
        if not (src.endswith(".md") or src.endswith(SOURCE_EXTS)):
            continue
        try:
            with open(os.path.join(root, src), encoding="utf-8") as f:
                text = f.read()
        except (OSError, UnicodeDecodeError):
            continue
        if src.endswith(".md"):
            for n, raw in md_targets(text):
                if not raw or SCHEME.match(raw.strip("<>")):
                    continue
                path, frag = split_target(raw)
                if path or frag:
                    resolve(src, path, frag, n, path or "#" + frag)
        else:
            for n, line in enumerate(text.split("\n"), 1):
                for m in SOURCE_REF.finditer(line):
                    path = m.group(1)
                    if path.startswith("."):
                        path = os.path.join(os.path.dirname(src), path)
                    resolve(src, "/" + os.path.normpath(path), m.group(2).lower(), n, m.group(1))
    return problems


def main():
    root = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()
    files = subprocess.check_output(["git", "-C", root, "ls-files"], text=True).splitlines()
    problems = check(root, files)
    for p in problems:
        print(p)
    if problems:
        print(f"{len(problems)} broken doc link(s)")
        sys.exit(1)
    print(f"ok: doc links and anchors resolve across {sum(f.endswith('.md') for f in files)} Markdown files")


if __name__ == "__main__":
    main()
