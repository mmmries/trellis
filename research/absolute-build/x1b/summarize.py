"""Summarize soak.jsonl into the X1 soak table (markdown) or a one-line note."""

import json
import sys
from collections import defaultdict

EXPECT = {"T0": "exact", "A": "exact", "B": "exact", "A-gc": "wrong", "A-2pass": "deadlock",
          "A-noenum": "wrong", "N": "wrong", "T0-early": "wrong", "B-early": "wrong"}


def load(path):
    return [json.loads(l) for l in open(path) if l.strip()]


def table(rows):
    g = defaultdict(list)
    for r in rows:
        g[(r["variant"], r.get("jitter_ms", 40))].append(r)
    out = ["| variant | jitter ms | expect | runs | divergent runs (final / any checkpoint) | page deadlocks | build deadlocks "
           "| builds | stragglers / enumerated (fresh; in place; corrupted) | page lock wait p99 / max ms | harness errors |",
           "|---|---|---|---|---|---|---|---|---|---|---|"]
    order = list(EXPECT)
    for (v, j) in sorted(g, key=lambda k: (order.index(k[0]), -k[1])):
        rs = g[(v, j)]
        he = sum(1 for r in rs if "harness_error" in r or r.get("errors"))
        ok = [r for r in rs if "harness_error" not in r]
        fin = sum(1 for r in ok if r.get("divergent_final"))
        anyd = sum(1 for r in ok if r.get("divergent"))
        pdl = sum(r["page_deadlocks"] for r in ok)
        bdl = sum(r["build_deadlocks"] for r in ok)
        builds = sum(r["builds_total"] for r in ok)

        def st(mode):
            s = sum((r["stragglers"][mode] or {}).get("stragglers", 0) for r in ok)
            e = sum((r["stragglers"][mode] or {}).get("enumerated", 0) for r in ok)
            return f"{s}/{e}"

        strag = f"{st('fresh')}; {st('inplace')}; {st('corrupt')}" if v.startswith("A") else "n/a"
        p99 = sorted(r["lock_wait_ms"]["p99"] or 0 for r in ok)
        mx = max((r["lock_wait_ms"]["max"] or 0) for r in ok) if ok else 0
        p99m = p99[len(p99) // 2] if p99 else 0
        out.append(f"| {v} | {j} | {EXPECT[v]} | {len(rs)} | {fin} / {anyd} | {pdl} | {bdl} | {builds} | {strag} "
                   f"| {p99m} (median run) / {mx} | {he} |")
    return "\n".join(out)


def oneline(rows):
    g = defaultdict(lambda: [0, 0, 0])
    for r in rows:
        x = g[r["variant"]]
        x[0] += 1
        x[1] += 1 if r.get("divergent") else 0
        x[2] += r.get("page_deadlocks", 0) + r.get("build_deadlocks", 0)
    return "; ".join(f"{v} {n} runs {d} div {dl} dl" for v, (n, d, dl) in g.items())


if __name__ == "__main__":
    rows = load(sys.argv[1])
    if "--oneline" in sys.argv:
        print(oneline(rows))
    else:
        print(table(rows))
        errs = [r for r in rows if "harness_error" in r or r.get("errors")]
        for r in errs[:5]:
            print("ERR", r["variant"], r["seed"], r["appliers"], r.get("errors"), r.get("harness_error", "")[-400:])
        dd = [d for r in rows if r["variant"] in ("A", "B", "T0") for d in r.get("deadlock_details", [])]
        for d in dd[:5]:
            print("DEADLOCK(exact-expected variant)", d)
