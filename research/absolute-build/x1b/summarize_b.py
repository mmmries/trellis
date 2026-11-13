"""Summarize an X1b soak .jsonl into the results table, or a one-line note."""

import json
import sys
from collections import defaultdict

EXPECT = {"A": "exact", "A-preRHS": "exact", "A-preRHS-late": "exact", "A-s2gb": "exact", "A-s2probe": "exact",
          "A-s2each": "exact", "A-x+T": "exact", "L-A": "exact", "L-A-preRHS": "exact", "L-A-preRHS-span": "exact",
          "L-A-preRHS+T": "exact", "L-B": "exact", "L-B+T": "exact", "L-Bdef": "exact",
          "A-preRH": "wrong", "L-A-preRH": "wrong", "A-preRHS-nogc": "wrong", "L-A-nogc": "wrong",
          "A-notarget+T": "wrong", "L-N": "wrong"}


def load(path):
    return [json.loads(l) for l in open(path) if l.strip()]


def med(xs):
    xs = sorted(xs)
    return xs[len(xs) // 2] if xs else 0


def table(rows):
    g = defaultdict(list)
    for r in rows:
        g[(r["variant"], r.get("jitter_ms", 40))].append(r)
    out = ["| variant | jitter ms | expect | runs | divergent runs (final / any checkpoint) | page / build deadlocks "
           "| builds | stragglers / enumerated (fresh; in place; corrupted; resume) | page wait s per s (median run) "
           "| page lock wait p99 median run / max ms | cross-leaf groups | harness errors |",
           "|---|---|---|---|---|---|---|---|---|---|---|---|"]
    order = list(EXPECT)
    for (v, j) in sorted(g, key=lambda k: (order.index(k[0]) if k[0] in order else 99, -k[1])):
        rs = g[(v, j)]
        he = sum(1 for r in rs if "harness_error" in r or r.get("errors"))
        ok = [r for r in rs if "harness_error" not in r]
        fin = sum(1 for r in ok if r.get("divergent_final"))
        anyd = sum(1 for r in ok if r.get("divergent"))
        pdl = sum(r["page_deadlocks"] for r in ok)
        bdl = sum(r["build_deadlocks"] for r in ok)
        builds = sum(r["builds_total"] for r in ok)

        def st(mode):
            s = sum(((r["stragglers"].get(mode) or {}).get("stragglers", 0)) for r in ok)
            e = sum(((r["stragglers"].get(mode) or {}).get("enumerated", 0)) for r in ok)
            return f"{s}/{e}"

        strag = (f"{st('fresh')}; {st('inplace')}; {st('corrupt')}; {st('resume')}"
                 if "-B" not in v and v != "L-N" else "n/a")
        pw = med([r["page_wait_s"] / r["elapsed"] for r in ok if r.get("elapsed")])
        p99 = med([r["lock_wait_ms"]["p99"] or 0 for r in ok])
        mx = max((r["lock_wait_ms"]["max"] or 0) for r in ok) if ok else 0
        xl = sum(r.get("xleaf", 0) for r in ok)
        out.append(f"| {v} | {j} | {EXPECT.get(v, '?')} | {len(rs)} | {fin} / {anyd} | {pdl} / {bdl} | {builds} | {strag} "
                   f"| {pw:.3f} | {p99} / {mx} | {xl} | {he} |")
    return "\n".join(out)


def oneline(rows):
    g = defaultdict(lambda: [0, 0, 0])
    for r in rows:
        x = g[r["variant"]]
        x[0] += 1
        x[1] += 1 if r.get("divergent") else 0
        x[2] += r.get("page_deadlocks", 0) + r.get("build_deadlocks", 0)
    return "; ".join(f"{v} {n}r {d}div {dl}dl" for v, (n, d, dl) in g.items())


if __name__ == "__main__":
    rows = load(sys.argv[1])
    if "--oneline" in sys.argv:
        print(oneline(rows))
    else:
        print(table(rows))
        errs = [r for r in rows if "harness_error" in r or r.get("errors")]
        for r in errs[:8]:
            print("ERR", r["variant"], r["seed"], r["appliers"], r.get("errors"), r.get("harness_error", "")[-400:])
        for r in rows:
            if r.get("divergent") and EXPECT.get(r["variant"]) == "exact":
                print("DIVERGENT(exact-expected)", r["variant"], r["seed"], r["appliers"], r.get("first_checkpoint_divergence"), r.get("divergence"))
        dd = [(r["variant"], d) for r in rows if EXPECT.get(r["variant"]) == "exact" for d in r.get("deadlock_details", [])]
        for d in dd[:8]:
            print("DEADLOCK(exact-expected variant)", d)
