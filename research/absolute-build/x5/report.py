#!/usr/bin/env python3
"""Markdown tables from analyze.py's TSV. usage: report.py results/analysis.tsv > results/tables.md"""
import csv, re, sys

rows = list(csv.DictReader(open(sys.argv[1]), delimiter="\t"))


def key(r):
    m = re.match(r"(full10m|full)/(base|proto)/(s2_agg(?:_hi)?)-n(\d+)-rate(\d+)(-noindex)?-r(\d)", r["run"])
    lab, b, sh, n, rate, ni, rep = m.groups()
    return dict(bin=b, shape=sh, n=int(n), rate=int(rate), noidx=bool(ni), rep=int(rep))


for r in rows:
    r["ach"] = r["rate"]
    r.update(key(r))
rows.sort(key=lambda r: (r["n"], r["shape"], r["noidx"], r["rate"], r["rep"], r["bin"]))
f1 = lambda x: f"{float(x):.1f}" if x not in ("", None) else ""
f2 = lambda x: f"{float(x):.2f}" if x not in ("", None) else ""
nm = lambda r: f"`{r['shape']}`{' no index' if r['noidx'] else ''} {r['n'] // 1000000}M"


def table(head, fn):
    print("| " + " | ".join(head) + " |")
    print("|" + "---|" * len(head))
    for r in rows:
        v = fn(r)
        if v:
            print("| " + " | ".join(str(x) for x in v) + " |")
    print()


print("### Build, correctness\n")
table(["shape", "target rate", "rep", "binary", "achieved rows/s", "exact", "plain INSERT s", "define→live s",
       "ratio", "build txn s", "build WAL MB"],
      lambda r: [nm(r), r["rate"], r["rep"], r["bin"], r["ach"] or "-", r["exact"],
                 f1(r["ins_s"]), f1(r["build_s"]), f1(r["ratio"]), f1(r["build_tx_s"]), r["build_wal_mb"]])
print("### Stragglers and step 2 (prototype)\n")
table(["shape", "target rate", "rep", "step 1 ms", "stragglers (step-2 rows)", "of them with basis S2 at the end",
       "step-2 read", "step-2 ms"],
      lambda r: None if r["bin"] != "proto" else
      [nm(r), r["rate"], r["rep"], r["step1_ms"],
       r["abs_update_probe_rows"] or r["abs_update_groupby_rows"] or 0, r["stragglers_by_basis"],
       "probe" if r["abs_update_probe_calls"] else ("GROUP BY" if r["abs_update_groupby_calls"] else "-"),
       r["abs_update_probe_ms"] or r["abs_update_groupby_ms"] or ""])
print("### Drain hold-up (runs with writers)\n")
table(["shape", "target rate", "rep", "binary", "build txn s", "seal stall s", "normal seal gap s",
       "page lock-wait on build: longest run s", "longest single wait s", "peak backlog rows",
       "peak oldest-sealed age s", "drain after writers stop s"],
      lambda r: None if r["rate"] == 0 else
      [nm(r), r["rate"], r["rep"], r["bin"], f1(r["build_tx_s"]), r["seal_stall_s"], r["seal_gap_post_med_s"],
       r["hold_longest_s"], r["hold_max_wait_s"], r["peak_backlog_rows"], r["peak_pending_age_s"], f1(r["drain_s"])])
print("### Writer transaction latency, ms (p50 / p99)\n")
table(["shape", "target rate", "rep", "binary", "achieved rows/s (pre / build / post)", "pre (no trigger)",
       "trigger install", "build txn", "after live", "tx errors"],
      lambda r: None if r["rate"] == 0 else
      [nm(r), r["rate"], r["rep"], r["bin"], f"{r['rate_pre']} / {r['rate_build']} / {r['rate_post']}",
       f"{r['lat_pre_p50']} / {r['lat_pre_p99']}", f"{r['lat_install_p50']} / {r['lat_install_p99']}",
       f"{r['lat_build_p50']} / {r['lat_build_p99']}", f"{r['lat_post_p50']} / {r['lat_post_p99']}", r["tx_err"]])
