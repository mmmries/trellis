#!/usr/bin/env python3 -I
"""usage: summarize_micro.py <prefix>   e.g. agg100k  -> aggregates results/micro-<prefix>_r*/ ."""
import sys, glob, re, statistics as st
pre = sys.argv[1]
res = {}  # variant -> list per rep of dict
for d in sorted(glob.glob(f'results/micro-{pre}_r*')):
    for v in 'abcd':
        if not glob.glob(f'{d}/{v}.explain.txt'): continue
        t = open(f'{d}/{v}.explain.txt').read()
        stmts = t.split('QUERY PLAN')[1:]
        items = []
        for s in stmts:
            ex = float(re.search(r'Execution Time: ([\d.]+) ms', s).group(1))
            w = re.search(r'WAL: records=(\d+)(?: fpi=(\d+))? bytes=(\d+)', s)
            wal = (int(w.group(1)), int(w.group(2) or 0), int(w.group(3))) if w else (0, 0, 0)
            tmp = re.search(r'temp read=(\d+) written=(\d+)', s)
            items.append((ex, wal, int(tmp.group(2)) * 8 / 1024 if tmp else 0))
        tot = re.search(r'wal_total_bytes=(\d+)', t); sz = re.search(r'sizes \w: hidden=(\d+) target=(\d+)', t)
        res.setdefault(v, []).append(dict(ex=sum(i[0] for i in items), each=[i[0] for i in items], fpi=sum(i[1][1] for i in items),
             wal=sum(i[1][2] for i in items), walrec=sum(i[1][0] for i in items), tmpmb=sum(i[2] for i in items), walall=int(tot.group(1)), hid=int(sz.group(1)), tgt=int(sz.group(2))))
name = {'a': 'a: emitted (fill + lock + rebuild)', 'b': 'b: sorted hidden upsert only', 'c': 'c: b + target write, hash join', 'd': 'd: c but with the emitted per-group laterals'}
print('| variant | exec s (median, min-max) | per-statement s (rep 1) | WAL (stmt) MB | FPI | WAL bytes pg_current_wal MB | temp MB written | hidden size MB | target size MB |')
print('|---|---|---|---|---|---|---|---|---|')
for v, rs in res.items():
    f = lambda k, c=1: st.median([r[k] for r in rs]) / c
    ex = [r['ex'] / 1000 for r in rs]
    print(f"| {name[v]} | {st.median(ex):.2f} ({min(ex):.2f}-{max(ex):.2f}) | {', '.join(f'{x/1000:.2f}' for x in rs[0]['each'])} | {f('wal',1e6):.0f} | {f('fpi'):.0f} | {f('walall',1e6):.0f} | {f('tmpmb'):.0f} | {f('hid',1e6):.0f} | {f('tgt',1e6):.0f} |")
