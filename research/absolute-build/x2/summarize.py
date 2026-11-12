#!/usr/bin/env python3
"""X2 summary. usage: summarize.py <results_dir> [label...]
Reads results/<label>/<shape>-<mode>-r<rep>/<variant>.explain.txt, writes results/runs.tsv (one row per run) and prints
one markdown table per (label, shape, mode): median (min-max) over reps."""
import sys, os, re, glob, statistics as st

root = sys.argv[1]
labels = sys.argv[2:] or sorted(d for d in os.listdir(root) if os.path.isdir(f'{root}/{d}'))
lsn = lambda s: (int(s.split('/')[0], 16) << 32) + int(s.split('/')[1], 16)
rows = []
for label in labels:
    for d in sorted(glob.glob(f'{root}/{label}/*-*-r*')):
        shape, mode, rep = re.match(r'(\w+)-(\w+)-r(\d+)$', os.path.basename(d)).groups()
        for f in sorted(glob.glob(f'{d}/*.explain.txt')):
            v = os.path.basename(f)[:-len('.explain.txt')]
            t = open(f).read()
            stmts, cur = [], None
            for line in t.splitlines():
                m = re.match(r'STMT (\S+)$', line)
                if m:
                    cur = [m.group(1), None, None]; stmts.append(cur); continue
                m = re.match(r'LSN\|(\S+)$', line)
                if m and cur and cur[1] is None:
                    cur[1] = lsn(m.group(1)); continue
                m = re.match(r'Time: ([\d.]+) ms', line)
                if m and cur and cur[2] is None:
                    cur[2] = float(m.group(1)) / 1000
            each = []
            for a, b in zip(stmts, stmts[1:]):
                if a[2] is None:
                    continue
                each.append((a[0], a[2], (b[1] - a[1]) if (a[1] is not None and b[1] is not None) else None))
            g = lambda k: (re.search(k, t) or [None, None])
            rc = int(re.search(r'rc=(\d+)', t).group(1)) if re.search(r'rc=(\d+)', t) else -1
            psw = re.search(r'pg_stat_wal=(\d+) (\d+) (\d+)', t)
            tmp = re.search(r'temp=(\d+) (\d+)', t)
            sts = re.search(r'stats=(\d+) (\d+) (\d+) (\d+)', t)
            sz = re.search(r'sizes hidden=(\d+) hidden_heap=(\d+) target=(\d+)', t)
            hm = re.search(r'hidden_md5=(\w+) rows=(\d+)', t); tm = re.search(r'target_md5=(\w+) rows=(\d+)', t)
            pm = re.search(r'plain_md5=(\w+)', t); ns = re.search(r'nstrag=(\d+)', t)
            err = re.search(r'ERROR:.*', t)
            rows.append(dict(label=label, shape=shape, mode=mode, rep=int(rep), v=v, rc=rc,
                             err=err.group(0) if err else '',
                             each=each, total=sum(e[1] for e in each),
                             wal=int(re.search(r'wal_total_bytes=(\d+)', t).group(1)) if re.search(r'wal_total_bytes=(\d+)', t) else None,
                             walrec=int(psw.group(1)) if psw else None, fpi=int(psw.group(2)) if psw else None,
                             temp=int(tmp.group(2)) if tmp else None,
                             ins=int(sts.group(1)) if sts else None, upd=int(sts.group(2)) if sts else None,
                             hot=int(sts.group(3)) if sts else None,
                             hid=int(sz.group(1)) if sz else None, hidheap=int(sz.group(2)) if sz else None,
                             tgt=int(sz.group(3)) if sz else None,
                             hmd5=hm.group(1) if hm else '', hrows=int(hm.group(2)) if hm else None,
                             tmd5=tm.group(1) if tm else '', trows=int(tm.group(2)) if tm else None,
                             pmd5=pm.group(1) if pm else '', nstrag=int(ns.group(1)) if ns else None))

with open(f'{root}/runs.tsv', 'w') as o:
    keys = ['label', 'shape', 'mode', 'rep', 'v', 'rc', 'total', 'wal', 'walrec', 'fpi', 'temp', 'ins', 'upd', 'hot', 'hid', 'hidheap',
            'tgt', 'hrows', 'trows', 'nstrag', 'hmd5', 'tmd5', 'pmd5', 'err']
    o.write('\t'.join(keys + ['statements(name:s:walbytes)']) + '\n')
    for r in rows:
        o.write('\t'.join(str(r[k]) for k in keys) + '\t' +
                ' '.join(f'{n}:{s:.3f}:{w}' for n, s, w in r['each']) + '\n')

order = ['plain', 'a', 'c1', 'c2', 'A0', 'A0.1', 'A1', 'A10', 'Au', 'B0', 'B0.1', 'B1', 'B10', 'B']
MB = 1e6
def mm(xs, fmt='{:.2f}'):
    xs = [x for x in xs if x is not None]
    if not xs: return '-'
    if len(xs) == 1: return fmt.format(xs[0])
    return f'{fmt.format(st.median(xs))} ({fmt.format(min(xs))}-{fmt.format(max(xs))})'
groups = {}
for r in rows:
    groups.setdefault((r['label'], r['shape'], r['mode']), []).append(r)
for (label, shape, mode), rs in groups.items():
    by = {}
    for r in rs: by.setdefault(r['v'], []).append(r)
    plain = st.median([r['total'] for r in by.get('plain', [])]) if 'plain' in by else None
    ref = [r for r in by.get('a', []) if r['rc'] == 0] or by.get('c1')
    refh = {r['hmd5'] for r in (ref or [])}; reft = {r['tmd5'] for r in (ref or [])}
    plainmd5 = {r['pmd5'] for r in by.get('plain', [])}
    print(f'\n### {label} {shape} {mode} (reps: {max(r["rep"] for r in rs)})\n')
    print('| variant | total s | x plain | per statement s (median) | WAL MB | FPI | temp MB | hidden MB | HOT/upd | stragglers | rows = ref |')
    print('|---|---|---|---|---|---|---|---|---|---|---|')
    for v in sorted(by, key=lambda v: order.index(v) if v in order else 99):
        x = by[v]
        if any(r['rc'] != 0 for r in x):
            print(f'| {v} | rc={[r["rc"] for r in x]} {x[0]["err"][:80]} | | | | | | | | | |'); continue
        tot = [r['total'] for r in x]
        names = [n for n, _, _ in x[0]['each']]
        per = ', '.join(f'{n} {st.median([dict((e[0], e[1]) for e in r["each"]).get(n, 0) for r in x]):.2f}' for n in names)
        ratio = mm([t / plain for t in tot], '{:.1f}') if plain else '-'
        same = '-'
        if v != 'plain':
            hs = all(r['hmd5'] in refh for r in x); ts = all(r['tmd5'] in reft for r in x)
            tp = all(r['tmd5'] in plainmd5 for r in x) if plainmd5 else None
            same = ('hidden ok' if hs else 'HIDDEN DIFF') + ', ' + ('target ok' if ts else 'TARGET DIFF') + \
                   ('' if tp is None else (', =plain' if tp else ', !=plain'))
        hot = f"{st.median([r['hot'] for r in x]):.0f}/{st.median([r['upd'] for r in x]):.0f}" if x[0]['upd'] is not None else '-'
        print(f"| {v} | {mm(tot)} | {ratio} | {per} | {mm([r['wal'] / MB for r in x], '{:.0f}')} | {mm([r['fpi'] for r in x], '{:.0f}')} | "
              f"{mm([r['temp'] / MB for r in x], '{:.0f}')} | {mm([r['hid'] / MB for r in x], '{:.0f}')} | {hot} | "
              f"{mm([r['nstrag'] for r in x], '{:.0f}') if x[0]['nstrag'] is not None else '-'} | {same} |")
