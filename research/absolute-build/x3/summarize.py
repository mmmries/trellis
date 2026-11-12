#!/usr/bin/env python3 -I
"""Aggregate results/<label>/<shape>-r<rep>/summary.tsv into a markdown table.  usage: summarize.py <label>"""
import sys, glob, statistics as st, os, re
label = sys.argv[1]
rows = {}
for f in sorted(glob.glob(f'results/{label}/*/summary.tsv')):
    p = open(f).read().strip().split('\t')
    if len(p) < 12: continue
    _, sh, rep, ins, insw, ctas, ctasw, b, bw, state, tgt, pln = p
    sizes = 0
    for l in open(os.path.dirname(f)+'/sizes.txt'):
        n, k, z = (l.strip().split('|') + ['', '', '0'])[:3]
        if re.match(r'trellis\.(agg_|join)', n) or n.endswith('__ledger'): sizes += int(z or 0)
    state = sizes
    rows.setdefault(sh, []).append(dict(ins=float(ins), insw=int(insw), ctas=float(ctas), b=float(b), bw=int(bw), state=int(state), tgt=int(tgt), rep=rep,
        ver=open(os.path.dirname(f)+'/verify.txt').read().split()))
def rng(v, f='{:.1f}'): return (f+' ({}–{})').format(st.median(v), f.format(min(v)), f.format(max(v))) if len(v) > 1 else f.format(v[0])
mb = lambda x: f'{x/1e6:.0f} MB'
print('| shape | n | plain INSERT s | CTAS s | Trellis build s | build ratio vs INSERT | plain WAL | build WAL | state (trellis schema) | target | verify (cnt, plain cnt, diff, diff) |')
print('|---|---|---|---|---|---|---|---|---|---|---|')
for sh, rs in rows.items():
    ratios = [r['b']/r['ins'] for r in rs]
    v = rs[0]['ver'][:4]
    ok = all(r['ver'][:4][2:] == ['0','0'] and r['ver'][0]==r['ver'][1] for r in rs)
    print(f"| {sh} | {len(rs)} | {rng([r['ins'] for r in rs])} | {rng([r['ctas'] for r in rs])} | {rng([r['b'] for r in rs])} | {rng(ratios)} | {mb(st.median([r['insw'] for r in rs]))} | {mb(st.median([r['bw'] for r in rs]))} | {mb(st.median([r['state'] for r in rs]))} | {mb(st.median([r['tgt'] for r in rs]))} | {'OK '+'/'.join(v) if ok else 'MISMATCH '+str(v)} |")
