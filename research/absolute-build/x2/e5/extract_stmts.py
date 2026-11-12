#!/usr/bin/env python3 -I
"""Extract the aggregate-build statements (recompute-set fill, lock, rebuild) from an auto_explain
log slice, with bound parameters inlined.  usage: extract_stmts.py <server-log-slice> <outdir>"""
import re, sys, os
t = open(sys.argv[1]).read(); out = sys.argv[2]; os.makedirs(out, exist_ok=True)
blocks = re.split(r'\n(?=\d{4}-\d\d-\d\d \d\d:)', t)
def parts(b):
    m = re.search(r'\tQuery Text: (.*?)\n(?=\t(?:Query Parameters:|[A-Z]\w*  \(|Result|Insert|Update|Delete|Seq|Aggregate|HashAgg|Sort|Limit|Nested|Hash|CTE|Group|Unique|Merge|Append|Index|Bitmap|Gather|Subquery|Function|Values|Tid|Materialize|ProjectSet|WindowAgg|Custom|Foreign))', b, re.S)
    p = re.search(r'\tQuery Parameters: (.*)\n', b)
    return (m.group(1) if m else None), (p.group(1) if p else None)
def inline(q, p):
    if not p: return q
    items = re.split(r", (?=\$\d+ = )", p)
    vals = {}
    for it in items:
        k, v = it.split(' = ', 1)
        vals[int(k[1:])] = v
    def lit(v):
        if v == 'NULL': return 'NULL'
        v = v[1:-1].replace("''", "'") if v.startswith("'") else v
        if re.fullmatch(r'-?\d+', v): return v
        return "'" + v.replace("'", "''") + "'"
    q = re.sub(r'\s*/\*, \.\.\. \*/', '', q)
    return re.sub(r'\$(\d+)', lambda m: lit(vals[int(m.group(1))]) if int(m.group(1)) in vals else m.group(0), q)
want = {'recompute_fill': r'^insert into pg_temp\."trellis_group_recompute', 'lock': r'^insert into "trellis"\."agg_\d+" \(.*select distinct',
        'rebuild': r'^with snap as materialized'}
for b in blocks:
    q, p = parts(b)
    if not q: continue
    for name, rx in want.items():
        if re.match(rx, q.strip()):
            d = re.search(r'LOG:\s+duration: ([\d.]+) ms', b)
            open(f'{out}/{name}.sql', 'w').write(inline(q.strip(), p) + ';\n')
            print(name, 'duration_ms=', d.group(1) if d else '?', 'params=', bool(p))
