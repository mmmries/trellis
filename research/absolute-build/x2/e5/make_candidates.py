#!/usr/bin/env python3 -I
"""Build hand-written candidate bulk statements from the emitted rebuild statement.
usage: make_candidates.py <stmts_dir>   (reads rebuild.sql, writes cand_b.sql, cand_c.sql)
cand_b: one sorted upsert into the hidden table (each hidden row written once), no target write (floor).
cand_c: cand_b + the emitted nv/cur/written/deleted tail, with the per-group laterals into the target
        replaced by one hash left join (NULL-group-key arm dropped)."""
import re, sys, os
G = os.environ.get('GCOL', 'author_id')
d = sys.argv[1]
q = open(f'{d}/rebuild.sql').read().strip().rstrip(';')
i_rg = q.index(', rg as (')
i_a = q.index(', a as (')
i_r = q.index(', r as (')
i_up = q.index(', up as (')
i_nv = q.index(', nv as (')
i_cur = q.index(', cur as (')
i_written = q.index(', written as (')
snap = q[:i_rg]
a = q[i_a:i_r]
nv = q[i_nv:i_cur]
cur = q[i_cur:i_written]
tail = q[i_written:]
hid = '"trellis"."agg_1"'
L = lambda x: x.replace('author_id', G)
cols = 'author_id, __n, __count_1, __sum_1, __scales_1, __count_2, __sum_2, __basis, __stamp'
ins = (f', ins as (insert into {hid} ({cols}) select a.author_id, a.__rn, a.__rc1, a.__rs1, a.__rk1, a.__rc2, a.__rs2, snap.__snap, snap.__seg '
       'from a, snap order by a.author_id on conflict (author_id) do update set __n = excluded.__n, __count_1 = excluded.__count_1, '
       '__sum_1 = excluded.__sum_1, __scales_1 = excluded.__scales_1, __count_2 = excluded.__count_2, __sum_2 = excluded.__sum_2, '
       '__basis = excluded.__basis, __stamp = excluded.__stamp '
       'returning author_id, __n as __nn, __count_1 as __nc1, __sum_1 as __ns1, __scales_1 as __nk1, __count_2 as __nc2, __sum_2 as __ns2)')
ins = L(ins)
open(f'{d}/cand_b.sql', 'w').write(snap + a + ins + ' select count(*) from ins;\n')
nv2 = nv.replace('from up cross join lateral', 'from ins cross join lateral').replace('select up.*,', 'select ins.*,')
nv2 = nv2.replace('up.__', 'ins.__')
# replace the per-group lateral lookups into the target by a hash left join
m = re.search(L(r'left join lateral \(select x\.ctid, (.*?) from ("public"\."\w+") x where x\."author_id" = nv\."author_id" union all .*? limit \d+\) as t\(([^)]*)\) on \d+|left join lateral \(select x\.ctid, (.*?) from ("public"\."\w+") x where x\."author_id" = nv\."author_id" union all .*? limit [^)]*\) as t\(([^)]*)\) on (?:true|\$\d+)'), cur, re.S)
assert m, 'lateral pattern not found'
cols_sel, tbl, alias = (m.group(1), m.group(2), m.group(3)) if m.group(1) else (m.group(4), m.group(5), m.group(6))
cur2 = cur[:m.start()] + f'left join (select x.ctid, {cols_sel}, x."{G}" as __k from {tbl} x) as t({alias}, __tk) on t.__tk = nv."{G}"' + cur[m.end():]
cur2 = cur2.replace('t.*', 't.__tc, t.__vf0, t.__vf1, t.__vf2')
open(f'{d}/cand_c.sql', 'w').write(snap + a + ins + nv2 + cur2 + tail + ';\n')
open(f'{d}/cand_d.sql', 'w').write(snap + a + ins + nv2 + cur + tail + ';\n')  # c, but with the emitted per-group laterals kept
