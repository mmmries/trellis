#!/usr/bin/env python3
"""X2: derive every variant's statements from E5's captured statements (one shape).
usage: mk_stmts.py <e5_stmts_dir> <out_dir> <GCOL> <TGT>
Reads lock.sql, rebuild.sql, cand_c.sql (captured from a real build by E5, parameters inlined).
Writes into <out_dir> (names of the pg_temp tables are kept as captured):
  fill.sql       today's recompute-set fill (rebuild_groups_select: hidden, target, source, ring old images)
  lock.sql       today's lock statement, as captured
  rebuild.sql    today's write, as captured (a)
  c1_update.sql  today's write, with the hidden rows reaching `Update` in key order (c1, c2)
  A1.sql         candidate A statement 1: enumeration + GROUP BY + insert ... on conflict do update ... where false,
                 target tail from RETURNING, stragglers (enumerated, not returned) into pg_temp.x2_strag
  A2_probe.sql   A statement 2 over pg_temp.x2_strag: key-ordered absolute update, source read by one index
                 probe per group (the engine's RebuildRead::Probe form)
  A2_scan.sql    A statement 2 over pg_temp.x2_strag: c1_update with the full GROUP BY (RebuildRead::Source form)
  B.sql          candidate B: cand_c as captured (absolute upsert), run after pg_advisory_xact_lock
  plain.sql      the plain INSERT ... SELECT GROUP BY (E5's reference)
"""
import re, sys, os

src, out, G, TGT = sys.argv[1:5]
os.makedirs(out, exist_ok=True)
rd = lambda f: open(f'{src}/{f}').read().strip().rstrip(';')
lock, reb, candc = rd('lock.sql'), rd('rebuild.sql'), rd('cand_c.sql')
RECOMP = re.search(r'pg_temp\."(trellis_group_recompute_[0-9a-z_]+)"', reb).group(1)
QG = f'"{G}"'
HID = '"trellis"."agg_1"'

def w(name, s):
    open(f'{out}/{name}', 'w').write(s.strip().rstrip(';') + ';\n')

# ---- today's fill: Trellis's rebuild_groups_select, unscoped, ring = trellis.seg_0..3, source 'public.posts'
images = ' union all '.join(
    f"select old_image as __i from trellis.seg_{i} where src_table = 'public.posts' and jsonb_typeof(old_image) = 'object' "
    f"union all select transition_images -> 'old' from trellis.seg_{i} where src_table = 'public.posts' "
    f"and jsonb_typeof(transition_images -> 'old') = 'object'" for i in range(4))
fill = (f'insert into pg_temp."{RECOMP}" ({QG}) select {QG} from {HID} h union all select t.{QG} from "public"."{TGT}" t '
        f'union all select distinct s.{QG} from "public"."posts" s '
        f'union all select s.{QG} from ({images}) as r cross join lateral jsonb_populate_record(null::"public"."posts", r.__i) as s')
w('fill.sql', fill)
w('lock.sql', lock)
w('rebuild.sql', reb)

# ---- c1: rows reach Update in heap order, which is key order here (the lock statement inserts in key order, and every
# later version of a row is written in that order too). r is materialized and read sorted by ctid; the runner sets
# enable_hashjoin = off for this statement so the join to the hidden table is a merge join on ctid (a hash join with
# more than one batch emits rows batch by batch, i.e. scattered over the whole table: today's plan).
i_r, i_up = reb.index(', r as ('), reb.index(', up as (')
r = reb[i_r:i_up]
assert r.startswith(', r as ( select h.__hc,')
r1 = r.replace(', r as ( select h.__hc,', f', r as materialized ( select h.__hc, rg.{QG} as __g,', 1)
up = reb[i_up:]
up1 = up.replace('from r, snap where u.ctid = r.__hc', 'from (select * from r order by __hc) r, snap where u.ctid = r.__hc', 1)
assert up1 != up
c1 = reb[:i_r] + r1 + up1
w('c1_update.sql', c1)

# ---- A statement 2: c1 over the stragglers
STRAG = 'pg_temp.x2_strag'
a2_scan = c1.replace(f'pg_temp."{RECOMP}"', STRAG)
assert a2_scan != c1
w('A2_scan.sql', a2_scan)
# probe form: drop the a CTE, read the source by one index probe per straggler group
i_a = a2_scan.index(', a as (')
i_r2 = a2_scan.index(', r as materialized (')
a_cte = a2_scan[i_a:i_r2]
m = re.match(r', a as \(select s\."(\w+)"::integer as "\w+", (.*) from "public"\."posts" s group by 1\)$', a_cte, re.S)
assert m, a_cte[:200]
aggs = m.group(2)
probe = (f'cross join lateral (select {aggs} from (select s.* from "public"."posts" s where s.{QG} = rg.{QG} '
         f'union all select s.* from "public"."posts" s where (rg.{QG} is null) and (s.{QG} = rg.{QG} or (rg.{QG} is null and s.{QG} is null))) s) as a')
a2_probe = a2_scan[:i_a] + a2_scan[i_r2:]
old_join = f'left join a on array[a.{QG}] = array[rg.{QG}]'
assert old_join in a2_probe
a2_probe = a2_probe.replace(old_join, probe, 1)
w('A2_probe.sql', a2_probe)

# ---- B: cand_c as captured
w('B.sql', candc)

# ---- A statement 1: cand_c with the enumeration, the lock-only conflict arm, and the straggler capture
i_ins = candc.index(', ins as (insert into')
i_nv = candc.index(', nv as (')
head = candc[:i_ins]          # snap, a
ins = candc[i_ins:i_nv]
tail = candc[i_nv:]
enum = (f', e as (select h.{QG} from {HID} h union all select s.{QG} from ({images}) as r '
        f'cross join lateral jsonb_populate_record(null::"public"."posts", r.__i) as s)'
        f', c as (select coalesce(a.{QG}, e.{QG}) as {QG}, coalesce(a.__rn, 0) as __rn, coalesce(a.__rc1, 0) as __rc1, '
        f'coalesce(a.__rs1, 0::numeric) as __rs1, a.__rk1 as __rk1, coalesce(a.__rc2, 0) as __rc2, coalesce(a.__rs2, 0::numeric) as __rs2 '
        f'from a full join (select distinct {QG} from e) e on a.{QG} = e.{QG})')
ins1 = ins.replace('from a, snap order by a.', 'from c a, snap order by a.', 1)
assert ins1 != ins
m = re.search(r'on conflict \((\w+)\) do update set .*? returning', ins1, re.S)
ins1 = ins1[:m.start()] + f'on conflict ({G}) do update set __n = excluded.__n where false returning' + ins1[m.end():]
strag = (f', strag as (insert into {STRAG} ({QG}) select c.{QG} from c where not exists '
         f'(select 1 from ins where ins.{G} = c.{QG}))')
w('A1.sql', head + enum + ins1 + strag + tail)

w('plain.sql', f'insert into plain_ins select {G}, count(*) as n, sum(score) as total_score, sum(likes) as total_likes '
                f'from posts group by {G}')
print(RECOMP)
