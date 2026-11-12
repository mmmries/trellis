# X2 results: statement micro-benchmark of the plain aggregate's build (#1917)

Research only. Spec: [`../DESIGN.md`](../DESIGN.md), X2. Logbook: `20261112T104320-r-bench` (ok).
(`20261112T104247-r-bench` is an aborted first start that measured nothing; its row count is wrong.)

## Setup

E5's method and data (#1888): Postgres 18.6 scratch cluster on NVMe, `fsync=on`, `shared_buffers=128MB`,
`max_wal_size=1GB`, `work_mem=4MB`, checksums on, non-superuser owner. `posts` at 1M and 10M rows
(`gen_x2.sql`: E5's generator without `post_tags`). Hidden table and target with Trellis's DDL. Each variant in
its own cloned database, `CHECKPOINT` first, one transaction. 3 reps at 1M rows (median, min–max), 1 rep at 10M.
WAL = `pg_current_wal_insert_lsn` difference; FPI from `pg_stat_wal`. Every variant's hidden partials and target
rows were compared (md5 of ordered dumps) with today's statements' and the target with the plain query's: all equal.

Variants (`mk_stmts.py`, statements in `st-hi/`, `st-auth/`):

- `plain`: `INSERT INTO t SELECT … GROUP BY` into a table with a PK.
- `a`: today's emitted statements (fill, lock, rebuild). Not rerun for the 10M fresh build: E5 measured 1,999 s
  and 101 GB of WAL.
- `c1` (candidate C): today's two writes with the update arriving in heap order (= key order, since the lock insert
  is key-ordered). Needed `r` materialized `order by ctid` and `enable_hashjoin = off` for that statement, giving a
  merge join on ctid; the planner's default is a batched hash join that scatters the updates, and disabling both
  hash and merge join gave a quadratic nested loop.
- `c2`: `c1` with fillfactor 50 on the hidden table (the HOT attempt).
- `A<p>` (candidate A): statement 1 = snapshot, enumeration (source groups full-joined with hidden keys and ring old
  images), `GROUP BY`, key-ordered `insert … on conflict do update … where false returning`, target write fed from
  `RETURNING`; stragglers by anti-join; statement 2 = today's rebuild over the stragglers through the source's
  group-key index (`A2_probe`). `<p>` % of the groups pre-exist as page-created rows (NULL basis, one row's
  partials, with their target rows), spread over the key space.
- `Au`: A used for a rebuild in place: statement 1 only locks (every row exists), statement 2 is a second full
  aggregate.
- `B<p>` (candidate B): `pg_advisory_xact_lock`, then one statement: snapshot, `GROUP BY`, key-ordered
  `insert … on conflict do update set <absolute>`, target write.
- A-rebuild (today's lock, key-ordered update) is `c1`. `L` (lock existing, then insert absent) is two passes out of
  key order, DESIGN's deadlock control; its single-pass form is A's statement 1. Neither was run separately.

Rebuild base: a finished fresh build, then 10 % of `posts` rows updated and 1 % new groups inserted, `vacuum analyze`.

## Results

Ratio to `plain` (total seconds), fresh build:

| groups (rows) | plain s | a today | c1 key order | c2 ff 50 | A, 0 % | A, 1 % | A, 10 % | B, 0 % | B, 10 % |
|---|---|---|---|---|---|---|---|---|---|
| 100k (1M) | 0.55 | 2.9× | 2.9× | 2.9× | **1.9×** | 2.0× | 2.2× | 1.8× | 2.0× |
| 1M (1M) | 1.34 | 9.3× (12.5 s) | 8.2× | 8.4× | **3.9×** (5.2 s) | 4.8× | 5.2× | 3.8× | 4.4× |
| 10M (10M) | 15.3 | 130× (1,999 s, E5) | 9.6× (146 s) | 10.0× | **3.8×** (58.8 s) | 5.0× | 5.3× | 3.9× (59.6 s) | 5.6× |

WAL, fresh build: 1M groups: plain 155 MB, a 917, c1 815, A 462, B 462. 10M groups: plain 1.5 GB, a 101 GB (E5),
c1 9.0 GB, A 5.2 GB, B 4.6 GB. Hidden table: 278 MB (a, c1) against 149 MB (A, B) at 1M groups; 2.8 GB against
1.5 GB at 10M.

Rebuild in place:

| groups (rows) | plain s | a today | c1 key order | c2 ff 50 | Au | B |
|---|---|---|---|---|---|---|
| 100k (1M) | 0.62 | 2.7× | 2.6× | 2.7× | 3.4× | **2.0×** |
| 1M (1M) | 1.38 | 10.3× (14.2 s) | 7.9× | 7.7× | 8.7× | **4.0×** (5.5 s) |
| 10M (10M) | 28.6 | 47× (1,350 s, 108 GB WAL) | 6.1× (173 s, 7.4 GB) | 7.9× | 7.4× (211 s) | **3.6×** (102 s, 6.1 GB) |

Full tables with per-statement times, FPI, temp and HOT counts: `results/summary-tables.md`; raw: `results/`.

## Reading

1. **Writing once is worth 2.1–2.5× over the best two-write form** at 1M and 10M groups (10M: A 58.8 s against c1
   146 s at 10M), and halves the WAL and the hidden table. A and B cost the same on a fresh build.
2. **Key order alone removes the collapse at 10M** (1,999 s → 146 s fresh; 1,350 s → 173 s rebuild) but leaves the
   build near 10× the plain query. At 1M groups it saves only 12 %: the order effect needs a table larger than
   `shared_buffers` to show.
3. **HOT does not help.** A fresh build has no HOT updates in any form (`__zero` is the partial index's predicate
   and flips on every row). In a rebuild with fillfactor 50, 97 % of updates were HOT and WAL fell by a fifth to a
   half, but time did not improve (10M: 226 s against 173 s) and the table is larger.
4. **Stragglers are cheap.** 10 % of the groups already created by pages adds 30–40 % to A (index probes for
   statement 2), and about the same to B (those rows are updated instead of inserted).
5. **A does not suit a rebuild in place** as a uniform rule: its first aggregate is wasted (Au 7.4× against c1 6.1×
   at 10M). A rebuild under A keeps today's lock statement with the key-ordered update (c1). B is the only form
   that writes an existing row once without a separate lock pass: 3.6× at 10M.
6. **Twice the plain query is met only at low cardinality** (100k groups: A 1.9×, B 1.8×). At 1M and 10M groups the
   floor measured here is 3.8×. The statement writes two tables (hidden and target) where the plain query writes
   one, and the hidden rows are wider.

## Differences from E5

- `a` at 1M groups took 12.5 s (12.4–12.5) here against 16–26 s in E5; WAL (917 MB) and FPI (15.8k) are the same.
  The box was quieter.
- The plain query at 10M took 15.3 s here against 18.7 s in E5.

## Limits

- 1 rep at 10M. No concurrent writers: nothing here measures how long pages wait, or A's stragglers under a real
  load (0–10 % were planted).
- B's statement omits the hidden/ring enumeration (no group vanishes in this workload); A's includes it.
- No `MIN`/`MAX`, no NULL group keys, no partitioned source. One group per source row at 1M and 10M groups.
- Statements run alone, not through the engine: the fixed define-to-live latency and the target-mutation seam are
  not included.
