# X3 results: candidate A prototyped in the engine (#1917)

Research only. Nothing here merges. Spec: [`../DESIGN.md`](../DESIGN.md), X3; correctness model [`../x1/X1-results.md`](../x1/X1-results.md);
statement forms [`../x2/X2-results.md`](../x2/X2-results.md). Logbook: `20261112T123222-r-bench` (ok).

## Verdict

- **A holds in the engine.** Every targeted suite passes except one test that pins today's enumeration order (expected).
  Six new deterministic tests of A's own interleavings pass, and the GC counterexample is reproduced on the engine:
  wrong without the GC skip, exact with it. A needed no rule beyond the GC skip.
- **End to end (measured):** 1M groups, build 19.4 s → 11.9 s (14.8× → 9.1× the plain `INSERT`; net of the ~5.5 s
  fixed define-to-live floor, 10.7× → 4.9×). 10M groups: 81.7 s, **6.0×** the plain `INSERT` (5.6× net), against E5's
  1,534.9 s (82×) for the base. WAL at 10M: 5.9 GB against E5's 102.6 GB. 100k groups: no change worth having (the floor
  dominates).
- Partition-leaf builds, rebuilds in place and `JOIN`s stay on today's path (below).

## What changed (commits `5cf7de4b`, `c8dfdada` on this branch)

- `trellis/src/staging/group_basis.rs`
  - `rebuild` takes `fresh`. When `fresh`, `scope` is `None` and the writer is the plain aggregate, it calls the new
    `write_once`; everything else runs today's three statements unchanged.
  - `write_once`: step 1 (`GroupTargetPlan::write_once_sql`), then `select count(*)` of the stragglers, then, if any,
    step 2 = today's `rebuild_sql` over them (already locked), reading the source by `RebuildRead::Probe` when there are
    at most 10,000 stragglers and the source has an index whose first column is a group column, else by
    `RebuildRead::Source` (one `GROUP BY`).
  - `write_once_sql`, one statement: `snap` (snapshot + stamp) → `a` = the source's `GROUP BY` (`fresh_select`, as
    today) → `e` = distinct groups of the hidden table ∪ visible target ∪ ring old images (`rebuild_groups_select` with
    the source branch dropped, new `own` flag) → `c` = `a full join e on array[a.g] = array[e.g]` (NULL-safe, hashable)
    → `up` = `insert into agg as u (…partials, extremes, __basis, __stamp[, __xbasis, __xstamp]) select … from c, snap
    order by <group> on conflict (<group>) do update set __n = excluded.__n where false returning …` → `strag` = insert
    into the recompute temp table every `c` group not in `up` (NULL-safe anti-join) → today's `held_tail_sql` fed from
    `up` (visible rows compared with the held target row, prior images for readers). Emitted text (normalised by
    `pg_stat_statements`): [`stmt-step1-s2_agg_hi.sql`](stmt-step1-s2_agg_hi.sql).
  - Extremes (`MIN`/`MAX`) fell out naturally: the insert writes them and `__xbasis`/`__xstamp` from `S1`.
- `trellis/src/staging/build.rs`: `run_group_structure` passes `fresh = (readers == ReadersOnCommit::Park)`, i.e. the
  plan job of a fresh build or a resume. A sweep job (rebuild in place) passes `false`.
- `trellis/src/staging/retire.rs`: the zero-group pass now returns each table's definition id and whether it is an
  `agg_<id>` table, and for those runs `NOT_BUILDING_SQL` (the 1-1 ledger's skip) in the batch transaction before the
  delete. A test-only switch `X3_GC_IGNORES_BUILD` turns the skip off for the negative control.
- Tests: `trellis/tests/group_basis_write_once.rs` (5), `trellis/tests/group_basis_write_once_gc.rs` (1).

### Hook points (prototype adaptation)

A has no separate enumeration or lock statement. In `write_once`, `AfterRebuildEnumerate` fires before step 1 (nothing
locked or enumerated yet) and `AfterRebuildLock` after step 1 (every enumerated group inserted or locked, step 2's
snapshot not yet taken). `BeforeRebuildCommit` is unchanged. No test needed any other change.

## Kept on today's path, and why

| case | why |
|---|---|
| Rebuild in place (`request_backfill`, capture re-install, automatic and upstream rebuilds: the sweep job) | DESIGN/X2: every row exists, so step 1's aggregate would be wasted (X2 `Au`). Not changed. |
| Partition-leaf and remainder structures (`scope` is `Some`) | **Not anticipated.** The routed enumeration's predicates must run under the pinned settings (`search_path`, `datestyle`, `timezone`: `pin_settings`), while the `GROUP BY` runs under the session's. A needs the enumeration and the `GROUP BY` in one statement (`S1`), and a statement has one set of GUCs. Converting needs the scope predicates made setting-independent, or proof that the aggregate's expressions are. |
| `JOIN` `P`/`M`/`T` | Out of scope (`join::build`). |
| The numeric-scale shortcut (`strictly_or_generally`) | Only the `JOIN` build uses it; the plain aggregate's build never did. |
| NULL group keys, truncate floor | Kept, not routed: NULL-safe matching throughout; the hidden truncate takes `ACCESS EXCLUSIVE` and conflicts with step 1's `ROW EXCLUSIVE` (taken before the statement's snapshot); the source truncate waits on step 1's `ACCESS SHARE`. |

## "Under a build" for the whole span: every path to the changed build

The predicate is `NOT_BUILDING_SQL`: status `backfilling`, or `build` set. The only caller of the changed path is the
plan job of a plain aggregate over an unpartitioned source (or a partitioned root with no rows and no record).

| path | under a build from before `S1` to commit? |
|---|---|
| Fresh build | Yes. `start` takes the definition row `for update`, sets `backfilling`/`rederive` and enqueues the plan job, and commits before the job can be claimed. A GC batch holding `for key share` makes `start` wait; one that checks after `start` commits skips. `try_complete` clears it in a later transaction. |
| Resume (`complete_resume`, also after a restore or a capture-failure resume) | Yes: it sets `waiting_to_backfill`, `build = null`, recreates the hidden table, and then `start` runs as above. A resume can't commit during a build in flight: it takes the definition row `for update` against the build's `for key share`. |
| Pause or quarantine during the build | Still skipped: they keep `build` set. |
| `numeric_sums` attach (recreates the group table, `build = null`) | Can't commit during the build: it needs the hidden table `ACCESS EXCLUSIVE`, which waits on step 1's `ROW EXCLUSIVE`. Its new build goes through `start`. |
| `start_rebuilds` of a definition already building | Enqueues only a sweep job for a group-basis definition (today's path); status stays `backfilling`. |

None found that is not under a build. Side effect: the skip also holds during rebuilds in place (they are
`backfilling` too), which don't need it. Zero groups made during any build wait for its end, bounded by the write rate
times the build's length, as the ledger's tombstones are.

## Tests

Targeted only; no generative tier. `cargo test -p trellis --lib staging::group_basis`: 19 passed.

| suite | result | notes |
|---|---|---|
| `group_basis_build` | 19/20 | `pages_between_the_enumeration_and_the_lock` fails at "the build didn't write a group it hadn't enumerated": (a), it pins today's order. A page that creates group 9 before step 1 is enumerated by A (the hidden branch, in `S1`) and rewritten by step 2: `(1,1,4,t)` instead of `(1,1,4,f)`. Values are right; `pages_before_step_one_make_stragglers` below runs the same scenario to the oracle. |
| `group_basis_interleavings` | 38/38 | |
| `group_basis_rebuild` | 9/9 | in-place rebuilds unchanged; their fresh builds go through A |
| `group_basis_resume` | 11/11 | |
| `group_basis_resume_after_source_key_change` | 3/3 | |
| `group_basis_min_max` | 26/26 | |
| `group_basis_min_max_index` | 7/7 | |
| `group_basis_numeric_scales` | 16/16 | |
| `group_basis_numeric_nan` | 8/8 | |
| `group_basis_chained` | 20/20 | |
| `group_basis_self_check` | 26/26 | |
| `group_basis_collation` | 9/9 | |
| `group_basis_alias_columns` | 3/3 | |
| `partition_group_basis` | 52/52 (1 ignored, as on base) | only the empty-unrecorded-root case reaches A |
| `partition_truncate` | 4/4 | |
| `retire` | 14/14 | |
| `numeric_sums_upgrade` | 3/3 | |
| `rederive_build` | 49/49 | |
| `join_interleavings` | 102/102 | (the zero-group pass's table list changed) |
| **new** `group_basis_write_once` | 5/5 | below |
| **new** `group_basis_write_once_gc` | 1/1 | below |

Coverage check: with a temporary `panic!` in `write_once`, every suite above except `partition_truncate`, `retire` and
`join_interleavings` (not run) failed somewhere (all of `interleavings`, `min_max`, `rebuild`, `resume`, `self_check`; 19/20 of `build`), so A is what
those suites ran.

New tests. To stall step 1 after `S1` and before its insert reaches group `g`, a page holds a lower group's hidden row
(frozen after its lock statement); the build's key-ordered insert waits on it. No new pause point was needed.

- `a_page_creates_a_group_inside_step_one_{probe,group_by}`, `a_page_creates_the_null_group_inside_step_one`: after
  `S1`, a change to group 9 (or the `NULL` group) is drained by a page that creates the row; the build's insert finds it,
  locks it, and step 2 rewrites it from `S2` (`(2,2,101,t)`). Writing it from `S1` would lose the change (`(1,1,1,t)`).
  Step 2 by index probes and by one `GROUP BY`.
- `a_page_blocked_on_an_inserted_row_applies_only_what_s1_missed`: a page waits on a row step 1 inserted; after the
  commit it applies the change committed after `S1` once; the one `S1` saw, drained later, is refused.
- `pages_before_step_one_make_stragglers`: today's `pages_between_…` under A, with a `NULL` group that is both a
  source group and a hidden row at `S1` (the full join must match them as one group).
- `the_zero_group_gc_must_skip_a_definition_under_a_fresh_build`: X1's counterexample. `c_a` before `S1`, `c_b` after,
  both drained into a `+1 −1` zero row before step 1 reaches it, then a GC pass. With the skip: nothing collected, group
  rewritten to zero by step 2, exact. With `X3_GC_IGNORES_BUILD`: the row is collected, the build inserts `(1,1,3,t)` from
  `S1`, the target keeps `(9,3,1)`: **wrong**.

## Planted violations (`trellis/src/plant.rs`)

Run only against their pinning tests and the new tests, not the generative tier.

| plant | touches changed code? | under A |
|---|---|---|
| `gb_build_snapshot_before_locks` | yes | Moved. On A's path it now rewrites the stragglers from a `GROUP BY` and snapshot taken before step 1 (A's rule "step 2 takes a snapshot after step 1"). Caught by 4 of the 5 new tests; its pin (`group_basis_rebuild`'s `a_page_that_commits_between_the_enumeration_and_the_locks`, an in-place rebuild) still fails under it. |
| `gbx_absolute_write_keeps_extremes` | yes | Moved into step 1's insert (extremes and `__xbasis` left `NULL`). Its pin `a_build_writes_the_extremes_and_their_basis` still fails under it. |
| `gb_early_zero_gc`, `gbx_gc_ignores_xstamp` | the zero-group pass | Unmoved; the skip comes before them. Their pins (`r4_…`, `f6_…`) still fail under them. They can no longer fire during a fresh build (the pass skips it). |
| `gb_skip_group_lock`, `gb_lsn_not_visibility`, `gb_fold_sides_per_key`, the other `gbx_*` | no (pages) | Unchanged. |

A has no plant of its own yet: "no GC skip" exists only as the test switch, and "step 1 without the hidden/ring branch"
(X1's A-noenum) has none.

## End-to-end benchmark

E5's method (`run.sh`, `shapes.sh`, `gen.sql`, `summarize.py` from `research/issue-1888-operator-model`; changes: binary
from `TR`, cluster dir and port). Postgres 18.6 scratch cluster `target/research-pg-x3` (deleted), `fsync=on`,
`shared_buffers=128MB`, `max_wal_size=1GB`, `work_mem=4MB`, checksums, `auto_explain` (200 ms, no analyze),
`pg_stat_statements`; non-superuser `app` (`setup_cluster.sh`). Base: `trellis-cli --release` at `54cfddb2`; prototype:
`c8dfdada`'s code. Base and prototype alternate within each rep. Box load 1–3, no `rustc` during the runs. Build = wall
time from `apply` to the first `live`. Runner `run_x3.sh`, raw output `results/`, tables `results/summary-tables.md`.

All 13 targets equal the plain query (counts equal, `EXCEPT ALL` both ways 0).

| shape (rows, groups) | binary | plain INSERT s | build s, median (min–max) | ratio raw | ratio net of 5.5 s | build WAL | hidden table |
|---|---|---|---|---|---|---|---|
| `s2_agg` (1M, 100k) | base | 0.6 | 6.8 (6.8–6.8) | 11.3× | 2.2× | 81 MB | 29 MB |
| | proto | 0.6 | 6.6 (6.2–6.6) | 10.6× | 1.8× | 48 MB | 16 MB |
| `s2_agg_hi` (1M, 1M) | base | 1.3 | 19.4 (19.1–21.1) | 14.8× | 10.7× | 917 MB | 278 MB |
| | proto | 1.3 | 11.9 (11.8–11.9) | 9.1× | 4.9× | 559 MB | 149 MB |
| `s2_agg_hi` (10M, 10M) | base (E5, not rerun) | 18.7 | 1,534.9 | 82.0× | | 102.6 GB | 2.8 GB |
| | proto | 13.5 | 81.7 | **6.0×** | 5.6× | 5.9 GB | 1.5 GB |

Target size is unchanged (83 MB / 827 MB). The plain `INSERT` at 10M took 13.5 s here against E5's 18.7 s (quieter box);
against E5's plain time the prototype would be 4.4×.

Top statements (`pg_stat_statements`, prototype):

| run | statement | ms | rows | WAL | FPI | temp |
|---|---|---|---|---|---|---|
| 1M groups (r2) | step 1 | 5,785 | 1,000,000 | 459 MB | 4 | 566 MB |
| | everything else | < 50 each (the two `EXCEPT ALL` checks are the harness's) | | 0 | | |
| 10M groups | step 1 | 68,115 | 10,000,000 | 4.59 GB | 78 | 6.65 GB |
| | everything else | < 500 each (`select count(*)` of the target is the harness's) | | | | |
| base, 1M groups (r2), for reference | rebuild update | 10,389 | 1M | 605 MB | 15,781 | 665 MB |
| | lock insert | 2,322 | 1M | 287 MB | 0 | |
| | fill | 198 | | | | |
| | zero-group GC delete | 129 (3 calls, 0 rows) | | | | |

Step 2 never ran in the benchmark: no concurrent writes, so no stragglers. The zero-group GC delete does not appear at
all for the prototype (skipped under the build); at 10M E5 saw it cost 653 s of statement time for the base.

What dominates outside the build statement:

- **The fixed define-to-live floor**, ~5.5 s (E5): at 100k groups the 6.6 s build is the floor plus 1.1 s of statement.
  At 1M groups, 11.9 s wall less 5.8 s of statement ≈ the floor.
- **At 10M, about 8 s beyond the floor and the statement** (81.7 − 68.1 − 5.5). Not in `pg_stat_statements`. Inferred,
  not measured: the client reading the 10M rows the statement returns (`written_groups`) and recording 10M keys in
  `TargetMutations`, which today's build does too.
- **Temp spill**: 6.65 GB at 10M, 566 MB at 1M (`c` is materialised for the insert and the straggler anti-join, and
  hashed at 4 MB `work_mem`).

Compared with X2's statement alone (A0, 10M: 58.8 s total, 5.2 GB WAL), the engine's step 1 is 68.1 s and 4.6 GB: the
same order; the engine's includes the visible target branch of the enumeration and the target lookups.

## What DESIGN.md / X1 did not anticipate

1. **Partition leaves can't be converted as is**: the pinned-settings conflict above.
2. **The enumeration needs the visible target branch** (a resume keeps the target). X2's `A1` omitted it.
3. **X2's statement form is not NULL-safe**: it joins `a.g = e.g` and anti-joins `ins.g = c.g`. The engine uses array
   equality (`group_match`), which pairs `NULL` with `NULL` and hashes. Inferred, not run: with `a.g = e.g` a `NULL`
   group present in both would appear twice in `c`, and the insert would fail with "ON CONFLICT DO UPDATE command cannot
   affect row a second time".
4. **Step 2's read needs a rule**: probe per group needs a usable index, and a fresh build's source may have none
   (`WITHOUT SOURCE INDEX`). The prototype checks for an index led by a group column and caps probes at 10,000
   stragglers. Untuned.
5. **The GC skip needs the definition id in the zero-group pass**, which listed tables only, and it then applies to
   rebuilds in place as well.
6. **Hook points**: the engine's interleaving tests freeze "between enumeration and lock" and "between lock and
   snapshot"; A has neither, so both moved, and one test pins the old meaning.

## What a production implementation would still need

- Convert or explicitly exclude partitioned leaves and the remainder (settings), and decide the rebuild-in-place path
  (today's, or key-ordered `c1`, or B).
- Golden SQL for `write_once_sql`; module docs (`group_basis` "A build or rebuild", "Zero groups", which says the GC
  doesn't skip builds; `retire::collect_zero_groups`; `interleave`'s hook descriptions); ADR-0002/ADR-0028 text for the
  insert-from-read exception for groups. (No doc or ADR was edited here.)
- Plants for A's own rules (GC skip removed; step 1 without the hidden or ring branch; stragglers from `S1`) and a
  generative sweep with them; a real pause point inside step 1 instead of the holder-page trick.
- Step 2's read heuristic, measured with real stragglers under load (X2 planted 0–10 % only; this benchmark had none).
- The ~8 s of client time at 10M (returned rows and mutation records) and the temp spill of materialised `c`.
- The skip's cost: zero groups made during a long build stay until it ends.
