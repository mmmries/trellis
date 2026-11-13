# X4 results: candidate A for partition-leaf builds (#1917)

Research only. Nothing here merges. Builds on [`../DESIGN.md`](../DESIGN.md), [`../x1/X1-results.md`](../x1/X1-results.md)
and [`../x3/X3-results.md`](../x3/X3-results.md). Code: commits `02589c72` (leaf path, plants, tests) and `021ecfd1`
(measurement switch, harness) on `research/issue-1917-absolute-build-write-once`.

## Verdict

- **Leaves can use A, as one statement (option (a)).** X3's blocker was not real: the "session's settings" a leaf's
  `GROUP BY` runs under are themselves pinned. Every Trellis connection sets `DateStyle = 'ISO, YMD'` and
  `TimeZone = 'UTC'` (`pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`), the same values `pinned_settings` sets. The two
  settings sets differ only in `search_path`, and one path serves both halves of step 1:
  `pg_catalog, pg_temp, <session path>`.
- **The extra rule the leaf form adds:** step 1 of a leaf runs under `pin_settings` followed by
  `search_path = pg_catalog, pg_temp, <session path>`, and only when the session's `DateStyle`/`TimeZone` are the
  pinned ones (checked; otherwise today's path). Everything else is A's existing rules, per leaf.
- Only leaves of a tree **as recorded** (`read_node`) take A. A leaf read through the root and the **remainder** stay on
  today's three statements.
- Tests: every partition suite that reaches a group-basis fresh build or resume passes (table below); four new
  deterministic leaf tests pass; each A plant that applies to leaves is caught by one of them.
- Measured cost: see "Measurements".

## 1. Why the leaf path pins settings, and what it reads

A group-basis rebuild of a partitioned source is one transaction per leaf of the recorded tree, then one for the
remainder (`build::run_group_rebuild_held`). For a leaf, `build::locked_leaf_scope` locks the leaf `ACCESS SHARE`,
checks the tree against the record (`read_node`), and builds a `RebuildScope` from the leaf's recorded
`pg_get_partition_constraintdef` text (`partition_correction::leaf_scope`).

| statement (today, `group_basis::rebuild` with a scope) | reads | under |
|---|---|---|
| enumerate (`rebuild_groups_sql`) | hidden rows `h` **where route(h)**; visible target `t` **where route(t)**; the leaf's distinct groups (no predicate); ring old images `s` **where route(s)** | **pinned settings** |
| lock (`lock_sql`) | the enumerated groups | session |
| replace (`rebuild_sql`) | `read_node`: one `GROUP BY` of the leaf; else one index probe of the **root** per group | session |

The settings matter only for the three route predicates. `route_predicate` wraps the recorded text,
`exists (select 1 from (select (h."g")::<key type> collate <coll> as "g") as __route where <text>)`. The text was
deparsed under the pinned settings (`capture::partitions::read_tree_once`), so names outside `pg_catalog` are
schema-qualified and literals are ISO/UTC spellings. Evaluated under a path that lists another schema before
`pg_catalog` (an application's `public, pg_catalog`), an operator or collation there shadows the built-in and rows
misroute (`partition_routing`'s `an_operator_in_public_does_not_misroute_under_the_pinned_search_path`). `DateStyle`
and `TimeZone` change how a bound literal parses. The routing of a group to a leaf is what needs them; the recorded
text is never re-read here.

The remainder's scope is `not (route_1 or … or route_n)` over `h`, `t`, `s`, with no own rows; its replace probes the
root per group.

## 2. Options

### (a) One statement after all: **sound, implemented**

The two halves of step 1 need:

- the route predicates: the meaning they have under `search_path = pg_catalog, pg_temp`, `ISO, YMD`, `UTC`;
- the `GROUP BY`, the enumeration's reads of Trellis's tables, the insert, and the visible-row tail: the meaning they
  have under the Trellis session, `search_path = "<schema>"`, `ISO, YMD`, `UTC` (`pool::session_setup`).

`DateStyle` and `TimeZone` are the same in both. For the path, step 1 runs under `pg_catalog, pg_temp, <session
path>`:

- **Routing.** Every name the pinned path resolves, this path resolves the same way: it lists the pinned path first,
  in the same order. The session schema, appended, can only add a candidate for a name the pinned path doesn't have,
  or an operator/function overload. Trellis creates no operators there and only argument-less functions named unlike
  built-ins (`pool::search_path_sql`'s doc, which the session already relies on), and a bound's text calls none.
- **Everything else.** The session's own path `"<schema>"` searches `pg_temp` then `pg_catalog` implicitly first for
  relations and types, and `pg_catalog` first for functions and operators (`pg_temp` never). The combined path has
  the same members; only `pg_temp` and `pg_catalog` swap for relations and types, which only a temporary relation
  named like a built-in type would tell apart. Trellis's temporary tables are `trellis_…`. Unqualified names of
  Trellis's catalog (`segments` in the stamp) still resolve through the appended schema.

So both halves mean what they mean today, in one statement with one snapshot. No catalog function, no
`set_config` inside the statement (it would not be statement-scoped, and the parse-time meaning of the predicate is
fixed before execution anyway). The settings are set by `set_config(…, true)` before the statement and restored
after it, as `pin_settings`/`restore_settings` already do around today's enumeration.

What the argument rests on, and how the prototype guards it:

| assumption | guard |
|---|---|
| the session's `DateStyle`/`TimeZone` are the pinned values | `session_is_pinned` checks them before taking A; otherwise today's path. Every Trellis connection sets them, so the fallback is dead code in practice. |
| the routing text resolves only through `pg_catalog`/`pg_temp` or qualified names | it is deparsed under the pinned path (existing) |
| Trellis's schema holds no operator and no built-in-named function | existing assumption of `search_path_sql` (hand edits of Trellis's schema are out of scope) |
| no temporary relation named like a built-in type | Trellis names its temporary tables `trellis_…` |

Test evidence: `a_leaf_build_under_other_database_defaults_routes_exactly` builds per leaf over a `date`-ranged
source on a database whose defaults are `German, DMY`, `America/Los_Angeles` and `search_path = public, pg_catalog`,
with a reversed `>=` for `date` in `public`: exact. `partition_routing` (unchanged) covers the predicate itself.

### (b) Pre-enumeration at an earlier snapshot E: **sound only with the source's groups at E; not implemented**

The analysis in the brief holds. Counterexample without the source branch at E: group `g` has one row `r` that
predates capture. Change `c` deletes `r` and commits in `(E, S1)`, undrained at `S1`. At E the ring doesn't hold `c`
and the hidden table has no row for `g`; at `S1` the source has no row in `g`. `g` is enumerated nowhere, so step 1
writes nothing for it; `c`'s page later inserts a zero row on no basis for `g` and applies `−1`: wrong (`__n = −1`).

With the source's groups also read at E it is sound, for today's reason: today's enumeration is exactly such a
pre-enumeration (its own statement, before the lock and before the write's snapshot), and R8's argument does not
need it to share the write's snapshot. Under A the group set becomes `E-enumeration ∪ GROUP BY(S1)`; a group in it
with no row at the insert is written from `S1` (no page touched it: the GC skip is in force from the build's start,
before E), one with a row is locked and rewritten after `S1`. A group in neither was created after E with every
change captured, so its hidden row (or none) is already exact.

Cost: the extra pass over the leaf (distinct groups, into a temp table) is today's enumeration, so (b) keeps one of
the three passes A removes. With (a) available, (b) is not worth implementing.

### (c) Keep leaves on today's path

Not needed for correctness. Cost: see "Measurements".

## 3. What the prototype does

- `group_basis::rebuild`: a fresh build or resume (`fresh`) of a plain aggregate whose scope is a leaf with
  `node` and `read_node` takes `write_once(…, Some(leaf), …)` when `session_is_pinned`; the remainder and a leaf read
  through the root don't. (`TRELLIS_X4_LEAF_TODAY` turns the leaf path off, for the measurement only.)
- `write_once`: for a leaf, `write_once_settings` (`pin_settings`, then `search_path = pg_catalog, pg_temp, <saved
  path>`) before step 1, `restore_settings` after it. Step 2 (stragglers) runs under the session's settings and reads
  them by index probes of the root (≤ 10,000 stragglers and an index led by a group column) or by one `GROUP BY` of the
  leaf.
- `write_once_sql(…, leaf, …)`: the `GROUP BY` reads the leaf; the enumeration is `enumeration_branches(leaf, "$1",
  false)`: hidden, target and ring branches with the leaf's route predicates, no own-rows branch (the leaf's rows come
  through the `GROUP BY`).
- `rebuild_groups_select_with` now joins `enumeration_branches` (same text as before).

Statement as emitted (leaf step 1, normalised by `pg_stat_statements`): [`stmt-leaf-step1.sql`](stmt-leaf-step1.sql).

## 4. Tests

Targeted only. Partition suites that reach a group-basis fresh build or resume (found by a temporary `panic!` on the
leaf path: the count is how many tests reached it):

| suite | reached the leaf path | result |
|---|---|---|
| `partition_group_basis` | 3 | 52/52 (1 ignored, as on base) |
| `partition_restore` | 5 | 5/5 |
| `partition_row_security` | 3 | 3/3 |
| `capture_partition_pass` | 18 | 18/18 (1 ignored) |
| `capture_disruption_partitioned` | 12 | 13/13 |
| `group_basis_resume_after_source_key_change` | 1 | 3/3 |
| `resume_after_source_key_change` | 1 | 12/12 |
| `partition_routing`, `partition_sweep`, `partition_truncate`, `self_check_partitioned`, `partition_lifecycle_cells`, `define_partitioned_source`, `capture_partitioned`, `capture_partitioned_staging`, `partition_thousand_leaves` | 0 | all pass |
| **new** `partition_group_basis_write_once` | 4 | 4/4 |

X3's suites, re-run: same results as X3 (`group_basis_build` 19/20, the expected `pages_between_the_enumeration_and_the_lock`;
everything else green, `group_basis_write_once` now 8/8).

New leaf tests (`trellis/tests/partition_group_basis_write_once.rs`; a 4-leaf `RANGE` source, a second aggregate
`agg2` defined over the seeded source so its build runs per leaf):

| test | interleaving | plants it catches |
|---|---|---|
| `a_page_creates_a_group_in_the_leaf_inside_step_one` | a page holds the leaf's lowest group; the leaf's step 1 takes `S1` and waits on it; a page then adds a row to group 2 (a group `S1` sees) and creates its row on no basis, and creates group 3 (new after `S1`). Group 2 becomes a straggler, rewritten from step 2's snapshot `(3,3,1033,t)`; group 3 keeps its page's row. | `ab_stragglers_from_s1`, `gb_build_snapshot_before_locks`, `ab_leaf_unrouted` |
| `a_leaf_build_leaves_another_leafs_groups_alone` | before the first leaf's step 1, pages create rows for group 1 (first leaf) and group 5 (second leaf). Between the leaves' transactions group 1 is rewritten and group 5 is untouched `(1,1,1000,f)`; after the build both are exact. | `ab_leaf_unrouted`, `gb_build_snapshot_before_locks` |
| `a_group_vacated_in_the_leaf_by_an_undrained_change_is_zeroed` | group 2's rows deleted before the build, undrained at `S1`: the routed ring branch names it, step 1 inserts it at zero, the late page is refused. | `ab_enum_no_ring` |
| `a_leaf_build_under_other_database_defaults_routes_exactly` | the settings argument above | none (no plant targets it: see X6) |

The "vacated across E and S1" test of option (b) does not apply: (b) was not implemented.

## Measurements

Logbook `20261112T174604-r-bench` (ok). One release binary at the X4 commit; `TRELLIS_X4_LEAF_TODAY=1` selects
today's leaf path. Postgres 18.6 scratch cluster with X3's settings. Fresh build of the 1M-group / 10M-group
aggregate (one group per row) over a hash-partitioned source, define to `live`. Every target equals the plain
`INSERT … SELECT` (`EXCEPT ALL` both ways: 0 rows). 2 reps at 1M (both shown), 1 rep at 10M.

| source | path | plain s | build s | x plain | WAL | hidden table |
|---|---|---|---|---|---|---|
| 1M rows, 32 leaves | today | 0.8–1.0 | 16.3 / 16.4 | 18x | 859–940 MB | 269 MB |
| 1M rows, 32 leaves | A per leaf | 0.8 | 11.8 / 11.8 | 14x | 455–518 MB | 140 MB |
| 1M rows, 1 leaf | today | 0.8–0.9 | 16.9 / 16.7 | 19x | 1,011 MB | 269 MB |
| 1M rows, 1 leaf | A per leaf | 0.8–1.1 | 9.3 / 9.3 | 10x | 454–518 MB | 140 MB |
| 10M rows, 32 leaves | today | 8.9 | 144.2 | 16.2x | 10.5 GB | 2.7 GB |
| 10M rows, 32 leaves | A per leaf | 8.6 | 90.7 | 10.5x | 6.0 GB | 1.4 GB |

- A per leaf cuts the build by 28 % (32 leaves) to 44 % (1 leaf) at 1M and by 37 % at 10M, and roughly halves the WAL
  and the hidden table.
- Partitioning already avoids the collapse the unpartitioned build shows at 10M groups (1,535 s in E5): each leaf's
  hidden rows stay small enough to update in place. So A's gain per leaf is the double write, not the order effect.
- The ~5.5 s define-to-live floor (X3) is inside every 1M number. At 10M, A per leaf (90.7 s) is slower than A over
  the unpartitioned source (81.7 s in X3): 32 transactions, and each leaf's rows also pass the routed enumeration.
  Not broken down further.
- The 1M/10M sources were loaded through capture with a warm definition (the loader waits until the warm aggregate
  has counted every row), so the ring was drained before each measured build.

## What remains open

- The remainder stays on today's path. Converting it needs step 1's `a` to be a probe of the root per enumerated
  group (it has no leaf to `GROUP BY`), at `S1`, inside the statement. Possible; the remainder is small by design
  (groups that route to no recorded leaf), so not worth it.
- A leaf read through the root (tree not as recorded) stays on today's path; the change's own correction repairs it.
- `JOIN` sides per leaf (`join::build`) are untouched.
- The settings argument is a reasoned one plus one test. A production version should state the rule in the routing
  docs (one path for routing and reading) and pin it with the existing `partition_routing` matrix run under the
  combined path.
