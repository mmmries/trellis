# X7: read-only analysis for #1917 (JOIN cells, other absolute writers, `xmax` audit, rebuild paths)

Research only. Nothing changed, nothing run. Code read: `/home/mike/code/trellis` at `main` `36439689`.
Spec: [`../DESIGN.md`](../DESIGN.md) (candidate A), [`../x1/X1-results.md`](../x1/X1-results.md),
[`../x3/X3-results.md`](../x3/X3-results.md).

Each answer marks **[read]** (seen in code or SQL) and **[inferred]** (my reasoning from it). Function names are
`main`'s; "S" is an absolute write's snapshot.

## Summary

| Q | verdict |
|---|---|
| Q1 `P`/`M` | **A does not apply as is, and isn't needed for keyed cells.** The `JOIN` build already writes every keyed `P`/`M` cell once, after its `J` lock (the lock class is `J`, not the cell). A literally applied (cell inserted from S1 before the `J` lock) breaks JR1/I5 and deadlocks with a page. A applies **with extra rules** only to `P` cells with a `NULL` key part (row-locked, no `J`), a small set. `J` rows of new keys are written twice (insert, then the product's `__matches` update); A can't fix that. |
| Q2 release | **leave as is**: two writes, a handful of groups. |
| Q2 partition correction | **candidate**, with extra rules: enumeration moved into S1's statement, and X3's pinned-settings problem. The GC-skip predicate already holds (the definition is `backfilling` under a build for the correction's span). |
| Q2 page `Recompute` | **leave as is**: two writes, ≤ a few groups; not under a build, so A's GC rule can't hold. |
| Q3 `xmax` audit | **No production use relies on the unsound form.** Every `pg_snapshot_xmax`/`xip` use is of a fence snapshot built with the autocommit `pg_current_xact_id()` bump, or uses `xmax` only as "≤ latestCompletedXid + 1". The only `xid >= xmax` reasoning is in a test-only plant. One edge window (1-1 sweep's `start_xid` test) is sound for its stated purpose; whether its side window matters I couldn't settle. |
| Q4 rebuild paths | **X3's "none is not under a build" holds on `main`.** Every caller is a plan or sweep job whose enqueuing transaction set `backfilling` + `build = 'rederive'` under the definition row `FOR UPDATE`; that holds until `try_complete`, which can only run after the rebuild's commit. One discrepancy: on `main` a partitioned root with no rows and no record **never reaches** `rebuild` (it takes the PK walk), unlike X3's note. |

---

## Q1. Does candidate A's form apply to a `JOIN … GROUP BY` transform's `P` and `M` cells?

### Answer

No as is. The `JOIN` build doesn't need it for keyed cells. It already does what A is for. The lock for a keyed
cell `(g, k)` is `J[k]`, a row in another table. The build locks `J` first, takes its snapshot, then writes each
cell once: an update of a held cell, an insert of an absent one (`GroupTargetPlan::join_build_sql`). No placeholder
cell row is written. A literal A (insert the cell from S1 in the lock statement) would put the cell write **before**
the `J` lock. That breaks JR1's order (ADR-0028 I5: `J`, then cells, then `T`) and deadlocks (interleaving below).
Moving the insert after the `J` lock gives back today's code.

Two places still write twice:

- **`P` cells with a `NULL` key part** take no `J` row. The build locks them with the plain aggregate's lock
  statement (`lock_sql_where(null_key_filter)`: zero row for an absent cell), then the write updates it. A could
  apply to these with **extra rules**:
  - the zero-cell GC skips the definition's `P` table under a build. X3 skipped `agg_<id>` tables only.
  - the stragglers get a second pass after the lock.
  - the step-1 statement must also sit after the `J` statement in I5 order. It holds no `J` row, so that's fine.

  The set is small (facts whose join key is `NULL`), so the gain is small.
- **`J` rows of new keys**: `j_insert_sql` inserts them (`__matches` 0), `j_lock_sql` locks them, and the product's
  `matched` CTE updates `__matches`. The final value is `Σ M.n` after the build's `M` write, read under the lock (JR2).
  The product also reads the old `__matches` for the `LEFT` flip terms. So the lock statement can't write `J` from
  its own read. A doesn't help here. #1876 D (sort that update) is the cheaper fix. **[inferred]**

`T` is settled: the `Single`/`Final` build already locks and writes it in one sorted upsert (#1877, closed, in
`upsert_hidden_build_sql`).

### The build's statements today (`join::build`, `BuildWork::Single`, both sides unpartitioned) [read]

| # | statement (function) | structure | locks | snapshot | written twice? |
|---|---|---|---|---|---|
| 1 | `lock_row_sql(Share)` | truncate lock row | `FOR SHARE` | own | no |
| 2 | `group_basis::enumerate_cells` ×2: `create temp table … as` `rebuild_groups_select` (hidden ∪ table ∪ ring old images; no target branch for `JoinPartials`) | `P`, `M` (read) | `ACCESS SHARE` on tables | E_P, E_M (own) | – |
| 3 | `write_locked` → `j_insert_sql` (`insert … order by k on conflict do nothing`) over `staged_keys_sql` (both sides' keys, non-null) | `J` | inserts new rows | own | **yes, for new keys** (with #9) |
| 4 | `j_lock_sql` (`select … order by k for update of j`) | `J` | `FOR UPDATE` on every key | own | lock record only |
| 5 | per side, `side.plan.lock_sql_where(null_key_filter)` (`bulk_node` is `Some`) | `P` cells with a `NULL` key part (`M` has none: `stage_filter`) | inserts zero rows / locks existing | own | **yes, for absent ones** (with #7) |
| 6 | `write_join_build_cells` → `join_group_sql` (`create temp table grouped as` `GROUP BY` ∪ a `__meta` row of `pg_current_snapshot()` and `max(seg_seq)`), possibly `strictly_or_generally` | `P` (read source) | – | **S_P = the cells' basis**, after every lock statement | – |
| 7 | `declare trellis_join_match_hold cursor …; fill` (`join_build_sql`[0]: match each enumerated cell to its hidden row's `ctid` and held partials, and to `grouped`) | `P` (read) | – | C_P, kept by the cursor until #8 | – |
| 8 | `join_build_sql`[1]: `up` = `update … where u.ctid = r.__hc`; `ins` = insert where `not exists` in `up`; `d` = deltas (new − held) into `__dp` | `P` | row locks of updated rows | own | keyed cells: **once** |
| 6–8′ | the same for `M` (S_M, C_M, `__dm`) | `M` | | S_M | once |
| 9 | `product_sql`: `ΔP ⊗ M_new + P_old ⊗ ΔM` (+ `LEFT` terms, reading `J.__matches` as old) into `T`'s staging; `matched` CTE `update J set __matches = __matches + Σ ΔM.n` | `T` staging, `J` | `J` row update | own (after both writes) | `J` see #3 |
| 10 | `enumerate_target_groups` (`target_groups_sql`) | `T` staging (+ visible target and `T` groups, R8) | – | own | – |
| 11 | `upsert_join_build_groups` (`insert … order by g on conflict do update set += where nonzero`) | `T` | row locks via upsert (W3) | own | once |
| 12 | `compare_join_build_groups` (`compare_held_sql`) | visible target | – | own | once per changed row |

Partitioned-side builds (`BuildWork::Side`, `run_join_steps`) differ:

- `T` is written the page's way (`T.lock_sql` zero insert, then `apply_locked_groups`): twice for absent groups.
- A leaf whose tree isn't as recorded has `bulk_node` `None`, so every cell, keyed or not, gets `lock_sql` (zero row),
  then `recompute_locked_groups_from` (update): twice for absent cells.

`release` and `recompute_routed` (non-bulk `write_locked`) take that same path.

### What `J` (JR1), JR2 and the product require that the plain aggregate doesn't

- **JR1**: a keyed cell's writer holds `J[k]` before writing the cell. A page locks `J` and then the cells it writes,
  in that order. A build step that writes a cell before `J` reverses it.
- **JR2**: the other side's partials and `J.__matches` are read only after the `J` lock. The build's product (#9)
  reads `P`, `M`, `J` and the deltas after both writes, all under `J`.
- **Product**: the build computes it itself, in #9, from its own statement's snapshot: the hidden tables as of after
  its writes, with every page that held `J[k]` before the build already committed, and none since.
  - It doesn't read S_P/S_M at all. The bases filter source changes. The product is bookkeeping between hidden
    rows that `J` freezes.
  - Write-once (insert vs. update) doesn't change what #9 sees: it reads `__dp`/`__dm` (new − held) and the rows
    after the write. **[read + inferred]**

### The interleaving asked for (A literal on a keyed `P` cell) [inferred]

Build step 1 takes S1 and inserts `P[ga,k]` (absent) with S1's values, uncommitted, before locking `J[k]`.
Two cases:

1. **An `M`-side page holds `J[k]`, applies `ΔM` and computes `P_old ⊗ ΔM`.** The page reads `P[ga,k]`: the build's
   row is uncommitted and invisible, so `P_old = 0`. It commits. The build then locks `J[k]` and reads `M_new`
   (with `ΔM`) and adds `P_S1 ⊗ M_new`. The sum is exact by bilinearity. **The arithmetic survives**, provided the
   build's product reads `M` after its own `J` lock.
2. **An `a`-side (or mixed) page for the same cell.** It locks `J[k]`, then its cell lock statement (`lock_sql`) on
   `P[ga,k]` waits on the build's uncommitted insert. The build then waits on `J[k]`. **Deadlock (I5).** A page that
   only reads the cell in its product doesn't block; one that writes it does.

So the obstacle is lock order, not the delta.

### Does "nothing removes a row between S1 and commit" hold for `P`/`M`/`J` today? [read]

- **`J`**: yes. `hidden_tables_sql` lists `agg`, `P`, `M`, `T` but never `J` ("A `JOIN`'s `J` rows are never
  collected"). A truncate clear only sets `__matches = 0`, under the lock row `FOR UPDATE`, which waits on the
  build's `FOR SHARE`.
- **Keyed `P`/`M` cells**: no, and it doesn't need to.
  - `collect_zero_groups` does not skip a build, takes no `J` lock, and claims `for update skip locked`, so it can
    delete a zero keyed cell during the build.
  - The build tolerates it (doc on `join_build_sql`, `write_join_build_cells`). The cursor holds C_P, so the dead
    row's `ctid` can't be reused. The `update` then misses and `ins` inserts the cell. Its held partials were zero,
    so the delta is the same.
  - This is safe for a different reason than A's: `J[k]` is held from before S_P, so no page wrote the cell since
    S_P, whether its row exists or not. **[inferred]**
- **`P` cells with a `NULL` key part**: they are row-locked by #5 before S_P, and the GC skips locked rows, so
  nothing removes them during today's build. Under A they would be written from S1 without a lock, and the X1
  counterexample would apply. So the skip would have to cover `P`.
- **Truncate lock row**: held `FOR SHARE` from #1 to commit. A `TRUNCATE` page needs it `FOR UPDATE`, so its
  `DELETE`s of `P`/`M`/`T` can't land between S and the commit. A `TRUNCATE` statement queues on the tables'
  `ACCESS SHARE`.

### Verdict and dependency on #1876 / #1877

- **Keyed `P`/`M`: does not apply** (the interleaving above). **Not needed**: already one write per cell under `J`.
- **`NULL`-key `P` cells: applies with extra rules** (GC skip extended to `P`, stragglers after the lock, in I5
  order). Small gain.
- **`J`: not addressable by A.**
- **Partitioned-leaf `JOIN` steps, release and correction of a `JOIN`.** Their keyed cells take a redundant zero-row
  lock plus update. They could write once by reusing `join_build_sql`'s update-else-insert after `J`, with no A
  rule needed. **[inferred]**

Dependencies:

- **#1877** is closed, and its fused `T` upsert is on `main` (`upsert_hidden_build_sql`).
- **#1876** (open) A, D and K change, in order:
  - `join_build_sql`'s insert order (A);
  - `product_sql`'s `J.__matches` update order (D);
  - the product's `__dp`/`__dm` re-aggregation (K).

  None changes which rows are written how many times, or which snapshot a read uses. My conclusions depend on two
  things they touch:
  - (a) keyed cells written once by update-or-insert after `J`, with the cursor holding C;
  - (b) the product reading the hidden state after both writes.

  If #1876 turned the cell write into an upsert, or dropped the cursor, the GC-deleted-cell argument should be
  re-checked.

---

## Q2. The other producers of absolute group writes

All three use the same two statements: `lock_sql` (zero row for an absent group, lock the others), then
`recompute_sql` (`SNAPSHOT_AND_STAMP`, read by probe or node `GROUP BY`, `update … where u.ctid = any(…)`). Every
absent group is written twice. **[read]**

| producer | code path | absent groups → 2 writes? | typical groups | A correct as written? what would have to hold | verdict |
|---|---|---|---|---|---|
| **Quarantine release** | `quarantine` (`release_key` body) → `group_basis::release` → `recompute_groups`: `name_groups` (reads source, no lock), `lock_sql`, `recompute_locked_groups`. `JOIN`: `join::release` → `write_locked(build=false)` | yes | the held key's `agg_groups` plus its current row's group: one to a few | Not under a build (definition usually `live`). Instead, the release bumps the source's version fence as its first lock and holds it to commit, so no page of the source applies between S1 and commit. A page applies only committed changes that S1 sees. So the X1 counterexample (a change after S1 drained into `g` and GC'd before the insert) can't occur **[inferred]**. A would need "no page of the source applies between S1 and commit", which the fence gives. Not verified for a partitioned source's fence name. | **leave as is** (no gain) |
| **Partition correction** | `partition_correction/group.rs` → `routed_groups` (its **own earlier transaction**), then in the correction transaction `group_basis::recompute_routed` → `recompute_named_groups` (`lock_sql`, `recompute_locked_groups_from(node)`). `JOIN`: `join::recompute_routed` | yes | every group routing to the node: few for a `DROP`/`DETACH`/`TRUNCATE` (rows exist, mostly zeroed); **potentially a whole leaf** for an `ATTACH` of a populated table or a new leaf written before capture (rows absent → all written twice) | The span is under a build. `enqueue` takes the definition row `FOR UPDATE`, moves `live → backfilling` with `build = 'rederive'`, and inserts a `correction` `backfill_chunks` row. `try_complete`/`build_done` won't flip until that row is done, which the correction's own transaction records. So `NOT_BUILDING_SQL` would skip for the whole span **[read]**. Extra rules A needs: (1) the enumeration must be read in S1, in step 1's statement (X1 rule 3), but today it's a separate earlier transaction; (2) its routing predicates run under pinned settings, the same conflict X3 found for leaves; (3) the read through the root (probe per group) when the tree isn't as recorded; (4) a `JOIN` correction's keyed cells need no A (Q1). | **candidate**, for large `ATTACH`/new-leaf corrections only; same blockers as X3's partition leaves |
| **Page `Recompute`** (image-less change) | `apply_group_target` → `lock_groups` (`stage_sides`, `name_groups`, `lock_sql`) → `apply_locked_groups` → `recompute_locked_groups` | yes | the key's current row's group and its hint's: ≤ 2 per change | Not under a build, and pages and the GC run concurrently, so X1's counterexample applies: another page drains a later change into `g` after S1, the `+1 −1` zero row with a `NULL` basis is collected, then this page's insert writes S1's values. Also, the page's own sides apply between its lock and its recompute, and ADR-0028 "Pages keep their forms". | **leave as is** |

---

## Q3. Audit: snapshot `xmax` reasoning

**Answer.** No engine code relies on "every transaction with an xid at snapshot time is below `xmax` and finished,
or in `xip`". The places that reason from `xmax`/`xip` do so about **fence snapshots**, and the fence is built so
that the reasoning is true:

- `seal_phase2` runs `select pg_current_xact_id()` in **autocommit** right before `pg_current_snapshot()`. The
  function takes `&Client`, so it can't run inside a transaction.
- Every capture writer assigns its xid before it reads the slot mirror: `case when pg_current_xact_id() is not null
  then pg_sequence_last_value(mirror) end`, in the trigger (`capture/sql.rs`) and in `append::active_ring_slot`.

`docs/staging-and-claiming/03-sealing-and-the-fence.md` "The `xmax` trap" describes the hazard. Every other test is
`pg_visible_in_snapshot`, `xmin` vs an assigned xid, or `xmax` used only as a lower bound.

| # | where | expression | assumes | verdict |
|---|---|---|---|---|
| 1 | `seal::seal_gate_blocked` | `pg_snapshot_xmin(pg_current_snapshot()) < pg_snapshot_xmax(fence)` | every writer of slot k has xid < `xmax(S_k)`. Established, not assumed: the writer's xid is assigned before its mirror read, which comes before phase 2's mirror set, which comes before the autocommit bump, which comes before S_k. So `xmin(now) ≥ xmax(S_k)` means they have all ended. | **sound** |
| 2 | `retire::LATE_ROWS_CANDIDATES_SQL` | `xmin(now) >= xmax(fence_snapshot)` and `exists (select 1 from pg_snapshot_xip(fence))` | same invariant: a late row of slot k is a transaction in `xip(S_k)`, so it has finished once `xmin ≥ xmax(S_k)` | **sound** (relies on #1's construction) |
| 3 | `page::ring_slot_applied_sql` | `not exists (select 1 from pg_snapshot_xip(s.fence_snapshot))` ⇒ no late row | same invariant (its doc cites "The `xmax` trap") | **sound** |
| 4 | `retire::RETIRE_CANDIDATES_SQL` | `xmin(now) > seal_step2` | `seal_step2` = the successor's phase-1 xid, assigned after `S_k` was published (the gate needs it), so it is above every slot-k writer's xid. `xmin` > it means all of them have ended. | **sound** (`xmin` + assignment order) |
| 5 | `build::fence_settled` | `xmin(now) > fence_xid` | `fence_xid` is assigned after the start commits. Every seam writer took its xid before reading the reader list (`target_mutations`: `select pg_current_xact_id()` before the first check). | **sound** |
| 6 | `app.rs` status `build_fence` | `xmin(now) <= fence_xid` | report only | **sound** (display) |
| 7 | `one_to_one_ledger::stale_entries_statement` (1-1 and 1-1 `JOIN` sweeps) | stale ⇔ `basis is null or pg_snapshot_xmax(basis) <= start_xid` | Uses `xmax` = latestCompletedXid + 1 correctly. A basis taken before `start_xid` was assigned has `xmax ≤ start_xid`, so it is stale (sound direction). A chunk's basis is taken after the start commits, so `xmax > start_xid` and it is fresh (the comment on `start` says exactly this). | **sound for its stated purpose**. Not the X1 form. Side window: a non-chunk Re-derive basis taken after `start_xid` was assigned but before the start committed can read "fresh" and be skipped. **Could not determine** whether such an entry can exist and matter (a definition being resumed doesn't apply before the start commits; a live one's entry is a correct live read). I lean harmless. |
| 8 | `group_basis` plant `GbLsnNotVisibility` (`apply_sql_with`: `sg.__txid >= pg_snapshot_xmax(b.__basis)`) and `planted_in_flight_sql` (`txid < xmax and not visible`) | – | test-only, deliberately wrong. Production uses `not pg_visible_in_snapshot(sg.__txid, b.__basis)`. The counter counts exactly the cases the plant changes. | **n/a (test-only)** |
| 9 | `pg_visible_in_snapshot` everywhere: page apply (R2, `group_basis` `apply_sql_with`), ledgers (`staging/ledger.rs`, `self_check_ring`), `seal::fenced_window`/`window_over_fence`, `predecessor_has_unfenced_row`, `page.rs` owning segment, `converge` conditions, `quarantine::discard_drained_changes`, `count_late_rows` | Postgres's own visibility test | "did this snapshot see that xid" | **sound**. The owning-segment uses rely on #1's invariant. |
| 10 | stamps: `SNAPSHOT_AND_STAMP` (rebuild, recompute, probe), `join_group_sql`'s `__meta` row, `build/one_to_one.rs` chunk `snap`, `one_to_one_ledger` read rows | `pg_current_snapshot()` and `max(seg_seq) from segments` **in one statement** | "every change S saw is owned by a segment ≤ stamp" | **sound [inferred]**. A change S sees committed before S. Its owner is its slot's segment k, or k+1 for a late row. A late row committed after `S_k`, which follows phase 1's commit of k+1, so S sees k+1. |
| 11 | `retire::DRAINED_PREFIX_SQL` and `collect_zero_groups`' `__stamp <= $2` | segment states, read once per pass and used in later transactions | "drained" is monotone (never reverts; retired only after drained) | **sound [inferred]**. X1's s11n hazard is about position prefixes read while a writer's row is invisible. Here, a segment ≤ the prefix is drained, which is a fact that stays true. |
| 12 | `build::stamp_rebuild_seg` / `ledger::page_may_predate_build` | `build_seg` = active segment, read `for share` on `segment_pointer` | lock-based, no `xmax` | **sound** (argument in `start`'s doc; read, not re-derived in depth) |
| 13 | row-version `xmin` (`drift_memo`, `capture::partitions` `inherits_xmin`, `quarantine` root-row mark, `capture_triggers.trigger_xmin`) | equality of a tuple's `xmin` | identity fingerprints, not snapshot bounds | **out of scope** (not snapshot reasoning) |

The `xmax` uses are safe only because of the bump's two conditions (autocommit; the writer's xid before its mirror
read). Tests pin the order: `capture/sql.rs` asserts "the xid comes first". **[read]**

---

## Q4. Every path into `group_basis::rebuild`

### Answer

`rebuild` has one caller: `rebuild_definition`. Its only caller is `run_group_structure`, in its non-`JOIN` branch
(`build.rs`). `run_group_structure` is reached from `run_group_rebuild_held` in two ways:

- the **plan job** (`run_plan`, `ReadersOnCommit::Park`);
- the **sweep job** (`run_sweep` → `run_group_rebuild`, `Keep`).

In the per-leaf case it is called once per leaf, then once for the remainder.

Every such job is enqueued by a transaction that holds the definition row `FOR UPDATE` and leaves it `backfilling`
with `build = 'rederive'` (`start_rederive`). The definition stays that way until `try_complete`. `build_done` sees
the job done only after the rebuild's own transaction commits: the rebuild marks the job done in that transaction.
So `NOT_BUILDING_SQL` would skip for the whole span, from before any snapshot of the rebuild to its commit.
**X3's claim holds** **[read]**.

Today the zero-group pass doesn't call `NOT_BUILDING_SQL`. This answers whether it would hold if it did.

| path | job | enqueued by (lock, flag) | hidden table empty at entry? | under a build from before S to commit? |
|---|---|---|---|---|
| fresh build (define) | plan, `scope` none (unpartitioned) | `start_ready_builds` → `start`: column-pause lock, then `select status … for update`, requires `waiting_to_backfill`, `start_rederive`, insert plan row (`start_xid`), `stamp_rebuild_seg`, commit | empty at the start's commit. **Not necessarily at entry**: pages apply from that commit and may have created rows before the rebuild's S. | yes |
| resume (`complete_resume`, incl. after a restore or capture failure) | plan | `complete_resume`: definition row `for update`, `start_over` (`waiting_to_backfill`, `build = null`, new fuse epoch), deletes unclaimed chunk rows, locks claimed ones `for update` (waits out in-flight writes), `recreate_group_table`; then `start` as above | recreated empty; pages after `start` as above | yes. The gap between `complete_resume` and `start` (`build = null`) has no rebuild running: an old claim fails `fence.hold` (stale epoch / row deleted) before its rebuild. |
| numeric-sums attach (`numeric_sums::build_afresh`) | plan | attach transaction: `for update` on the definitions, `recreate_group_table`/`recreate_hidden_tables`, deletes undone chunk rows, `start_over`; then `start` | recreated empty | yes (as resume) |
| dump restore | plan | rows reset to `waiting_to_backfill`, `build` null; hidden tables are `Runtime` class (not dumped); then `start` | empty | yes |
| `request_backfill` (`app.rs`) | sweep | `catch_up::park_table_catch_ups` → `rebuild`: `for update` in id order, `rebuildable` (applying), `start_rebuilds` (`live → backfilling`, `build = 'rederive'`; a `backfilling` one stays), sweep row; `stamp_rebuild_seg` | no (full) | yes |
| capture re-install (`capture/install.rs`) | sweep | same `park_table_catch_ups` | no | yes |
| automatic rebuild (`capture/baseline.rs`) | sweep | same `park_table_catch_ups` | no | yes |
| upstream rebuild | sweep | `park_target_catchup_if_read` in the upstream build's finishing transaction (`run_group_structure`) → `rebuild` → `start_rebuilds` | no | yes |
| partition leaf / remainder transactions | plan (fresh/resume over a **recorded** partitioned root) or sweep | as the rows above; each leaf transaction takes the definition row `for key share` and `locked_leaf_scope`'s `ACCESS SHARE`; the remainder transaction marks the job done | fresh: rows from earlier leaves and pages; sweep: full | yes. `try_complete` needs the remainder's done mark. |
| `start_rebuilds` while already building | sweep | adds rows; status stays `backfilling` | – | yes |
| anything else? | – | `start_field_build` with `fields` on a group-basis definition takes the PK walk, not `rebuild` (`aggregate = chunk.fields.is_none() && …`). The 1-1 `JOIN` `start_rebuilds` callers (`apply.rs` 5b, `quarantine` column resume) don't reach `group_basis::rebuild`. `JOIN`s go to `join::build`. | – | – |

Every rebuild transaction then does, in order **[read]**:

1. `fence.arm`, then `select … for key share` on the definition row, then `fence.hold`, before any statement of
   `rebuild`.
2. A resume, drop or `start_over` that committed first fails `fence.hold`. One that comes later waits on the
   `for key share`, because it takes `for update`.
3. Pause and quarantine change `status` only. `build` stays set, so the skip still holds.

The GC batch's `for key share nowait` conflicts with the starts' `for update`. So a batch that checked before a
start either commits first or makes the start wait.

### Discrepancies with X3 / things to note

- **The empty, unrecorded partitioned root doesn't reach `rebuild` on `main`.** In `run_plan`,
  `recorded = aggregate && (!partitioned || recorded tree)`. A partitioned root without a record and with no rows
  falls through to the PK walk, which enqueues no chunks, then `try_complete`. With rows, it errors.
  `run_group_rebuild_held` sends every partitioned root to the per-leaf path. On `main`, `scope: None` is reached
  only for unpartitioned sources. X3 lists the empty-unrecorded root among A's callers, so it may describe its own
  branch. The difference only narrows the set.
- `build_done` ignores `chunk_queue::STALE` rows. A stale claim can't be in its rebuild when the flip happens: the
  resume waits out in-flight writes through the claim row's `for key share`, and a later attempt fails
  `fence.hold`. This rests on `ClaimFence`'s comments; I didn't re-derive it.

---

## What I could not determine

- Q3 #7: whether a 1-1 ledger entry with a basis taken inside the start transaction's lifetime (after `start_xid`
  was assigned, before commit) can exist and be wrongly skipped by the sweep. I lean harmless.
- Q2 release: whether the version fence's source name covers every page of a partitioned source (root vs. leaf
  `src_table`). It only matters if release were converted, which isn't recommended.
- Q1 has no experiment. The deadlock and the exact-delta claims are reasoning from the lock order and the product
  SQL. If wanted: an X1-style model with a `J` table, a build that inserts a keyed `P` cell from S1 before its `J`
  lock, and a mixed page on the same cell, which should deadlock. Also the same with an `M`-only page, which should
  be exact.
