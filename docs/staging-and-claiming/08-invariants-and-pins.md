# Invariants and the tests that pin them

← [Convergence and await](07-convergence-and-await.md) · [Overview](README.md)

A condensed summary of what the design guarantees and the tests that pin each
guarantee. Each item links to the document that argues for it.

## What is essential vs. what is Postgres-specific

| Essential to the guarantees | Postgres-specific |
|---|---|
| A change is staged in the transaction that makes it | `AFTER … FOR EACH STATEMENT` triggers with transition tables |
| The staging area is append-only on the hot path | a *ring* of N tables, `TRUNCATE` as the retirement primitive |
| Batches are cut in visibility space, with a published, write-once fence | `pg_current_xact_id()` defaults and `pg_snapshot` columns |
| A batch is immutable once sealed; every producer writes to the active batch | [producers](02-the-staging-ring.md#the-producers) in two languages: capture triggers in SQL, the rest through one Rust append |
| Claim exclusivity comes from a unique constraint, not a lock | bucket count, share formula, `hashtextextended` routing |
| Apply ∪ mark-complete are one transaction | the bit-mask coverage of buckets |
| Merging happens at read time, under four fixed rules | ordered `array_agg` arg-extremes |
| Pending work is measured by asking storage, never a boundary aggregate | index + hand-written `ANALYZE` on statistics-free tables |
| Quarantined work still blocks the read-your-writes predicate | the three-table poison track |
| A non-commutative write commits only if its basis is current (I2), under a per-key lock | the `<target>__ledger` table (1-1) or the group's basis (aggregate), `pg_current_snapshot()` as the basis |

## Foundations

- **The staging tables and the source tables share a transaction** — capture
      triggers stage each change in the writer's own transaction, so a change is
      staged exactly when it commits. Same Postgres database. Non-negotiable.
      → [01](01-capture-by-triggers.md)
- **The staging tables and the *target* tables share a transaction** — same
      database — what makes exactly-once for non-idempotent effects possible.
      → [05](05-apply-and-exactly-once-deltas.md)
- **The fence is a store-assigned per-row writer identity (`pg_current_xact_id()`)
      plus an inspectable transaction snapshot.**
      → [03](03-sealing-and-the-fence.md)
- **Enumerate which measures are invertible.** Everything else goes on a
      recompute path. Never approximate an inverse.
      → [05](05-apply-and-exactly-once-deltas.md)
- **Every 1-1 target keeps a per-key ledger** (last-applied position,
      basis snapshot, tombstone) and every non-commutative write checks it
      under the key's entry lock; an aggregate group keeps a basis and a
      stamp, and a page adds a change only if its basis hasn't counted it.
      → [05](05-apply-and-exactly-once-deltas.md#the-ledger)
- **No item may stay undrainable** — one would stop the whole instance. A
      schema error pauses the definitions it reaches and drains without them,
      and `status()` names the halt (`capture_failure` of kind `halt`), so
      "halted" and "slow" stay distinguishable.
      → [05](05-apply-and-exactly-once-deltas.md#failure-classification),
      [06](06-cleanup-and-reclaim.md)

## Rule index

One row per numbered rule. Each rule is defined in its ADR, and the id links
there. A pin is the test that fails when the rule is broken; a plant is the
test-only hook that breaks it
([`plant.rs`](../../trellis/src/plant.rs), armed by `TRELLIS_TEST_PLANT=<name>`).
`-` means none. `check-rule-ids.py` fails CI when a `trellis/src` comment
cites an id missing from these tables.

### Ordering rules

| Id | Rule | Owning section | Enforced by | Pin | Plant |
|---|---|---|---|---|---|
| [I0](../decisions/0002-async-data-flow.md#invariants) | One database orders every commit | [capture function](01-capture-by-triggers.md#the-capture-function) | `capture::sql::function_statements` | - | - |
| [I1](../decisions/0028-locking-and-writing.md#i1-a-writer-reads-after-its-lock-under-read-committed) | Read after lock, snapshot taken in the read | [lock ordering](05-apply-and-exactly-once-deltas.md#lock-ordering-because-parallel-workers-will-overlap) | `lock_entries`, `read_rows`, `lock_groups`, `pool::READ_COMMITTED` | `a_page_rederive_reads_its_row_after_its_entry_lock_under_a_repeatable_read_default`, `r1_a_page_reads_the_basis_after_its_lock` | `chunk_without_entry_lock`, `join1_snapshot_after_read` |
| [I2](../decisions/0002-async-data-flow.md#invariants) | Apply only what the basis hasn't seen, above `applied_lsn` | [1-1 ledger](05-apply-and-exactly-once-deltas.md#absolute-writes-do-not-commute-the-1-1-ledger) | `update_entries`, `apply_locked_groups` | `exp2_9c_one_to_one`, `a_rederives_tombstone_outlives_the_changes_its_read_saw_one_to_one` | `stale_one_to_one_write`, `join1_lsn_only`, `join1_vis_only` |
| [I3](../decisions/0002-async-data-flow.md#invariants) | Ordering state at the target's grain; group values are sums | [the ledger](05-apply-and-exactly-once-deltas.md#the-ledger) | `update_entries`, `apply_locked_groups` | - | - |
| [I4](../decisions/0002-async-data-flow.md#invariants) | Tombstones live until the batch watermark passes | [the ledger](05-apply-and-exactly-once-deltas.md#the-ledger) | `collect_tombstones` | `exp2_9_gc_waits_for_the_older_update_one_to_one`, `an_out_of_order_drain_holds_gc_back_one_to_one`, `gc_under_a_concurrent_reinsert_one_to_one` | `early_tombstone_gc` |
| [I5](../decisions/0028-locking-and-writing.md#i5-every-writer-takes-its-locks-in-one-order-each-class-in-one-sorted-statement) | One lock order, one sorted statement per class | [lock ordering](05-apply-and-exactly-once-deltas.md#lock-ordering-because-parallel-workers-will-overlap) | `lock_entries`, `lock_groups`, `j_statements` | `u1_the_j_statements_are_one_insert_and_one_lock_sorted_by_position_and_key` | `star_j_unsorted` |
| [I6](../decisions/0002-async-data-flow.md#invariants) | Never block or fail an application writer | [user-table DDL](05-apply-and-exactly-once-deltas.md#ddl-on-a-user-table-never-blocks-a-writer-adr-0002-i6) | `locks::DdlRetry`, `capture::install::install` | `a_user_table_ddl_retry_never_queues_writers_behind_it`, `renaming_a_read_column_pauses_its_reader_and_never_fails_a_write` | - |
| [I7](../decisions/0002-async-data-flow.md#invariants) | No lock wait inside a transaction holding a snapshot | [lock waits](05-apply-and-exactly-once-deltas.md#no-lock-wait-holds-a-snapshot-open-adr-0002-i7) | `locks::LOCK_TIMEOUT` | `a_drain_waits_out_a_held_lock_outside_its_transaction` | - |
| [I8](../decisions/0002-async-data-flow.md#invariants) | Memory bounded by a batch cap, not segment size | [paging a share](04-claiming-and-the-fold.md#paging-a-share-larger-than-the-cap) | `drain_many_with_cap` | `a_share_over_the_cap_drains_in_at_most_ceil_share_over_cap_pages` | - |

### Group rules

R1 and R2 are I1 and I2 per group, R4 is I4, and R7 is I5.

| Id | Rule | Owning section | Enforced by | Pin | Plant |
|---|---|---|---|---|---|
| [R1](../decisions/0002-async-data-flow.md#apply-sums-sides-per-group-and-transaction) | Lock groups in one sorted statement, read the basis after | [a page](group-basis.md#a-page) | `lock_groups`, `apply_locked_groups` | `r1_a_page_reads_the_basis_after_its_lock`, `r1_a_page_waits_for_a_page_that_applied_to_its_group` | `gb_skip_group_lock`, `gb_build_snapshot_before_locks`, `join_t_skip_lock` |
| [R2](../decisions/0002-async-data-flow.md#apply-sums-sides-per-group-and-transaction) | Sides summed per (group, transaction), checked against each group's basis, never folded per key | [a page](group-basis.md#a-page) | `apply_locked_groups`, `GroupTargetPlan::push` | `r2_a_move_is_checked_against_each_groups_basis`, `r2_a_change_in_flight_at_the_basis_applies`, `f2_a_basis_between_two_changes_of_a_key` | `gb_lsn_not_visibility`, `gb_fold_sides_per_key` |
| [R3](../decisions/0002-async-data-flow.md#capture-by-statement-triggers) | A row's images chain, so sides telescope | [transition rows](01-capture-by-triggers.md#every-transition-row-counts-once) | `capture::sql::function_statements` | `r3_nested_writes_sum_once` | - |
| [R4](../decisions/0002-async-data-flow.md#convergence-and-status) | A zero group with a basis stays until the drained prefix passes its stamp | [zero groups](group-basis.md#zero-groups-r4-rx7) | `collect_zero_groups` | `r4_a_zero_group_with_a_basis_stays_until_the_prefix_passes_its_stamp`, `the_zero_group_gc_skips_a_definition_under_a_fresh_build` | `gb_early_zero_gc`, `gb_gc_ignores_build` |
| [R5](../decisions/0002-async-data-flow.md#absolute-group-writes-lock-first-and-cover-every-group-they-could-owe) | The stamp's segment is read in the snapshot's statement or after | [absolute writes](group-basis.md#absolute-writes) | `recompute_locked_groups`, `rebuild` | - | - |
| [R6](../decisions/0002-async-data-flow.md#absolute-group-writes-lock-first-and-cover-every-group-they-could-owe) | A recompute is never one unlocked `UPDATE … FROM (GROUP BY)` | [absolute writes](group-basis.md#absolute-writes) | `recompute_groups` | - | - |
| [R7](../decisions/0002-async-data-flow.md#convergence-and-status) | Pages lock groups in one order (I5 per group) | [a page](group-basis.md#a-page) | `lock_groups` | `r7_concurrent_pages_over_shared_groups_never_deadlock` | - |
| [R8](../decisions/0002-async-data-flow.md#absolute-group-writes-lock-first-and-cover-every-group-they-could-owe) | Each producer covers every group it could owe | [absolute writes](group-basis.md#absolute-writes) | `enumerate_cells`, `rebuild` | `an_image_less_change_recomputes_the_groups_it_names`, `a_release_recomputes_every_group_the_held_key_moved_through` | `gb_build_leaf_unrouted` |
| [R9](../decisions/0002-async-data-flow.md#truncate-ddl-drop) | A truncate is a barrier on both sides | [truncate propagation](truncate-propagation-spec.md#the-ordering-hazard) | `next_claimable_segments`, `seal_phase1` | `the_truncate_page_waits_for_earlier_pages_and_later_ones_for_it`, `no_segment_is_sealed_past_an_unfenced_one` | - |
| [W1](../decisions/0028-locking-and-writing.md#w1-a-bulk-write-writes-each-row-once) | A bulk write writes each row once | [absolute writes](group-basis.md#absolute-writes) | `write_once_sql`, `rebuild` | `a_group_only_the_hidden_table_names_goes_to_zero`, `a_group_only_an_old_image_names`, `the_null_group_of_the_target_and_the_source_is_one_group`, `a_page_waits_on_a_straggler_the_first_statement_locked` | `gb_build_no_hidden_branch`, `gb_build_no_target_branch`, `gb_build_no_ring_branch`, `gb_build_null_key_by_equality`, `gb_build_straggler_unlocked` |
| [W2](../decisions/0028-locking-and-writing.md#w2-a-bulk-write-writes-in-the-destinations-order) | A bulk write writes in the destination's order | [absolute writes](group-basis.md#absolute-writes) | `rebuild` | - | - |
| [W3](../decisions/0028-locking-and-writing.md#w3-where-a-page-can-write-the-same-rows-the-one-write-is-a-sorted-upsert-and-it-is-the-lock) | Where a page can write the same rows, one sorted upsert is the lock | [`JOIN` targets](05-apply-and-exactly-once-deltas.md#join-targets) | `write_locked` | - | - |

### `MIN` and `MAX`

[Rx1 to Rx8](../decisions/0002-async-data-flow.md#min-and-max-recompute-on-retract) are R-rules for the extremes, which keep their own basis `__xbasis` and stamp `__xstamp`.

| Id | Rule | Owning section | Enforced by | Pin | Plant |
|---|---|---|---|---|---|
| Rx1 | Extremes have their own basis and stamp | [layout](group-basis.md#where-a-groups-state-lives) | `apply_locked_groups` | `f4_an_insert_below_the_next_value_racing_the_probe`, `f2_every_sequence_drained_out_of_order` | `gbx_shared_basis` |
| Rx2 | I2 for extremes: filtered by `__xbasis` | [a page](group-basis.md#a-page) | `apply_locked_groups` | `f2_every_sequence_drained_out_of_order` | - |
| Rx3 | An insertion moves an extreme by `least`/`greatest` | [a page](group-basis.md#a-page) | `apply_locked_groups` | `f2_every_sequence_drained_out_of_order` | - |
| Rx4 | A retraction reaching an extreme, or a `NULL` extreme, probes | [a page](group-basis.md#a-page) | `apply_locked_groups` (its `__probe` column) | `only_a_retraction_reaching_an_extreme_probes`, `f5_an_all_null_group_gains_and_loses_a_value_in_reverse` | `gbx_null_extreme_no_probe` |
| Rx5 | The probe runs after the lock and apply, one statement | [a page](group-basis.md#a-page) | `probe_locked_groups` | `f4p_the_next_value_deleted_and_drained_before_the_lock`, `f5_the_null_group_probes_its_own_rows` | `gbx_probe_no_xbasis`, `gbx_probe_before_lock`, `gbx_null_key_by_equality` |
| Rx6 | Absolute writes write extremes, basis and stamp | [absolute writes](group-basis.md#absolute-writes) | `recompute_locked_groups`, `rebuild` | `a_build_writes_the_extremes_and_their_basis`, `min_and_max_through_a_leaf_truncate_are_recomputed` | `gbx_absolute_write_keeps_extremes` |
| Rx7 | R4 holds for both stamps | [zero groups](group-basis.md#zero-groups-r4-rx7) | `collect_zero_groups` | `f6_a_zero_group_a_probe_stamped_stays_until_the_prefix_passes_it` | `gbx_gc_ignores_xstamp` |
| Rx8 | A truncate removes the extremes with the partials | [zero groups](group-basis.md#zero-groups-r4-rx7) | `truncate_groups` | - | - |

`self_check` compares each extreme with the source's under its own basis
([ADR-0013](../decisions/0013-self-check-production-recompute-audit.md)).
It has no id. Pins: `self_check_compares_each_extreme_with_the_sources`,
`self_check_reads_the_extremes_under_their_own_basis`,
`self_check_compares_the_groups_only_the_extremes_have`. Plants:
`sc_extremes_under_basis`, `sc_extremes_uncompared`,
`sc_extreme_groups_uncompared`.

### `JOIN`

[JR1 to JR3](../decisions/0019-join-transforms.md#a-writer-locks-jk-for-update-reads-the-other-side-after-the-lock-and-writes-t-in-its-own-transaction)
are ADR-0028's I5, I1 and one-transaction rules with `J` as the lock.

| Id | Rule | Owning section | Enforced by | Pin | Plant |
|---|---|---|---|---|---|
| JR1 | Lock `J[k]` for every key touched, in ADR-0028's order | [`JOIN` targets](05-apply-and-exactly-once-deltas.md#join-targets) | `write_locked` | `jr1_the_j_lock_orders_two_sides_with_no_shared_cell` | `join_no_j_lock` |
| JR2 | Read the other side only after the `J` lock | [`JOIN` targets](05-apply-and-exactly-once-deltas.md#join-targets) | `write_locked` | `family1_fact_vs_dimension_move_left` | `join_other_side_before_j` |
| JR3 | A page's change to `T` is in its own transaction, and a `matches` flip moves `P` | [`JOIN` targets](05-apply-and-exactly-once-deltas.md#join-targets) | `write_locked` | `family2_flips_left`, `family14_t_in_the_pages_own_transaction` | `join_drop_flip` |

### 1-1 `JOIN` and the mutation seam

[ADR-0021](../decisions/0021-one-to-one-join-transforms.md) states its rules
in prose, without ids. Its writers follow I1, I2 and I5 with `J` and the
row's `R` entry as the locks, so those plants also appear above.

| Rule | Owning section | Enforced by | Pin | Plant |
|---|---|---|---|---|
| [No writer reads another table's source, only its copy, under `J` and the row's `R` entry](../decisions/0021-one-to-one-join-transforms.md#no-writer-reads-another-tables-source-only-its-copy-under-j-and-the-rows-r-entry) | [a 1-1 `JOIN`'s hidden state](05-apply-and-exactly-once-deltas.md#a-1-1-joins-hidden-state) | `join_one_to_one::apply_page`, `lock_entries` | `an_a_page_holds_its_j_row_until_it_commits`, `a_row_moving_off_a_key_locks_the_key_it_leaves`, `two_root_pages_on_a_new_row_queue_on_the_placeholder`, `a_key_that_moved_under_the_page_rolls_it_back_and_the_retry_locks_both`, `f1_an_a_page_on_a_key_reads_m_only_after_the_b_page_on_it_commits`, `cx_f1a_…` (`join_star_joined`), `cx_f3b_…` (`join_star_crossing`) | `join1_no_j_lock`, `join1_no_recheck`, `join1_no_entry_lock`, `join1_old_key_unlocked`, `join1_no_placeholder`, `join1_m_before_j`, `join1_rd_no_key`, `join1_brd_no_key`, `join1_chunk_no_key`, `star_root_skip_j_last`, `star_root_skip_j_first`, `star_root_skip_j_middle`, `star_late_skip_last`, `star_late_skip_first`, `star_own_j_only`, `star_r_after_m`, `star_last_no_r`, `star_r_split`, `star_recheck_j`, `star_recheck_skip_last`, `star_chunk_no_r`, `star_chunk_skip_j_last`, `star_chunk_skip_j_first` |
| A truncate page holds the lock row `FOR UPDATE` and empties its side's copy | [`JOIN` targets](05-apply-and-exactly-once-deltas.md#join-targets) | `truncate_sides` | `pp_t8_a_stale_page_after_the_truncate_is_refused`, `pp_t1_to_t7_a_truncate_races_a_page_of_either_table`, `px4_a_truncate_of_a_empties_r_and_the_target_and_later_changes_apply`, `px5_a_truncate_of_a_joined_table_empties_its_m_before_the_rebuild_runs`, `px6_a_truncate_of_a_joined_table_rebuilds_the_target`, `px7_a_truncate_of_a_joined_table_and_a_rebuild_start_take_the_definition_row_first` | `join1_truncate_no_floor`, `join1_truncate_share_lock`, `join1_truncate_keeps_r`, `join1_truncate_keeps_m`, `join1_truncate_no_rebuild`, `join1_truncate_def_after_lock` |
| [A `b` change's fan-out runs in chunks under a pending marker](../decisions/0021-one-to-one-join-transforms.md#a-b-changes-fan-out-runs-in-chunks-under-a-pending-marker) | [a 1-1 `JOIN`'s hidden state](05-apply-and-exactly-once-deltas.md#a-1-1-joins-hidden-state) | `fan_out` | `xt2_…` (`join_star_fan_out`) | `star_truncate_keeps_other_markers` |
| [A row a page fails to write is held in the page](../decisions/0021-one-to-one-join-transforms.md#a-row-a-page-fails-to-write-is-held-in-the-page) | [quarantine](06-cleanup-and-reclaim.md#quarantine-when-a-change-deterministically-kills-its-worker) | `lock_definitions` | `a_fan_out_leaves_a_held_row_as_it_was`, `a_release_before_a_pending_fan_out_of_its_key_stays_released`, `a_row_a_middle_fan_out_holds_is_parked_with_the_changes_origin`, `a_middle_fan_out_that_holds_a_row_and_a_rebuild_start_never_deadlock` | `join1_fan_out_held_row`, `join1_release_no_rederive`, `star_held_origin_first_only`, `star_def_lock_first_only` |
| [A build fills every `M`, then `R` and the target](../decisions/0021-one-to-one-join-transforms.md#a-build-fills-every-m-then-r-and-the-target) | [builds](builds.md#a-1-1-build-re-derive-over-chunks) | `join_one_to_one::build` | `h1_a_resume_removes_the_rows_of_a_rows_deleted_while_paused`, `h3_a_row_reinserted_after_the_orphan_read_is_kept`, `the_m_pass_tombstones_the_m_entries_of_b_rows_gone_before_a_rebuild`, `c10_a_chunk_whose_row_moved_onto_a_key_it_did_not_lock_retries` | `join1_no_orphan_pass`, `join1_orphan_delete`, `join1_m_pass_b_only` |
| [A schema change of any table pauses the whole transform](../decisions/0021-one-to-one-join-transforms.md#a-schema-change-of-any-table-pauses-the-whole-transform) | [capture drift](capture-drift.md) | `pause_fence`, `lock_frozen` | `join_star_quarantine` and `join_one_to_one_build` column pause tests | `star_fence_first_only` |
| [A target's writes reach its readers through the mutation seam](../decisions/0002-async-data-flow.md#multi-hop-a-targets-writes-reach-its-readers-through-the-mutation-seam) | [apply](05-apply-and-exactly-once-deltas.md#downstream-propagation-and-why-it-terminates) | `target_mutations` | `s2_seam_rows_of_one_key_order_by_their_writers_commits`, `an_upstream_row_that_moves_group_leaves_its_old_group`, `a_star_with_a_one_to_one_target_at_the_middle_position_converges` | `seam_token_early`, `seam_imageless_row`, `star_reads_first_only` |

## Other pins

These guarantees have no rule id. Each fails only if written deliberately;
ordinary end-to-end tests pass against every one of these bugs.

- **The straddler.** Hold a writer's transaction open across a seal; assert its
      row is applied exactly once.
- **The phase-gap writer.** Resolve the pointer in the window between the
      flip's commit and the snapshot capture; assert exactly once.
- **The high-xid writer.** A writer that is the highest live transaction id
      when the snapshot is captured — the `xmax` case in
      [03](03-sealing-and-the-fence.md).
- **Buckets partition the batch exactly once** — assert the union of
      per-bucket folds equals the whole-batch fold, as a set identity.
- **Claim lost mid-drain.** Reclaim a worker's claim while it computes; assert
      its apply rolls back entirely.
- **A key born inside a batch** (insert-then-update) folds to a NULL old side,
      and an image-less trigger row never wins either arg-extreme.
- **Delta vs. oracle, byte-identical**, after every op and under randomized
      drain interleavings.
- **Retire-then-seal on the same slot.** Assert a re-seeded slot is never
      truncated by a stale reclaimer.
- **Quarantine release preserves the origin position**, so the predicate never
      reports converged across a parked key.
- **Crash in the seal's phase gap** wedges, and the age-gated recovery unwedges
      it without stomping a healthy in-flight seal.
- **A Re-derive build interleaved with live Apply pages** converges to a
      from-scratch oracle after the last chunk and drain.
      (`rederive_build.rs`, `one_to_one_build_interleavings.rs`)

## Anti-patterns to name in review

Each is a regression to something this design deliberately avoids:

- Merging staged rows at write time "to save the fold" — reintroduces mutable
  claimed batches, hence compare-and-delete and survivor rewriting.
- Locking the pointer so writers cannot read it stale — trades a read-time fence
  for per-append coordination.
- Recomputing the routing key at read time instead of storing it — unsound across
  any hash change.
- Sizing the bucket split from the live worker registry — samples a value that
  moves under a safety-critical partition.
- Narrowing the pending predicate to something cheaper — admits a false
  `converged` ([07](07-convergence-and-await.md)).
- Marking work complete in a separate transaction from the work — the exact thing
  this design exists to prevent.
- Quarantining an error that names a schema defect — converts a loud, fixable
  error into permanently hidden work.
- A new write path that neither commutes nor checks its basis — it is correct
  only while its batches happen to drain in order
  ([05](05-apply-and-exactly-once-deltas.md#absolute-writes-do-not-commute-the-1-1-ledger)).
- An unreferenced data-modifying or `FOR UPDATE` CTE — Postgres may plan it away,
  and it will lock nothing while looking like it does.
