-- Deferral plumbing for reverse records (issue #134, epic #127): a new
-- staged kind for a to-one relationship reverse whose Phase 3 apply one of
-- issue #132's four guards rejected. See `trellis/tests/spikes/issue-102-PLAN-DRAFT.md`
-- section 7's Phase 1 step 6, and `staging::apply::RelationshipReverseRecord`'s
-- doc comment for the mechanism this schema half supports.
--
-- Before this issue, a guard rejection fell back to the image-less
-- `Recompute` stopgap #131 shipped and #132/#133 reused — safe but
-- expensive (a full live recompute of the parent's from-side rows), and
-- wrong to reuse for *retries specifically*: staging the retry at
-- `hop_gen + 1` (the shape every other `Recompute` producer uses) would
-- burn a hop generation on every deferral and trip `MAX_HOP_GEN = 32`
-- (`apply.rs`) after 32 deferrals — turning routine, expected deferral
-- (measured as the *common* case for this mechanism, not the exception)
-- into a hard `HopBoundExceeded` error.
--
-- A `rel_reverse_deferred` row carries everything a later drain needs to
-- retry: `old_image`/`new_image` (the parent's own before/after images,
-- reusing the ring's existing jsonb columns exactly like a `Cdc` row does)
-- and `lsn` (this reverse's own identity in the projection's LSN chain,
-- reusing the existing `lsn` column). It deliberately does **not** persist
-- `prev_lsn`/`prev_gen`/the watermark `X` — issue #134's resolved design
-- fork: those three guard inputs are *re-derived fresh* at retry time (a
-- live read of the projection row, exactly like the original enumeration's
-- own capture), never replayed stale. A stale `prev_gen`/`prev_lsn` could
-- wrongly pass a guard that should now fail (state moved since the
-- original capture) or wrongly fail one that would now legitimately pass
-- (state moved back into agreement) — replaying it would silently
-- reintroduce exactly the class of bug #132/#133 closed. See
-- `staging::apply::compute`'s deferred-reconstruction loop, which calls the
-- same `capture_reverse_guard_state` the original enumeration path uses.
--
-- Two new columns, both meaningful only for `op = 'rel_reverse_deferred'`:
--   * `retry_count` — this reverse's own retry counter, exempt from
--     `hop_gen`/`MAX_HOP_GEN` entirely (this op never touches `hop_gen` —
--     Phase 3 always stages it with `hop_gen = 0`). Default 0 so every
--     other op's rows (which never set it) read as "not a retry."
--   * `relationship_id` — which `relationship_definitions.id` this reverse
--     belongs to, so a later drain knows which `ReverseRelationshipShape`
--     to rebuild without re-deriving it from `src_table` (which, for this
--     op, is a synthetic per-relationship identity — see
--     `staging::apply::relationship_reverse_deferred_src_table` — chosen
--     precisely so this op's rows never fold together with a genuine CDC
--     row on the parent's *real* table: that would let this op's rows
--     reach the ordinary per-source forward-evaluation loop and
--     double-apply a delta the original CDC row already forward-applied
--     when it first landed. Multiple deferred reverses for the *same*
--     relationship and parent key, staged across more than one rejection,
--     *do* fold together via this same synthetic identity — issue #134's
--     other resolved design question — reusing the ring's existing
--     first-old/last-new-image fold rule verbatim, the same "N changes to
--     one key fold to one record" property #131 already established for
--     raw parent CDC.
alter table seg_0 drop constraint seg_0_op_check;
alter table seg_0 add constraint seg_0_op_check
    check (op in ('insert', 'update', 'delete', 'recompute', 'truncate', 'rel_reverse_deferred'));
alter table seg_0 add column retry_count integer not null default 0;
alter table seg_0 add column relationship_id bigint;

alter table seg_1 drop constraint seg_1_op_check;
alter table seg_1 add constraint seg_1_op_check
    check (op in ('insert', 'update', 'delete', 'recompute', 'truncate', 'rel_reverse_deferred'));
alter table seg_1 add column retry_count integer not null default 0;
alter table seg_1 add column relationship_id bigint;

alter table seg_2 drop constraint seg_2_op_check;
alter table seg_2 add constraint seg_2_op_check
    check (op in ('insert', 'update', 'delete', 'recompute', 'truncate', 'rel_reverse_deferred'));
alter table seg_2 add column retry_count integer not null default 0;
alter table seg_2 add column relationship_id bigint;

alter table seg_3 drop constraint seg_3_op_check;
alter table seg_3 add constraint seg_3_op_check
    check (op in ('insert', 'update', 'delete', 'recompute', 'truncate', 'rel_reverse_deferred'));
alter table seg_3 add column retry_count integer not null default 0;
alter table seg_3 add column relationship_id bigint;

-- Unlike `truncate` (`V11__truncate_op.sql`), `rel_reverse_deferred` is
-- **not** image-less — it carries the parent's own old/new image, exactly
-- like a `Cdc` row — so the existing `seg_N_recompute_has_no_images`
-- constraint (scoped to `recompute`/`truncate` only) needs no widening.
--
-- `rel_reverse_deferred` rows never reach `poison_held` (`compute` filters
-- them out of the poison-check/per-source evaluation loop entirely, the
-- same way it already filters out truncate sentinels before the poison
-- check) — see `staging::apply::compute`'s own comment at that filter —
-- so `poison_held`'s `op` CHECK (`V13__quarantine.sql`, never widened for
-- `truncate` either) needs no change here.
