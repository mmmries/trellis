-- TRUNCATE propagation (issue #60): a source TRUNCATE must clear every row
-- a 1-1 transform ever derived from the truncated table, then let
-- post-truncate inserts repopulate normally. See
-- docs/staging-and-claiming/truncate-propagation-spec.md for the full
-- design; this migration is its schema half.

-- Widen each ring slot's `op` CHECK for the new sentinel, and extend the
-- image-less CHECK (previously scoped to 'recompute' only, by
-- `V3__staging_ring.sql`) to cover it too: a truncate sentinel is key-less
-- (see `staging::append::TRUNCATE_SENTINEL_KEY`) and image-less, the same
-- shape as a recompute trigger — it asserts nothing about any one row's
-- state, only "every row this source ever produced is gone as of this
-- position."
alter table seg_0 drop constraint seg_0_op_check;
alter table seg_0 add constraint seg_0_op_check
    check (op in ('insert', 'update', 'delete', 'recompute', 'truncate'));
alter table seg_0 drop constraint seg_0_recompute_has_no_images;
alter table seg_0 add constraint seg_0_recompute_has_no_images
    check (op not in ('recompute', 'truncate') or (old_image is null and new_image is null));

alter table seg_1 drop constraint seg_1_op_check;
alter table seg_1 add constraint seg_1_op_check
    check (op in ('insert', 'update', 'delete', 'recompute', 'truncate'));
alter table seg_1 drop constraint seg_1_recompute_has_no_images;
alter table seg_1 add constraint seg_1_recompute_has_no_images
    check (op not in ('recompute', 'truncate') or (old_image is null and new_image is null));

alter table seg_2 drop constraint seg_2_op_check;
alter table seg_2 add constraint seg_2_op_check
    check (op in ('insert', 'update', 'delete', 'recompute', 'truncate'));
alter table seg_2 drop constraint seg_2_recompute_has_no_images;
alter table seg_2 add constraint seg_2_recompute_has_no_images
    check (op not in ('recompute', 'truncate') or (old_image is null and new_image is null));

alter table seg_3 drop constraint seg_3_op_check;
alter table seg_3 add constraint seg_3_op_check
    check (op in ('insert', 'update', 'delete', 'recompute', 'truncate'));
alter table seg_3 drop constraint seg_3_recompute_has_no_images;
alter table seg_3 add constraint seg_3_recompute_has_no_images
    check (op not in ('recompute', 'truncate') or (old_image is null and new_image is null));

-- The two-directional drain barrier ("The ordering hazard" in the design
-- doc): whether a sealed batch contains any truncate sentinel, decided once
-- at seal time (`engine::staging::seal::seal_phase1`) alongside
-- `bucket_count`, and consulted by
-- `engine::staging::apply::next_claimable_segment` so no segment past an
-- undrained truncate-bearing one is ever handed to a worker — predecessors
-- must fully drain first, successors must wait until the truncate itself
-- has drained.
alter table segments add column has_truncate boolean not null default false;
