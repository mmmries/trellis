-- The convergence predicate's index (issue #12, stage 07). See
-- docs/staging-and-claiming/07-convergence-and-await.md, "Making the honest
-- question cheap".
--
-- Conditions 2 and 3 of `converged_through` filter on `origin_lsn`, which the
-- ring's only other index (the implicit ordering the append path relies on)
-- cannot serve. Without a dedicated index the probe is a sequential scan
-- whose cost grows exactly when the minimum origin sits late in a big sealed
-- slot — the case downstream propagation produces routinely. With the index
-- it is a cheap seek regardless of slot size.
--
-- One plain btree per ring table, not partial/expression: condition 2 reads
-- it via `min(origin_lsn) <= token` (a `Limit 1` scan, no statistics needed);
-- condition 3 reads it via `EXISTS (... origin_lsn <= token)`, which does
-- need statistics — see `trellis::staging::seal`'s ANALYZE-on-seal, since
-- these tables carry `autovacuum_enabled = off` and so get no autoANALYZE.
create index if not exists seg_0_origin_lsn_idx on seg_0 (origin_lsn);
create index if not exists seg_1_origin_lsn_idx on seg_1 (origin_lsn);
create index if not exists seg_2_origin_lsn_idx on seg_2 (origin_lsn);
create index if not exists seg_3_origin_lsn_idx on seg_3 (origin_lsn);

-- ---------------------------------------------------------------------------
-- Widen the registry's state CHECK. `V3__staging_ring.sql` deliberately
-- scoped `segments.state` to 'active'/'sealed' — the only two this repo's
-- claiming/apply stages (#14/#15) had produced so far — but condition 3 of
-- the convergence predicate (`trellis::staging::converge::converged_through`,
-- docs/staging-and-claiming/07-convergence-and-await.md) is defined in terms
-- of *every* state in `trellis::staging::state::SegmentState`'s graph:
-- "pending" means "not drained", which only means something once `draining`
-- and `drained` are legal values here too. Widening the CHECK now, ahead of
-- #14/#15 actually writing those states, is what lets this stage's own tests
-- exercise a part-drained batch without a schema change blocking them.
alter table segments drop constraint segments_state_check;
alter table segments add constraint segments_state_check
    check (state in ('active', 'sealed', 'draining', 'drained'));
