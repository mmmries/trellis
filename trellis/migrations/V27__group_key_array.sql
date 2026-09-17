-- Widen the staging ring's (and `poison_held`'s) reserved `group_key`
-- column from `text` to `text[]` (issue #133, epic #127). See
-- `trellis/tests/spikes/issue-102-PLAN-DRAFT.md` §3.1 for the full finding
-- this closes: `staging::fold` collapses a key's raw change history within a
-- batch to *first old image, last new image*, so a from-side row that's
-- inserted and then re-pointed within one batch (`ins(post 3)` +
-- `repoint(3 -> 2)`) folds to `new={post: 2}` — post 3 never appears in
-- either folded endpoint. Nothing signalled that post 3 was touched, so
-- issue #132's guard (b) (the optimistic generation check) never bumps its
-- settled-parent-projection row's `__trellis_gen`, and a reverse holding an
-- enumeration captured before the re-point wrongly passes its check and
-- moves a row that already left.
--
-- `group_key` (`V3__staging_ring.sql`, `V13__quarantine.sql`) is the ring's
-- reserved carrier for exactly this signal: every producer already threads
-- it through `append` -> `spill` -> `fold`, but writes `None`/`NULL` to it.
-- The real value #133 populates it with is the *union* of every join-key
-- value a from-side row's own raw (pre-fold) CDC history touched within the
-- batch — every column that is some relationship's `from_col`, read from
-- that row's own `old_image` (if present) and `new_image` (if present).
-- That's a set, not a single value, so the column has to become an array:
-- a bare `text` can hold at most one of the (potentially several) join-key
-- values a re-pointed row's history touched, which is exactly the shape
-- that erased post 3 above.
--
-- Every existing row's `group_key` is `NULL` (no producer has ever written
-- anything else), so this `ALTER ... TYPE` has nothing but `NULL`s to
-- convert in practice; the `USING` clause is still written as a real,
-- non-lossy `text` -> `text[]` conversion (wrap a non-null scalar in a
-- one-element array) rather than assuming that and blindly casting, so this
-- migration stays correct even if it's ever re-run against a database that
-- (somehow) already has real values in this column.
alter table seg_0 alter column group_key type text[]
    using case when group_key is null then null else array[group_key] end;
alter table seg_1 alter column group_key type text[]
    using case when group_key is null then null else array[group_key] end;
alter table seg_2 alter column group_key type text[]
    using case when group_key is null then null else array[group_key] end;
alter table seg_3 alter column group_key type text[]
    using case when group_key is null then null else array[group_key] end;

-- `poison_held` carries the same column, for the same reason
-- (`quarantine::park_batch_contribution` parks a `FoldedChange` verbatim,
-- `group_key` included) — widened identically.
alter table poison_held alter column group_key type text[]
    using case when group_key is null then null else array[group_key] end;
