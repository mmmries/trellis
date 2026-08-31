# TRUNCATE propagation — implementation spec (issue #60)

Scratch spec for the #60 fix. Scope: **the 1-1 scalar subset only** — matching
`staging::apply`'s existing scope. The aggregate delta back-out path (subtract
`f(old_image)` for every cleared group member) is explicitly deferred and must
be filed as a follow-up on the aggregate apply issue (#11).

## The bug

`pgoutput` decodes `TRUNCATE` fully (`Message::Truncate { options, relation_ids }`),
but `intake/mod.rs` drops it as an explicit no-op (`Message::Truncate { .. } => {}`).
There is no `'truncate'` ring op, no truncate sentinel, and no clear-keyspace
step in Phase-3 apply. A source `TRUNCATE` therefore silently does not propagate.

## Semantics (settled)

- A source `TRUNCATE` clears **all derived/target rows produced from each named
  source relation**, then post-truncate inserts re-populate normally.
- Postgres pre-expands `CASCADE` server-side: the single pgoutput TRUNCATE message's
  `relation_ids` list already enumerates every truncated table **that is in the
  publication**. We iterate that list; we do **not** implement cascade traversal.
  A cascaded child not in the publication is simply absent (and was never
  replicated) — note this in a comment.
- The `options` bits (bit0 CASCADE, bit1 RESTART IDENTITY) are informational; we do
  not act on them beyond clearing the listed relations.

## The ordering hazard (the crux of the issue)

A TRUNCATE is **whole-keyspace**, but drains are **per-bucket, parallel, and out of
`seg_seq` order** (`next_claimable_segment` explicitly does not enforce order). So a
truncate is a **two-directional drain barrier**:

- **Predecessors must drain first.** Otherwise an earlier batch's insert applies
  *after* the truncate clears the target and wrongly survives.
- **Successors must not drain first.** Otherwise a later batch's post-truncate
  insert is wiped when the truncate batch clears the whole target.

Enforcement (the settled "single-bucket truncate batch" decision, extended to a
barrier):

1. **Single bucket.** A batch containing any `op='truncate'` row seals with
   `bucket_count = 1` — one worker drains the whole batch, so the whole-keyspace
   `DELETE` + any same-batch post-truncate writes are one atomic Phase-3 txn.
2. **Barrier in `next_claimable_segment`.** Record `has_truncate` on the `segments`
   registry at seal time. Let `B` = min `seg_seq` among undrained truncate-bearing
   segments. A worker may be handed segment `s` only when `s <= B` (never a segment
   past an undrained truncate). Because the query returns the lowest undrained `s`,
   `B` itself is handed out only once every `s < B` has drained. This gives both
   directions with one clause.

Truncates are rare; fully serializing the drain around one is the correct
correctness/throughput trade.

## Per-key fold correctness

The fold telescopes per `(src_table, key)` ordered by `(lsn, change_id)`. A truncate
landing between two changes to a key voids the earlier one. So the fold must **void
image-bearing keyed changes at a position ≤ the src_table's max truncate position**
in the fenced window:

```sql
-- keep f unless a truncate for its src_table sits strictly above it
and not exists (
  select 1 from fenced t
  where t.op = 'truncate' and t.src_table = f.src_table
    and (t.lsn, t.change_id) > (f.lsn, f.change_id)
)
```

Key subtlety — **recompute rows (NULL `lsn`) are correctly never filtered**: the
row-comparison against a NULL lsn is NULL (not true), so recompute rows survive
regardless of truncate position. That is correct: a recompute re-reads *live*
current source state, which already reflects the truncate, so its position is
irrelevant. Only image-bearing rows (real `lsn`) carry stale state and must be
voided below the truncate. Call this out in a comment — it is load-bearing.

A truncate and inserts **in the same source transaction** share the commit `lsn`;
`change_id` (intake append order = execution order) breaks the tie, so
truncate-then-insert within one txn orders correctly.

## Flow: sentinel → fold → compute → apply

- **Intake** (`intake/mod.rs`): replace the no-op. For each `relation_id`, resolve
  the `Relation` (same `self.relations.get(id)?` path as DML — Postgres sends the
  Relation message before referencing the id), build a truncate `StagedChange`, push
  to the buffer with `xid`. `lsn`/`src_changed` are stamped at commit like DML.
- **`StagedChange`** (`staging/append.rs`): add a truncate representation. A truncate
  is key-less and image-less. Prefer a dedicated `StagedChange::Truncate { src_table,
  lsn, origin_lsn, src_changed }` variant over overloading `Cdc`. `ChangeRow::from`
  maps it to `op="truncate"`, images NULL, and a sentinel key (a module const, e.g.
  `TRUNCATE_SENTINEL_KEY`). Update `StagedChange::src_table()` and
  `stamp_commit_metadata` to cover the new variant.
- **`CdcOp`**: you may add `CdcOp::Truncate`, or keep the op string local to the new
  variant — your call; keep the `a typo can't reach the CHECK` property.
- **Fold** (`staging/fold.rs`): add the NOT-EXISTS void filter above; surface the
  truncate to the caller. Recommended: add `is_truncate: bool` to `FoldedChange`
  (`bool_or(op='truncate')` per group) **and/or** return the set of truncated
  `src_table`s. Whichever you choose, `compute` must be able to see, per drain, which
  source tables were truncated. Do **not** start filtering `op` in the arg-extremes
  (the existing comment at fold.rs ~163 explains why the truncate sentinel must
  survive the fold).
- **`compute`** (`staging/apply.rs`): for each truncated `src_table`, resolve its
  targets via `catalog::transforms_for_source` and record a "clear this target"
  instruction in `ApplyPlan`. A truncate sentinel FoldedChange itself produces no
  keyed write/delete.
- **`apply_and_mark_drained`** (`staging/apply.rs`): inside the one Phase-3 txn, run
  the version fence, then for each target to clear execute `DELETE FROM <target>`
  **before** that target's keyed upserts, then the normal per-target upsert/delete.
  Order within the txn (clear-then-write) is what makes same-batch post-truncate
  inserts land correctly. Downstream propagation: a truncate that empties a target
  should propagate a recompute to that target's downstream readers (the cleared keys
  are "changed"); at minimum do not regress termination/hop-bound. For the 1-1 scope
  a full-target clear's downstream set may be broad — a reasonable v1 is to propagate
  recompute for the keys the clear physically removed, consistent with the existing
  "physically written keys are the downstream signal" rule.

## Schema (new migration `V11__truncate_op.sql`)

- Extend each `seg_0..seg_3` `op` CHECK to include `'truncate'`.
- Extend the image-less CHECK so a `'truncate'` row also must have NULL images
  (currently only `'recompute'` is constrained).
- `alter table segments add column has_truncate boolean not null default false`.
- Bump the migration-count assertions that pin the ledger length
  (`tests/connectivity.rs` and `identity.rs` both assert the V1..VN range — grep
  for the hardcoded count and extend to V11).

## Seal (`staging/seal.rs`)

At seal (where `bucket_count` is decided from `row_count`), in the same txn also
detect whether the sealing ring slot contains any `op='truncate'` row. If so:
`bucket_count = 1` and set `segments.has_truncate = true`.

## `next_claimable_segment` (`staging/apply.rs`)

Add the barrier clause: never return a segment whose `seg_seq` exceeds the lowest
undrained truncate-bearing segment (see barrier rule above). One extra
sub-select over `segments`.

## Tests (add; follow existing style — `TestCluster`, oracle byte-match where apt)

- Decode→intake: a TRUNCATE message becomes a staged truncate sentinel (not dropped).
- Fold: image-bearing keyed changes at/below a truncate are voided; recompute rows
  and post-truncate inserts survive; the truncate is surfaced to the caller.
- Seal: a truncate-bearing batch seals `bucket_count=1`, `has_truncate=true`.
- Barrier: `next_claimable_segment` will not hand out a segment past an undrained
  truncate batch until predecessors drain.
- End-to-end (`client_e2e`-style): load rows → derive → TRUNCATE source → insert new
  rows → drain → target converges to a from-scratch oracle (only post-truncate rows
  present). Include the ordering case: a post-truncate insert in a **later** batch
  than the truncate is not wiped, and a pre-truncate insert in an **earlier**
  undrained batch does not survive.

## Invariants to preserve (for the reviewer)

- Immutable claimed batch; apply ∪ mark-drained is one commit (docs/.../05, §invariants).
- A row's bucket is a total function of row+batch — the single-bucket truncate rule
  must not put a row in zero or two buckets.
- The fold must not start filtering `op`.
- Do not re-widen `segments_state_check` (V6 already did).

## Validation

Toolchain resolves in-tree (edition 2024, `rust-toolchain.toml` pins 1.98.0 — plain
`cargo` works, no version prefix). From the worktree root:

- `cargo fmt --all`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --workspace` (run single-threaded if SysV shmem exhaustion trips:
  `cargo test --workspace -- --test-threads=1`)
