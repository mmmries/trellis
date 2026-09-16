# Draft plan — make the delta path work through a to-one relationship

*Supersedes the "Option 1 / named intermediate / D4" recommendations in #102's
comment thread. Those comments' D1–D4 numbers came from an interactive session
whose script was never self-contained; they are withdrawn, not merely revised.*

## 1. What the work is

Today, any aggregate whose plan carries a relationship join is maintained by
recomputing every touched group from scratch:

```rust
// trellis/src/staging/apply_aggregate.rs:562
let force_every_group = !plan.rel_joins.is_empty();
```

That one line is the whole cost of #94 in production, and it is why #102 was
written as a delta design in the first place. Replacing it with a real delta is
the project; #102's `GROUP BY tag, post.author` grammar is a small increment on
top of it, not the other way round.

Measured on a 1M-`posts` / 3M-`post_tags` / 409,600-group fixture (16 cores,
30 GB, `shared_buffers=4GB`, warm, median of 5, each statement in a rolled-back
transaction):

| per 1,000 changes | `force_every_group` (today) | delta + projection | 2-transform workaround |
|---|---|---|---|
| `posts.author`+`word_count` UPDATEs | 1114 ms | **37 ms** (30.5 + 6.5) | 49 ms |
| `post_tags` INSERTs | 557 ms | **10 ms** | 22 ms |

and on storage, the per-parent projection is 71 MB where the row-grain
enrichment table is 377 MB (5.3×) — the gap widens linearly with fan-out.

## 2. The mechanism

**A settled parent projection**: a per-to-side-row table holding exactly the
columns the transform reads, keyed by the to-side PK, advanced *transactionally
with the reverse deltas it justifies*. It is by construction "the parent state
the target currently reflects".

- **Forward** (a from-side change) resolves the relationship against the
  projection, never against a live read of the parent.
- **Reverse** (a parent change) is staged as a *parent-keyed* record carrying
  the parent's old and new image plus a `(prev_lsn, lsn)` chain, and on apply
  emits paired subtract-old / add-new over the parent's from-side rows, then
  advances the projection.

Three guards make the reverse's enumeration equal the **applied** set rather
than the live set. All three are measured to be load-bearing (§4):

| guard | rule | why |
|---|---|---|
| **(a) watermark barrier** | capture `X` = the source's write frontier with the enumeration; do not apply until intake has *staged* everything committed at or before `X` | otherwise a committed-but-unstaged from-side change is invisible to any reconcile |
| **(b) optimistic generation check** | re-read the projection row's `gen` in Phase 3 under `FOR UPDATE`; abort if it moved since capture | Phase 2 holds no locks, so a forward apply can land between capture and apply |
| **(c) in-flight check** | abort if any staged from-side change for this parent (old or new side) with `lsn <= X` is still undrained | this is the guard that does most of the work; it also repairs commutativity (§3) |

A reverse that aborts is deferred and retried; it is not a stall of the batch.

This lands on the existing Phase 2 / Phase 3 split without holding a snapshot:
the enumeration and `X` are captured in Phase 2 (no transaction, no locks), and
the guards re-validate inside the single Phase 3 transaction alongside the
delta and the projection advance. The earlier "hold a REPEATABLE READ snapshot
across the barrier wait, blocking vacuum" concern does not arise.

## 3. The finding that changes the shape of the problem

Segments are handed out in `seg_seq asc` (`apply.rs:2525`) but several can be
claimed at once and buckets drain in parallel, so **a later segment's bucket can
commit before an earlier segment's**. Trellis relies on delta commutativity to
make that safe (doc 05: "because the deltas are invertible they also commute").

That property holds only because today's deltas are *self-contained* — every
value comes from the change record's own images. **Any design that resolves part
of the delta against mutable external state loses it.** The v2 model reproduces
this:

- naive per-row attribution (**D3** — i.e. the enrichment-table shape) corrupts
  76/200 runs, because an earlier segment's insert, drained late, overwrites the
  attribution a later segment's re-point already wrote;
- guard (c) is what repairs it for the projection design: it refuses to run a
  reverse while any from-side change for that parent is partially applied.

This constraint appears nowhere in #102 or its comments and is the main reason
the published designs did not survive.

## 4. Evidence

`trellis/tests/spikes/issue-102-settled-state-v2.sql` — self-contained, runs end
to end under `psql -f`, models the staging ring (segments, buckets, out-of-order
drain), intake lag as a confirmed-LSN watermark, parent INSERT/DELETE, from-side
FK re-point, and NULL groups. Every run is diffed against a from-scratch
`LEFT JOIN … GROUP BY`. Instrumented: it reports how often each mechanism
actually fired, so "0 failures" can be distinguished from "never exercised".

200 runs × 50 ops per design (~740 out-of-order drains per design):

| design | corrupted | note |
|---|---|---|
| D0 — #102 as written, no settled state | **198/200** | negative control: the fuzzer is sharp |
| D1 — projection only | **97/200** | |
| D3 — per-row attribution, naive | **76/200** | new: broken by out-of-order drain |
| **D5 — projection + (a)+(b)+(c)** | **0/200** | |
| D5 minus (a) barrier | 6/200 | guard necessary |
| D5 minus (b) generation check | 4/200 | guard necessary |
| D5 minus (c) in-flight check | 75/200 | guard necessary |
| D5 minus per-parent reverse ordering | 0/200 | **not** shown necessary — subsumed by (b)+(c) |

## 5. Barrier cost — measured

`confirmed_lsn` is persisted transactionally with each staged commit
(`intake/mod.rs:88`) and, on a quiet stream, from a keepalive throttled to
`KEEPALIVE_PERSIST_INTERVAL = 10s` (`intake/mod.rs:413`).

Measured against a live logical-replication stream under continuous WAL load:
the server→client streaming position tracks the write frontier within
**3–34 KB of WAL**; it is only the *confirmation/persist* step that runs on a
10-second sawtooth (0 → ~2 MB).

So the barrier must **not** read the persisted `replication_progress` row. Intake
and the apply workers are `tokio::spawn`ed in the same process
(`client.rs:439`, `client.rs:477`), so intake should publish its
*staged-through* LSN in an `Arc<AtomicU64>`, advanced right after each
`stage_and_advance` commit and on every keepalive with no throttle (nothing
needs to be durable for the barrier's purposes — a crash re-derives it). That
makes the barrier wait ≈ 0 under load, and bounded by the walsender keepalive
cadence when the instance is otherwise idle.

## 6. Prerequisites — ship these first, independently

**P0.1 — native-typed relationship lookups.** `from_side_keys_for_join`
(`apply.rs:503`) and `fetch_to_side_rows` (`apply.rs:651`) render
`{col}::text = any($1::text[])`, which is unindexable; the sites that get it
right (`apply.rs:465`, `1633`, `1750`) use `= any($1::text[]::{ty}[])`.
Re-measured on the 1M/3M fixture, 100 join keys:

| lookup | as rendered today | native-typed | |
|---|---|---|---|
| to-side fetch (`posts`) | 25.8 ms | 0.094 ms | **275×** |
| from-side enumeration (`post_tags`) | 66.7 ms | 0.084 ms | **794×** |

Cost scales with *table* size, not with the number of changed rows. This hits
#94 in production today and is a contained fix. It should be its own issue and
should land regardless of everything else here.

**P0.2 — composite source primary keys on the from-side path.**
`ddl::source_primary_key` rejects them and the reverse path calls it
(`apply.rs:984`, `1136`, `1445`, `1516`). Re-confirmed on current `main`:
`spike_a3_a_composite_pk_from_side_cannot_drain_at_all` still fails with
`Ddl(CompositePrimaryKeyUnsupported { source_table: "post_tags" })`. Without
this, #102's own worked example cannot drain under any design.

**P0.3 — nullable grouping keys (decision needed before any #102 grammar
work).** An aggregate target's PK is the grouping tuple (`ddl.rs:785`) and
backfill filters NULL keys out (`backfill.rs:1026`). A relationship grouping key
is nullable by construction — an unmatched LEFT JOIN, a nullable FK, or a
deleted parent all produce a NULL group. Postgres's own `GROUP BY` keeps those
rows as a NULL group, so today's target diverges from the oracle for any
nullable plain-column key too. Recommend making grouping columns nullable with a
`NULLS NOT DISTINCT` unique key, as its own issue.

**P0.4 — small, latent.** `derive_group_key` (`apply_aggregate.rs:421`) renders
`None` and `Some("")` identically as `"0:"`, so a NULL and an empty-string text
grouping key collide in the delta path.

**P0.5 — docs.** `intake/replica_identity.rs`'s module doc still says
`needs_old_image` "always returns `false` for now because `KeySpace` has only
`KeySpace::OneToOne`"; the `Aggregate` arm has since landed and returns `true`.
(The `COUNT` staleness in `docs/transforms.md` that the review flagged was fixed
upstream in 6b310b9; `COUNT(*)` is supported and #102's example parses.)

## 7. Sequencing

**Phase 1 — the projection, for to-one relationship *values* (#94).** This is
where the payoff is (30–55×), and there is an existing test surface. No grammar
change, no NULL-group question, no new user-visible concepts.

1. Catalog + DDL for the projection; a third `assert_replica_identity_*` arm at
   `create_relationship` (`needs_old_image` already exists as the predicate).
2. Backfill the projection alongside the relationship backfill.
3. Forward: resolve relationship reads from the projection, not from
   `fetch_to_side_rows`.
4. Reverse: replace the image-less from-side recompute with a **parent-keyed**
   record carrying old/new image and the `(prev_lsn, lsn)` chain.
5. The barrier watermark (§5) and guards (b)/(c).
6. Deferral plumbing. Reverse work is staged today as image-less recomputes at
   `hop_gen + 1`; a deferred-and-re-staged reverse must not burn `hop_gen` or it
   trips `MAX_HOP_GEN = 32` after 32 deferrals. Needs its own staged kind with a
   retry counter, and an **escalation to `force_every_group` after N retries** —
   which keeps that path alive as a bounded safety valve rather than the default.
7. Flip `force_every_group` off for to-one plans.

**Phase 2 — relationship grouping keys (#102 grammar).** On top of Phase 1:
grammar/AST/validator for `rel.field` in `GROUP BY`; join-aware
`derive_group_key` reading the projection; `keyset_match`
(`apply_aggregate.rs:1378`) made alias-qualified and join-aware for both the
survivor probe and the recomputing INSERT (it binds against source alias `s`
today, and the grouping column is not a column of the source); backfill SQL (re-measured: the full 409,600-group build from 3M rows is a
hash left join into a hash aggregate, no spill, **1.35 s** — the easy part).

**Phase 3 — explicitly out of scope:** chained multi-hop to-one paths (one
projection per hop), and to-many relationship aggregates.

## 8. Validation

- Promote `issue-102-settled-state-v2.sql` into CI as a SQL-level pin, keeping
  the guard-ablation variants as mutation tests — a guard that stops being
  load-bearing is a signal, not a cleanup opportunity.
- Generative suite (#34 op-stream reordering): steer toward interleaving a
  relationship-parent change with from-side inserts/deletes/re-points **on the
  same parent**, across a seal boundary and across the intake-lag window; plus
  parent insert, parent delete, NULL parent, and a SIGKILL during Phase 3 so the
  re-drain must redo the moves and the projection advance together.
- Reuse the three engine probes on `spike/issue-102-validation` (`spike_102.rs`,
  all three green on current `main`) as regression pins for the preconditions.

## 9. Decisions I need from you

1. **Sequencing** — Phase 1 (#94) before Phase 2 (#102's grammar)? It inverts
   #102's framing but that is where the measured win and the test surface are.
2. **NULL grouping keys** (P0.3) — nullable + `NULLS NOT DISTINCT`, or reject at
   define time in v1?
3. **`force_every_group`** — keep as a bounded escalation fallback (recommended)
   or delete outright once the delta lands?
