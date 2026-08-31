# Cross-cutting — Convergence, and the read-your-writes predicate

← [Cleanup and reclaim](06-cleanup-and-reclaim.md) · next → [Implementation checklist](08-implementation-checklist.md)

**What this owns:** answering "has my write been reflected yet?" without lying.

**The guarantee:** *every un-reflected effect of a commit at position L is
represented by at least one pending position at or below L.* The predicate may
say "not yet" when it is actually done (over-reporting is safe); it must never
say "done" when it is not.

The naïve versions all produce **false `converged`** answers by narrowing the
question to something cheaper. If you build one thing from this series carefully,
build this one.

## Watermark tokens

A caller reads its own writes without waiting for global idleness:

1. after its mutation commits, the caller reads `pg_current_wal_lsn()` **on its
   own connection** — the **token**;
2. it polls a predicate until derived state reflects every commit at or below
   the token.

The token is taken *after* the write commits, so it bounds that write's position
from above. The engine's own workers do the work; the caller only waits — there is
no client-driven drain loop.

## The predicate

`converged_through(token)` is **one statement**, so every condition is evaluated
against one committed snapshot. It is true iff all four hold:

1. **intake has durably staged past the token** — `confirmed_lsn >= token` from
   the progress row ([01](01-intake-and-lsn-confirmation.md)). The row's first
   value is seeded at the slot's consistent point *before* the initial backfill
   is folded, and `confirmed_lsn` generally leads applied state even in steady
   state — so it can read a slot as converged while that work is still
   unfolded or unapplied. Gate on the pending work itself, never on
   `confirmed_lsn` alone;

2. **the active batch's table holds no row with `origin_lsn <= token`** — the
   active tail. The pointer is resolved in the same statement, so a concurrent seal
   cannot slip between the resolve and the scan;
3. **no non-active slot holds a still-pending row with `origin_lsn <= token`**,
   under one shared definition of "pending";
4. **no parked quarantine row has `origin_lsn <= token`** — the poison band
   ([06](06-cleanup-and-reclaim.md)).

`origin_lsn` is the **oldest un-reflected trigger position** carried by a pending
change. It LEAST-merges in the fold ([04](04-claiming-and-the-fold.md)) — a
re-stage can only make it older — while the ordinary position GREATEST-advances.
Downstream propagation carries the *minimum* trigger origin to the dependent it
stages, and `0` means "unknown, conservatively old".

> **Invariant (await soundness):** every un-reflected effect of a commit at
> position L is represented by ≥1 pending change with `origin_lsn <= L`. Any
> staging path that sets `origin_lsn` higher than the oldest un-reflected trigger
> — i.e. forgets the LEAST-merge — silently breaks read-your-writes.

## The trap: do not consult the summary band

The seal computes a per-batch summary band (`min_origin_lsn`, `max_lsn`) as part
of its phase-1 flip. Using it in the predicate is the obvious optimization — one
indexed comparison per batch instead of a scan — and it is **wrong**. The band is
aggregated *inside* the flip transaction, so it describes the rows *that
transaction* could see, not the slot. Two writers land in the slot outside it:

- a **straddler** — invisible in `S_k`, claimed by batch *k+1*
  ([03](03-sealing-and-the-fence.md));
- a **phase-gap writer** — commits between the flip's commit and the later
  snapshot capture, so it *is* visible in `S_k` and batch *k* claims it.

Each surfaces as a false `converged`. Condition 3 above replaces the whole pile:
**it asks the slot**, so it cannot disagree with the other pending readers.

The band is now write-only — nothing reads `min_origin_lsn` or `max_lsn` back.
Treat them as vestigial, not an observability surface, and do not restore a reader
on the assumption that one exists. Making the band *honest* by finalizing it at
snapshot capture stays unbuilt: it changes the seal's crash story, and a
snapshot-time aggregate still cannot see a straddler, so it would sit *beside* a
straddler term — the shape that leaked.

**"Narrowing condition 3" is a recurring, unsound temptation** — drained-only,
straddler-only, band-only all admit a false `converged`. Buy the honest question
back with an index and statistics, not with a weaker question. Condition 3 is
deliberately *not* narrowed to "buckets not yet drained" either: a part-drained
batch reports its whole slot as pending. That over-reports, which is the safe
direction — the batch settles moments later anyway.

## Making the honest question cheap

**Index it.** Conditions 2 and 3 filter on `origin_lsn`, which the ordering index
(on position and key) cannot serve. Without a dedicated index the probe is a
sequential scan whose cost grows large exactly when the minimum origin sits late
in a big sealed slot — the case downstream propagation produces routinely. With
the index it is a cheap seek regardless of slot size.

**The index is necessary but not sufficient**, and this is the half people miss.
Ring tables carry `autovacuum_enabled = off` ([02](02-the-staging-ring.md)), which
also disables **autoANALYZE**. With no statistics, the planner's default inequality
selectivity picks a sequential scan at *every* table size — not a threshold a big
table crosses. The two conditions get there differently, both load-bearing:

- **Condition 3** is served by statistics the engine writes itself: a single-column
  `ANALYZE (origin_lsn)` on a slot **as it is sealed** — the moment it stops growing
  and the moment condition 3 starts asking about it. Single-column so no other plan
  changes; best-effort, because a failed refresh costs plan quality and nothing else.
- **Condition 2 needs no statistics**, because it is written as `min(origin_lsn) <=
  token`, not `EXISTS (… origin_lsn <= token)`. The two are equivalent when the
  column is `NOT NULL`, but `min()` plans as a `Limit 1` over the index regardless of
  what the planner believes. Required, not tidy: condition 2's slot is the **active**
  one, which grows continuously, so no `ANALYZE` cadence keeps it current and a stale
  histogram from a previous lap actively points the wrong way.

## Ask for the sign, not the number

| Question | Shape | Cost | Use |
|---|---|---|---|
| "has my token converged?" | the four conditions, one statement | cheap — index-served | the await poll |
| "is anything pending at all?" | `EXISTS`, short-circuits on the first pending row | cheapest — short-circuits | any polled gate |
| "how much is pending?" | `count(*)` over every slot plus the quarantine | expensive — full linear scan | observability only, **never polled** |

`pending_count` is linear in the ring and no index helps — there is no predicate
to index away. It was taken **off the polling path**: every polled consumer wanted
the sign, not the number, so the sign became its own question. One asymmetry in the
`EXISTS` form: `true` short-circuits, `false` does not — it is a gate ("has my work
drained?"), not a work-arrival poll.

## "A round did no work" is not the same question

A loop that drains until a round claims nothing is ambiguous. A zero-work round
means *idle*, **or** the claim was **refused** —
seal-gate backpressure ([03](03-sealing-and-the-fence.md)), a pause lease held by
an auditor ([04](04-claiming-and-the-fold.md)), a lost seal race, or a sealed batch
another worker holds. The idle maintenance sweep makes no progress in the refused
cases, so "claimed nothing and the sweep did nothing" is not convergence. Split it:

| Zero round | Meaning | Action |
|---|---|---|
| nothing drainable pending anywhere | converged | return |
| the sweep freed something | more to do | loop |
| pause lease active | claiming is gated fleet-wide | wait; the lease auto-expires |
| pending **and** sealable here | this connection's seal is refused | wait in short steps against a bounded budget, then a **named backpressure error** — never a generic "values are not stabilizing" message, which would point at a schema cycle that does not exist |
| pending and not sealable here | a peer holds the batch | wait; bounded by the peer finishing, or by the claim TTL if it died. Timing out here names the *holder*, not a cycle |

**Convergence is cluster-wide**, deliberately: the check reads the registry and
every slot and does not care which worker holds a claim. A peer that *stalls* makes
this wait; a peer that *dies* is bounded by the reclaim TTL. That was chosen over
the cheaper "this connection drained what it could claim" because the production
caller is backfill, and backfill's contract *is* cluster-wide.

**The one exclusion is parked quarantine work, reported rather than swallowed.**
Parked work is un-applied work no worker will ever claim — only an operator release
re-admits it — so a gate that waited on it would hang with no peer to finish the
work, turning a quarantine into a wedge. It is dropped as a whole **arm**, never by
narrowing the shared row predicate, and the outstanding count is carried back to the
caller so the result is explicitly partial. Note the asymmetry: the
*drain-to-convergence helper* excludes parked work so it can terminate, while
`has_pending` and the await predicate **still count it**, because a quarantined key
must keep blocking a caller waiting on its band.

## An observability view, distinct from the fenced read

Exposing "every pending row" is useful — Trellis uses it for its self-check
auditor's exemption map and its overdue-work battery. That view is a **read view,
not a fenced one**: it can include a straggler the fold will attribute to a later
batch. Over-exempting and over-reporting are both conservative here. Keep it clearly
separate from the fenced claim path so nobody wires the loose one into the strict
one.
