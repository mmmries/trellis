---
status: accepted
date: 2026-09-02
deciders: Michael Ries
consulted:
informed:
---

# Source Schema Is User-Owned

Trellis reads source tables over logical replication and derives target tables
from them. A recurring temptation, as features grow, is for Trellis to *improve*
the source schema on the user's behalf — add an index to make a lookup fast, add
a `UNIQUE` constraint to guarantee an invariant a transform relies on, set
`REPLICA IDENTITY FULL` so deletes carry an old image. This ADR rejects that
class of behavior.

## Decision

**Trellis never modifies the source schema.** It does not create indexes, add or
alter constraints, change replica identity, or issue any other DDL against a
source table. The user owns those tables; Trellis is a reader.

Instead, Trellis:

1. **Validates strongly at definition time.** Every definition is checked against
   the introspected source schema (`pg_catalog` / `information_schema`) before it
   is accepted. If the source schema cannot support the definition, the
   definition is **rejected** — Trellis does not silently produce wrong answers
   and does not quietly reshape the source to make it work.
2. **Guides the user toward a working solution.** A rejection (or a performance
   warning) carries the most helpful, specific message we can produce, including
   the exact change the user should make to their own schema — e.g. "`products.id`
   must be a primary key or have a `UNIQUE` constraint to be the target of a
   to-one relationship; add one, or reference this relationship through an
   aggregate," or "no index on `order_line_items.product_id`; related-row updates
   will be slow — consider `CREATE INDEX ... ON order_line_items (product_id)`."

The distinction between the two is **correctness vs. performance**:

* A missing correctness prerequisite (a required type, a uniqueness guarantee a
  bare to-one relationship depends on) is a **hard rejection** at definition time.
* A missing performance prerequisite (an index that would make reverse
  propagation cheap) is a **warning**: the definition still succeeds and is
  correct without it; we tell the user what to add if they want it fast.

## Consequences

* Definitions are only as capable as the source schema the user has actually
  built. This is deliberate: a Trellis instance never leaves the user's schema in
  a state they didn't author.
* Validation logic must introspect real constraints (primary keys, unique
  indexes, column types, replica identity) rather than assume them. Requirements
  that today are stated as "checked user requirements" (e.g. `REPLICA IDENTITY
  FULL` for deletes/re-parents from one-to-many aggregates, see
  `staging-and-claiming/01-intake-and-lsn-confirmation.md`) are enforced this
  way: checked and surfaced, never applied by Trellis.
* Error and warning messages are a first-class part of the product surface, not
  an afterthought — the guidance *is* how a user learns to shape a schema Trellis
  can serve well.

## Scope

This is a project-wide stance, not specific to any one feature. It governs how
every current and future definition type validates against source tables —
transforms, relationships (see [0006-relationships](0006-relationships.md)), and
the transform-redefinition work still being designed.
