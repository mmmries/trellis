---
status: accepted
date: 2026-11-11
deciders: Michael Ries
---

# Fully-Qualified Names Are The Persisted Identity

A bare name like `posts` resolves to different tables depending on `search_path`.
Resolving an unqualified name against the wrong schema, at a later step than the
one that accepted the definition, produces errors such as
`relation "trellis.posts" does not exist`, or silently binds a definition to a
different table.

This ADR settles how table references are spelled and persisted across every
definition type — transforms and redefinition. It does not relax
[ADR-0005](0005-source-schema-is-user-owned.md): Trellis still issues no DDL
against source tables, only resolves, records, and reads their qualified names.
The `schema.table` string PostgreSQL itself uses is a table's only identity.
Trellis records oids only to notice that the object at a name was recreated or
restored, and then pauses rather than follows it.

## A table reference is resolved once, at definition time, and persisted qualified

**Trellis resolves every table reference to a fully-qualified `schema.table` name
once, at definition-acceptance time, and persists that qualified form as the
reference's identity. It never persists or re-resolves the bare spelling.**

1. **Definitions persist qualified names.** A bare name is resolved exactly once
   using PostgreSQL's `search_path` semantics over Trellis's schema, the target
   schema and `public`, in that order — first schema with a matching table wins
   — and the qualified result, not the bare spelling, is persisted.

2. **The dependency graph keys on qualified identity.** `public.posts` and
   `archive.posts` are distinct nodes with independent edges, versions, and
   quarantine state, walked and diffed on qualified strings, never re-resolved.
   The catalog's uniqueness constraints key on qualified identity too; a
   transform's target is also unique by its bare table name, since a
   transform is addressed by it.

3. **Every generated statement emits qualified names,** schema and table quoted
   independently — for capture triggers and functions, backfill enumeration, and
   target-table DDL alike.

4. **The grammar accepts, but does not require, an explicit `schema.table`.** A
   qualified spelling resolves to that exact relation; a bare one is resolved as
   in (1).

This applies to source tables, a `JOIN`'s joined table, and transform targets.

## Identity is the name, not an OID

Storing the relation's OID as authoritative identity would *attempt* to follow
renames, but the bug addressed here is a *definition-time name-resolution* bug —
capture was never confused, since the trigger is attached to one specific
relation. OIDs are also **not** stable across restore/`pg_upgrade`/DR-failover,
so every transform would break after a source-DB restore unless a missing OID
were tolerated — which reopens the very ambiguity this decision closes. It
would also impose permanent dual-path complexity.

**An oid is recorded only as a change signal, never as identity.** Trellis
records the oid (with `xmin` and enabled mode) of each capture trigger it makes
(`capture_triggers`), the oid of the table each recorded source key was taken
from (`definition_keys`), and beside its own catalog rows the oid of the
catalog table that holds them and the cluster's `system_identifier`
(`capture_triggers`, `partition_sources`). A table recreated at its name, its
triggers recreated, or the database restored from a logical dump shows up as a
recorded oid that is gone or differs from the live one. Trellis then pauses, with a
reason, every definition that reads the table (every definition, for a
restore), instead of following the new object or holding on to the old oid
([capture changed outside Trellis](../staging-and-claiming/01-capture-by-triggers.md#capture-changed-outside-trellis),
[ADR-0017 — a restore pauses for a rebuild](0017-partitioned-sources.md#pg_upgrade-re-baselines-the-recorded-state-a-restore-pauses-for-a-rebuild)).
`RESUME` re-validates against the table now at the name and rebuilds. A key's
backing index is matched by shape, never by its oid
([ADR-0026](0026-source-keys.md#a-key-is-backed-by-a-unique-index-matched-by-shape)).

## Names are not re-resolved on later operations

Re-applying `search_path` during backfill, capture-trigger reconciliation, and
apply makes behavior depend on the executing session and can bind one definition
to different objects over time. Persisting the qualified name avoids this and
the repeated work, extending to identity the resolve-once reasoning
[ADR-0005](0005-source-schema-is-user-owned.md) applies to validation.

## Consequences

* **The schema-confusion bug class is closed by construction.** Resolution
  happens once, against the configured list of schemas the bare name is meant
  to read against; nothing downstream re-resolves, so no later step can bind
  the same definition to a different object.

* **Same-named tables in different schemas are distinct definitions,** with no
  shared identity to confuse.

* **This does not survive renames or `SET SCHEMA`.** A renamed or moved source
  table no longer resolves, and the definition fails loudly rather than silently
  following the object. A deliberate trade: OID-binding would follow renames at
  the cost of breaking on the far more common restore / `pg_upgrade` /
  DR-failover case, where OIDs are reassigned but a restored `public.posts` is
  still `public.posts`. Recovering from a genuine rename is an explicit
  redefine — visible, not silent — consistent with
  [ADR-0005](0005-source-schema-is-user-owned.md).

* **Names remain human-readable and parameterizable.** Unlike OIDs, a
  `schema.table` string is a normal SQL identifier — usable directly in generated
  statements and in error/observability output, with no name-refresh path.
