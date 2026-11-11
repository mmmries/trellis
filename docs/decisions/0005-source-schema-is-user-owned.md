---
status: accepted
date: 2026-11-11
deciders: Michael Ries
---

# Source Schema Is User-Owned

Trellis reads source tables (changes are captured by triggers it installs; see
[capture by triggers](../staging-and-claiming/01-capture-by-triggers.md)) and
derives target tables from them. As features grow, it's tempting to *improve* the
source schema on the user's behalf — add an index, add a `UNIQUE` constraint a
transform relies on. This ADR rejects that. It is project-wide, not
feature-specific: it governs how every definition type validates against source
tables — transforms and transform redefinition (see
[0015-transform-redefinition](0015-transform-redefinition.md)).

## Trellis never modifies the source schema

No indexes, no constraint changes, no DDL against a source table other than the
capture triggers themselves. The user owns those tables; Trellis is a reader.
Instead:

1. **Validate strongly at definition time.** Every definition is checked against
   the introspected source schema (`pg_catalog` / `information_schema`) before
   acceptance. If the schema can't support it, the definition is **rejected** —
   Trellis never silently produces wrong answers, and never reshapes the source
   to make a definition work.
2. **Guide the user to a fix.** Every rejection or warning names the exact change
   to make — e.g. a source index dropped after define is reported in `status()`
   with the statement that restores it, `create index on public.orders (region)`.

## Define refuses a missing prerequisite; after define, a lost key pauses and a lost index is reported

Define and a whole-transform `RESUME` refuse a definition that lacks a
prerequisite ([ADR-0027](0027-operating-principles.md), rule 4):

* **Correctness prerequisites:** a supported type, and a source key: the
  table's primary key or a `KEY (…)` the definition names, backed by a unique
  index ([ADR-0026](0026-source-keys.md)).
* **The source index a `GROUP BY` or `JOIN` target's `self_check` walk pages
  by:** an index leading with the `GROUP BY` columns, or on each side of a
  `JOIN` with that side's `ON` columns. A definition can waive it with
  `WITHOUT SOURCE INDEX`; its `self_check` then scans the source once per
  page.
* **The index a `MIN` or `MAX` field's probe reads:** on the `GROUP BY`
  columns, then the argument. `WITHOUT SOURCE INDEX` doesn't waive it.

A lost correctness prerequisite pauses the transforms it affects (rule 4), such
as a change to the key a transform uses, or the index backing a `KEY (…)` going
missing ([ADR-0026](0026-source-keys.md#keeping-the-key)).

A lost performance prerequisite, an index dropped or invalidated after
define, leaves the transform running and its target correct, since an index
affects speed, never results. `status()` reports it (`source_index_gaps`,
`extreme_index_gaps`) with the `create index` that restores it. Unless the
definition waived its source index, `self_check` refuses to walk it and a
whole-transform `RESUME` refuses until the index is back; a `RESUME` always
refuses while a `MIN`/`MAX` probe index is missing. The exception to "keeps
running" is that probe index: without it each re-read of a group's extreme
scans the group, and a page can outrun `statement_timeout` and retry forever,
so once none has existed for five minutes the staging worker pauses the
transform, naming the index to create.

## Consequences

* Definitions are only as capable as the schema the user actually built — by
  design. Trellis never leaves a schema in a state the user didn't author.
* Validation must introspect real constraints (keys, unique indexes, types),
  never assume them. Requirements are checked and surfaced, never applied.
* Error and warning messages are first-class product surface: the guidance *is*
  how a user learns to shape a schema Trellis can serve well.
