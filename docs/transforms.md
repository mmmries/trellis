# Defining Transforms

A transform describes how one derived (target) table is produced from one or
more source tables. Its definition is built from three separately-chosen
pieces: the **granularity** of the target's primary-key space, the
**calculated fields** that populate its non-key columns, and an optional
**partial-data** predicate restricting which source rows participate.

Trellis maintains these tables **incrementally** as source data changes,
trading spikey read load for a steady write load that keeps the table cheap to
read (see the README for fuller motivation).

## Granularity

Granularity determines the target's primary-key space — what a single target
row represents relative to its source row(s). Trellis supports three:

### 1-1

Exactly one target row per source row, inheriting the source's primary key.

* Source row insertion/deletion maps 1-1 to a target row insertion/deletion.
* Always has a single source table. Deriving a target from a key-to-key join
  of two tables is a cross-join, not 1-1, even when the join is one-to-one in
  practice.

### Aggregate (`GROUP BY`)

The primary-key space is defined by a `GROUP BY` over the source table(s). Each
distinct combination of grouping values produces one target row, matching the
equivalent `GROUP BY` query against the source.

* Many source rows can map to one target row.
* The primary key is the tuple of grouping columns.
* Calculated fields typically use aggregate functions over the grouped rows.
  Today that's `SUM`, `AVG`, `MIN`, and `MAX` (numeric-only); `COUNT` is not
  yet implemented (see ADR-0004).
* Adding, removing, or changing a source row can change its group, inserting,
  deleting, or updating a target row.

For example:

```
TRANSFORM order_totals FROM order_line_items GROUP BY order_id
SELECT
  order_id AS order_id,
  SUM(amount) AS total_amount,
  AVG(amount) AS avg_amount,
  MIN(amount) AS min_amount,
  MAX(amount) AS max_amount
```

### Cross-join

The primary-key space is the join of two source tables, mirroring the rows a
`JOIN` query returns. Each unique pairing of source primary keys that satisfies
the join condition produces one target row.

* The primary key is the pair `{a.pk, b.pk}`. Both are kept because one row on
  either side may match many on the other; the pair uniquely identifies a
  target row.
* The join type is chosen per transform:
  * `INNER` — only matching pairs produce a row.
  * `LEFT` / `RIGHT` — every row on the retained side produces a row even with
    no match; the unmatched side's columns (and its half of the key) read null.
* A change to either side can insert or delete target rows.

## Relationships

Cross-join changes a table's granularity. Often you instead want to keep a
table's own granularity and merely *enrich* it with columns looked up from a
related table — order lines decorated with `product.category_name`.

A relationship is a named, directed link from one table to another, defined by
a join key (e.g. `order_line_items.product_id -> products.id`). A calculated
field on the source table can then reference the related table's columns
through qualified paths like `product.category_name` or
`account.customer_segment`.

The target keeps its own primary key and granularity — a 1-1 table stays 1-1 —
and each enriched column is populated from the single related row the join key
resolves to. A relationship must resolve to at most one related row; a link
that can match many is a cross-join, which produces a new pairing-grained table
rather than decorating an existing one.

## Calculated Fields

Every target table has a set of **calculated fields** — non-key columns
populated by a formula rather than copied from a source column. A formula may
reference:

* Columns on the source row(s) that feed the calculated row.
* Columns on related tables, through a declared relationship
  (`relationship.column`).
* Other calculated columns on the same target table.

The set of calculated fields is chosen independently of granularity, but
granularity determines which formula shapes are valid when reading source
columns:

| Granularity | How a formula references source columns |
| --- | --- |
| 1-1 | Reference source columns and other calculated columns directly; each resolves against the single source row. |
| Aggregate | Grouping-key columns may be referenced directly. Any other source column must be wrapped in exactly one of `SUM`, `AVG`, `MIN`, or `MAX` (numeric-only; `COUNT` is not yet implemented) over the group. |
| Cross-join | Reference source columns through the qualified name of the side they come from (`a.column`, `b.column`). |

Formulas may only use **immutable** functions and operators — those whose
output depends solely on their inputs. Anything depending on database state
outside the row (current time, timezone, collation, session settings, random
values) is disallowed: incremental maintenance requires that re-evaluating a
formula on unchanged inputs always yields the same result.

### Chaining and cycle detection

Calculated columns may reference each other, letting users build derivations
out of small named steps (a `margin` column defined in terms of `revenue` and
`cost` rather than inlining their formulas). Trellis builds a dependency graph
over a table's columns and evaluates them in dependency order.

Transforms also chain: a target table may itself be a source for another
transform — aggregated, cross-joined, or referenced through a relationship. The
dependency graph therefore spans multiple tables, adding table-to-table edges
from sources, joins, and relationships.

**Cycles are rejected at definition time across the whole graph** — both
column-to-column within a table and table-to-table across chained transforms,
joins, and relationships. A definition that would introduce a cycle, directly
or transitively, is invalid and rejected before it runs, keeping evaluation
order well-defined.

## Partial data

Any target table, regardless of granularity, may be defined over a *subset* of
its source rows via a row-level predicate, so the derived table materializes
only the rows that matter.

This is an optimization in the spirit of a partial index: maintaining a table
over a narrow, high-value slice keeps the steady write load small, and excluded
rows never enter the target or incur maintenance cost.

Row-level filtering is a distinct concern from granularity and calculated
fields: granularity decides what a target row represents, calculated fields
decide its non-key columns, and the partial-data predicate decides which rows
exist at all.

## Status

Every defined transform carries an observable **status** describing where it is
in its lifecycle:

* **`waiting_to_backfill`** — defined, but its pre-existing source rows have not
  been enumerated yet.
* **`backfilling`** — those pre-existing rows are being enumerated and staged.
* **`live`** — backfill is complete; the transform is tracking live changes
  only. This is the steady state.
* **`quarantined`** — the transform is broken and no longer maintained (the
  quarantine fuse has tripped); resuming it re-runs the backfill, returning it to
  `waiting_to_backfill`.

The client library exposes this: an application can list defined transforms and
read each one's status — enough to tell that a newly-defined transform is still
populating, without waiting on a metrics pipeline.

A transform can sit in `waiting_to_backfill` while a long-lived cluster
transaction holds the backfill's fence open — a safe wait, explained with its
remedy under
[observability — Transform status lifecycle](observability.md#transform-status-lifecycle),
which also covers the full lifecycle and the wider telemetry design.

## Scope

This document describes the logical model only, not the physical storage or
timing of derived data (e.g. synchronous vs. asynchronous updates); see
[data-flow](data-flow.md) for how changes propagate and
[0002-async-data-flow](decisions/0002-async-data-flow.md) for why that flow is
asynchronous.
