---
status: accepted
date: 2026-09-02
deciders: Michael Ries
consulted:
informed:
---

# Named Relationships

This ADR settles the design for named relationships: how one is declared and
referenced, how cardinality constrains that reference, and how a change to a
*related* row propagates back to the rows that depend on it.

## Declaration

A relationship is a **standalone, named declaration**, not a clause inside a
transform's `FROM`, so one relationship is reusable across many transforms:

```text
RELATIONSHIP <name> FROM <from_table>.<fk_col> TO <to_table>.<pk_col>
```

```text
RELATIONSHIP product  FROM order_line_items.product_id TO products.id
RELATIONSHIP author   FROM posts.author_id             TO users.id
RELATIONSHIP editor   FROM posts.editor_id             TO users.id
```

The statement grammar follows ADR-0004's approach: our own minimal grammar,
parsed at definition time into an AST, sharing the same lexer and expression
language used everywhere else.

### Naming and scope

* A relationship name is **unique per from-table**, not global. `posts` may
  declare both `author` and `editor` pointing at `users` as independent
  relationships.
* Relationships may be declared in any order relative to the transforms and
  tables they reference, as long as every endpoint resolves and the resulting
  dependency graph stays acyclic.

### Endpoints

* Either endpoint may be a **source table or a transform target**, in any
  combination.
* **No FK auto-discovery.** Every relationship names its join key explicitly.
  Inferring relationships from foreign-key metadata is a possible future
  convenience, not part of this design.

## Referencing a relationship

A calculated field references a relationship through a qualified path whose
**head is the relationship name** — not the target table name and not a
per-query alias:

```text
product.category_name
```

`category_name` is a column on the to-side table; per-from-table name uniqueness
guarantees `product` names exactly one relationship on the referencing table.

How a relationship may be referenced depends on its **cardinality**, which
parallels how granularity constrains formula shape in
[transforms](../transforms.md#calculated-fields):

### To-one relationships (at most one related row)

If the to-side column is a **primary key or has a `UNIQUE` constraint**, the join
resolves to at most one related row and enrichment columns are usable as **bare
paths** (`product.category_name`). A transform target always qualifies, since its
primary key comes from its key-space.

This "at most one related row" invariant is enforced **at definition time** from
the to-side constraint, per
[ADR-0005](0005-source-schema-is-user-owned.md): a bare to-one relationship whose
to-side is not provably unique is rejected with guidance (add a `UNIQUE`
constraint, or use the aggregate form). If data violates uniqueness at runtime,
the affected key is quarantined (per the existing per-key isolation mechanism),
never silently resolved to an arbitrary row.

### To-many relationships (many related rows)

If the to-side column is not unique, the relationship is **to-many** and its
columns may be referenced **only** wrapped in exactly one aggregate function
(`SUM`, `MIN`, `MAX`, `AVG`, `COUNT`) over the related rows:

```text
sum(comments.word_count)
```

This uses the same aggregate semantics and incremental-delta machinery as a
`GROUP BY` transform, keyed by the relationship's join value instead of a
grouping tuple. A **bare** reference to a to-many relationship is a definition-time
error.

### Relationship vs. cross-join

A relationship of either cardinality **keeps the referencing table's own
granularity**, folding related data into scalar enrichment columns. A
**cross-join changes granularity**, producing a new table at the pairing grain
(`{a.pk, b.pk}`). Same data shape, different output grain: a to-many relationship
decorates existing rows with an aggregate of related rows; a cross-join produces
one row per matching pair.

## Chaining

A path may cross more than one relationship, each segment naming a relationship
on the table the previous segment resolved to:

```text
post.author.name
```

Here `post` is a relationship on `comments`, `author` is a relationship on
`posts`, and `name` is a column on `authors`. Each hop resolves under its own
cardinality rule; the chain's cardinality is to-one only if **every** hop is
to-one, in which case the whole path stays a bare reference. A to-many hop
anywhere in the chain makes the whole path to-many, referenceable only wrapped
in exactly one aggregate at the outermost reference — a chain does not let a
later to-one hop "undo" an earlier to-many one:

```text
sum(comments.post.author.post_count)
```

Chained relationships add no new cycle-detection or storage machinery: each hop
is already an edge in the dependency graph from
[Dependency graph and cycles](#dependency-graph-and-cycles), so a chain is just
several edges traversed in sequence.

## Nullability

* A to-one relationship with **no matching related row** yields `NULL` for its
  enrichment columns (left-join semantics). It does not affect whether the
  referencing row exists.
* A to-many relationship with no related rows yields the aggregate's empty result
  (`COUNT` → `0`, `SUM` → `NULL`, etc.), matching PostgreSQL.

## Dependency graph and cycles

Relationship links are **edges in the same cross-table dependency graph** as
sources, joins, and chained transforms
([transforms](../transforms.md#chaining-and-cycle-detection)). Cycles — both
column-to-column and table-to-table — are rejected at definition time across the
whole graph, so evaluation order is well-defined and runtime propagation
terminates.

## Storage

Relationship definitions are persisted in Trellis's own catalog as source text,
re-parsed on read, immutable once created, mirroring transform definitions. No
storage change is made to source tables (see
[ADR-0005](0005-source-schema-is-user-owned.md)).

## Incremental maintenance

Trellis maintains enriched columns incrementally, like any other calculated
field, over the asynchronous staging/apply pipeline
([ADR-0002](0002-async-data-flow.md), [data-flow](../data-flow.md)). Two
directions:

* **Forward (a referencing row changes).** Its enrichment columns are re-derived
  in dependency order: a to-one relationship looks up the single related row by
  join key; a to-many relationship aggregates the related rows.
* **Reverse (a *related* row changes).** Trellis resolves, from the dependency
  graph, which relationships target the changed table, then re-derives the
  referencing rows whose join key matches the changed row's key. That key comes
  from the changed row's replica image. For a **to-one** relationship the join
  key is the to-side's own primary key, always present in the default replica
  identity, so no extra replica identity is needed. For a **to-many**
  relationship the join key is a *non-PK* column on the to-side, which the
  default (PK) replica identity omits from delete/re-parent pre-images; such a
  relationship therefore requires `REPLICA IDENTITY FULL` (or a replica-identity
  index covering the join column) on the to-side, enforced at define time. This
  reuses the existing "recompute" staging path rather than a bespoke persisted
  reverse index.

  Finding the affected referencing rows is a lookup on the from-side join column.
  That lookup is **correct without an index**; an index only makes it fast. Per
  [ADR-0005](0005-source-schema-is-user-owned.md), Trellis does not create the
  index — it detects whether a usable one exists and, if not, emits a performance
  warning naming the exact `CREATE INDEX` the user may run.

This is the case [ADR-0002](0002-async-data-flow.md) flagged: formulas that
reference across relationships are unsound under naive synchronous triggers. The
asynchronous staging/apply/fence design makes them sound; reverse propagation is
one more producer feeding that same machinery, not a new ordering regime.

## Redefinition

Relationships are **immutable once declared**, like transforms: to change a join
key you declare a new relationship and cut over. Editing the *calculated columns
that consume* a relationship is part of the separate transform-redefinition work
(see [open-questions](../open-questions.md)); its rules govern those columns, not
the relationship link itself.
