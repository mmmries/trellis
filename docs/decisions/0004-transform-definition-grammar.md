---
status: draft
date: 2026-08-20
deciders: Michael Ries
consulted: 
informed:
---

# Transform-Definition Grammar

[transforms](../transforms.md) describes the logical model — granularity,
calculated fields, relationships, partial-data predicates — but not how a user
writes one down, and [open-questions](../open-questions.md) left the surface
syntax undecided. This ADR settles it: transform definitions use a **minimal,
purpose-built grammar** that borrows SQL's spelling for familiarity but is our
own language, not a Postgres dialect.

The calculation grammar is an **immutable subset of PostgreSQL's operator and
function semantics**. The *statement* shape (`TRANSFORM / FROM / SELECT / WHERE`,
and later `GROUP BY` / `JOIN`) is ours; the *expression semantics* are Postgres's.
This constraint allows us to use Postgres as the primary correctness oracle, while
the internal evaluator serves as a secondary cross-check (see `docs/generative-test-suite.md`).

## Proposal

Define a small grammar of our own, parsed at definition time into an AST the
validator and execution layer consume. It looks like SQL — same operator and
function spelling, same shape for a `GROUP BY` list or join condition — so it's
familiar on sight, but the accepted language is exactly what we specify. We
start from the stable operations common to **PostgreSQL 15+**.

The grammar is **split by concern**, mirroring the pieces
[transforms](../transforms.md) already chooses separately. Splitting them lets
granularity-specific rules be enforced by *which* grammar applies, not by one
validator branching on context:

* **Key-space** — the granularity definition, which decides the target's
  primary key: a `GROUP BY` list for aggregates, a join condition and type for
  cross-joins, nothing for 1-1. Deliberately tiny and closed. Enforces that
  aggregate columns are wrapped in an aggregate function and cross-join columns
  are side-qualified.
* **Calculated-field expressions** — the scalar-expression language, reused for
  every calculated field.
* **Partial-data predicate** — the same expression grammar with a `-> bool`
  requirement, restricting which source rows participate.

## Why our own grammar, not the Postgres parser

The obvious alternative is Postgres's real grammar — `libpg_query` (via
`pg_query.rs`) exposes the server parser; `sqlparser-rs` offers a pure-Rust
approximation. We reject both as the *definition* surface because we execute
formulas ourselves.

Trellis does **not** evaluate formulas by issuing SQL to Postgres per batch. We
re-implement evaluation in our own execution layer, modeled on Postgres's
source, so we can do things Postgres-as-executor can't — notably small **atomic
delta adjustments** to aggregated `numeric` values, the basis of cheap
incremental maintenance (see [data-flow](../data-flow.md)).

That makes the grammar's function set and the execution layer's function set the
**same list**: we can only accept an operator or function we have actually
re-implemented. Every addition is a paired grammar-plus-evaluator change, not a
parser flipping on syntax we can't yet compute. Adopting a full SQL parser would
invert this — we'd inherit its entire grammar surface and forbid it piece by
piece, and we're parsing fragments (an expression, a predicate, a grouping
list), not whole statements. We want the accepted language to *be* the spec.

Re-implementing evaluation also sharpens the correctness bar. Because the
accepted grammar is an immutable subset of Postgres semantics, the
[correctness oracle](../data-flow.md#correctness) *is Postgres*: the generative
suite renders a definition back to a `SELECT` and asserts the persisted target
equals what Postgres computes for the same source data. The re-implemented
evaluator is retained as a secondary cross-check against that Postgres oracle.

## Concrete syntax (1-1 slice, issues #22, #62)

The 1-1 slice of the grammar (`engine/src/defs`) uses:

```text
TRANSFORM <target>
FROM <source>
SELECT <expr> AS <field> [, <expr> AS <field> ...]
[WHERE <predicate>]
```

`<expr>` is a column reference, a numeric literal, a single-quoted string
literal, `<expr> + <expr>`, `<expr> > <expr>`, or a function call
(`strpos`, `octet_length`, `char_length`, `regexp_count` — general
`func(args)` call syntax landed with these four; adding another function
means a registry entry plus a re-implemented evaluator arm, same bar as an
operator). Values carry one of three types — `Numeric`, `Text`, `Boolean` —
and every operator/function's argument and return types are type-checked at
definition time (see [transforms](../transforms.md)). `<predicate>` accepts
only the literal `TRUE` for now (the partial-data predicate is otherwise
deferred).

`FROM <source>` is deliberately where a future key-space clause slots in —
`GROUP BY <cols>` for the aggregate case, `JOIN <other> ON <cond>
[INNER|LEFT|RIGHT]` for cross-join — without changing the statement's outer
shape. `JOIN` is still only recognized and rejected by name (cross-join
remains out of scope). Relationship paths (`a.b`) are likewise still
rejected by name, ahead of the relationship feature landing.

`GROUP BY <col1>[, <col2>...]` itself has landed (issue #11's groundwork):
it's parsed in this same reserved slot, producing a `KeySpace::Aggregate`
whose calculated fields may reference a grouping column directly or wrap any
other numeric column in exactly one of `SUM`, `MIN`, `MAX`, or `AVG` — the
same paired grammar-plus-evaluator bar every other addition here meets.
**Aggregate function-call spelling is now settled**: it's the same
`func(args)` call syntax general function calls already use, just resolved
against a separate aggregate-function registry only reachable inside a
`GROUP BY` definition. `COUNT` is deliberately excluded — it's recognized by
name (inside or outside an aggregate definition) only to give a specific "not
yet implemented" error, the same reservation-by-name pattern `JOIN` uses.
Side-qualification for cross-joins (`a.column` vs. some other qualifier)
remains genuinely open, since no join syntax is parsed at all yet.

## Growth policy

We ship the stable core first and add functions and operators incrementally,
driven by community feedback. The bar for admitting one: (a) **semantics
identical to a stable PostgreSQL 15+ operator/function**; (b)
**immutable** per [transforms](../transforms.md#calculated-fields); and (c) a
re-implemented evaluator. Anything volatile, session- or collation-dependent
stays out.

Undecided:

* Cross-join side-qualification syntax (`JOIN` is still only recognized and
  rejected by name, not parsed). Aggregate function-call spelling and general
  non-aggregate function-call syntax are both settled — see above. `COUNT` is
  a deliberate exclusion, not an open question: it's rejected by name with the
  same "not yet implemented" spelling regardless of key-space.
* Whether the grammar and its stored schema are versioned independently of the
  transform-redefinition scheme (see [open-questions](../open-questions.md)).
* Operator precedence: the parser is currently flat left-associative with no
  precedence table. Safe today only because every Numeric-returning operator
  (`+`) outranks the sole Boolean-returning one (`>`) in the type lattice, so
  any regrouping that would produce a different result also fails
  type-checking rather than silently computing the wrong answer. This
  invariant must be re-verified before a second Boolean- or Numeric-returning
  operator at a different precedence tier is added.
