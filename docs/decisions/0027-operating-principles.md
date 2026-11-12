---
status: draft
date: 2026-11-11
deciders: Michael Ries
---

# Operating Principles

Five decisions shape every feature that watches a source, a target or
Trellis's own state. Without one statement of them, each feature ADR restates
them and the copies drift. This ADR states them once, as numbered rules to cite
("ADR-0027, rule 3"). Mechanisms stay in the feature ADRs and stage docs.

## 1. No design needs a superuser

Trellis runs as an ordinary role
([recommendations](../recommendations.md#one-trellis-role)). Some hosts don't
offer a superuser, and where one exists, the grant is too much to explain and
justify to operators. This rules out event triggers and packaging Trellis as an
extension.

## 2. When Trellis can't vouch for a target, it pauses with a reason

The pause names what Trellis found and the remedy, and holds until `RESUME`
([ADR-0014](0014-pause-and-drop-a-transform.md)). Trellis doesn't start an
expensive rebuild by itself, but it must tell the operator when it can no
longer guarantee correctness. One rule for every disruption of capture is also
simple to document and to understand.

The exception: when only data was missed and every reader is still valid as
written, Trellis rebuilds the readers itself and pauses nobody
([capture by triggers](../staging-and-claiming/01-capture-by-triggers.md#what-rebuilds-what-pauses-and-whats-missed),
[ADR-0017](0017-partitioned-sources.md)). A pause there would interrupt an
operator for a loss Trellis can repair.

This rules out reinstalling capture, rebuilding or resuming by itself to get
past a finding.

## 3. A detector that pauses ships only after a false-positive study

The study runs routine maintenance and migrations (`VACUUM` variants,
`CLUSTER`, `REINDEX CONCURRENTLY`, pg_repack, framework migrations, partition
maintenance, restores, `pg_upgrade`) against the signal, and the maintainer
decides on it. A detector that pauses on false positives is worse than no
detector. A pause caused by a change to Trellis's own capture is intended
(rule 2), not a false positive.

This rules out shipping a pause signal unstudied. A shape that can't be caught
without false positives is a [known correctness gap](../known-correctness-gaps.md)
instead.

## 4. Define-time validations hold for the definition's life

`RESUME` re-runs define's validation against the schema as it is then, and
refuses with define's error while define would. A condition define refuses
that appears while the definition runs pauses it, where that is practical.
The refusal tables of [transforms](../transforms.md#refusals) and
[expressions](../expressions.md#refusals) say what each refusal does after define.

This rules out a resume that runs a definition define would turn away.

## 5. Trellis does not guard what it owns against changes made outside it

Trellis is the only writer of its targets, the tables beside them and its own
schema. It doesn't prevent, watch for or repair other writes there, and it
trusts what it finds. This is a
[precondition](../known-correctness-gaps.md#preconditions) of its guarantees;
the operator's side is granting applications `SELECT` on targets and nothing
more. Capture triggers sit on application tables, so they fall under rule 2
instead.

This rules out designing for misbehaviour reachable only through such writes.

## Example

`ALTER TABLE orders DISABLE TRIGGER ALL`, a `COPY`, then `ENABLE TRIGGER ALL`
pauses every reader of `orders` (rule 2); `RESUME` re-validates (rule 4) and
rebuilds. A same-type `USING` rewrite of a read column instead rebuilds the
readers with no pause.

## Consequences

* Operators run `RESUME` after table swaps and trigger-disabled loads.
* A detector's false negatives are documented, not traded for false positives.
* A hand write into a target or Trellis's schema can leave a target wrong with
  nothing paused; `self_check` reports wrong target rows.
