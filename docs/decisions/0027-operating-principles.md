---
status: draft
date: 2026-11-11
deciders: Michael Ries
---

# Operating Principles

Five decisions shape most features that watch a source, a target or
Trellis's own state. Without one place that states them, each feature ADR
applies them again in a paragraph of its own, a contributor designing a new
check has to infer them from those paragraphs, and the paragraphs drift apart
as they are edited.

This ADR states them once, as numbered rules that other ADRs, docs and reviews
cite by number ("ADR-0027, rule 3"). Each rule is a constraint a design must
meet, not a description of one feature; where a feature ADR describes how it
meets a rule, that ADR stays the reference for the mechanism. Which conditions
Trellis refuses at define, and which ways a target can go wrong unnoticed, stay
in [Supported sources and targets](../transforms.md#supported-sources-and-targets)
and [known correctness gaps](../known-correctness-gaps.md).

## The rules

### 1. No design needs a superuser

Trellis runs as an ordinary role
([recommendations](../recommendations.md#one-trellis-role)), and no feature,
check or repair may need that role to be a superuser. Event triggers, which
would see DDL that fires no trigger, are therefore out
([ADR-0017](0017-partitioned-sources.md)), and so is packaging Trellis as an
extension ([ADR-0025](0025-schema-dumps-restore-definitions.md)). Capture runs
on statement triggers, which need ownership of the source (or membership in
its owning role) because `ENABLE ALWAYS` does, and nothing more
([ADR-0002](0002-async-data-flow.md)). The reasons: some hosts don't offer a
superuser at all, and where one exists, a superuser grant is too much to
explain and justify to the operators who would have to make it.

### 2. When Trellis can't vouch for a target, it pauses with a reason

When Trellis finds that a definition's target may be wrong, or that it can no
longer maintain the definition as written, it pauses the definition with a
`capture_failure` that names what it found and the remedy, and the pause holds
until an operator runs `RESUME` ([ADR-0014](0014-pause-and-drop-a-transform.md)).
It does not reinstall its capture, rebuild a target or resume a definition on
its own to get past the finding. The reason: Trellis shouldn't start an
expensive rebuild by itself, but it must tell the operator when it can no
longer guarantee a target's correctness. One rule for every disruption of
Trellis's capture is also simple to document and simple for operators to
understand: an operator who swaps a table or bulk-loads with triggers disabled
plans a `RESUME` afterwards.

The exception: when only data was missed and every definition reading the
table is still valid as written, the staging worker rebuilds the readers
itself, exactly as `Trellis::request_backfill` does, and pauses nobody. A
same-type `USING` rewrite of a read column, a stored generated column's
expression replaced, and a read column changed where every reader stays valid
are those cases; each is logged and counted (`capture_auto_rebuilds_total`)
([capture by triggers](../staging-and-claiming/01-capture-by-triggers.md#what-rebuilds-what-pauses-and-whats-missed)).
Partition maintenance under a partitioned source (a leaf attached, detached,
dropped or truncated) is the same kind of loss, and the staging worker
corrects each changed partition by itself
([ADR-0017](0017-partitioned-sources.md)). A pause there would interrupt an
operator for a loss Trellis can repair by itself, and a false positive costs
one rebuild rather than an operator's time.

A restore never starts a rebuild: a logical restore pauses every definition,
and a physical restore or `pg_upgrade` pauses and rebuilds nothing. A
condition of a whole process rather than a definition, such as another
release live on the instance, stands the process by instead of pausing
anything ([ADR-0023](0023-version-handover.md)).

### 3. A detector that pauses ships only after a false-positive study

Before Trellis pauses on a signal it reads (a catalog comparison, a
fingerprint, an index's validity), a study runs a matrix of routine
maintenance and migration operations against the signal: those the signal
could see, such as `VACUUM` variants, `CLUSTER`, `REINDEX CONCURRENTLY`,
pg_repack, the migrations frameworks generate, partition maintenance, a
restore and `pg_upgrade`. The maintainer
reviews the study and decides, even when it finds no false positive. A shape
that can't be detected without pausing on routine work is documented in
[known correctness gaps](../known-correctness-gaps.md) instead, and that is
always an acceptable outcome. The detector also meets rule 1.

The studies behind the key-rule pauses ([ADR-0026](0026-source-keys.md)) and
the partition-tree fingerprints ([ADR-0017](0017-partitioned-sources.md)) are
this rule applied: in the second, I found that adding the source's own
`pg_class.xmin` would fire on a `GRANT`, an owner change or an `ADD COLUMN`,
each forcing a full rebuild, so it is not part of the fingerprint.

The reason: a detector that pauses on false positives is worse than no
detector. A pause every couple of days on a busy database is unacceptable to
an operator, so how often operators are interrupted weighs more than
detecting every case. A pause that an operator's change to Trellis's own
capture triggers or functions trips is intended (rule 2), not a false
positive.

[OPEN: does the rule cover a detector that rebuilds by itself (rule 2's
exception) as well as one that pauses? In practice yes: the signals behind
rule 2's automatic rebuilds and ADR-0017's fingerprints were studied the same
way before they shipped. But the reason above is about interrupting
operators, and a false positive there costs a rebuild, not an interruption.
Recommendation: say "pauses or rebuilds", with that cost as the reason.]

### 4. Define-time validations hold for the definition's life

A rule worth refusing a definition for at define is one its target relies on
for as long as the definition exists. So `RESUME` re-runs define's validation
against the schema as it is then, before it re-types or rebuilds anything, and
while define would refuse the definition, the resume refuses with define's
error (or, when the staging worker finishes the resume, records it on the
`capture_failure`) and the definition stays paused
([ADR-0014](0014-pause-and-drop-a-transform.md)). A
resume can never be a way to run a definition define would turn away.

While a definition runs, a condition define refuses that appears later pauses
it where that is practical: the check is cheap, it has passed rule 3, and the
condition is worth a pause. An index that only speeds up `self_check` is not:
losing it slows an on-demand audit and pauses nothing. Every define-time
refusal states what happens if its condition appears after define, and the
remedy, in
[Supported sources and targets](../transforms.md#supported-sources-and-targets);
a way it can leave a target wrong unnoticed is also a
[known correctness gap](../known-correctness-gaps.md).

[OPEN: the decision behind this rule is "define-time validations are enforced
for a definition's lifetime wherever practical"; its reason (first sentence
of this rule) is drafted, not quoted. Recommendation: replace it with your own
words if they differ.]

### 5. Trellis does not guard what it owns against changes made outside it

Trellis is the only writer of what it owns: its targets, the tables it keeps
beside them, and its own schema (the catalog and the ring). It does not
prevent, watch for or repair rows written into them, or a change to its
catalog, made by anything else; it trusts what it finds there. A change that
breaks Trellis's own writes (a target dropped, one of its columns renamed)
fails the next drain or pauses like any other failure, and `self_check`
reports wrong rows in a target or a hidden table like any other divergence. Applying this is the operator's
side of the contract: grant application roles `SELECT` on targets and nothing
more ([recommendations](../recommendations.md#application-roles-read-targets-and-nothing-else),
[transforms](../transforms.md#target-tables-are-trellis-owned)). The rule is a
precondition of every guarantee in
[known correctness gaps](../known-correctness-gaps.md#preconditions), and the
entry for such writes there says what `self_check` reports and how each kind of
target is repaired. Trellis designs no mechanism for, and spends no work on, a
misbehaviour reachable only through such a write.

Trellis's capture triggers and functions are the exception: they sit on the
application's tables, where routine operations (a bulk load, a fixture load, a
table swap) disturb them, so the capture pass detects a change to them and
pauses under rule 2.

[OPEN: no reason not to guard is written down anywhere: the record only keeps
writes into what Trellis owns an operator rule and parks the guards (a guard
trigger on every write path, role separation, an identity record for
Trellis's tables) until users need enforcement. Recommendation: state your
reason, for example the cost a guard adds against a misuse that `SELECT`-only
grants already prevent.]

## Example

An operator loads a backfill file into `orders` with
`ALTER TABLE orders DISABLE TRIGGER ALL`, `COPY`, then
`ALTER TABLE orders ENABLE TRIGGER ALL`. The last statement leaves Trellis's
capture triggers enabled in `ORIGIN` mode, not `ENABLE ALWAYS`, so the next
capture pass pauses every definition that reads `orders` with a
`capture_failure` of kind `disruption` saying that changes made meanwhile may
be missing (rule 2); it reinstalls nothing. `RESUME` re-validates each
definition against `orders` as it is now (rule 4), and the next pass
reinstalls capture and rebuilds. Had the operator instead rewritten a `text`
column a definition reads in place,
`ALTER TABLE orders ALTER COLUMN note TYPE text USING trim(note)`,
only data would have been missed and every reader stays valid, so the pass
rebuilds the readers itself and pauses nobody.

## Consequences

* A shape only a superuser feature could see (DDL that fires no trigger, a
  rewrite logical decoding doesn't send either) is detected by comparing the
  catalog after the fact, under rule 3, or documented as a gap.
* Operators run `RESUME` after table swaps, trigger-disabled loads and fixture
  loads, and after any change that takes a definition outside what define
  accepts. A detector's false negatives are documented rather than traded for
  false positives.
* Every new define-time refusal brings the question of how its condition is
  caught later, answered in its Supported sources and targets entry, and a
  new pause brings a false-positive study before it ships.
* A hand write into a target or Trellis's schema can leave a target wrong
  with nothing paused; `self_check` reports wrong target and hidden-table
  rows, and nothing reports a change to the catalog.
