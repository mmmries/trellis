---
status: draft
date: 2026-11-11
deciders: Michael Ries
---

# Operating Principles

Five decisions shape most features that watches a source, a target or
Trellis's own state, and until now none of them was written down as a
decision: each feature ADR applied them again in a paragraph of its own. A
contributor designing a new check had to infer them from those paragraphs, and
the paragraphs drift apart as they are edited.

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
on row triggers, which need ownership of the source (or membership in its
owning role) because `ENABLE ALWAYS` does, and nothing more
([ADR-0002](0002-async-data-flow.md)). The reason: some hosts don't offer a
superuser at all.

[OPEN: is there a second reason to state, such as the cost to an operator of
justifying a superuser grant for a background process? Recommendation: state
it only if it is yours; the hosting reason alone carries the rule.]

### 2. When Trellis can't vouch for a target, it pauses with a reason

When Trellis finds that a definition's target may be wrong, or that it can no
longer maintain the definition as written, it pauses the definition with a
`capture_failure` that names what it found and the remedy, and the pause holds
until an operator runs `RESUME` ([ADR-0014](0014-pause-and-drop-a-transform.md)).
It does not reinstall its capture, rebuild a target or resume a definition on
its own to get past the finding. A pause is loud and leaves the decision of
when to pay for a rebuild, and whether the change that caused it was meant, to
the operator, who usually made it: a table swap, a bulk load with triggers
disabled, a migration.

The one exception: when only data was missed and every definition reading the
table is still valid as written, the staging worker rebuilds the readers
itself, exactly as `Trellis::request_backfill` does, pausing nobody, and logs
and counts it (`capture_auto_rebuilds_total`). A same-type `USING` rewrite of a
read column, a stored generated column's expression replaced, and a read
column changed where every reader stays valid are those cases
([capture by triggers](../staging-and-claiming/01-capture-by-triggers.md#what-rebuilds-what-pauses-and-whats-missed)).
`RESUME` would run the same rebuild, so the pause would add an operator step
and change nothing.

A restore never starts a rebuild: a logical restore pauses every definition,
and a physical restore or `pg_upgrade` pauses and rebuilds nothing. A
condition of a whole process rather than a definition, such as another
release live on the instance, stands the process by instead of pausing
anything ([ADR-0023](0023-version-handover.md)).

[OPEN: no ADR states why a pause is preferred to an automatic rebuild; the
reason above (the operator decides when to pay for a rebuild, and usually made
the change) is drafted, not quoted. Recommendation: replace it with your own
words if they differ, for example that an automatic rebuild is expensive and
Trellis should not start one unasked, but must say when it can no longer
guarantee a target.]

### 3. A detector that pauses ships only after a false-positive study

Before Trellis pauses on a signal it reads (a catalog comparison, a
fingerprint, an index's validity), a study runs a matrix of routine
maintenance and migration operations against the signal: every `VACUUM`
variant, `CLUSTER`, `REINDEX` and `REINDEX CONCURRENTLY`, pg_repack, a catalog
freeze, the migrations frameworks generate, partition maintenance, a restore
and `pg_upgrade`, and whatever else the signal could see. The maintainer
reviews the study and decides, even when it finds no false positive. A shape
that can't be detected without pausing on routine work is documented in
[known correctness gaps](../known-correctness-gaps.md) instead, and that is
always an acceptable outcome. The detector also meets rule 1.

The studies behind the key-rule pauses ([ADR-0026](0026-source-keys.md)) and
the partition-tree fingerprints ([ADR-0017](0017-partitioned-sources.md)) are
this rule applied: in the second, I found that adding the source's own
`pg_class.xmin` would fire on a `GRANT`, an owner change or an `ADD COLUMN`,
each forcing a full rebuild, so it is not part of the fingerprint. A pause
that a routine operation trips sends an operator to `RESUME` and a full
rebuild for nothing.

[OPEN: does the rule cover a detector that rebuilds by itself (rule 2's
exception) as well as one that pauses? ADR-0017's fingerprints start
corrections, not pauses, and were studied the same way. Recommendation: yes, say "pauses or rebuilds".]

[OPEN: is a pause tripped by an operator changing Trellis's own capture
triggers or functions a false positive for the study's purposes? The capture
pass treats it as an intended pause. Recommendation: say it is intended, not a
false positive.]

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
it when a check for it is cheap and has passed rule 3. Where no such check exists, the condition is a documented gap
until the next `RESUME` catches it. Every define-time refusal states what
happens if its condition appears after define, and the remedy, in
[Supported sources and targets](../transforms.md#supported-sources-and-targets).

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

[OPEN: the reason not to guard is not written in any ADR. Recommendation:
state the cost a guard would add (a trigger on every target write path, or a
second role) against a misuse that `SELECT`-only grants already prevent.]

## Example

An operator loads a backfill file into `orders` with
`ALTER TABLE orders DISABLE TRIGGER ALL`, `COPY`, then
`ALTER TABLE orders ENABLE TRIGGER ALL`. The last statement leaves Trellis's
capture triggers enabled in `ORIGIN` mode, not `ENABLE ALWAYS`, so the next
capture pass pauses every definition that reads `orders` with a
`capture_failure` of kind `disruption` saying that changes made meanwhile may
be missing (rule 2); it reinstalls nothing. `RESUME` re-validates each
definition against `orders` as it is now (rule 4), and the next pass
reinstalls capture and rebuilds. Had the operator instead rewritten a column
in place, `ALTER TABLE orders ALTER COLUMN note TYPE text USING trim(note)`,
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
