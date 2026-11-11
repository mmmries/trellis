---
status: accepted
date: 2026-10-09
deciders: Michael Ries
---

# Grammar Versions on Stored Definitions

Trellis stores each transform definition as text in its
catalog and parses it again on every read,
with the parser new statements go through
([ADR-0004](0004-transform-definition-grammar.md)). An `ALTER TRANSFORM` stores
the edited definition rendered back into the grammar. Validation, row
evaluation, the build's SQL and a `RESUME`'s re-validation each resolve names in
the parsed tree on their own.

So a stored definition means whatever the binary reading it says it means. If a
release changes what accepted text means (how a name resolves, what an
expression evaluates to), every stored definition containing that text silently
takes the new meaning on its next load, while its target still holds rows
computed under the old one.

This ADR settles how a stored definition keeps its meaning across grammar
changes. The definition version of
[ADR-0015](0015-transform-redefinition.md) counts edits to one definition, not
changes to the language, and the catalog's ordered schema migrations version
the catalog's own schema; both are out of scope.
Which way the expression grammar may change, toward Postgres's, is
[ADR-0022](0022-grammar-mirrors-postgres.md)'s.

## Decisions

### Every stored definition carries a grammar version, stamped when its text is written

Each transform definition stores an integer `grammar_version`
beside its text, written whenever the text is: at define,
`ALTER TRANSFORM` and rewrite. A pause, resume or rebuild writes neither. The
binary has a current version, and grammar 1 is the grammar when the marker is
introduced. One counter covers the whole grammar, since one parser reads every
statement.

Because only a write stamps it, reading a definition can never move it to rules
it wasn't written under.

### Text is stamped with the oldest version that reads it the same

The version stamped is not always the current one: it is the oldest version in
the binary's support window whose reading of the text equals the current
reading, the same tree after that version's compatibility rule. A definition
whose text an older grammar reads identically can then still be run by a
release that reads only that older grammar, so a definition's grammar version,
like its floor ([ADR-0024](0024-compatibility-floors-and-rollback.md)), comes
from what it uses, not from the release that wrote it. A definition that uses
a newer grammar's syntax, or that the older grammar would read differently,
gets the newer version.

### A change in meaning bumps the version; additive syntax and refusals don't

A change bumps the version when some text the current version accepts stays
accepted but would parse, resolve or evaluate differently. A new keyword that
was a valid identifier, and still reads where the identifier did, is such a
change. A change that only accepts text the current version refuses, such as a
new function under
[ADR-0004's growth policy](0004-transform-definition-grammar.md#growth-policy),
needs no bump: every stored text keeps its meaning. It raises the definition
floor of text that uses it ([ADR-0024](0024-compatibility-floors-and-rollback.md)),
so an older binary pauses such a definition without parsing it. Parsing it
could misread it: a grammar without the bare `NULL` literal reads
`COALESCE(amount, NULL)` as a read of a column named `"NULL"`.

A change that refuses text Postgres rejects, or reads differently
([ADR-0022](0022-grammar-mirrors-postgres.md)), needs no bump either. No stored
definition takes a new meaning: one whose text the release refuses is paused
when the binary attaches, its `capture_failure` naming the refusal and the fix
(drop the transform and define it again), and its target keeps the rows it
has. The release notes list the forms such a release newly refuses.

A bump that retires a meaning gives it a spelling in the new grammar, so every
older text can be translated. A bump ships only in a release semver allows to
break compatibility (a 0.x minor before 1.0, a major after).

[OPEN: whether a fix that makes evaluation match what the documentation already
says is a change in meaning. Recommendation: no; it ships with the definitions
to rebuild, since a compatibility rule would keep the wrong values and a rewrite
couldn't spell them.]

### Loading translates older text into the current grammar, in one place

Every read of stored transform text goes through one entry point that takes the
text and its version and returns the definition in the current grammar's terms:

- **Current version:** parses as today.
- **Older, inside the support window:** parses under that version's rules, and
  its compatibility rule translates the tree into one that means the same under
  the current rules.
- **Older, outside the window:** not read. The load fails and the transform
  pauses, its `capture_failure` naming the stored version, the oldest version
  this binary reads, and the fix: drop the transform and define it again.
  `RESUME` refuses; `DROP TRANSFORM` works without reading the text.
- **Newer than the binary knows:** not read. The transform pauses, its
  `capture_failure` naming the stored version and the newest this binary
  reads, with the remedy of a definition above the running version
  ([ADR-0024](0024-compatibility-floors-and-rollback.md)): roll forward to a
  release that reads it, then `RESUME` the transform or drop it there. The
  other definitions keep running.
- **Text that fails to parse at a known version:** refused, and the definition
  is neither read nor written. This is how a load outside an attach sees text
  this release refuses; drop the transform and define it again.

Nothing after the entry point sees a version. Names are resolved in several
places after the parse; translating once keeps every one of them on the current
rules, lets an older definition pass a resume's re-validation, and gives
rewriting the same code.

Attaching (`migrate`) checks every stored definition's version before anything
else runs. It pauses each one newer than this binary reads, each one outside
the window, and each one inside it whose text this release refuses. A paused
definition of any of these kinds reads nothing until it is dropped, or resumed
by a binary that reads it, so the rest of the instance runs beside it. While a
process of another version is live, the attach pauses nothing: it refuses
instead when it would
pause something ([ADR-0023](0023-version-handover.md)). The load-time check
stands on its own, since a process can connect without attaching and a newer
binary can write after the attach.

Too old and too new both pause, each with its own fix in the reason. Pausing
the one definition rather than refusing the attach keeps every other
definition running after a rollback.

### Rewriting moves a definition to the current grammar without a rebuild

A client can rewrite a stored definition's text to the current version. Trellis
renders the loaded definition in the current grammar, loads the new text, and
refuses the rewrite unless the two trees match. Matching trees mean the same
target columns, types and values, so the target's rows stay correct: a rewrite
needs no rebuild and doesn't bump the definition version. An `ALTER TRANSFORM`
of an older-version definition is a rewrite plus the edit.

### Each major version reads the grammar versions of the previous two

*Each major version reads every grammar version written by the previous two
major versions.* Before 1.0, each 0.x minor counts as a major: 0.9 reads what
0.7 and 0.8 wrote, and 1.0 reads what 0.8 and 0.9 wrote. An operator can skip
one major in an upgrade, and a binary carries compatibility rules for at most
three majors' versions. A definition that is never rewritten leaves the window
after two majors, which is why rewriting ships with every bump.

### Each bump ships its compatibility rule and fixtures

A bump brings the previous version's compatibility rule (its parser rules and
the tree translation that loading and rewriting share), and stored fixtures at
every version still in the window. How to translate a particular change is
decided with that change.

## Example

Grammar 1 kept an unquoted name's case and had no quoted identifiers, so

```text
TRANSFORM cust_totals FROM invoices GROUP BY customer_id WITHOUT SOURCE INDEX
SELECT SUM(Amount) AS total
```

read the column `"Amount"`. Grammar 2 reads identifiers as Postgres does
([ADR-0022](0022-grammar-mirrors-postgres.md)): the same text reads `amount`.
Accepted text now resolves differently, so this is a bump. Without a
compatibility rule, a stored definition over a table midway through a rename
(`"Amount"` beside a new `amount` of the same type) would load, validate and
resume cleanly while its target mixed the two columns.

Grammar 1's compatibility rule reads its text as if every name were quoted:
lexing without folding gives the tree grammar 1 gave, whose names are the
catalog's exact names. The one name grammar 1 folded, a table alias, is
lower-cased. So every stored grammar-1 definition keeps reading `"Amount"`, and
none pauses. A rewrite or `ALTER TRANSFORM` renders that tree in grammar 2 as
`SUM("Amount")`, which loads to the same tree. Grammar 1 had no `TRUE`,
`FALSE` or `NULL` literal: a bare one, in any case, was a name, so the rule
reads it as one, quoted (`COALESCE(null, a)` reads the column `"null"`). Text
with no quoted name, no upper-case letter in a name and no such literal, such
as `SUM(amount)`, reads the same in both, so it is stamped grammar 1 whichever
release writes it.

## Consequences

- A meaning change to the grammar costs a breaking release, a version bump, a
  compatibility rule for the version it replaces, and fixtures, all carried for
  two majors.
- A refusal toward Postgres costs no bump, but a stored definition that uses
  the refused form pauses at the upgrade until its operator redefines it.
- Upgrades may skip one major. Skipping more without rewriting in between pauses
  the definitions written before the window.
- A rollback across a grammar change pauses the definitions whose text only
  the newer grammar reads that way rather than running them re-interpreted,
  and the rest keep running ([ADR-0024](0024-compatibility-floors-and-rollback.md)). An older
  binary never runs beside a newer one
  ([ADR-0023](0023-version-handover.md)).
