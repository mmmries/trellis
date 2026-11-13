# The group basis: keeping an aggregate exact from deltas

← [Applying](05-apply-and-exactly-once-deltas.md) · up → [Staging and claiming](README.md)

How an aggregate target (and a `JOIN`'s `P` and `M` cells) is kept from signed
deltas with no per-row record. The rules R1 to R9 and Rx1 to Rx8, and why, are
in [ADR-0002](../decisions/0002-async-data-flow.md#aggregates-a-basis-per-group-kept-by-deltas);
column and statement detail is in `staging::group_basis`'s module doc.

## Where a group's state lives

The visible target holds the declared columns and the
`UNIQUE NULLS NOT DISTINCT` group identity, nothing else. A hidden table in the
catalog schema, `agg_<id>`, holds per group the row count `__n`, per distinct
argument its non-null count and its sum (`numeric` whatever the argument type),
the basis and stamp, and with `MIN`/`MAX` each extreme at the argument's type
under its own `__xbasis` and `__xstamp`. Readers get no grant on it and its
shape is internal; a drop removes it by definition id, and a resume drops and
recreates it. A write updates the hidden row, then the visible row (computed
from the partials and extremes), and skips the visible write when no value
changed.

## A page

1. **Stage.** One statement splits each ring row into sides (old image negated,
   new image; never folded per key, R2), evaluates the group and each argument
   on each (an expression over the image as the source's row type), and sums
   them per (group, `row_txid`, statement) into a temporary table, insertion
   sides' extremes apart from retraction sides' (Rx3). No lock is held yet.
2. **Lock (R1, I5).** One sorted statement inserts every group the sides and
   any image-less change name, and locks the ones that exist.
3. **Apply (R2, Rx2).** One later statement, whose `READ COMMITTED` snapshot
   sees every committed basis, adds each (group, transaction) sum to the
   partials if `__basis` doesn't see the transaction, to the extremes by
   `least`/`greatest` if `__xbasis` doesn't, both only above the truncate
   floor; upserts the visible row while `__n > 0` or deletes it; and returns
   per group the change it applied.
4. **Probe (Rx4, Rx5).** For each group whose retraction reached an extreme
   (or whose `NULL` extreme met a non-`NULL` retraction), one statement takes
   the snapshot and newest segment, reads each extreme as one index endpoint
   (`arg is not null`; a `NULL` group key matched as `NULL`, as the identity
   does), and stores them with `__xbasis` and `__xstamp`. It takes the
   source's `ACCESS SHARE` under `lock_timeout`; a timeout retries the page
   (I6, I7).
5. **Recompute.** An image-less change's groups (its key's current row's and
   its prior-image hint's) were locked in step 2 and are rewritten as below.

Each changed visible row reaches the target-mutation seam with its prior image.

## Absolute writes

One sorted lock statement, then one statement that takes the snapshot and
newest segment, runs the source's `GROUP BY` over the groups and replaces their
partials, extremes, bases, stamps and visible rows (R1, R5, R6, Rx6). A build
updates the hidden rows in the order they are stored
([W2](../decisions/0028-locking-and-writing.md#w2-a-bulk-write-writes-in-the-destinations-order)).
Pages keep applying, against the new bases once it commits. What each covers (R8):

- **Build or rebuild**, the plan job's whole build
  ([data flow](../data-flow.md#an-aggregates-or-a-joins-build)): the target's
  groups, the source's, and those undrained ring changes' old images name; over
  a partitioned source, one transaction per leaf and one for the rest.
- **Release** of a held key, in its own transaction: the groups
  `poison_held.agg_groups` names (all its parked images') and its current row's.
- **A page's `Recompute`**: step 5's groups, under the page's locks.
- **Partition correction**: the groups routed to the partition, including those
  an undrained page from before the DDL can create.

## `numeric` sums

The sum covers finite values; a `jsonb` map beside it counts the values off the
reference scale, per display scale (0 to 16383; none is negative), and each
`NaN`, `Infinity` and `-Infinity`, with no zero entries and `NULL` when empty.
The reference scale is the column's declared `s` (a negative `s` as 0) when the
hidden table is created, stored as the map column's comment, so a later retype
never changes a stored map's meaning; with no declared scale the map counts
every value. The count at the reference scale is the non-null count less the
map's counts. A `JOIN` product multiplies the counts by the match count
(`max(n, 1)` for `LEFT`). The visible `SUM` is `NULL` with no non-null value,
`NaN` or the infinity as Postgres's would be, else the finite sum at the largest
scale with a non-zero count; `AVG` divides it by the non-null count, as
`numeric_div` does. While a
`numeric(p,s)` argument's values are finite, no statement writes its map; a
build's shortcut for that case falls back under a savepoint on an off-scale
value, so it never changes a result.

## Zero groups (R4, Rx7)

An all-zero group has no visible row, but its hidden row stays until the
contiguous drained prefix reaches its stamp and `__xstamp`: a change its basis
or a probe saw may not have drained. The maintenance loop's tombstone pass
(`staging::retire::collect_tombstones`) collects such rows `for update skip
locked`, so it never waits on a page; a page that meets one being deleted waits
and inserts it afresh. A source `TRUNCATE` empties the hidden table, extremes
included (Rx8; [05](05-apply-and-exactly-once-deltas.md#aggregate-groups-the-group-basis)).
