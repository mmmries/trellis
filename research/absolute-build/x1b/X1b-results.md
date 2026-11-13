# X1b results: pre-enumeration, partition leaves, per-leaf B, resume, step 2 (#1917)

Research only. Nothing here merges. Extends [X1](../x1/X1-results.md) (model, harness, rules) with the questions
[X3](../x3/X3-results.md) raised. Everything marked *run* was executed on Postgres 18.6; *reasoned* is argument only.

## Q1. Pre-enumeration (partition leaves need it) — verdict: `A-preRHS` exact, `A-preRH` wrong

Run, hand-driven (`scenarios_b.py pre`, output `scn_pre.txt`). Shape: an earlier statement `E` of the same
transaction, after the GC-skip lock, reads the enumeration; step 1 at `S1` inserts-or-locks over
(pre-enumerated keys ∪ the source's groups at `S1`), the `GROUP BY` in the same statement as before.

| variant | E reads | GC-skip lock | verdict |
|---|---|---|---|
| `A-preRH` | hidden rows + ring old images | before E | **wrong** (p1, p1n, p2, p3, p5, p5d, p8) |
| **`A-preRHS`** | hidden rows + ring old images + **the source's groups at E** | before E | **exact** in all 11 |
| `A-preRHS-late` | as `A-preRHS` | after E, before S1 | exact in all 11 |
| `A-preRHS-nogc` (control) | as `A-preRHS` | none | **wrong** (p7 = X1's s3 in this form) |

The same split holds for the per-leaf forms (`L-A-preRH` wrong in the same 7, `L-A-preRHS` and
`L-A-preRHS-span` exact, `L-A-nogc` wrong in p7).

**The counterexample to `A-preRH`** (p1, exact interleaving): source row `r` in group 2, `h` empty (fresh build).
1. Build: GC-skip lock; `E` reads `h` (empty) and the ring (no old image of 2). Group 2 is not enumerated.
2. A writer moves `r` 2 → 7 and commits. Not drained.
3. Build: step 1 at `S1`: the source at `S1` has no row in 2; group 2 is not a candidate. Group 7 is inserted
   (count 1, basis `S1`). Commit.
4. A page drains the move: 7's side is refused (`S1` saw it); 2's side `−1` lands on a fresh NULL-basis row.
   `h(2) = −1`, the source has nothing in 2: **wrong**.

The same hole with the move drained before `S1` (p2), drained by a page that commits after `S1` (p3), the group
emptied by deletes (p5/p5d), and a move out and back (p8). Every one is a group that **has a source row at `E`**
and none at `S1`: only the source branch read at `E` names it.

**Why `A-preRHS` is exact** (reasoned, matches every scenario): a group the build must write is one that `h` gets
wrong without it, i.e. one whose history before the build is not in `h`. Such a group has a source row at `E`
(source branch), or an undrained change at `E` naming it as an old image (ring branch; under in-order retirement
also drained, unretired ones), or an `h` row at `E` (hidden branch). A group that has none of these at `E` has no
rows and no lost history at `E`: everything after `E` reaches it through pages, and step 1 then sees a page's row
by conflict (straggler) or inserts from `S1`, as in X1's A.

Interleavings tried against `A-preRHS` that came out exact: changes between `E` and `S1` undrained (p1, p5, p8),
drained before `S1` (p2, p5d), drained by a page committing after `S1` (p3); groups created, created-and-emptied
and created-undrained between `E` and `S1` (p4); a zero row collected by the GC between `E` and `S1` (p6, with
`A-preRHS-late` the GC really deleted it); the s3 shape after `S1` (p7); the build rolled back after step 1 with a
page blocked on its insert (p9).

**Rules** for the pre-enumerated form:

1. `E` must read the source's groups (in the leaf form: the leaf's rows' groups) as well as the hidden rows and the
   ring's old images (and the visible target, Q4). `S1`'s `GROUP BY` does not replace that branch: it misses groups
   that empty between `E` and `S1`.
2. The GC skip must be in force from before `S1` until commit, as in X1. Holding it from before `E` is not needed
   (`A-preRHS-late` exact; p6 shows a GC collecting a zero row `E` saw, harmlessly). Taking it before `E` is the
   simple choice.
3. Step 1's candidates are (pre-enumerated keys ∪ the `GROUP BY`'s groups at `S1`); stragglers are still
   "candidates minus returned", recomputed from a snapshot after step 1.

## The reset hole the X1 model had (found while building X1b)

Run (`n1`, `n1r`, table in Scenarios below). X1's ring deleted a change when a page drained
it. With `h` emptied (a fresh rebuild over a populated `h`, or a resume) after an **out-of-order** drain on one
row, nothing names the group: `c1` inserts `r` into 2 (undrained), `c2` moves `r` 2 → 7 (drained, then deleted
from the ring), `h` emptied, build: 2 is in neither the source, `h`, nor any old image; `c1` then drains `+1` into
a fresh row. **Wrong under every variant, T0 and B included.** X1's soak "fresh" mode could hit it; it never
did in 570 runs.

The engine reads drained, unretired ring rows too, and retires in position order (`c2` can't retire before `c1`
drains), which closes it. X1b models that (`RETAIN`: a page marks its rows drained; the GC retires drained rows
only up to its sound prefix; the enumeration reads both) and every X1b result below uses it. A ring new-image
branch (`A-newimg`) also closes it. **Rule:** the enumeration's ring branch must cover every change not yet
retired in order, not only undrained ones; the engine's "may name the groups of drained changes … which costs
only their recompute" (`rebuild_groups_sql`'s doc) is a necessity under a reset, not a cost.

## Q2. Partitioned source: per-leaf A — verdict: exact; per-leaf GC skip suffices

Model: groups route to leaves by `g % 3`, the NULL group to a remainder (leaf 3). A build is 4 transactions (leaf
0, 1, 2, remainder), each running A over only its routed groups (every branch of the enumeration and the
`GROUP BY` filtered by route), with pages, writers and the GC running throughout and between them.

Run, hand-driven (`scenarios_b.py leaf`, `scn_rest.txt`): `L-A`, `L-A-preRHS`, `L-A-preRHS-span`,
`L-A-preRHS-s2each` exact in all 10 leaf scenarios (and the pre ones above); controls fail where they should
(`L-A-nogc` l3a/p7, `L-A-preRH` the 7 pre cases). Soak below.

- **Does a leaf transaction touch or need another leaf's group?** No. By construction it locks/inserts only its
  own groups (soak counter "cross-leaf groups" = 0), and it is still exact: every enumeration branch and every
  page side is a function of the group, and the group decides the leaf, so R8's argument holds per leaf. A page
  whose change spans leaves (l5, l6) waits only on the row of the leaf being built and never deadlocks with the
  build (key order inside each leaf; the build holds one leaf at a time). *Reasoned caveat:* this needs the
  routing to be a function of the group (as the engine's scope predicates assume); with a group whose rows
  span leaves, per-leaf absolute values would be partials, not totals.
- **GC skip: per leaf transaction is enough.** l3b (GC collects zero rows of a built and of an unbuilt leaf
  between leaf transactions) and l3c (X1's s3 history on an unbuilt leaf's group, with the zero row collected
  between leaf 0's commit and leaf 1's start) are exact with the per-leaf lock (the GC really deleted rows:
  3 and 1). Why: the s3 hazard needs a removal after the owning transaction's `S1`; between leaf transactions
  the next leaf's `S1` has not been taken, so it sees both changes. Spanning (`L-A-preRHS-span`) is also exact,
  only longer-held (zero rows wait for the whole build). l3a (s3 inside leaf 1's transaction) fails without the
  skip (`L-A-nogc`).
- **Row moving built leaf → unbuilt leaf → back** (l2: drained between; l2u: undrained across leaf 1's build;
  l2r: unbuilt → built after leaf 1's `S1` and back after its insert): exact for every per-leaf A and B form.

Rule for the leaf form: each leaf transaction is a complete A build of its groups (GC skip from before its `S1`
to its commit, enumeration incl. the source/leaf branch at `E` when pre-enumerated, step 2 from a later snapshot).
Nothing has to span leaves.

## Q3. Candidate B per leaf — verdict: exact, no deadlock; per-leaf gates block far fewer pages

Pages take shared gates for every leaf their groups route to, in leaf order, before the lock statement
(`L-B`); or one per-definition gate that every leaf transaction takes exclusive (`L-Bdef`); `L-N` = no gate
(control).

Run: `L-B` and `L-Bdef` exact in every leaf/pre scenario, no deadlock; `L-N` wrong in l2r, l3a, l4, p7 (lost
deltas). l5 shows the difference: with leaf 1's transaction stalled after its read, a page touching groups of
leaves 0 and 1 waits under both forms, a page touching only leaf 0 runs under `L-B` and waits under `L-Bdef`;
under per-leaf A neither waits (the stalled transaction has not inserted yet). B needs no GC skip (as in X1).

Deadlock: with one builder at a time (the model), page gates are shared-only and a page takes all its gates
before any row lock, so no cycle exists (reasoned; 0 deadlocks in the soak). With **concurrent** leaf builders,
pages taking gates in opposite orders form a soft cycle (Postgres queues a shared request behind a waiting
exclusive one). Run (`gate_queue_check.py`, raw advisory locks, 2 pages + 2 builders): Postgres's deadlock
detector resolves it by reordering the wait queues, with no error, after `deadlock_timeout` (both pages waited
200 ms); in leaf order nobody waited. So leaf order is not needed for correctness, only to avoid that stall.

## Q4. Resume shape (visible target kept) — verdict: A with the target branch exact; `A-notarget` wrong

Model: `tgt(g, n, s)` maintained by pages (after apply) and builds (for every enumerated group, at the end of
the transaction) from `h`, visible only while `n > 0`. Resume = changes the ring never saw (session replica
role, a capture disruption), then `h` emptied with `tgt` kept, then the build. Enumeration gets a `tgt` branch
(read at `S1`, or at `E` when pre-enumerated).

Run (`scenarios_b.py resume`): `A-notarget+T` **wrong** in r1 and r2: a group whose last row vanished uncaptured
keeps its stale target row (r1: `tgt(6) = (1, 4)` with no source rows; r2 the same with pages running between
the reset and the build). Every form with the branch (`A-x`, `A-preRHS`, `A-preRH`, `L-A`, `L-A-preRHS`, `L-B`,
`B-x`) is exact and removes the target row (r1: `tgt` after the build is `(2, 1, 5), (3, 1, 1)`; 6 is gone).
r3 (resume after an out-of-order drain on one row) is exact with in-order retirement (the hole above).

Rules: the enumeration needs the visible target's groups whenever the hidden table can be emptied while the
target is kept, and the build must write (or delete) the visible row of every enumerated group, including zero
groups. `A-preRH+T` is exact in r1/r2 only because the target branch happens to name the groups there; it stays
wrong in Q1's cases.

## Q5. Step 2's read — verdict: exact in every form, also at 95 % stragglers

Forms: `A` (X1: full `GROUP BY` joined to the stragglers), `A-s2gb` (one `GROUP BY` filtered to the
stragglers), `A-s2probe` (one statement, one lateral probe per straggler, one snapshot `S2`), `A-s2each` (one
statement per straggler, each its own snapshot, each after the straggler's lock). The rule is the same for all:
the snapshot is taken after step 1 locked the group.

Run: all 20 X1 scenarios exact under each (table below; s16 is the one that targets step 2), q1 (200
pre-capture groups, pages touch 190 after `S1`, so 191 of 201 enumerated groups are stragglers, 95 %) exact
under every form incl. the leaf form, and the soak below. In-place rebuilds in the soak are ~100 % stragglers.

## Scenarios

### Pre-enumeration (Q1), `scenarios_b.py pre`

| scenario | A-x | A-preRH | A-preRHS | A-preRHS-late | A-preRHS-nogc | L-A | L-A-preRHS | L-A-preRH | L-A-preRHS-span | L-A-nogc | L-B | L-Bdef | L-N |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| p1_moveout_undrained | exact | wrong | exact | exact | exact | exact | exact | wrong | exact | exact | exact | exact | exact |
| p1n_moveout_undrained_null | exact | wrong | exact | exact | exact | exact | exact | wrong | exact | exact | exact | exact | exact |
| p2_moveout_drained_before_s1 | exact | wrong | exact | exact | exact | exact | exact | wrong | exact | exact | exact | exact | exact |
| p3_moveout_drain_in_flight | exact | wrong | exact | exact | exact | exact | exact | wrong | exact | exact | exact | exact | exact |
| p4_group_created_between | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| p5_emptied_between | exact | wrong | exact | exact | exact | exact | exact | wrong | exact | exact | exact | exact | exact |
| p5d_emptied_between_drained | exact | wrong | exact | exact | exact | exact | exact | wrong | exact | exact | exact | exact | exact |
| p6_gc_between_e_and_s1 | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| p7_s3_after_s1 | exact | exact | exact | exact | wrong | exact | exact | exact | exact | wrong | exact | exact | wrong |
| p8_move_out_and_back | exact | wrong | exact | exact | exact | exact | exact | wrong | exact | exact | exact | exact | exact |
| p9_build_rollback_with_pre | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact |

### Leaves and per-leaf B (Q2, Q3), `scenarios_b.py leaf`

| scenario | L-A | L-A-preRHS | L-A-preRHS-span | L-A-preRH | L-A-nogc | L-A-preRHS-s2each | L-B | L-Bdef | L-N |
|---|---|---|---|---|---|---|---|---|---|
| l1_pages_between_leaves | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| l2_cross_leaf_move_and_back | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| l2u_cross_leaf_move_undrained | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| l2r_reverse_move_during_leaf | exact | exact | exact | exact | exact | exact | exact | exact | wrong |
| l3a_s3_inside_leaf | exact | exact | exact | exact | wrong | exact | exact | exact | wrong |
| l3b_gc_between_leaves | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| l3c_s3_across_leaf_boundary | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| l4_remainder_null_absent | exact | exact | exact | exact | exact | exact | exact | exact | wrong |
| l5_page_spans_two_leaves | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| l6_page_holds_built_leaf_group | exact | exact | exact | exact | exact | exact | exact | exact | exact |

### Resume (Q4), `scenarios_b.py resume`

| scenario | A-x+T | A-notarget+T | A-preRHS+T | A-preRH+T | L-A+T | L-A-preRHS+T | L-B+T | B-x+T |
|---|---|---|---|---|---|---|---|---|
| r1_resume_lost_changes | exact | wrong | exact | exact | exact | exact | exact | exact |
| r2_resume_pages_running | exact | wrong | exact | exact | exact | exact | exact | exact |
| r3_resume_drained_sibling | exact | exact | exact | exact | exact | exact | exact | exact |

### Step 2 (Q5), `scenarios_b.py step2`

| scenario | A | A-x | A-s2gb | A-s2probe | A-s2each | A-preRHS | L-A-preRHS | L-A-preRHS-s2each |
|---|---|---|---|---|---|---|---|---|
| q1_many_stragglers | exact | exact | exact | exact | exact | exact | exact | exact |

### The reset hole, `scenarios_b.py hole`

`n1`: X1's ring (drained = deleted). `n1r`: in-order retirement.

| scenario | T0 | A | B | A-newimg | A-preRHS | L-A-preRHS | L-B |
|---|---|---|---|---|---|---|---|
| n1_reset_drained_sibling | wrong | wrong | wrong | exact | wrong | wrong | wrong |
| n1r_reset_drained_sibling_retain | exact | exact | exact | exact | exact | exact | exact |

### X1's 20 scenarios (Q6), in-order retirement on, all old and new non-leaf variants

`scn_orig_retain.txt`. The 9 original columns are identical to X1's table. With retirement off
(`scn_orig_regress.txt`, X1's exact model) the table is byte-identical to X1's `scn_final.txt`.
`A-preRH` is exact here: X1's scenarios make their changes before the build starts, not between `E` and `S1`.

| scenario | T0 | A | A-gc | A-2pass | A-noenum | B | N | T0-early | B-early | A-x | A-newimg | A-preRH | A-preRHS | A-preRHS-late | A-preRHS-nogc | A-s2gb | A-s2probe | A-s2each | B-x |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| s1_absent_group_post_s1 | exact | exact | exact | exact | exact | exact | wrong | wrong | wrong | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| s1n_absent_null_group | exact | exact | exact | exact | exact | exact | wrong | wrong | wrong | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| s2_blocked_page_refuses_seen | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| s3_gc_counterexample | exact | exact | wrong | exact | exact | exact | wrong | wrong | wrong | exact | exact | exact | exact | exact | wrong | exact | exact | exact | exact |
| s4a_vacated_drained | exact | exact | exact | exact | wrong | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| s4b_vacated_undrained | exact | exact | exact | exact | wrong | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| s4bn_vacated_null | exact | exact | exact | exact | wrong | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| s4c_vacated_drain_in_flight | exact | exact | exact | exact | wrong | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| s5a_key_ordered_wait | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| s5b_two_pass_deadlock | exact | exact | exact | exact+deadlock | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| s7_build_rollback | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| s8_rebuild_in_place | exact | exact | exact | exact | exact | exact | wrong | wrong | wrong | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| s8c_rebuild_corrupted | exact | exact | exact | exact | exact | exact | wrong | wrong | wrong | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| s9_page_claimed_before_s1 | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| s11_gc_prefix_soundness | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| s11n_gc_prefix_naive | wrong | wrong | wrong | wrong | exact | wrong | wrong | wrong | wrong | wrong | wrong | wrong | wrong | wrong | wrong | wrong | wrong | wrong | wrong |
| s14_commit_order_differs | exact | exact | exact | exact | exact | exact | wrong | wrong | wrong | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| s15_gc_mid_txn_when_build_starts | exact | exact | wrong | exact | exact | exact | wrong | wrong | wrong | exact | exact | exact | exact | exact | wrong | exact | exact | exact | exact |
| s16_page_blocked_on_straggler | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| s18_hidden_truncate_vs_s1 | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact | exact |

## Soak (Q6)

Logbook id **`20261112T183905-r-soak`** (FINISHED 20261112T183905-r-soak ok rows=1089472 at 2026-11-12T19:07:55-08:00). 20 s runs, 8 in parallel, appliers 2/4/8, X1's workload plus: in-order retirement on; leaf variants build as 4 transactions with 0-40 ms jitter between them; `+T` variants maintain the target and add a `resume` mode (1-3 uncaptured source changes, `h` emptied, `tgt` kept). Table generated by `summarize_b.py` .

| variant | jitter ms | expect | runs | divergent runs (final / any checkpoint) | page / build deadlocks | builds | stragglers / enumerated (fresh; in place; corrupted; resume) | page wait s per s (median run) | page lock wait p99 median run / max ms | cross-leaf groups | harness errors |
|---|---|---|---|---|---|---|---|---|---|---|---|
| A | 40 | exact | 15 | 0 / 0 | 0 / 0 | 1616 | 483/2805; 5197/5239; 2497/2900; 0/0 | 0.041 | 27.53 / 41.3 | 0 | 0 |
| A-preRHS | 40 | exact | 60 | 0 / 0 | 0 / 0 | 5718 | 2987/9731; 19501/19682; 8358/9444; 0/0 | 0.041 | 27.22 / 40.91 | 0 | 0 |
| A-preRHS-late | 40 | exact | 15 | 0 / 0 | 0 / 0 | 1459 | 739/2593; 4591/4663; 2374/2660; 0/0 | 0.040 | 26.53 / 38.97 | 0 | 0 |
| A-s2gb | 40 | exact | 15 | 0 / 0 | 0 / 0 | 1582 | 512/2685; 5088/5141; 2518/2889; 0/0 | 0.045 | 27.99 / 40.7 | 0 | 0 |
| A-s2probe | 40 | exact | 15 | 0 / 0 | 0 / 0 | 1598 | 509/2813; 5058/5107; 2575/2938; 0/0 | 0.044 | 29.46 / 39.4 | 0 | 0 |
| A-s2each | 40 | exact | 15 | 0 / 0 | 0 / 0 | 1627 | 568/2868; 5283/5346; 2412/2775; 0/0 | 0.049 | 29.53 / 42.81 | 0 | 0 |
| A-x+T | 40 | exact | 30 | 0 / 0 | 0 / 7 | 2856 | 715/3202; 7298/7382; 1718/1992; 3827/6532 | 0.044 | 28.57 / 41.77 | 0 | 0 |
| L-A | 40 | exact | 45 | 0 / 0 | 0 / 0 | 2371 | 1964/4116; 7264/7761; 3430/3951; 0/0 | 0.047 | 28.74 / 40.33 | 0 | 0 |
| L-A | 0 | exact | 15 | 0 / 0 | 0 / 0 | 1964 | 130/3432; 6107/6178; 3169/3665; 0/0 | 0.009 | 1.24 / 2.7 | 0 | 0 |
| L-A-preRHS | 40 | exact | 75 | 0 / 0 | 0 / 0 | 3202 | 3545/5669; 9787/10647; 4490/5079; 0/0 | 0.041 | 27.11 / 41.04 | 0 | 0 |
| L-A-preRHS | 0 | exact | 15 | 0 / 0 | 0 / 0 | 1959 | 164/3360; 6311/6372; 2966/3484; 0/0 | 0.009 | 1.09 / 2.76 | 0 | 0 |
| L-A-preRHS-span | 40 | exact | 30 | 0 / 0 | 0 / 0 | 1275 | 1471/2304; 4232/4267; 2028/2162; 0/0 | 0.043 | 27.94 / 40.48 | 0 | 0 |
| L-A-preRHS+T | 40 | exact | 30 | 0 / 0 | 0 / 4 | 1245 | 883/1416; 2893/3171; 680/758; 2160/2906 | 0.043 | 27.4 / 40.85 | 0 | 0 |
| L-B | 40 | exact | 45 | 0 / 0 | 0 / 0 | 2884 | n/a | 0.066 | 29.81 / 41.08 | 0 | 0 |
| L-B | 0 | exact | 15 | 0 / 0 | 0 / 0 | 1983 | n/a | 0.013 | 1.56 / 4.06 | 0 | 0 |
| L-B+T | 40 | exact | 15 | 0 / 0 | 0 / 1 | 893 | n/a | 0.060 | 30.0 / 45.36 | 0 | 0 |
| L-Bdef | 40 | exact | 30 | 0 / 0 | 0 / 0 | 1925 | n/a | 0.114 | 33.93 / 41.4 | 0 | 0 |
| L-Bdef | 0 | exact | 15 | 0 / 0 | 0 / 0 | 2013 | n/a | 0.011 | 1.31 / 4.23 | 0 | 0 |
| A-preRH | 40 | wrong | 30 | 0 / 1 | 0 / 0 | 2907 | 1507/5174; 9570/9650; 4229/4777; 0/0 | 0.042 | 28.66 / 42.25 | 0 | 0 |
| L-A-preRH | 40 | wrong | 30 | 0 / 0 | 0 / 0 | 1291 | 1465/2354; 3691/4006; 1957/2232; 0/0 | 0.044 | 28.13 / 40.89 | 0 | 0 |
| A-preRHS-nogc | 40 | wrong | 30 | 0 / 2 | 0 / 0 | 2910 | 1231/4998; 9323/9493; 4531/5157; 0/0 | 0.038 | 27.75 / 41.82 | 0 | 0 |
| L-A-nogc | 40 | wrong | 30 | 0 / 2 | 0 / 0 | 1589 | 1411/2948; 4561/4981; 2285/2629; 0/0 | 0.047 | 29.13 / 41.42 | 0 | 0 |
| A-notarget+T | 40 | wrong | 30 | 0 / 2 | 0 / 5 | 2843 | 583/3183; 7202/7271; 1717/1990; 3956/6572 | 0.046 | 30.33 / 41.91 | 0 | 0 |
| L-N | 40 | wrong | 30 | 16 / 30 | 0 / 0 | 1932 | n/a | 0.008 | 1.13 / 15.01 | 0 | 0 |

675 runs, 1,089,472 source rows, 1730 s. Readings:

- **Exact-expected variants: 495 runs, 0 divergent** (final or any checkpoint), **0 page deadlocks**, 0 harness
  errors, ~38,000 builds (leaf builds are 4 transactions each). Cross-leaf groups: 0.
- **Build deadlocks only in `+T` runs** (A-x+T 7, L-A-preRHS+T 4, A-notarget+T 5, L-B+T 1) and 0 in every other
  run. They are the harness: the resume step's uncaptured source writes run in the builder's connection and pick
  `src` rows unordered against the writers (the sampled deadlock reports name a tuple of `src`; writers deadlock
  among themselves the same way, hundreds per variant, as in X1). Not attributed row by row: the reports with only
  transaction waits are assumed to be the same cycle.
- Controls: `L-N` 30/30; `A-preRHS-nogc` 2/30, `L-A-nogc` 2/30, `A-notarget+T` 2/30, `A-preRH` 1/30 (checkpoints
  only); **`L-A-preRH` 0/30**: the soak never hit the E→S1 window for it. The deterministic scenarios (p1–p8) are
  the evidence for the pre-enumeration rule; the soak is weak there (6-group source, short window).
- Page waiting (pages blocked × time, "page wait s per s" = Σ page gate+lock wait / run time, median run). Jitter
  0 (builds sub-millisecond, the fair comparison of mechanism alone): L-A 0.009, L-A-preRHS 0.009, L-Bdef 0.011,
  L-B 0.013. With 40 ms jitter (stands in for a long `GROUP BY`: under A the read sleeps before any insert, under
  B inside the gate): **L-A-preRHS 0.041, L-A 0.047, L-B 0.066, L-Bdef 0.114**; L-N (no waiting on builds, wrong)
  0.008. Per-leaf gates cut B's page waiting by ~40 % against a definition gate; A stays lowest. Real numbers
  belong to X4/X5.
- Stragglers: in-place rebuilds ~99 % (two aggregates, as X1); fresh leaf builds 63 % with jitter (the jitter
  between leaf transactions lets pages reach unbuilt leaves first), 4–5 % without.

## Counterexamples found

- **To `A-preRH`** (and `L-A-preRH`): Q1, p1 — exact interleaving above. Expected; it is the reason for rule Q1.1.
- **To every variant, T0 and B included, in X1's ring model**: the reset hole (`n1`). Closed by reading drained,
  unretired ring rows with in-order retirement (the engine's design) or by a new-image branch.
- **None to `A-preRHS`, per-leaf A (`L-A`, `L-A-preRHS`, `-span`, `-s2each`), per-leaf B (`L-B`, `L-Bdef`), or A
  with the target branch**, in 27 new hand-driven scenarios plus X1's 20, and 495 soak runs.

## Every extra rule

Pre-enumerated form (leaves):
1. `E` reads the source's (leaf's) groups as well as hidden rows, ring old images and the visible target.
2. GC skip in force from before `S1` to commit (before `E` is not required; it is the simple choice).
3. Candidates = pre-enumerated ∪ the `GROUP BY`'s groups at `S1`; stragglers = candidates − returned, recomputed
   from a later snapshot.

Leaf form:
4. Each leaf transaction is a complete A (or B) build of its own groups, GC skip per leaf transaction. Nothing
   needs to span transactions. Requires routing to be a function of the group.
5. Per-leaf B: pages take every touched leaf's gate shared before the lock statement; leaf order only avoids a
   `deadlock_timeout` stall when leaf builds run concurrently.

Resume form:
6. The enumeration includes the visible target's groups; the build rewrites or removes the visible row of every
   enumerated group, zero groups included.

Any form that empties the hidden table (fresh rebuild over a populated table, resume):
7. The ring branch must cover every change not yet retired in order (drained, unretired rows included), or add
   new images. Drained-and-deleted out of order is unsound.

Step 2:
8. Any read form works (filtered `GROUP BY`, per-straggler probes in one statement, one statement per straggler)
   as long as each straggler's snapshot is taken after step 1 locked it; exact at 95 % stragglers.

## Limits of the model

X1's limits apply (one builder per database, no source `TRUNCATE`/truncate floor, no `MIN`/`MAX`, no JOIN, no
image-less recompute pages, tiny tables, oracle at quiescent checkpoints). Plus:

- Leaves are a routing function (`g % 3`) over one table, not real partitions: no cross-partition update
  mechanics (delete+insert in two relations), no per-leaf relation locks, no pinned settings (the reason the engine
  needs pre-enumeration is not modeled, only its consequence).
- One builder: concurrent leaf builds are covered only by the raw lock-queue check (`gate_queue_check.py`), not by
  the model.
- The target is a mirror of `h` written in the same transactions; no target-mutation seam, no user writes to it.
  "Resume" is modeled as uncaptured source changes + `h` emptied; the engine's real pause/resume path and its ring
  state at resume are not modeled.
- In-order retirement is modeled as "drained and ≤ the GC's sound prefix", not as segments.
- The soak is weak on the E→S1 window (`L-A-preRH` 0/30); the hand scenarios carry Q1.

## Files

`model.py` (X1's, plus `tgt`, routing, `RETAIN`, the generic builder `build_x` and its variants `XV`),
`scenarios.py` (X1's; s16's stall picks A's step-2 hook for the new A variants; `X1B_RETAIN=1`),
`scenarios_b.py` (new scenarios), `soak.py` (plan `x1b`, resume mode, page-wait metric), `summarize_b.py`,
`run_soak_b.sh`, `fill_soak.sh`, `gate_queue_check.py`. Outputs: `scn_orig_regress.txt`, `scn_orig_retain.txt`,
`scn_pre.txt`, `scn_rest.txt`, `smoke.jsonl`, `soak_b.jsonl`, `soak_b_matrix.log`, `run_soak_b.out`.

