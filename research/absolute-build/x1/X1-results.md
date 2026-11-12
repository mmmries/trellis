# X1 results: correctness model of the absolute build (#1917)

Research only. Nothing here merges. Spec: [`../DESIGN.md`](../DESIGN.md), X1.

## Verdict

| id | expectation | hand-driven scenarios | soak |
|---|---|---|---|
| T0 | exact | exact in all 20 | 60 runs, 0 divergent, 0 deadlocks |
| **A** | exact, no deadlock | exact in all 20, no deadlock | 165 runs, 0 divergent, 0 deadlocks |
| A-gc | wrong | **wrong** (s3, s15) | caught in 1 of 30 runs (one checkpoint) |
| A-2pass | deadlocks | **deadlock** (s5b) | deadlock in 30 of 30 runs (90 page + 24 build victims); 0 divergent |
| A-noenum | wrong | **wrong** (s4a, s4b, s4bn, s4c) | caught in 10 of 30 runs |
| **B** | exact, no deadlock | exact in all 20, no deadlock | 165 runs, 0 divergent, 0 deadlocks |
| N | wrong | **wrong** (s1, s1n, s3, s8, s8c, s14, s15) | caught in 30 of 30 runs |
| T0-early | wrong | **wrong** (s1, s1n, s3, s8, s8c, s14, s15) | caught in 29 of 30 runs |
| B-early (added) | wrong | **wrong** (same as N) | caught in 27 of 30 runs |

No counterexample to A or B was found. Every negative control was observed failing in at least one
deterministic scenario, so the harness has teeth for every rule in the table. A needs nothing
beyond DESIGN.md's GC rule, but that rule has to be stated more precisely (below).

## Method

Real Postgres 18.6 (scratch cluster, `fsync=off`, `deadlock_timeout=200ms`), Python 3 + psycopg 3.
Files in this directory:

- `model.py`: schema, page, GC, the nine build variants, the check.
- `scenarios.py`: 20 deterministic scenarios × 9 variants (`python3 scenarios.py [-v] [names]`).
  Output: `scn_final.txt`; re-run twice (`scn_rep2.txt`, `scn_rep3.txt`) with identical tables.
- `soak.py`, `run_soak.sh`, `summarize.py`: the soak matrix; output `soak.jsonl`, `soak_matrix.log`.

The model:

- `src(id, g nullable, v)`; an `AFTER` row trigger writes every change to `ring(pos, txid xid8, old_g,
  old_v, has_old, new_g, new_v, has_new)` inside the writer's transaction.
- `h(g, n, s numeric, basis pg_snapshot, stamp)`, `unique nulls not distinct (g)`. Zero = `n = 0 and
  s = 0`.
- Page = one transaction: claim a batch `for update skip locked` (asc, desc or random order), stage
  sides summed per (g, txid), [B: `pg_advisory_xact_lock_shared(gate)`], lock statement (`insert …
  order by g on conflict (g) do update set n = excluded.n where false`), apply statement (adds each
  (g, txid) sum unless `pg_visible_in_snapshot(txid, basis)`), delete the claimed ring rows, commit.
- Every absolute write stores `basis = pg_current_snapshot()` and `stamp = last_value of the ring
  sequence`, both read in the statement that reads the source.
- Enumeration (R8) = `src` groups ∪ `old_g` of ring rows ∪ `h` groups, read in the build's snapshot
  (T0: in its own statement before the lock statement).
- Builds, all one `READ COMMITTED` transaction:
  - **T0**: enumerate; lock statement over the enumeration; one statement: snapshot, `GROUP BY`,
    update every enumerated row.
  - **T0-early**: snapshot and `GROUP BY` first, then the lock statement, then the update (R1 broken).
  - **A**: `pg_advisory_xact_lock(gc_key)`; step 1 = one statement: snapshot S1, stamp, enumeration,
    `GROUP BY`, `insert … order by g on conflict (g) do update … where false returning g`;
    stragglers = enumerated − returned; step 2 (if any) = snapshot S2, `GROUP BY` over the
    stragglers, update.
  - **A-gc**: A without the GC lock. **A-noenum**: A enumerating `select distinct g from src` only.
    **A-2pass**: step 1 with `on conflict do nothing`, then `select … where g = any(stragglers)
    order by g for update`, then step 2.
  - **B**: `pg_advisory_xact_lock(gate)`, then one statement: snapshot, enumeration, `GROUP BY`,
    `insert … order by g on conflict (g) do update set <absolute>`. Pages take the gate shared.
  - **N**: B's statement with no gate.
  - **B-early** (added control): B with the gate taken inside the statement, after its snapshot.
    It checks that B's rule "the snapshot comes after the gate is granted" has teeth.
- Stall hooks: `x1_hook(tag, g)` inside the build and page statements waits on an advisory lock the
  harness holds when `tag:g` is listed in `x1.hooks`. Points: `read` (after the build's snapshot,
  before its first write; for T0-early between its snapshot statement and its lock statement),
  `ins:g` / `lock:g` / `plock:g` (per row, before that row of the build's insert / T0's lock
  statement / a page's lock statement), `strag`, `pass2`, `precommit`. Row order inside a statement is
  forced with `order by g offset 0` subqueries pulled row by row by the insert.

### "GC skips a definition under a build" (A), made race-free

The build takes `pg_advisory_xact_lock(gc_key)` (exclusive) in a statement **before** S1 and holds it
to commit. Each GC transaction first takes `pg_try_advisory_xact_lock_shared(gc_key)` and collects
nothing if it fails. A GC transaction that already holds the shared lock makes the build wait
(s15 checks this: the build is seen waiting on the GC key, and its S1 comes after the GC's delete
commits). Locks are released after the commit is visible, so no GC delete commits after S1.

Residual windows the real engine has to close the same way:

- Every remover of hidden rows counts, not only the zero-group GC. The engine's other one is the
  source-`TRUNCATE` page's `truncate` of the hidden table. s18 checks it can't land between S1 and
  commit: the build's statement takes `ROW EXCLUSIVE` on `h` before its execution snapshot, so (a) a
  truncate already holding the lock makes the build wait and every basis the build writes sees
  the truncate, (b) a truncate after S1 waits for the build's commit. That relies on Postgres taking
  relation locks before the portal's snapshot (true for the extended protocol psycopg uses; the
  engine uses it too, but X3 should keep a test of it).
- The engine already has this mechanism for the 1-1 ledger (`retire.rs` `NOT_BUILDING_SQL`: GC batch
  takes the definition row `for key share nowait` and skips `backfilling`/`build`; the build's start
  takes it `for update` and commits before any snapshot). The zero-group GC (`collect_zero_groups`)
  explicitly does **not** skip today ("a build or recompute locks its groups before its snapshot").
  Under A it must, and X3 must check that every path that runs `group_basis::rebuild` (fresh build,
  resume, `request_backfill`, capture re-install, automatic rebuild, upstream rebuild, each
  partition-leaf transaction) is "under a build" by that predicate from before its S1 until its
  commit.

### The drained prefix, computed soundly

The GC's prefix (R5) must not count a ring position held by an uncommitted writer as drained. The
model uses: read `L` = last assigned position; then, in a later statement of the same transaction,
`X = pg_current_xact_id()`. Every position ≤ L belongs to a transaction whose xid was assigned
before its `nextval`, so its xid < X. In a later pass, once `pg_snapshot_xmin(pg_current_snapshot())
> X`, all of them have finished, and `prefix = least(L, min(visible ring pos) − 1)` is a true,
monotone fact. Two findings on the way:

- **`pg_snapshot_xmax` is not a bound on assigned xids.** It is `latestCompletedXid + 1`; an
  in-progress writer's xid can equal it and is then absent from `xip`. A first version of the
  model used it and s11 caught the bug (wrong in every variant). Anything in the engine that
  reasons "every xid below `xmax(σ)` is finished or in `xip(σ)`" is wrong; use an xid assigned
  after the read, or `xmin` alone.
- **s11n** shows the unsound alternative fails: a prefix read while a writer's ring row is
  invisible (ring looks empty, prefix = last assigned position), then a build stamps a zero group
  with that position, then the delete runs in a later statement with the stale prefix: the zero row
  with a basis that saw the writer's undrained delete is collected and the page applies the delete
  to a fresh row (−1). The engine's segment-based prefix is a different mechanism; this only says
  the model's GC is not the weak link.

## Scenarios (pass/fail per variant)

Every scenario runs on a fresh database, asserts its stall points were reached, drains the ring,
and compares `h` with `src` (both directions, as multisets). `wrong` = divergence; `+deadlock` =
a 40P01 was raised (the victim is retried and the end state is still checked).

| scenario | T0 | A | A-gc | A-2pass | A-noenum | B | N | T0-early | B-early |
|---|---|---|---|---|---|---|---|---|---|
| s1_absent_group_post_s1 | exact | exact | exact | exact | exact | exact | wrong | wrong | wrong |
| s1n_absent_null_group | exact | exact | exact | exact | exact | exact | wrong | wrong | wrong |
| s2_blocked_page_refuses_seen | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| s3_gc_counterexample | exact | exact | wrong | exact | exact | exact | wrong | wrong | wrong |
| s4a_vacated_drained | exact | exact | exact | exact | wrong | exact | exact | exact | exact |
| s4b_vacated_undrained | exact | exact | exact | exact | wrong | exact | exact | exact | exact |
| s4bn_vacated_null | exact | exact | exact | exact | wrong | exact | exact | exact | exact |
| s4c_vacated_drain_in_flight | exact | exact | exact | exact | wrong | exact | exact | exact | exact |
| s5a_key_ordered_wait | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| s5b_two_pass_deadlock | exact | exact | exact | exact+deadlock | exact | exact | exact | exact | exact |
| s7_build_rollback | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| s8_rebuild_in_place | exact | exact | exact | exact | exact | exact | wrong | wrong | wrong |
| s8c_rebuild_corrupted | exact | exact | exact | exact | exact | exact | wrong | wrong | wrong |
| s9_page_claimed_before_s1 | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| s11_gc_prefix_soundness | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| s11n_gc_prefix_naive | wrong | wrong | wrong | wrong | exact | wrong | wrong | wrong | wrong |
| s14_commit_order_differs | exact | exact | exact | exact | exact | exact | wrong | wrong | wrong |
| s15_gc_mid_txn_when_build_starts | exact | exact | wrong | exact | exact | exact | wrong | wrong | wrong |
| s16_page_blocked_on_straggler | exact | exact | exact | exact | exact | exact | exact | exact | exact |
| s18_hidden_truncate_vs_s1 | exact | exact | exact | exact | exact | exact | exact | exact | exact |

What each one does:

- **s1 / s1n** (group 2 / NULL group): fresh build; build stalls after S1; a writer inserts into the
  absent group; a page applies it. A: the page inserts the row first, the build's insert conflicts,
  the group is a straggler and step 2 recomputes it from S2 (exact). N, T0-early, B-early overwrite
  it with S1's values and lose the delta. Under T0 and B the page waits for the build instead.
- **s2**: a page blocked on the build's uncommitted row (A: the inserted one) applies after the
  commit: the change S1 saw is refused, the later one applies once (checked: `h` = 3 after the page).
- **s3**: DESIGN.md's counterexample. A-gc wrong (the GC deletes the `+1 −1` zero row, the build
  inserts count 1); A exact (GC skipped).
- **s4a / s4b / s4bn / s4c**: the vacated group, fresh build (`h` emptied after the history was
  drained): the move out drained before S1 (`h(g) = −1`, only the hidden enumeration names it), undrained
  at S1 (only the ring's old image names it; also with a NULL group), and drained by a page that
  commits after S1 (ring row visible at S1, `h` row not). A-noenum wrong in all four.
- **s5a**: a page holds group 7 (uncommitted insert) and stalls before 9; every build waits on 7;
  no deadlock (key order).
- **s5b**: group 7 exists, 9 absent; the build has handled 7 and stalls before 9 (A-2pass: after
  pass 1 inserted 9); a page locks 7 and waits on 9. A-2pass: deadlock (the page was the victim).
  A in the same shape holds 7 and the page simply waits.
- **s7**: the build rolls back after its insert; the blocked page proceeds; exact both after the
  rollback and after a later build.
- **s8 / s8c**: rebuild in place (every row exists, bases set); c1 before S1, c2 + two moves after;
  s8c corrupts one row and deletes another first. A: every enumerated group is a straggler.
- **s9**: a page claimed a change S1 sees but reaches the group after the build inserted it: refused.
- **s11 / s11n**: GC prefix soundness (above).
- **s14**: the lower ring position commits after S1, the higher before: refusal is by visibility,
  not position.
- **s15**: the counterexample with a GC transaction already holding the skip lock when the build
  starts.
- **s16**: a page waits on a straggler locked by step 1; changes after S1 and after the lock are
  both refused under A (S2 saw them).
- **s18**: hidden-table `TRUNCATE` vs the build's snapshot (mechanism check, above).

Attempts to break A and B that came out exact: s5a, s7, s9, s14, s15, s16, s18 above, plus the
soak. I found no interleaving that breaks either.

## Soak

Logbook id **`20261112T123923-r-soak`** (outcome ok, 1460 s, 938,111 source rows written including seeds). The finish note says "estimate if aborted" because the runner always writes that label; the run was not aborted and the row count is exact.

570 runs of 20 s, 8 at a time, each on its own database: 4 writers (1–4 statements per
transaction: insert, update `v`, move to another group incl. NULL, delete; 10 % rollbacks; 15 % of
statements followed by a 10–150 ms hold so commit order differs from ring order), a source of about
8 rows over 6 groups + NULL with a hot group (groups empty and refill constantly), 2, 4 or 8 appliers
(batches of 1–20 claimed asc/desc/random, 0–30 ms pauses so a backlog builds), one GC every 5 ms, one
builder looping with 0–300 ms gaps: first build fresh over a source seeded without capture, then at
random in place (50 %), after corrupting `h` (25 %: 30 % of rows locked in key order, half perturbed,
half deleted) or fresh (25 %: `h` emptied). Jitter: the build sleeps 0–40 ms at `read`, `strag` and
`pass2` (rows "jitter 40"); rows "jitter 0" have none and are the fair lock-wait comparison. Every 2 s
and at the end the run quiesces (writers idle, builder idle after a successful build, ring drained) and
compares. "divergent runs" counts final / any checkpoint.

| variant | jitter ms | expect | runs | divergent runs (final / any checkpoint) | page deadlocks | build deadlocks | builds | stragglers / enumerated (fresh; in place; corrupted) | page lock wait p99 / max ms | harness errors |
|---|---|---|---|---|---|---|---|---|---|---|
| T0 | 40 | exact | 45 | 0 / 0 | 0 | 0 | 5422 | n/a | 33.89 (median run) / 45.81 | 0 |
| T0 | 0 | exact | 15 | 0 / 0 | 0 | 0 | 1998 | n/a | 1.16 (median run) / 13.7 | 0 |
| A | 40 | exact | 150 | 0 / 0 | 0 | 0 | 15977 | 4500/21253; 50070/50074; 21504/23999 | 28.64 (median run) / 46.1 | 0 |
| A | 0 | exact | 15 | 0 / 0 | 0 | 0 | 2001 | 86/2654; 5721/5728; 2861/3234 | 1.19 (median run) / 14.36 | 0 |
| B | 40 | exact | 150 | 0 / 0 | 0 | 0 | 17668 | n/a | 32.58 (median run) / 47.47 | 0 |
| B | 0 | exact | 15 | 0 / 0 | 0 | 0 | 2014 | n/a | 1.24 (median run) / 11.82 | 0 |
| A-gc | 40 | wrong | 30 | 0 / 1 | 0 | 0 | 3234 | 766/4306; 9908/9964; 4385/4943 | 29.08 (median run) / 42.72 | 0 |
| A-2pass | 40 | deadlock | 30 | 0 / 0 | 90 | 24 | 2843 | 887/3895; 8514/8514; 3975/4375 | 92.34 (median run) / 994.01 | 0 |
| A-noenum | 40 | wrong | 30 | 1 / 10 | 0 | 0 | 3191 | 890/4226; 8126/8127; 3606/4123 | 27.58 (median run) / 41.33 | 0 |
| N | 40 | wrong | 30 | 7 / 30 | 0 | 0 | 3565 | n/a | 1.14 (median run) / 15.85 | 0 |
| T0-early | 40 | wrong | 30 | 4 / 29 | 0 | 0 | 3543 | n/a | 1.13 (median run) / 7.58 | 0 |
| B-early | 40 | wrong | 30 | 9 / 27 | 0 | 0 | 3538 | n/a | 1.41 (median run) / 14.76 | 0 |

Readings:

- **A and B: 330 runs, 0 divergences, 0 deadlocks** (page or build), 0 harness errors, ~37,700
  builds between them. T0: 60 runs, clean.
- Controls: N, T0-early, B-early caught in nearly every run; A-noenum in a third; **A-2pass
  deadlocked in all 30 runs (114 deadlocks)**, never a divergence; **A-gc caught once in 30 runs** (a
  checkpoint showed the NULL group at count 1 above the source, the counterexample's signature; I did
  not trace that run's interleaving). The deterministic s3/s15 are the main evidence for the GC rule.
- Stragglers under A: in-place rebuilds 99.99 % of enumerated groups (two aggregates, as predicted);
  corrupted rebuilds ~90 %; fresh builds 21 % with jitter, **3 % without** (the groups pages touched
  in the window before the insert reached them).
- Page lock wait without jitter (p99 of the median run): T0 1.16 ms, A 1.19 ms, B 1.24 ms. On this
  tiny table every build is sub-millisecond, so the model cannot show A's shorter wait; that is for
  X2/X3. With jitter the waits are dominated by the injected sleeps.

## Rules A and B turned out to need (beyond DESIGN.md)

A:

1. **No removal of hidden rows between S1 and the build's commit, by anything.** DESIGN.md names
   the GC; the rule is general. The model enforces it with an exclusive lock taken before S1 that
   every remover checks (GC: shared try-lock; hidden truncate: the table lock, s18). The engine's
   zero-group GC must adopt the 1-1 ledger's skip (it does not today).
2. **The GC lock is taken in a statement before S1**, not inside step 1's statement (whose snapshot
   precedes anything it executes). Same pattern as B-early: a lock taken inside the snapshotting
   statement is taken too late.
3. **Step 1's enumeration must be read in S1 itself** (same statement), all three parts. A-noenum
   shows the ring and hidden parts are each needed (s4a needs hidden, s4b/s4c need the ring).
4. **Stragglers are "enumerated minus returned"**, including rows another transaction committed
   after S1 (the `on conflict` arbiter sees them although S1 does not); step 2 must use a fresh
   snapshot taken after step 1, not S1.

B:

1. **The snapshot must be taken after the gate is granted**, in a later statement (B-early shows
   the failure).
2. Pages take the gate before their lock statement and hold it to commit; the order of claim vs.
   gate does not matter (a page blocked at the gate refuses what S saw).
3. B needs no GC skip in the model: its write is absolute for every enumerated group, and pages are
   excluded for its whole span.

Both: the stamp read in the same statement as the snapshot, and a sound drained prefix (above).

## Where DESIGN.md is wrong or incomplete

- "**GC skips a definition under a build**" is necessary but too narrow as stated: the rule is "no
  hidden-row removal between S1 and commit", and the engine's existing skip mechanism is the
  ledger's, which the zero-group GC deliberately does not use today. Also: "the rule I4 already has
  for the 1-1 ledger" lives in `NOT_BUILDING_SQL`, which keys on `status = 'backfilling'` or `build`
  set; X3 must confirm every group rebuild path sets one of them before its S1.
- The **A-noenum** expectation needs a precondition DESIGN.md does not state: enumeration only
  matters when `h` is not consistent with the ring at S1 (a fresh build or resume over an existing
  source, or a repair of a wrong `h`). On a consistent `h` an untouched group is already right, so
  A-noenum is exact there. The model creates the inconsistency the way a fresh build/resume does
  (source rows that predate capture; `h` emptied while pages run).
- **T0's enumeration snapshot**: DESIGN.md's R8 argument ("a change is either undrained at S or its
  row is visible at S") is stated for A's S1. For T0 the enumeration is its own earlier snapshot E0;
  the same argument holds at E0 and groups first named after E0 need no absolute write. Exact in
  every scenario and soak run.
- "Pages wait less than today": in s1 a T0 page waits for the whole stalled read, an A page doesn't
  wait at all. In the soak, see the jitter-0 rows of the table.
- A rebuild in place under A makes every enumerated group a straggler (s8: all; soak in-place
  builds: 100 %), so it pays two aggregates, as DESIGN.md predicts. Fresh builds had few
  stragglers (soak table).

## Limits of the model

- One builder per database; no concurrent builds of one definition.
- No source `TRUNCATE` and truncate floor (only the hidden-table truncate's lock interaction, s18),
  no partitions (B's per-leaf gate question untested), no `MIN`/`MAX` extremes or `__xbasis`, no
  numeric scale maps, no `JOIN`, no image-less recompute pages, no target-mutation seam.
- Segments are positions: the ring is one row per change, the drained prefix a position.
- Pages and builds are correct SQL by construction; Postgres's own behaviour (EPQ under `READ
  COMMITTED`, `on conflict` waits, lock-then-snapshot order) is real, not modeled.
- The soak oracle runs at quiescent checkpoints (every 2 s) and at the end; a wrong state that a
  later build repaired before a checkpoint is not seen. The deterministic scenarios are the main
  evidence for each control.
- The harness's `h` resets and corruption take row locks in key order (an unordered `delete from h`
  deadlocked with pages: a harness artifact, fixed before the soak).
