# Handoff: finishing the #558 experiment battery

Written 2026-09-26 (evening, box time). The design thinking is done and posted; what is left is
running the rest of the battery, writing the numbers up in the same shape, and keeping the box
honest. Read this whole file before touching anything. `RESULTS.md` (this directory) is the
canonical write-up; every posted comment links into it by anchor.

## 1. Where things stand

| item | state | where |
|---|---|---|
| Exp 1–2 (invariants) | done, posted | #558 comments; `RESULTS.md` §1–2 |
| Exp 3 (ledger hot-path cost) | done, posted | `RESULTS.md` §3 |
| Exp 4 (relationship without projection) | done, posted; hot parent at the bar | `RESULTS.md` §4 |
| Exp 4b (to-side factored out of I3) | done, posted, **recommendation: keep it factored** | #558 comment 2026-09-26 ("Experiment 4b"), `RESULTS.md` §4b |
| Disk tier, cheap shapes (item 3) | done, posted with a noisy-box caveat | #558 comment ("Disk tier, item 3"), `RESULTS.md` §Disk tier; #565 comment (E1 disk) |
| #595 fence fix (item 2) | merged as PR #597; #598 filed for the truncate-barrier hole | upstream |
| Exp 5 (100M build under load, joint with #565) | **in progress**: scenario done (lane h), control at 10M done, ledger-writing build WIP (lane f) | §3 below |
| Exp 6 (generative) | waits on #557 | – |
| Side issues | #581 (fold O(n²) plan), #582 (`main` loses updates under churn), #598 | upstream |

The user owns #558 and decides; agents run experiments and post results as comments in the
style of the existing ones (short verdict line, method, table, bullets, link to `RESULTS.md`).

## 2. Branches, worktrees, ownership

All on the fork (`origin` = mmmries/trellis); issues and comments go upstream with
`GH_TOKEN=$SF_GH_TOKEN gh ... --repo salesforce-misc/trellis`. Never ship any of these branches
(`ship`/PR); they are experiments. Lanes a, b, e belong to the eng-manager session: never touch.

| lane | worktree | branch | what |
|---|---|---|---|
| c | `/home/mike/code/trellis-lane-c` | `exp/issue-558-ledger-experiments` | ledger prototype (`TRELLIS_EXP558_LEDGER=off|contrib|membership`), `rel-churn` probe, `experiments/issue-558/` (RESULTS, logs, tools, exp 1–2 scratch crate) |
| d | `/home/mike/code/trellis-lane-d` | `exp/issue-558-relchurn-main` | true-`main` control + the probe + fold nestloop-off; **control only** |
| g | `/home/mike/code/trellis-lane-g` | `exp/issue-558-factored` | lane c + the factored variant (`TRELLIS_EXP558_LEDGER=factored`), stacked on c |
| f | `/home/mike/code/trellis-lane-f` | `exp/issue-558-exp5-build` | ledger-writing chunked build (`TRELLIS_EXP558_BUILD=ledger`), stacked on c; WIP commit, 4/4 tests green, see §3a |
| h | `/home/mike/code/trellis-lane-h` | `exp/issue-558-exp5-bench` | `build-under-load` scenario + disk-tier columns + testkit `TRELLIS_TESTKIT_PG_OPTIONS`, stacked on c |
| – | `/home/mike/code/trellis-565` | `spike/565-trigger-capture` | **the other session's** spike (trigger capture). Read only. Coordinate on #565 before rebasing onto it. |

Logs for every run live in lane c under `experiments/issue-558/logs/` (tmpfs runs), `logs/disk/`
(NVMe runs), `logs/exp5/`. Commit logs with the write-up.

## 3. Experiment 5: what is left, in order

Spec (issue #558 body scenario 1 + the "Amendment … joint experiment" comment): a 100M-row
source, chunked build on 8 workers with a write load throughout, the definition applying from
the first chunk, no go-live re-read, converging to a from-scratch `GROUP BY` after the ring
drains; run on the #565 spike branch with the ledger Apply; disk-backed from the start with
`shared_buffers` below the working set and short checkpoints; report WAL MB/s, fsyncs/s,
p99 commit latency, checkpoint volume.

Why it is real work: today's aggregate build is ONE `GROUP BY` into a temp table (one row per
group; only the overwrite writes are chunked; one worker), and CDC for a `backfilling` target
is dropped by the `dependents_of` status gate (`defs/catalog.rs`) and recovered by the go-live
full re-read + orphan sweep. The control run shows the cost: at 10M rows the build takes 4.6 s
and going live takes 226 s (the re-read pushes 10M recompute rows through the ring and laps it).

### 3a. Finish the ledger-writing build (lane f)

State at handoff (commit "WIP (#558 exp 5)" on `exp/issue-558-exp5-build`, pushed): the design
below is implemented as described in the module doc of `trellis/src/staging/ledger_build.rs`
(read it first: it states the rules), and the four tests in
`trellis/tests/exp558_ledger_build.rs` pass (last run 2026-09-26 20:55: 4 passed, 14 s):
`a_ledger_build_on_eight_workers_matches_a_fresh_group_by`,
`a_ledger_build_under_concurrent_writes_converges_to_the_oracle`,
`changes_meet_their_chunks_in_every_order_and_land_exactly_once`,
`re_running_a_chunk_leaves_the_target_unchanged`. Not yet done: (a) the flag-off regression
run (`cargo test -p trellis --test backfill*` and the aggregate/relationship apply tests, flag
unset) to show today's path is untouched; (b) `cargo clippy -p trellis --tests -- -D warnings`
on the touched files; (c) any run above 200k rows. The implementer was stopped before its
final report, so treat its self-review as not done: read `ledger_build.rs` and the diff in
`apply_aggregate.rs` (+382 lines) with the rules below in hand.

Design as built: PK-range chunks over the source planned with `discover_pk_ranges` and enqueued as
Range chunks so all workers claim them; each chunk one fenced transaction that (1) reads the
keys in `(lo, hi]`, (2) locks their ledger entries sorted by `from_key`, (3) in one statement
reads `pg_current_snapshot()` and the rows' group key + contributions, (4) deltas = new − ledger
old (full when no entry; a vanished key with an entry is a tombstone), (5) upserts ledger rows
with `basis` = that snapshot, (6) pre-locks groups ascending and adds deltas ADDITIVELY;
the status gate treats a flagged `backfilling` target as applying; completion goes straight to
`live` (no `park_catch_up`); the horizon/extinct-horizon checks do not fire for flagged
targets; chunk re-run is idempotent. Rule under the flag: "no ledger entry" means "not yet
contributed", so a CDC change with no entry is applied as insert-of-NEW and a delete with no
entry writes a tombstone and subtracts nothing.

Tests are in `trellis/tests/exp558_ledger_build.rs` (200k rows, 8 workers; no writes; writes
during the build including rows whose chunk has not run; chunk re-run; flag off unchanged).
Get them green with targeted runs only. Files touched: `defs/backfill.rs`, `defs/catalog.rs`,
`defs/chunk_queue.rs`, `intake/publication.rs`, `staging/apply.rs`, `staging/apply_aggregate.rs`,
`staging/ledger_build.rs` (new).

### 3b. Merge f + h into one branch and dry-run at 10M under slot capture

Create `exp/issue-558-exp5` on lane f (or h) with both sets of commits (they touch disjoint
files: f = `trellis/src`, h = `benchmark/`, `testkit/`). Then, disk-backed, same session:

```
# control (today's path) — already done, numbers below; rerun only if the binary changed
/home/mike/exp558/run-exp5.sh <lane> control-10m-disk --rows 10000000 --groups 100000 --duration-secs 20 --grace-secs 600
# ledger build
TRELLIS_EXP558_LEDGER=contrib TRELLIS_EXP558_BUILD=ledger \
/home/mike/exp558/run-exp5.sh <lane> ledger-10m-disk --rows 10000000 --groups 100000 --duration-secs 20 --grace-secs 600
```

`run-exp5.sh` (copy in `tools/`) sets the NVMe dir, `TRELLIS_TESTKIT_PG_OPTIONS="shared_buffers=1GB
checkpoint_timeout=1min max_wal_size=4GB"`, and writes `logs/exp5/<tag>.{jsonl,log}`. Control at
10M (2026-09-26): load 9.3M rows/s, build 4.6 s / 1 chunk, live 226 s, converged 399 s, tail
153 s, oracle ok, WAL 14.5 MB/s, 502 fsyncs/s, 358k checkpoint buffers, writers p99 21 ms and
1930/2000 rate (`kept_target_rate=false`), 3 deadlocks. Bar for the ledger build: oracle ok,
live ≈ build time (no re-read), tail small, WAL and fsync rate reported next to the control's.
If the ledger build's oracle mismatches, stop and post the failing shape rather than tuning.

### 3c. 100M on disk, both paths

Same two commands with `--rows 100000000 --groups 1000000 --build-timeout-secs 7200
--grace-secs 1800`. Expect ~1 h for the control (the re-read alone ≈ 40 min at the 10M rate)
and the ledger run's build to be bounded by chunk throughput on 8 workers. Disk: ~144 GB free on
the NVMe at handoff; the run needs roughly 5 GB source + ring + ledger (~15 GB) + WAL; delete
`/home/mike/exp558/tmpdisk/*` leftovers between runs (`chattr +C` is on the dir). Run the two
back to back, nothing else on the box (see §5). Post the pair on #558 as "Experiment 5, slot
capture" with the disk columns; it is a legitimate result on its own.

### 3d. The joint run on the #565 spike

Only after 3b passes. Steps: rebase `exp/issue-558-exp5` onto `spike/565-trigger-capture` (the
spike contains #597; the only overlap is `V52__ring_src_xid.sql`, which the trigger makes
redundant: under triggers the ledger identity is the ring row's `row_txid`, so `ledger_row`'s
`xid` comes from `row_txid` instead of `src_xid`, and V52 can stay unused). Capture: the spike's
triggers are generated by `experiments/issue-565/capture_sql.py` (statement trigger, use the
`new_only` + `skip_noop` shapes) and installed on `agg_src` before `define`; run Trellis with
`TRELLIS_SPIKE_TRIGGER_CAPTURE=1` (maintenance loop, no intake). The scenario needs a hook to
install that DDL after the load and before `define` (a `--capture trigger` flag that shells the
generated SQL in is fine for an experiment). Check on #565 first whether the spike owner has
landed a Rust installer by then; do not edit the spike branch. Report as the amendment asks:
convergence to the oracle after the last chunk and drain, with the disk columns, control =
the slot run from 3c.

## 4. The rest of the battery (independent of exp 5)

1. **Quiet-box reruns of the disk tier.** Everything disk-backed on 2026-09-26 afternoon ran
   while another session's trigger-capture harness (16 pgbench clients + a drain) shared the
   box; the same binaries on tmpfs measured 33–55k rows/s where the day before they measured
   76–90k. Rerun, on a quiet box, in one session each: (a) `fold-in-ratio --ratios 100,1000` in
   `off` and `contrib` (lane c) plus the tmpfs `off` for the ratio; (b) `rel-churn --children
   100000 --parent-rate 100,1000` on lanes d, c (contrib) and g (factored). Use `bench --disk`
   with `TRELLIS_BENCH_DISK_DIR=/home/mike/exp558/tmpdisk`. Merge lane h's `benchmark/` commits
   into c/g/d first so the runs carry the disk columns (`wal_mb_per_sec`, `fsyncs_per_sec`,
   `checkpoint_buffers_written`, writer p99). Restate the disk bars in `RESULTS.md` §Disk tier
   and post a short follow-up on #558.
2. **Factored deadlocks on disk** (9–16 per 100k run, 0 on tmpfs; all retried, oracle ok). Find
   the cycle: a parent batch holds T then P rows then groups; a child batch holds L then T then
   P then groups. Suspect the group pre-lock order vs a parent batch's P-driven group set. Add
   the deadlock's `DETAIL` from the Postgres log to the write-up; fix only if it is a lock-order
   bug in the prototype (sorted statements), otherwise document.
3. **Forward-path cold-page cost on disk**: `rel-churn --children 10,1000 --parent-rate 0
   --child-rate 5000` disk-backed, contrib (lane c) vs factored (lane g); tmpfs showed +37% WAL
   at fan-out 10 and nothing at 1k. This is the one open cost of the factored design.
4. **Exp 3's 40k/400k-group shapes** never drain on `main` either (#326 probe cost); leave them
   unless the user asks.
5. **Exp 6** waits on #557 (generative). Nothing to do.

## 5. Box rules (these have bitten, every one)

- `bench` is the only way to measure; it builds in the lane it runs from, then takes
  `/tmp/trellis-bench.lock` exclusively. `verify` holds it shared. **Wrap every ad-hoc cargo
  command as `flock -x /tmp/trellis-bench.lock cargo …`** while anything is measuring, or the
  numbers are contaminated (confirmed, not theoretical). Never `verify` in the background.
- Never edit a lane's tree while a queued `bench` will build there. New lane: `lane start <x>
  <branch> --on <parent>` then `cp -a --reflink=always <warm>/target <new>/target` (8 s for 111 GB).
- Queue scripts: launch with `setsid nohup ./script.sh > out 2>&1 < /dev/null &`; wait loops must
  use anchored patterns (`pgrep -f '^bash \./queue-x\.sh'`), because an unanchored `pgrep -f`
  matches the launching shell's own command line (two queues deadlocked for 2.5 h on that, and a
  `kill $(pgrep -f …)` killed its own shell). `exec`'d scripts show with an absolute path.
- Never kill a process you did not start; the other session's harness (`queue-phase2*.sh`,
  `v_e2e.py`, clusters under `/tmp/tc-565/`) is not yours. A dead benchmark's testkit postgres
  inherits the lock fd: `pg_ctl -D <cluster>/data -m immediate stop` on YOUR cluster only.
- Storage: default clusters live on `/tmp` (tmpfs, CPU-only numbers). Disk = `bench --disk`
  (or `TMPDIR` on a real dir with `chattr +C`); every write-up states which.
- Postgres/tokio-postgres: a `String` bound to `$n::numeric` fails ("error serializing
  parameter N", 0-based) — use `$n::text::numeric`. Tracing output has ANSI codes between a
  field name and its value; grep for the message text, not `name=value`.
- Migrations: the ledger branch's `V52__ring_src_xid.sql`; upstream took `V53` for the mirror,
  so a rebase keeps V52. `identity.rs` hard-codes the applied-migration list.
- `rel-churn` and the fold run with `set local enable_nestloop = off` on every lane (#581).
- `bench` prints `WARNING: other cargo/rustc processes` when contaminated: rerun, do not report.

## 6. Tooling

- `tools/tables.py rel|exp3 label=file …` renders jsonl into the markdown tables used in
  `RESULTS.md` and the comments (rel keys: converged_secs, tail_secs, oracle, wal_bytes,
  deadlocks, xact_rollbacks; exp3 keys: in-window folded rows/s, WAL/row, drained).
- `tools/run-exp5.sh <lane> <tag> [flags]`: one detached build-under-load run on the NVMe.
- `tools/queue-*.sh`: the chained queues used for items 1 and 3; copy the pattern (mark lines,
  anchored waits, one log pair per run).
- `build-under-load` flags: `--rows --groups --loaders --writers --write-rate --pre-define-secs
  --duration-secs --build-timeout-secs --grace-secs --oracle-poll-min-secs`; jsonl keys are
  listed in `benchmark/src/streaming/build_under_load.rs`'s module doc.
- #565's harness (`~/tc-565`, the spike's `experiments/issue-565/`): `e1.py` honours
  `TC565_BASE` for the cluster dir; results in `~/tc-565/results/`.

## 7. Writing it up

Match the existing comments: verdict in the heading, method in one paragraph, one table,
three to five bullets, the bar restated, a link to the `RESULTS.md` anchor. Same-session
controls for every comparison; state tmpfs vs disk on every number; say when a run was
contaminated instead of quoting it. Commit logs + `RESULTS.md` together on lane c and push
before posting, so the links resolve.
