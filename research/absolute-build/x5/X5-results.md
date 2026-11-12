# X5 results: the plain aggregate's fresh build under concurrent writes (#1917)

Research only. Nothing here merges. Spec: [`../DESIGN.md`](../DESIGN.md); prototype and its no-load benchmark:
[`../x3/X3-results.md`](../x3/X3-results.md). Logbook: `20261112T151045-r-bench` (ok, 37.1M rows: 31.9M generated,
5.2M writer row changes).

## Answers

1. **Exact under load, both binaries.** All 28 runs (12 base, 16 prototype; 0, ~1k and ~10k row changes/s; 1M and
   10M rows; with and without the group-key index) end with the target equal to the plain query: `EXCEPT ALL` 0/0
   both ways, row counts equal, no quarantined rows. No database had to be kept.
2. **Stragglers are few and step 2 is cheap.** 1M rows: about 300 at ~1k/s and 1,300–3,700 at ~10k/s (1.3–3.5 %
   of `s2_agg`'s 100k groups, 0.4 % of `s2_agg_hi`'s 1M). 10M rows at ~10k/s: 2,024 (0.02 %). Step 2 by index probe:
   10–261 ms (one 1.4 s outlier on a loaded box); 3.5 s at 10M. Without the group-key index step 2 reads by one
   `GROUP BY`: 0.8–1.6 s for 3,151–3,744 stragglers. No run came near the 10,000-probe cap (max 3,744).
3. **The drain is held up for the whole build transaction in both binaries, and A shortens it only as much as it
   shortens the build.** No batch seals while the build's transaction is open (the seal gate, below), so the
   stall equals the build transaction: at 1M `s2_agg_hi` and ~10k/s, base 42.7 s vs prototype 12.5 s (quiet rep),
   base 52.4 s vs prototype 31.0 s (loaded rep); at 10M the prototype stalls 262 s. Peak backlog: base 215k–258k
   rows, prototype 116k–127k; 858k at 10M. Page lock waits on the build's group rows, which DESIGN.md's argument is
   about, are the minor effect: base up to 29.8 s once (2.6–4.6 s at 10k/s on `s2_agg`), prototype at most 0.35 s.
   Under A those pages go ahead and make stragglers instead of waiting.
4. **Build ratio under load.** Quiet rep, define→live over the plain `INSERT … SELECT` on the same loaded database:
   `s2_agg_hi` 1M, base 13.5× (~1k/s) and 16.5× (~10k/s), prototype 9.6× and 6.6×. `s2_agg` 1M: base 8.5× and 10.9×,
   prototype 7.5× and 8.8× (there the ~4 s define-to-live floor dominates). 10M `s2_agg_hi` at ~10k/s, prototype only:
   267.5 s against 53.1 s, **5.0×** (no load: 78.6 s against 13.8 s, 5.7×, as X3's 6.0×). Load slows the build and the
   plain query about equally, so the ratio holds.

## The seal gate stalls the drain during any build (surprise)

The seal refuses to advance while `pg_snapshot_xmin(now) < xmax(predecessor's fence)` (`staging/seal.rs`,
[03](../../../docs/staging-and-claiming/03-sealing-and-the-fence.md)): any transaction with an xid that was running
at the previous seal holds it. A build's transaction holds an xid for about its whole length (the stall starts with it), so until its commit at
most one more batch seals, for **every** definition on the ring, not only the one building. Measured: the gap
between two seals that spans the build equals the build transaction, within about one normal seal gap, in 14 of
the 15 runs with writers, base and prototype (the 15th, base under load 26: a 25.0 s gap in a 34.4 s transaction);
the normal gap is 0.3–3 s. The mechanism is read from the code, not instrumented.

Consequences:

- **Drain hold-up = build transaction length**, so A's gain here is its build-time gain (2–3.5× at 1M groups), not
  the per-group lock argument. The backlog then drains at the page path's speed: 295 s after the writers stopped
  at 10M (858k rows into a 10M-group hidden table with 128 MB `shared_buffers`). Inferred, not run: the base's
  1,535 s build at 10M (E5) would stall the ring ~6× as long.
- **It bounds the stragglers.** Only pages of the batches sealed before the gate closed can reach groups during
  step 1, so the straggler count follows the write rate times about one seal interval, not the build length
  (2,024 at 10M with a 249 s step 1 against 3,600 at 1M with 12–30 s).

This is not specific to A; it is a property of any long build transaction. Not filed here; listed for the caller.

## Setup

- Binaries: base `trellis-cli --release` at `54cfddb2` (sha256 `6b876e7e…`), prototype at `b3b7856c` (this
  branch, `2512703d…`), both `trellis 0.2.0`, side by side in `target/x5-bin/`.
- Postgres 18.6 scratch cluster `target/research-pg-x5` (deleted): X3's settings (`setup_cluster.sh`: `fsync=on`,
  `shared_buffers=128MB`, `max_wal_size=1GB`, `work_mem=4MB`, checksums, `auto_explain` 200 ms without analyze,
  `pg_stat_statements`), non-superuser owner `app`. `trellis run` with defaults (2 drain threads).
- Data: X3's `gen.sql` (1M rows: 100k authors, 1M `grp_hi` groups; 10M: 1M / 10M). Shapes `s2_agg` (`author_id`)
  and `s2_agg_hi` (`grp_hi`), from `shapes.sh`. No-index case: `posts_grp_hi_idx` dropped and the transform defined
  `GROUP BY grp_hi WITHOUT SOURCE INDEX`.
- Writers (`loadgen.py`): 16 processes, paced transactions of 4 operations: 30 % insert into an existing group, 10 %
  insert creating a new `grp_hi` and a new author, 30 % update of `likes`/`score`, 15 % move to another group (both
  keys), 13 % delete of one row (in `s2_agg_hi` that empties its group), 2 % delete of one author's rows (empties an
  `s2_agg` group). 80 % of keys uniform over the key space, 20 % from a hot subset (ids 1–1000, authors 1–100,
  `grp_hi` 0–999). Targets 1,000 and 10,000 row changes/s; achieved rates are in the tables. The box sustains
  ~10k/s before the capture trigger exists, 3k–9k/s after it (Python writers plus capture, and the drain's
  catch-up competing for WAL).
- Per run (`run.sh`): `trellis run`, sampler and writers start; 8 s warm-up (15 s at 10M); the plain
  `INSERT … SELECT` into a keyed scratch table, timed, writers running; 3 s; `trellis apply`; poll `trellis status`
  every 0.25 s until `live`; 20 s more; stop writers; wait until the ring is idle (no sealed or draining segment and
  the active slot empty for 8 samples, 2 s); then `EXCEPT ALL` both ways against the plain query, the poison table,
  the hidden table's `__basis` distribution. `trellis` has no CLI self-check; not run.
- Sampler (`sampler.py`, 0.25 s): every non-idle Trellis session's wait event, statement age and
  `pg_blocking_pids`, and the ring (`segments` row counts, the active slot's rows, `active_seq`).
- Analysis (`analyze.py` → `results/analysis.tsv`, `report.py` → `results/tables.md`). Build transaction: the oldest
  transaction among sessions running a build statement. Page hold-up: a Trellis session waiting on a heavyweight
  lock whose root blocker is a Trellis transaction older than 0.5 s. Stragglers: rows of step 2's statement in
  `pg_stat_statements` (one row per rewritten group; cross-checked against the hidden rows whose `__basis` is the
  second snapshot after the run, which is lower because straggler groups that later emptied were collected).
- Order: rep 1 then rep 2, each `rate × shape × {base, proto}`, then the no-index runs, then 10M.
- **Box load**: rep 1 ran while other agents compiled (load 10–34, 1–3 `cargo`); rep 2, the no-index rep 2 and
  10M at load 1.2–5.6 (`box.txt` per run). Rep 2 is the clean one. Absolute times of rep 1 are inflated
  (base `s2_agg_hi` no load: 34.9 s vs 17.4 s in rep 2; X3: 19.4 s).

## Tables

Build and correctness (exact = `EXCEPT ALL` 0/0; ratio = define→live over the plain `INSERT`, both on the same
loaded database; build txn = the build transaction from the samples).

| shape | target rate | rep | binary | achieved rows/s | exact | plain INSERT s | define→live s | ratio | build txn s | build WAL MB |
|---|---|---|---|---|---|---|---|---|---|---|
| `s2_agg` 1M | 0 | 1 | base | - | 1 | 1.7 | 6.3 | 3.7 | | 78 |
| `s2_agg` 1M | 0 | 1 | proto | - | 1 | 1.5 | 8.4 | 5.4 | 2.3 | 45 |
| `s2_agg` 1M | 0 | 2 | base | - | 1 | 0.6 | 5.7 | 10.1 | 1.5 | 77 |
| `s2_agg` 1M | 0 | 2 | proto | - | 1 | 0.6 | 4.9 | 8.7 | 1.1 | 45 |
| `s2_agg` 1M | 1000 | 1 | base | 1108 | 1 | 0.6 | 11.6 | 18.6 | 7.0 | 115 |
| `s2_agg` 1M | 1000 | 1 | proto | 935 | 1 | 1.7 | 4.5 | 2.7 | 1.0 | 65 |
| `s2_agg` 1M | 1000 | 2 | base | 1135 | 1 | 0.7 | 5.7 | 8.5 | 1.7 | 99 |
| `s2_agg` 1M | 1000 | 2 | proto | 1138 | 1 | 0.7 | 4.9 | 7.5 | 1.2 | 67 |
| `s2_agg` 1M | 10000 | 1 | base | 6965 | 1 | 2.2 | 8.8 | 4.1 | 6.1 | 112 |
| `s2_agg` 1M | 10000 | 1 | proto | 9185 | 1 | 1.6 | 6.8 | 4.3 | 3.6 | 79 |
| `s2_agg` 1M | 10000 | 2 | base | 9799 | 1 | 0.7 | 7.6 | 10.9 | 3.8 | 124 |
| `s2_agg` 1M | 10000 | 2 | proto | 9321 | 1 | 0.8 | 6.8 | 8.8 | 3.0 | 73 |
| `s2_agg_hi` 1M | 0 | 1 | base | - | 1 | 1.8 | 34.9 | 19.9 | 32.2 | 863 |
| `s2_agg_hi` 1M | 0 | 1 | proto | - | 1 | 2.8 | 14.6 | 5.2 | 12.0 | 440 |
| `s2_agg_hi` 1M | 0 | 2 | base | - | 1 | 1.7 | 17.4 | 10.4 | 14.5 | 864 |
| `s2_agg_hi` 1M | 0 | 2 | proto | - | 1 | 1.4 | 9.7 | 7.0 | 6.5 | 440 |
| `s2_agg_hi` 1M | 1000 | 1 | base | 809 | 1 | 2.9 | 36.1 | 12.4 | 34.4 | 1194 |
| `s2_agg_hi` 1M | 1000 | 1 | proto | 1130 | 1 | 2.0 | 12.8 | 6.4 | 10.2 | 541 |
| `s2_agg_hi` 1M | 1000 | 2 | base | 1068 | 1 | 1.6 | 21.4 | 13.5 | 18.2 | 1043 |
| `s2_agg_hi` 1M | 1000 | 2 | proto | 1104 | 1 | 1.3 | 12.4 | 9.6 | 9.0 | 529 |
| `s2_agg_hi` 1M | 10000 | 1 | base | 4938 | 1 | 4.7 | 58.4 | 12.6 | 52.2 | 1575 |
| `s2_agg_hi` 1M | 10000 | 1 | proto | 4430 | 1 | 3.2 | 32.3 | 9.9 | 31.0 | 668 |
| `s2_agg_hi` 1M | 10000 | 2 | base | 5082 | 1 | 2.7 | 44.4 | 16.5 | 42.2 | 1476 |
| `s2_agg_hi` 1M | 10000 | 2 | proto | 7136 | 1 | 2.3 | 15.1 | 6.6 | 12.5 | 678 |
| `s2_agg_hi` no index 1M | 10000 | 1 | proto | 7531 | 1 | 4.4 | 19.2 | 4.3 | 13.9 | 680 |
| `s2_agg_hi` no index 1M | 10000 | 2 | proto | 8114 | 1 | 2.9 | 10.4 | 3.7 | 8.4 | 648 |
| `s2_agg_hi` 10M | 0 | 1 | proto | - | 1 | 13.8 | 78.6 | 5.7 | 73.5 | 4402 |
| `s2_agg_hi` 10M | 10000 | 1 | proto | 3330 | 1 | 53.1 | 267.5 | 5.0 | 261.9 | 14417 |
| `s2_agg_hi` 10M, base | | | | | | E5: 18.7 | E5: 1,534.9 | 82× | | 102.6 GB |

Build WAL under load includes the writers' and the capture's. The base rep-1 `s2_agg` no-load run's directory was
overwritten by a harness bug (fixed after the first two runs); its row comes from the runner's log
(`run_x5.out`), without samples. Define→live includes a 1–5 s floor before the build transaction (the capture
trigger install waits for writer transactions in flight; X3's ~5.5 s floor is this plus the rest).

Stragglers and step 2 (prototype; step 2 runs only with stragglers):

| shape | target rate | rep | step 1 ms | stragglers (step-2 rows) | with basis `S2` at the end | step-2 read | step-2 ms |
|---|---|---|---|---|---|---|---|
| `s2_agg` 1M | 1000 | 1 | 1,342 | 305 | 291 | probe | 12 |
| `s2_agg` 1M | 1000 | 2 | 1,159 | 318 | 308 | probe | 11 |
| `s2_agg` 1M | 10000 | 1 | 2,101 | 3,469 | 3,305 | probe | 1,418 |
| `s2_agg` 1M | 10000 | 2 | 2,789 | 1,318 | 1,238 | probe | 175 |
| `s2_agg_hi` 1M | 1000 | 1 | 9,341 | 371 | 241 | probe | 67 |
| `s2_agg_hi` 1M | 1000 | 2 | 8,302 | 355 | 242 | probe | 10 |
| `s2_agg_hi` 1M | 10000 | 1 | 29,930 | 3,597 | 2,356 | probe | 154 |
| `s2_agg_hi` 1M | 10000 | 2 | 11,549 | 3,663 | 2,496 | probe | 261 |
| `s2_agg_hi` no index 1M | 10000 | 1 | 11,165 | 3,151 | 2,156 | `GROUP BY` | 1,613 |
| `s2_agg_hi` no index 1M | 10000 | 2 | 7,133 | 3,744 | 2,385 | `GROUP BY` | 844 |
| `s2_agg_hi` 10M | 10000 | 1 | 249,370 | 2,024 | 1,358 | probe | 3,520 |

No-load runs: 0 stragglers, no step 2. The `s2_agg` rep-1 1k/s step 2 is from `pgss-build.txt` (the fuller dump's
filter missed it before it was widened). The 1.4 s probe outlier ran at load 20. Without the index, step 2 is one
`GROUP BY` of the source: 0.8–1.6 s at 1M rows; at 10M roughly ten times that (inferred, not run), still small
against step 1.

Drain hold-up (runs with writers). Seal stall: the gap between the two seals around the build. Normal gap: the
median after `live`. Page lock-wait: Trellis sessions blocked on the build's transaction.

| shape | target rate | rep | binary | build txn s | seal stall s | normal seal gap s | page lock-wait: longest run s | longest single wait s | peak backlog rows | peak oldest-sealed age s | drain after writers stop s |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `s2_agg` 1M | 1000 | 1 | base | 7.0 | 7.3 | 0.8 | 0.0 | 0.0 | 7404 | 1.2 | 4.2 |
| `s2_agg` 1M | 1000 | 1 | proto | 1.0 | 1.8 | 0.8 | 0.0 | 0.0 | 1281 | 2.3 | 3.3 |
| `s2_agg` 1M | 1000 | 2 | base | 1.7 | 1.8 | 0.3 | 1.25 | 1.41 | 1951 | 1.4 | 2.2 |
| `s2_agg` 1M | 1000 | 2 | proto | 1.2 | 1.3 | 0.3 | 0.0 | 0.0 | 1355 | 1.1 | 2.2 |
| `s2_agg` 1M | 10000 | 1 | base | 6.1 | 6.0 | 0.5 | 4.75 | 4.57 | 37886 | 6.0 | 2.8 |
| `s2_agg` 1M | 10000 | 1 | proto | 3.6 | 3.3 | 0.3 | 0.0 | 0.0 | 20265 | 1.1 | 2.8 |
| `s2_agg` 1M | 10000 | 2 | base | 3.8 | 3.5 | 0.3 | 3.05 | 2.62 | 39975 | 3.4 | 2.1 |
| `s2_agg` 1M | 10000 | 2 | proto | 3.0 | 3.0 | 0.3 | 0.0 | 0.0 | 14383 | 2.4 | 2.1 |
| `s2_agg_hi` 1M | 1000 | 1 | base | 34.4 | 25.0 | 2.7 | 23.75 | 29.83 | 32903 | 34.0 | 6.3 |
| `s2_agg_hi` 1M | 1000 | 1 | proto | 10.2 | 10.0 | 0.3 | 0.25 | 0.35 | 11733 | 1.8 | 2.5 |
| `s2_agg_hi` 1M | 1000 | 2 | base | 18.2 | 18.3 | 0.3 | 0.25 | 0.19 | 19199 | 0.9 | 3.6 |
| `s2_agg_hi` 1M | 1000 | 2 | proto | 9.0 | 9.0 | 0.3 | 0.0 | 0.0 | 8835 | 0.4 | 1.9 |
| `s2_agg_hi` 1M | 10000 | 1 | base | 52.2 | 52.4 | | 0.0 | 0.0 | 257860 | 20.1 | 15.7 |
| `s2_agg_hi` 1M | 10000 | 1 | proto | 31.0 | 31.0 | 3.3 | 0.0 | 0.0 | 115898 | 9.9 | 6.5 |
| `s2_agg_hi` 1M | 10000 | 2 | base | 42.2 | 42.7 | | 0.0 | 0.0 | 215307 | 19.4 | 13.1 |
| `s2_agg_hi` 1M | 10000 | 2 | proto | 12.5 | 12.5 | 1.3 | 0.0 | 0.0 | 126645 | 15.7 | 5.8 |
| `s2_agg_hi` no index 1M | 10000 | 1 | proto | 13.9 | 13.3 | 0.5 | 0.0 | 0.0 | 144812 | 13.0 | 5.4 |
| `s2_agg_hi` no index 1M | 10000 | 2 | proto | 8.4 | 8.8 | 1.5 | 0.0 | 0.0 | 105133 | 7.9 | 4.3 |
| `s2_agg_hi` 10M | 10000 | 1 | proto | 261.9 | 262.1 | | 0.25 | 0.24 | 857826 | 20.1 | 294.8 |

"Peak oldest-sealed age" is the oldest sealed-but-undrained batch; it misses the active slot, which is where the
backlog sits during a stall, so the real oldest change is about the seal stall old. Rows in the backlog are capture
rows (~one per row change).

Writer transaction latency, ms, p50 / p99 (4 operations per transaction):

| shape | target rate | rep | binary | achieved rows/s (pre / build / post) | before trigger | trigger install | build txn | after live |
|---|---|---|---|---|---|---|---|---|
| `s2_agg` 1M | 1000 | 2 | base | 1167 / 1118 / 1105 | 1.3 / 173 | 1.2 / 5.8 | 1.9 / 6.3 | 1.7 / 166 |
| `s2_agg` 1M | 1000 | 2 | proto | 1167 / 1125 / 1106 | 1.2 / 2.3 | 1.5 / 19.1 | 2.4 / 6.2 | 1.9 / 76 |
| `s2_agg` 1M | 10000 | 2 | base | 7084 / 10853 / 9994 | 1.2 / 15.5 | 1.4 / 4.8 | 2.1 / 20.4 | 2.0 / 12.9 |
| `s2_agg` 1M | 10000 | 2 | proto | 9821 / 3579 / 8359 | 1.2 / 22.1 | 1.3 / 13.7 | 2.6 / 63.5 | 2.2 / 27.9 |
| `s2_agg_hi` 1M | 1000 | 2 | base | 1167 / 1041 / 984 | 1.4 / 25.7 | 1.3 / 25.8 | 4.0 / 852 | 4.2 / 1261 |
| `s2_agg_hi` 1M | 1000 | 2 | proto | 1164 / 949 / 1112 | 1.2 / 110 | 1.2 / 5.8 | 4.4 / 91.5 | 3.1 / 118 |
| `s2_agg_hi` 1M | 10000 | 2 | base | 8822 / 4327 / 2981 | 1.4 / 65.9 | 1.4 / 2.6 | 2.7 / 199 | 3.6 / 883 |
| `s2_agg_hi` 1M | 10000 | 2 | proto | 11314 / 8867 / 3141 | 1.0 / 22.6 | 1.0 / 1391 | 2.2 / 61.7 | 2.5 / 606 |
| `s2_agg_hi` 10M | 10000 | 1 | proto | 7853 / 3187 / 1244 | 3.0 / 70.0 | 3.3 / 86.9 | 3.8 / 441 | 6.7 / 1202 |

Rep 1 and the no-index runs are in `results/tables.md`. Writers take no lock the build holds (the build's source
read is `ACCESS SHARE`), and no writer transaction failed in any run. p50 stays at 1–4 ms through the build in both
binaries; p99 moves by hundreds of ms from run to run with no pattern by binary or phase, so the build's effect on
writers is below this harness's noise. The worst p99s are after `live` (the drain catching up competes for WAL
flushes: `WALWrite` waits in the samples) and during the trigger install (it waits for writer transactions in flight
and then blocks new ones).

## Limits

- One box, shared: rep 1 under heavy unrelated load. Two reps at 1M, one at 10M; no base at 10M (E5's figure).
- Writers are Python processes; the 10k/s target is reached only before the capture trigger exists.
- Seal and hold-up times have the sampler's 0.25 s resolution. The seal-gate mechanism is inferred from the code
  and the timing match, not traced. Page waits are counted only for heavyweight `Lock` waits on a Trellis root
  blocker; a page that never ran (no batch sealed) does not appear there, which is why the seal stall is the
  measure that matters.
- The straggler count relies on step 2's statement returning one row per group it rewrote (true of the base's
  same statement: 1,000,000 rows for 1M groups with no writers).
- No self-check (the CLI has none); exactness is `EXCEPT ALL` against the plain query after the drain went idle.
- Not measured: a rebuild in place under load (stays on today's path), partitioned sources, `JOIN`s.

## Files

`run_x5.sh` (runner, one logbook reservation), `run.sh` (one run), `loadgen.py`, `sampler.py`, `analyze.py`,
`report.py`, `setup_cluster.sh`, `gen.sql`, `shapes.sh` (from X3). `run_x5.out` (runner log), `results/analysis.tsv`,
`results/tables.md`, `results/full{,10m}/{base,proto}/<run>/` (summary, verify, bases, `pg_stat_statements` dumps,
status, samples `.gz`, writer logs `.tgz`).
