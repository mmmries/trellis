# Issue #102 / #94 spike harnesses

Scratch harnesses for the "can a to-one relationship be maintained by deltas?"
question. None of this is engine code; it exists so the design decision is made
against measurements rather than argument.

| file | what it is | how to run |
|---|---|---|
| `issue-102-delta-model.sql` | the original Spike B scratch model: #102's design as written, diffed against a from-scratch `GROUP BY`. Self-contained. | `psql -f` on an empty database |
| `issue-102-settled-state-v2.sql` | **the current one.** Five settled-state designs plus guard-ablation variants, over a model of the staging ring (segments, buckets, out-of-order drain), intake lag, parent insert/delete, FK re-point and NULL groups. Self-contained; prints how often each mechanism fired so "0 failures" can be told apart from "never exercised". | `psql -f`, then `select * from fuzz('D5', 200, 50, 31000);` |
| `issue-102-bench-v2.py` | maintenance cost: `force_every_group` vs. delta+projection vs. the 2-transform workaround, on a 1M/3M fixture | `python3 issue-102-bench-v2.py` against a fixture DB |
| `issue-102-cast-bench.sql` | the `::text`-cast cost at the two relationship lookup sites | `psql -f` against the same fixture |
| `issue-102-bench.sh` | the original Spike C/D driver | see the file |

`issue-102-settled-state.sql` (the first settled-state harness) has been
**removed**: it was not self-contained — it referenced views and tables it never
created, and its `A()` dispatcher routed `D4` to `D3`'s body — so the D1–D4
numbers published from it were never reproducible. `-v2` replaces it.

Designs in `-v2`:

- **D0** — issue #102 as written: no settled state, live join in both directions.
- **D1** — per-parent settled projection only.
- **D3** — per-from-side-row settled attribution (the enrichment-table shape).
- **D5** — D1 plus three guards: an LSN barrier, an optimistic per-parent
  generation check, and an in-flight check on the parent's from-side changes.
- **D5-a / D5-b / D5-c / D5-d** — D5 with exactly one guard disabled, so each
  guard's necessity is measured rather than asserted.
