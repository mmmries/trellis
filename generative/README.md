# `generative`

The generative correctness test suite (design doc:
`docs/generative-test-suite.md`, epic #2). See that doc for architecture,
oracle design, and the property list; this file is the crate-local
operational notes.

## A real full run is minutes, not seconds

If `cargo test -p generative` reports green in about a second, it executed
nothing — check `PROPTEST_CASES` and that the properties actually ran, not
just the meta-tests. Each proptest case round-trips through a real Postgres
cluster (`testkit`), so a healthy run at the design doc §9 case-count band
(12–24 per property) takes real wall-clock time. A suite that reports green
too fast to have done that work is a bug in the harness, not a fast pass —
see `docs/generative-test-suite.md` §6 and `generative/tests/meta.rs`. As of
`local_docs/generative-suite-improvement-plan.md`'s workstream A, this
wall-clock heuristic is backed by `run::Coverage`'s floor assertions
(`tests/coverage.rs`) for the fast, DB-free half of "did it actually run" —
this note stays as a smell to notice, not the only check.

## Current cost profile, and the CI split this still needs (plan workstream C)

At the design doc's default case count (16), a full `cargo test -p generative
--test convergence` run takes **~130–290s**, and that variance is understood,
not mysterious: per-op timing instrumentation (`GENERATIVE_QUIESCE_TIMING=1`,
`ManualBackend::quiesce`) across 10 runs (894 samples) found a clean bimodal
distribution — ~77% of quiesce calls resolve under 2s, and a distinct ~22%
cluster lands at 8.2–10.2s, matching the ~10s pipeline stall independently
suspected in `local_docs/transit-comparison.md` §3.3
(`SealConfig::age_gate`). A second pass (`GENERATIVE_COST_TIMING=1`,
covering cluster startup, per-case database provisioning, install, apply,
snapshot, and the oracle's recompute) found those phases collectively
account for **under 3% of wall-clock** — so the suite's cost is, almost
entirely, that stall, not test-harness overhead. This is a real engine bug,
not a suite problem, and is out of scope for this crate to fix; see the git
history for `generative/src/backend/manual.rs` (`ManualBackend::quiesce`) for
the full measurement writeup.

One consequence: `cargo test --workspace` in CI currently pays this full
cost on **every** push and every PR (`.github/workflows/ci.yml`'s `Test`
step has no fast/deep split). The plan's recommended split — not yet
applied, because it requires editing a `.github/workflows/*.yml` file, which
needs a token with the `workflow` OAuth scope to push — is:

- **PR/push job (fast):** skip the expensive property (`cargo test -p
  generative --test convergence -- --skip
  convergence_holds_for_trivial_programs`) — measured at ~22s, since the
  file's 5 remaining hand-built pin tests still each round-trip through a
  real cluster and can themselves hit the stall, just far less often than a
  16-case property's ~80 ops. The other DB-backed targets are each a handful
  of ops and measure well under a minute individually (`meta` ~0.7s,
  `oracle` ~1.3s, `backend_seam` ~1.6s, `backfill` ~11.3s — `backfill`'s
  single test reliably hits the stall once); `coverage` and `--lib` are
  sub-second (no cluster at all).
- **A new scheduled (nightly or on-demand) workflow (deep):** run the full
  property at a much higher case count (e.g. `PROPTEST_CASES=200`). The
  property's `FileFailurePersistence::SourceParallel` config (see
  `tests/convergence.rs`'s `proptest_config`) already writes any failing
  case to `generative/tests/proptest-regressions/convergence.txt` and
  replays it first on the next run — commit that file if a deep run ever
  produces one, so the failure becomes a real, replayable regression the
  fast path picks up too.

Once someone with `workflow`-scope push access applies that split, the
"minutes, not seconds" heuristic above should be re-read as describing the
*deep* job, not the PR-path one.
