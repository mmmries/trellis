# Testing Strategy

Trellis makes one hard promise (`docs/data-flow.md#correctness`): *once caught up
to a given LSN, each incrementally-maintained target is exactly equal to a full
recompute of its definition against the source data at that LSN, for any
interleaving of source changes.* Everything below exists to make that
promise — and the liveness, safety, and resource behavior around it — falsifiable.

Each tier is defined by its **trigger** (what makes it run a case), its **judge**
(what decides pass/fail), and its **reproducibility contract** (can a failure be
replayed deterministically). Tiers are ordered cheapest-first. The governing rule:
**push every check to the lowest tier that can hold it** — a race a generative run
finds must be driven down into a deterministic pin; a tier never re-litigates what a
lower one already guarantees.

| Tier | Trigger | Judge | Reproducible? | Cadence |
|---|---|---|---|---|
| 1. Compiler & type system | every build | `rustc` accepts/rejects | fully | every edit / CI |
| 2. Linters & format | every build | `clippy`/`rustfmt` | fully | every edit / CI |
| 3. Unit tests | scripted | assertion | fully | every PR / CI |
| 4. Integration tests | scripted, real Postgres | assertion | fully | every PR / CI |
| 5. Generative tests | random valid program, seeded | independent oracle at quiescence | at program level (shrinking) | nightly (`nightly.yml`, the one deep property), and on demand with `-- --ignored` |

---

## 1. Compiler & type system

**Make illegal states unrepresentable, so whole bug classes never reach a test.**
`rustc` + `edition = 2024` across the workspace enforce memory safety, exhaustive
`match`, ownership/lifetime discipline, and `Result` propagation for free. We spend
the type system on:

- **Newtypes over primitives** for values that must not be confused — LSNs, primary
  keys, column identifiers, exact-decimal `numeric`. Distinct types are a
  zero-runtime-cost correctness check.
- **Enums for closed vocabularies** (op kinds, outcome classes, AST node kinds) so a
  new variant forces every `match` to be revisited.
- **`#[non_exhaustive]` / sealed traits** where a public shape must stay evolvable.

It cannot check runtime values, Postgres semantics, or timing. Everything below
assumes the code compiles.

## 2. Linters & format

**One non-negotiable house style plus curated correctness lints, enforced in CI so
review never spends attention on them.** A merge is blocked on:

- `cargo fmt --all -- --check` — formatting settled by the tool, never by review.
- `cargo clippy --all-targets --all-features -- -D warnings` — **every warning is a
  hard error.** Catches mechanical correctness smells (needless clones, bad
  comparison chains, fallible-conversion foot-guns, `.unwrap()` where it should
  propagate).

CI installs current stable Rust with `rustfmt`/`clippy` via the vendored
`.github/actions/rust-toolchain` composite action (org policy allows only repo-local
or allowlisted actions). Style and lint only; no claim about behavior.

## 3. Unit tests

**Logic in isolation, beside the code it tests** (`#[cfg(test)]` modules in
`trellis/src`) — fast, deterministic, run on every `cargo test`. Most need no
Postgres. A module whose logic is SQL it renders or a catalog it reads (the
capture functions, the group-basis and ledger statements, the key and index
checks) starts a `testkit::TestCluster` of its own and runs the statement there,
still one fixed case per test. These own:

- The AST and parser — accept/reject, precedence, error messages.
- The `numeric` exact-decimal type — arithmetic, scale, comparison-by-value.
- The AST→`SELECT` printer as pure string production.
- Dependency-order resolution, batch/fold bookkeeping, outcome classification —
  anything expressible as *value in, expected value out*.

The cheapest place to pin a specific computed answer, and the home of any
generative failure that root-causes to a pure function.

## 4. Integration tests

**Real behavior against a real Postgres, scripted end-to-end.** These live in
`trellis/tests/*` and lean on `testkit`, which owns a disposable cluster (its
own `postgres` on a private socket, started from a copy of a migrated template
data directory and torn down on `Drop`); CI provisions the server binaries. They own:

- The capture path end-to-end: source DML → capture trigger → staging ring, with
  each family's images pinned against a golden fixture (`capture_parity.rs`).
- **Scripted interleavings** — hand-authored concurrency scenarios that must
  always hold (`*_interleavings.rs`). A test freezes one drain worker at a named
  **pause point** inside its page transaction (`staging::interleave`, reached
  through `trellis::dev`), does something else, then lets it go; the test learns
  the worker is frozen from a channel, never by polling. The `Driver` in
  `trellis/tests/support/drain_driver.rs` seals and drains one batch at a time
  on a worker the test names. `testkit::crash::OpenTransaction` holds a
  straddling transaction open across a batch boundary.
- Any behavior that needs a database but is a *fixed* case, not a generated one.

Tests that step the pipeline themselves share one set of helpers rather than
private copies: `trellis/tests/support/pipeline.rs` (connect, define, a capture
or reconcile pass, a seal, a settle), which the `Driver` is built on, and
`testkit`'s `connect_raw`, `wait_until`/`wait_for` and `wait_blocked_behind`
for raw connections and for the few waits a test can't step to, each with a
hard deadline and a failure naming what it waited for.

**This tier is the terminal home of every race bug found above it** — a
generative finding is not fixed until it exists here (or in tier 3) as a
deterministic pin that fails before the fix and passes after. A pin that is
easiest to state as a generated program stays in the `generative` crate with its
operations written out (`generative/tests/pins`), in the same action vocabulary
the generators draw from.

**Opt-in third-party tier.** `trellis/tests/pg_partman.rs` drives pg_partman's
partition maintenance (premake, `run_maintenance`, `partition_data_*`,
retention by detach and by drop, `undo_partition`, its background worker,
concurrent writers) under real capture. Its tests are `#[ignore]`d because they
need pg_partman built for the `postgres` on `PATH` (the file's header says how);
`cargo test -p trellis --test pg_partman -- --ignored` runs them, one PG version
per `ASDF_POSTGRES_VERSION`. `TestCluster::start_with_settings` starts the
cluster with the `shared_preload_libraries` the worker needs.

## 5. Generative tests

**The combinatorial correctness claim: byte-identical convergence to an independent
oracle over random valid programs, with failures shrunk to a minimal reproducing
program.** This is the `generative` crate; full architecture lives in
`docs/generative-test-suite.md`. In brief:

- **Trigger:** a randomly generated *valid* program — schema, transform definitions,
  and a sequence of source mutations — driven through the real engine against a
  `testkit` cluster.
- **Judge — Postgres itself is the oracle.** Because the calculation grammar is
  committed (ADR-0004) to an immutable subset of PostgreSQL operators/functions, the
  oracle renders each definition back to a `SELECT` (per-row projection for 1-1,
  `GROUP BY` for aggregates), runs it in the same cluster, and asserts the persisted
  target equals it. It shares no rendering code with the engine, so it catches even
  a bug in the engine's own SQL compiler.
- **Reproducibility:** at the *program* level via shrinking; seeds replay the
  program, and findings terminate as hand-minimized pins in tiers 3–4.
- **Fault actions:** a worker `SIGKILL`ed mid-drain (`testkit::crash::CrashGuard`
  over the `engine_subprocess` binary), a server restart, a backup and restore,
  each an action the oracle still judges at quiescence.
- **Planted bugs keep it honest:** `trellis::plant` holds known-wrong engine
  behavior, compiled only for tests and armed one at a time per process by
  `TRELLIS_TEST_PLANT`. The planted-bug sweep (`generative::plant_sweep`) runs a
  tier's seeded cases once with no plant, then once per plant, and fails when the
  tier misses one it should catch.
- **Properties it checks:** per-op convergence,
  operation-error handling, idempotency (at-least-once treated as exactly-once),
  order-insensitivity over commuting ops, read-your-own-writes via `await`, and — the
  hardest — non-idempotent aggregate delta convergence.

Its properties are opt-in: each is `#[ignore]`d, so PR-time CI (plain
`cargo test --workspace`) runs only the crate's fast non-property tests, and deep
runs opt in with `-- --ignored` plus a `PROPTEST_CASES` override (a healthy deep
run is minutes, not seconds — a "pass" in one second executed nothing). It
assumes the system *reaches* quiescence and judges *what the answer is* there; it
structurally cannot judge timing, liveness, or long-horizon resource behavior. No
test tier judges those: the streaming benchmark and soak runs measure them, by
hand, before a release (a chaos tier for them is
[#1884](https://github.com/salesforce-misc/trellis/issues/1884)).

---

## Benchmarks: building a `JOIN` over seeded tables

The streaming benchmark (`benchmark/`, run with `cargo run -p benchmark --features engine-access --release -- join ...`) is
not a test tier, but its `join` scenario has a mode that measures a `JOIN` *build*: by
default (`--load direct`) it seeds the fact and dimension tables server-side
with parallel `INSERT ... SELECT` before any definition exists, so nothing
enters the capture ring and a 100M-row dataset loads in minutes. It then
defines each target and reports, per target, define-to-`live` time, the build
transaction's own duration, WAL and temp bytes, how long a snapshot was held
and the oracle result. `--rebuild` adds in-place rebuilds (`request_backfill`
of each source table, `PAUSE` and `RESUME` of each target), `--build-only`
stops after the builds, `--build-timeout-secs` bounds each one, and
`--reconcile-interval-ms` (250 ms for `join`) bounds the padding a reconcile
tick adds to define-to-`live`. `--load capture` loads the same rows through
capture instead, and each target's row digest is printed so the two paths can
be compared.

## How CI runs the tiers

The `ci` workflow runs `lint` (format, clippy, the rustdoc build with warnings denied, the public-API list check, the shard-plan check) beside four `test`
shard jobs, and a final job named `ci` that passes only when `lint` and every shard
passed; `ci` is the one required status check. Each shard builds and runs its own test
binaries, one at a time, with `cargo test --workspace --features benchmark/engine-access
--no-fail-fast` narrowed to them, the flags `verify` uses.

`.github/scripts/test-shards.py` assigns the binaries. It lists them from `cargo
metadata`, so a new test file joins a shard with no edit, and balances them by the
seconds in `.github/test-shards.txt`, one `unit seconds` line per binary. A binary the
file does not list runs at the median weight and the run prints a warning. Correctness
never rests on the weights: `lint` fails if any binary is in no shard or two, or if the
`shard:` matrix in `ci.yml` is not `1..TEST_SHARDS`, and a shard fails if cargo built
anything other than its assignment.

To refresh the weights after the suite has changed shape, take the `cargo test` output
of one full CI run (`gh run view <id> --log > log`, or a local `cargo test --workspace
--features benchmark/engine-access`) and run `python3 .github/scripts/test-shards.py
weights <log> > .github/test-shards.txt`. `test-shards.py plan -v` prints the resulting
balance. Refresh when the warnings name many unlisted binaries or one shard runs
noticeably longer than the others; a stale file costs balance, never coverage.

The PR-time shards run against the newest PostgreSQL on the runner image. The
`pg-versions` job in `nightly.yml` runs the whole workspace suite (the shards' `cargo
test` flags and `jit=off`, doc tests included) every night against PostgreSQL 15, 16, 17
and 18, each installed from the PGDG apt repository (built with libxml, which the XML
tests need), one job per version, so one version's failure does not hide another's. It
runs nightly rather than per PR because SQL that only some versions accept is rare; a
construct the oldest or newest grammar rejects shows up the next morning. The harness
finds its server binaries through `PATH`, so each job puts that version's `bin`
directory on `PATH` and fails if `postgres` or `pg_config` is not the matrix version's.
Locally, `ASDF_POSTGRES_VERSION=15.x` does the same for an asdf install, though one built
without libxml fails the XML tests.

## How the tiers compose

The tiers form a ratchet; when a bug is found, work flows one direction:

1. A generative run (5), a benchmark or soak run, or a report finds a failure.
2. It is reproduced as a seeded generative program (5), or — if not combinatorial —
   a scripted scenario (4).
3. The minimal case is pinned as an integration test (4) or, if it reduces to pure
   logic, a unit test (3).
4. Where possible, the shape is made unrepresentable (1) or caught by a lint (2).

The higher a fix lands, the cheaper and more permanent it is. Every upper tier's
design intent is to *shrink its own surface* by feeding durable pins downward — so
that over time the expensive tiers exercise genuinely new ground, not what the fast
tiers already guard.
