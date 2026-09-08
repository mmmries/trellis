# Testing Strategy

Trellis makes one hard promise (`docs/data-flow.md#correctness`): *once caught up
to a given LSN, each incrementally-maintained target is exactly equal to a full
recompute of its definition against the source data at that LSN, for any
interleaving of source changes.* Everything below exists to make that promise —
and the system's liveness, safety, and resource behavior around it — falsifiable.

This document is the map of our validation tiers. Each tier is defined by three
things: its **trigger** (what makes it run a case), its **judge** (what decides
pass/fail), and its **reproducibility contract** (can a failure be replayed
deterministically). Tiers are ordered cheapest-and-fastest first. The governing
rule is **push every check to the lowest tier that can hold it**: a bug caught by
the type system should never be left for a unit test; a race found by chaos must
be driven down into a deterministic pin. A tier should not re-litigate what a
lower tier already guarantees.

| Tier | Trigger | Judge | Reproducible? | Cadence |
|---|---|---|---|---|
| 1. Compiler & type system | every build | `rustc` accepts/rejects | fully | every edit / CI |
| 2. Linters & format | every build | `clippy`/`rustfmt` | fully | every edit / CI |
| 3. Unit tests | scripted | assertion | fully | every PR / CI |
| 4. Integration tests | scripted, real Postgres | assertion | fully | every PR / CI |
| 5. Generative tests | random valid program, seeded | independent oracle at quiescence | at program level (shrinking) | TBD |
| 6. Chaos tests | wall-clock random workload + faults | invariants over recorded history | **no** | TBD |

The line between tiers 5 and 6 is the one that most needs stating explicitly, so
it has its own section (§6).

---

## 1. Compiler & type system

**Target: make illegal states unrepresentable, so whole bug classes never reach a
test.** The strongly-typed layer is the first oracle. `rustc` and `edition = 2024`
across the workspace (`engine`, `benchmark`, `generative`, `testkit`) enforce
memory safety, exhaustive `match`, ownership/lifetime discipline, and `Result`
propagation for free on every build.

What we deliberately spend the type system on:

- **Newtypes over primitives** for the values that must not be confused — LSNs,
  primary keys, column identifiers, the exact-decimal `numeric` type. A raw `u64`
  LSN and a raw `u64` offset being distinct types is a correctness check that
  costs zero runtime.
- **Enums for closed vocabularies** (op kinds, outcome classification, AST node
  kinds) so a new variant forces every `match` to be revisited — the compiler
  becomes the checklist.
- **`#[non_exhaustive]` / sealed traits** where a public shape must stay evolvable.

What this tier does **not** do: it cannot check runtime values, Postgres
semantics, or timing. Everything below assumes the code already compiles.

## 2. Linters & format

**Target: a single, non-negotiable house style and a curated set of correctness
lints, enforced in CI so review never spends attention on them.** CI runs, and a
merge is blocked on:

- `cargo fmt --all -- --check` — formatting is settled by the tool, never by
  review.
- `cargo clippy --all-targets --all-features -- -D warnings` — **every clippy
  warning is a hard error.** This is where we catch the mechanical correctness
  smells clippy knows about (needless clones, incorrect comparison chains,
  fallible-conversion foot-guns, `.unwrap()` in paths that should propagate).

CI installs current stable Rust with `rustfmt` and `clippy` components via the
vendored `.github/actions/rust-toolchain` composite action (the org's Actions
policy allows only repo-local or allowlisted actions). This tier is style and
lint only; it makes no claim about behavior.

## 3. Unit tests

**Target: pure logic in isolation, at the function/module boundary, with no
Postgres.** Fast, deterministic, run on every `cargo test`. These own:

- The AST and parser — accept/reject, precedence, error messages.
- The `numeric` exact-decimal type — arithmetic, scale, comparison-by-value.
- The AST→`SELECT` printer (once it exists, #36) as pure string production,
  independent of running it.
- Dependency-order resolution, batch/fold bookkeeping, outcome classification —
  anything expressible as *input value in, expected value out*.

Unit tests are the cheapest place to pin a specific computed answer. They do
**not** touch replication, concurrency, or a real database — that is the next
tier up. When a generative or chaos failure is root-caused to a pure function, the
regression pin lands here.

## 4. Integration tests

**Target: real behavior against a real Postgres, scripted end-to-end.** These live
in `engine/tests/*` and lean on `testkit`, which owns a disposable cluster
(`initdb`/`postgres`/`pg_ctl` on a private socket, `wal_level=logical`, torn down
on `Drop`). CI provisions the Postgres server binaries so these run in the same
pipeline as everything else. They own:

- The logical-replication ingestion path end-to-end: source DML → pgoutput decode
  → staging → the tuple-marker and LSN-confirmation validation that stages 01/07
  already carry.
- **Scripted interleavings** — the specific, hand-authored concurrency and crash
  scenarios that must always hold: `testkit::crash::CrashGuard` (SIGKILL a child
  mid-drain) and `crash::OpenTransaction` (a straddling transaction across a batch
  boundary). These are exact, named, reproducible scenarios.
- Any behavior that needs a database but is a *fixed* case, not a generated one.

**This tier is the terminal home of every race bug found above it.** A finding
from generative or chaos testing is not considered fixed until it exists here (or
in tier 3) as a deterministic pin that fails before the fix and passes after. The
generative suite and the integration suite deliberately **share one action
vocabulary** so a scripted scenario is just a generated program with its draws
pinned.

## 5. Generative tests

**Target: the combinatorial correctness claim — byte-identical convergence to an
independent oracle, over random valid programs, with failures shrunk to a minimal
reproducing program.** This is the `generative` crate; its full architecture and
tradeoffs live in `docs/generative-test-suite.md`, and the build plan is the
tracking epic (salesforce-misc/trellis#2). In brief:

- **Trigger:** a randomly generated *valid* program — a schema, transform
  definitions, and a sequence of source mutations — driven through the real engine
  against a `testkit` cluster.
- **Judge:** **Postgres itself is the oracle.** Because the calculation grammar is
  committed (ADR-0004) to an immutable subset of PostgreSQL's operators and
  functions, the oracle renders each definition back to a `SELECT` (a per-row
  projection for 1-1, a `GROUP BY` for aggregates), runs it in the same cluster,
  and asserts the persisted target equals it. It shares no evaluation code with
  the engine, so it catches even a bug in the engine's own evaluator. The
  evaluator-driven `recompute` is retained as a secondary parity check.
- **Reproducibility:** at the *program* level, via shrinking to a minimal
  counterexample; seeds replay the program, and findings terminate as
  hand-minimized pins in tiers 3–4.
- **The properties it lights up as engine stages land:** per-op convergence,
  operation-error handling, idempotency (at-least-once delivery treated as
  exactly-once), order-insensitivity over commuting ops, read-your-own-writes via
  `await`, and — the hardest — non-idempotent aggregate delta convergence.

Generative testing runs in CI at low case counts (a healthy run is minutes, not
seconds — a "pass" in one second executed nothing), with an env override for deep
nightly runs. It assumes the system **reaches** quiescence; it judges *what the
answer is* at that quiescent point. It does not, and structurally cannot, judge
real-time timing, liveness, or long-horizon resource behavior — that is tier 6.

## 6. Chaos tests

**Target: everything that survives *without* quiescence and *without*
reproducibility.** Chaos is the black-box, wall-clock tier: continuous randomized
workload plus continuous operational faults against a running system, judged not
by an oracle snapshot but by **invariants over recorded history**. It is
non-deterministic by design and cannot block a merge on a coin flip.

### The dividing line: quiescence + reproducibility

**If a bug can be caught by "program + deterministic fault placement → quiesce →
compare to the oracle," and replayed from a seed, it belongs to tier 5 (or a pin
in tier 4). Chaos owns only what that mold cannot hold.**

#### What chaos must NOT cover — generative already owns it

- **Converged-state correctness / oracle equality.** This is generative's entire
  purpose. Chaos cannot quiesce on demand or shrink, so it cannot reliably assert
  byte-identical equality to a recompute — and it should not try. Asserting
  correctness is delegated *downward*.
- **Per-op convergence, idempotency, order-insensitivity, read-your-own-writes.**
  All are seeded, reproducible, oracle-judged properties. Re-rolling them under
  wall-clock randomness only adds flake, not coverage.
- **Fine-grained race exploration at *known* sites.** Tier 5's fault layer (layer
  3 in the generative doc) places **named failpoints** deterministically at the
  sites where races are known to live and judges at quiescence — reproducibly.
  Chaos re-rolling those same sites at random adds noise, not signal.

In short: chaos does not re-verify *what the answer is*. It assumes the lower
tiers pin that, and it periodically borrows the oracle once (below) to keep them
honest.

#### What chaos MUST cover — generative cannot

- **Emergent real-time races.** The interleavings you did not know to place a
  failpoint at. Only genuine wall-clock concurrency on real hardware surfaces the
  timing-dependent bug that no seeded schedule was written to hit.
- **Liveness and progress.** Generative asserts what the converged state *is*,
  taking for granted that convergence is *reached*. Chaos asserts convergence is
  still reached — and reached within a bound — under sustained disruption:
  no deadlock, no livelock, no permanent stall, no unbounded lag runaway.
- **Long-horizon degradation and resource safety.** Leaks and slow bleed only
  visible over hours of soak: memory growth, connection/file-handle leaks,
  replication-slot lag, WAL retention, disk consumption.
- **Operational and environmental faults not modeled as oracle identities.**
  Postgres restart/failover under live load, network partition and latency
  injection, disk-full, the OOM killer, clock skew, connection-pool exhaustion.
  (Process SIGKILL is *shared* — `testkit::CrashGuard` gives tiers 4–5 a
  deterministic version — but *sustained, compound, randomly-timed* operational
  failure is chaos alone.)
- **Compound fault overlap too large to enumerate or seed.** The cross-product of
  several faults landing in the same window, which no finite seed set covers.

### Architectural requirements for the chaos tier

Building this tier means standing up, alongside the generative crate's harness:

- **A continuous workload generator** — a steady, randomized stream of source
  mutations under real load, *reusing the generative `model`/`generate`
  vocabulary* so a chaos scenario is describable in the same terms and its
  findings translate into seeded programs.
- **A wall-clock fault scheduler** — coarse operational faults (restart, kill,
  partition, disk pressure) injected on random real-time timing. Distinct from the
  named, deterministic failpoints of tier 5; chaos operates at operational
  granularity, not code-site granularity.
- **A history recorder.** Because you cannot quiesce-and-compare, the run must
  record observable history — LSN/watermark progression, periodic target
  snapshots, worker lifecycle events, lag and resource metrics — so invariants can
  be checked *post hoc* over the timeline rather than at a single quiet point.
- **Invariant checkers over history**, split into:
  - *Safety* (must hold at every moment): no LSN/watermark regression; no target
    value that never corresponded to any real source state (no torn or phantom
    reads); no observable double-apply; monotonic progress.
  - *Liveness* (must hold eventually): once faults quiesce, the system converges
    within a bounded time; steady-state lag stays bounded under load; no permanent
    stall.
  - *Resource* (bounded over the whole run): slot lag, WAL, memory, and
    connections do not grow without bound.
- **A quiet-window convergence gate.** Periodically stop the workload, let the
  system settle, and run the generative **oracle once** as ground truth. This is
  the single place chaos reaches back to tier 5's oracle — it bridges "the
  invariants held throughout" to "and the answer is actually correct," without
  requiring quiescence on every step.
- **Non-reproducibility as a first-class output contract.** A chaos run does not
  *terminate* a bug; it *discovers* one. Every finding must be mined down into a
  deterministic tier-4 pin or a seeded tier-5 program before it counts as fixed —
  a seed that "reproduces" a race is not trusted, because it does not survive
  timing changes or harness refactors. Chaos is lead generation for the
  reproducible tiers.
- **Isolation and cadence.** Its own environment, bounded blast radius, nightly /
  pre-release schedule. It is never a per-commit merge gate.

---

## How the tiers compose

The tiers form a ratchet, and work flows in one direction when a bug is found:

1. Chaos (6) discovers an emergent failure over real time.
2. It is root-caused and reproduced as a seeded generative program (5) or, if it
   is not fundamentally combinatorial, directly as a scripted scenario (4).
3. The minimal case is pinned deterministically as an integration test (4) or,
   if it reduces to pure logic, a unit test (3).
4. Where possible, the shape is made unrepresentable so it cannot recur (1), or a
   lint is added so it is caught mechanically (2).

The higher a fix lands on that list, the cheaper and more permanent it is. The
design intent of every upper tier is to *shrink its own surface* by feeding
durable pins downward — so that over time the expensive, non-deterministic tiers
are exercising genuinely new ground, not re-finding what the fast tiers already
guard.
