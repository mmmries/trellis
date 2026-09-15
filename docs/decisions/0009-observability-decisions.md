---
status: accepted
date: 2026-09-15
deciders: Michael Ries (riesmmm@gmail.com)
consulted: 
informed:
---

# Observability Decisions

[docs/observability.md](../observability.md) laid out a first design pass for
metrics, logs/traces, and transform-status visibility (epic #49), and
collected a set of open questions it deliberately left unresolved. This ADR
settles those questions so the follow-on implementation issues (#51-#56) have
a fixed design to build against, and amends `docs/observability.md` in place
to mark them settled and point back here.

## Proposal

Settle six questions from `docs/observability.md`'s "Open questions" section,
plus the dependency choice its "Proposed dependencies" section left as a
proposal:

1. Dependencies: `metrics` + `metrics-exporter-prometheus`, and `tracing` +
   `tracing-opentelemetry` + `opentelemetry-otlp`.
2. End-to-end latency is keyed by terminal transform only.
3. Traces/spans are adopted as a first-class signal; per-hop and per-transform
   latency derive from span durations rather than a separate measurement.
4. Transform status reuses the existing `transform_definitions.status` field;
   no new schema.
5. The staging ring gets a cheap `staging_segments{state}` gauge instead of
   the originally-proposed per-transform `staging_ring_depth{transform}`
   gauge, which is dropped.
6. Histogram buckets are exponential, ~10ms-60s, one global default for every
   histogram.
7. The rollup job (#54) uses a 5-minute interval and 7-day retention, both
   configurable, storing raw buckets rather than pre-computed quantiles.

Each is discussed below with its rationale. None of this changes
`docs/observability.md`'s goals/non-goals or its two-subsystem split
(metrics vs. logs/traces) — it fills in the specifics that section left
undecided.

## Decisions

### 1. Dependencies: `metrics` facade, not `prometheus` directly

**Decision:** `metrics` + `metrics-exporter-prometheus` for the in-process
metrics registry and Prometheus text encoding; `tracing` +
`tracing-opentelemetry` + `opentelemetry-otlp` for logs/traces.

Both are pure in-process registry/encoder libraries — neither adds an HTTP
server to the core `trellis` crate.  `metrics-exporter-prometheus` has an
optional Cargo feature that bundles a Hyper listener; that feature is **not**
enabled here, preserving `docs/observability.md`'s "mountable handler, not a
bound port" design (`render_prometheus()` stays a plain function the operator
serves from their own HTTP stack — see `cli/src/commands/prometheus.rs`,
already a placeholder for exactly this).

The facade was chosen over depending on the `prometheus` crate directly
because issue #54's rollup job needs to read the *same* registry that
`render_prometheus()` (issue #53) renders from, to compute periodic
aggregates without a second, parallel recording path. `metrics` separates
"recording an observation" (via its `Recorder` trait, implemented once by
`metrics-exporter-prometheus`) from "reading the registry back out." Both
`render_prometheus()` and the rollup job can consume the registry through
`metrics`/`metrics-exporter-prometheus`'s own inspection surface, rather than
the rollup job reaching into the `prometheus` crate's concrete `Registry`/
`HistogramVec` types directly. That keeps the two readers decoupled from each
other's implementation and avoids hardwiring Prometheus's own type shapes
into code (the rollup table) that outlives any one exposition format.

### 2. End-to-end latency keying: terminal transform only

**Decision:** end-to-end latency is one histogram per **terminal (sink)
transform**, summed across every source feeding it — not a histogram per
source→sink pair.

`docs/observability.md`'s "What 'latency' means" section left this as an
open cardinality question. A DAG can have several sources feeding one sink;
keying by pair multiplies the metric's cardinality by fan-in for no operator
benefit most of the time — "how stale is this transform's output" is the
question an operator actually asks, and that's answered by the terminal
transform alone. Per-source breakdowns, if ever needed, are a debugging tool
better served by traces (see decision 3) than by a permanently-exported
high-cardinality metric series.

### 3. Traces vs. flat logs: spans are first-class

**Decision:** adopt `tracing` spans as a first-class signal for propagation.
A change moving source → hop → hop → apply is modeled as a span tree, and
per-hop/per-transform latency is *derived* from span durations rather than
instrumented a second time with independent timers.

This resolves the framing question `docs/observability.md`'s "Logs and
traces" section left open, and has two concrete downstream effects:

* It gates issue #56's design: #56 is a span-based instrumentation of
  `trellis/src`'s propagation path (source intake, staging, fold, apply),
  not a flat-log-plus-metrics design. As noted during research, there is no
  existing `log`/`tracing` usage anywhere in the `trellis` crate today, so
  this is greenfield instrumentation, not a conversion.
* It clarifies #51: the per-transform latency histogram is **populated from
  span/timing data captured during fold**, not a second, independently-timed
  measurement. Fold already has the origin timestamp in hand (see decision 5
  below) — a span covering a staged row's journey from that origin to its
  applied output gives the histogram its `.observe()` value directly from the
  span's duration, so the two signals (traces and the per-transform latency
  metric) stay consistent by construction instead of by convention.

### 4. Transform status storage: no new field

**Decision:** transform lifecycle status continues to live in the single
existing `transform_definitions.status` column — the `TransformStatus` enum
(`WaitingToBackfill | Backfilling | Live | Quarantined`,
`trellis/src/defs/model.rs`), persisted as text and check-constrained in
`V19__transform_status.sql`. No new schema is introduced for status.

This was an open question in `docs/observability.md`'s "Where transform
status is stored and read" bullet: whether the lifecycle status should live
alongside [ADR-0003](0003-quarantine-storage-and-api.md)'s quarantine model
or in its own row. It's already the same field: ADR-0003's whole-transform
fuse tier already writes `Quarantined` into this column today, so quarantine
and the backfill lifecycle are two arcs of one state machine sharing one
field, exactly as `docs/observability.md`'s "Transform status lifecycle"
diagram already depicts. ADR-0003's *column*-level quarantine tier
(`column_status`/`column_failures`) is intentionally a separate, finer-grained
mechanism that does not touch this field — a `live` transform can carry
individually paused columns without its overall `status` moving, so there is
no conflict between the two tiers sharing this column's semantics.

`WaitingToBackfill` and `Backfilling` exist in the enum today but no writer
sets them yet. Issue #55 wires the actual transitions: the `xmin`-fence wait
(`trellis/src/intake/publication.rs`, `Snapshot::settled_since`) and
backfill-enumeration path both become transitions on this same field, rather
than introducing a parallel status source.

### 5. Staging-ring metrics: drop the per-transform depth gauge, add a cheap segment-state gauge

**Decision:** the originally-proposed `staging_ring_depth{transform}` gauge
from `docs/observability.md`'s supporting-series list is **dropped as
literally specified**. It's replaced by two independent pieces:

* **Per-transform latency histogram** (#51/#52), computed at **apply
  completion**, not on a separate live-counter path. The origin timestamp
  already rides on every staged row — `src_changed: Option<SystemTime>` on
  `StagedChange::Cdc`/`Truncate` (`trellis/src/staging/append.rs`), sourced
  from the replication `Commit` event's `commit_time_micros`
  (`trellis/src/intake/mod.rs`) — and the apply path already groups folded
  changes by which transform(s) consume them (`compute()`'s `by_source` map
  in `trellis/src/staging/apply.rs`, downstream of fold). Tagging one
  histogram `.observe()` call per transform-group there is effectively free:
  it reuses data and grouping apply already computes, with no new I/O and no
  new join.
* **`staging_segments{state}` gauge** (new, cheap, system-level): a count of
  segments by `SegmentState` (`Active`/`Sealed`/`Draining`/`Drained`, from
  `trellis/src/staging/state.rs` and the `segments` registry table), read
  on-demand from existing segment metadata rather than incremented on the
  hot append/fold path.

The rejected gauge would have needed bookkeeping on the hot append/fold
path (increment on stage, decrement on fold) to stay live and per-transform —
a throughput risk not worth taking for what `docs/observability.md` itself
called a "supporting" series. It's also the wrong shape for how the ring
actually partitions: the staging ring is partitioned by source-table/key-hash
bucket (`trellis/migrations/V3__staging_ring.sql`,
`trellis/src/staging/state.rs`), not by transform, so a genuinely live
per-transform depth reading would need a join/aggregation the ring's own
layout doesn't offer for free. `staging_segments{state}` gives an operator
the same "is the ring backing up" signal at effectively zero cost, using data
that's already tracked for segment lifecycle management.

### 6. Histogram bucket boundaries

**Decision:** exponential buckets spanning roughly 10ms to 60s — matching
`docs/observability.md`'s stated "sub-second to tens-of-seconds propagation
range" — as a single global default applied to every histogram (per-transform
and end-to-end alike):

```
[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 60.0]  // seconds
```

Not configurable per transform in this pass. A single default keeps the
initial implementation (#51/#52) simple and every histogram directly
comparable; if a specific transform's latency profile turns out to need
different boundaries (much faster or slower than this range), that's a
targeted follow-up once real data justifies it, not a speculative knob added
up front.

### 7. Rollup interval and retention (issue #54)

**Decision:**

* **Interval:** 5 minutes.
* **Retention:** 7 days.
* **Storage shape:** raw histogram buckets, not pre-computed quantiles.
* Both interval and retention are **configurable** (env var or config), with
  the above as defaults.

`docs/observability.md`'s "Retention: Postgres rollup tables" section already
settled *where* this lives (`trellis.metric_rollup`, a pruned Postgres table
Trellis owns) and *why* (survives restarts, SQL-queryable, shared across
engine instances) but left the interval/retention numbers and the
bucket-vs-quantile storage question as "TBD." Storing raw buckets rather than
pre-computed quantiles is the more important half of this decision:
pre-computed quantiles (e.g. a stored p50/p99) can't be re-aggregated across
rollup periods or across engine instances after the fact — you can't average
two p99s into a valid p99. Raw buckets can be summed and re-queried with
`histogram_quantile`-style math at any later time and at any slice, which
directly serves `docs/observability.md`'s stated "self-retained history ...
to later analyze usage patterns and suggest optimizations" goal. The 5-minute/
7-day defaults are a reasonable starting point for an operator dashboard's
resolution vs. storage tradeoff; making both configurable means they can be
tuned without a schema change once real usage patterns are observed.

This decision doesn't itself create the migration — the rollup table lands
in the next available migration number when #54 is implemented (latest as of
this writing is `V21__column_quarantine.sql`, so the rollup table would be
`V22__...`).

## Options considered

Dependency choice (decision 1) was the one item here with a real alternative
weighed against it:

* **`prometheus` crate directly.** The obvious default for Prometheus text
  exposition, and what `docs/observability.md`'s "Proposed dependencies"
  section listed as an explicit alternative. Rejected because it would force
  issue #54's rollup job to either depend on `prometheus`'s own concrete
  registry/collector types to read back what #53 renders, or maintain a
  second, independently-recorded set of aggregates alongside the exposition
  registry — either way coupling two issues' implementations to one crate's
  internal type shapes more tightly than necessary.
* **`metrics` + `metrics-exporter-prometheus` (chosen).** A thin recording
  facade in front of a Prometheus-flavored exporter. Costs one extra crate in
  the dependency graph relative to using `prometheus` directly, in exchange
  for #53 (exposition) and #54 (rollup) both reading the same registry
  through the facade's `Recorder`/inspection traits instead of one hardcoding
  the other's concrete types.

The other six decisions were framing/design questions
`docs/observability.md` posed explicitly as open (end-to-end keying, traces
vs. flat logs, status storage location, staging-ring metrics shape, bucket
boundaries, rollup interval/retention/storage shape); each is a single
settled choice rather than a field of alternatives, so they're recorded above
under "Decisions" with their rationale rather than re-listed here.

## Related

* [ADR-0003](0003-quarantine-storage-and-api.md) — the quarantine storage and
  fuse model that decision 4 reuses `transform_definitions.status` alongside.
* [docs/observability.md](../observability.md) — the design doc this ADR
  settles the open questions from; amended in place to link back here.
* Issues #49 (epic), #50 (this ADR), #51/#52 (per-transform and end-to-end
  latency), #53 (Prometheus exposition), #54 (rollup job), #55 (backfill
  status wiring), #56 (span-based tracing instrumentation).
