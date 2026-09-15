# Observability

How an operator running Trellis sees what the pipeline is doing: how fast
changes are propagating, where they're stuck, and what work is pending or
blocked. This is the counterpart to [data-flow](data-flow.md) — that document
describes the flow; this one describes how we *measure* it.

This is a first design pass; settled decisions are stated as such. The
questions originally collected under [Open questions](#open-questions) are
now all settled — see [ADR-0009](decisions/0009-observability-decisions.md)
for the decisions and their rationale.

## Goals and non-goals

**Goals**

* **Latency visibility** — median and p99 for each individual transform, plus
  median and p99 for the end-to-end path (a source commit propagating through
  the whole chain of transforms to its final apply).
* **Pull-based export** — those metrics exposed in Prometheus text format so
  operators can scrape them into whatever they already run for dashboards and
  alerting.
* **Self-retained history** — Trellis keeps a time-bounded rollup of its own
  metrics independent of any external scraper, so it can later analyze usage
  patterns and suggest optimizations.
* **Structured logs** — emitted through a facade that can export in
  OpenTelemetry form.
* **Transform status** — every transform carries an observable lifecycle status
  (`waiting_to_backfill` → `backfilling` → `live`, plus `quarantined`), so an
  operator can see a newly-defined transform is still populating rather than
  live — the right-sized answer to the silent-stall problem (#14; see the
  [`xmin` caveat below](#backfill-status-and-the-xmin-caveat)).

**Non-goals (for this pass)**

* Being a metrics *backend*. Trellis exposes and briefly retains; it does not
  replace Prometheus/Grafana/etc.
* Distributed tracing across the *application's* code. We instrument Trellis's
  own pipeline, not the caller's write path.

## The two subsystems

Metrics and logs are deliberately **separate subsystems** rather than one
unified telemetry pipeline. Each is idiomatic on its own and they evolve
independently:

```
metrics ──► in-process registry ──┬─► render_prometheus()  (operator scrapes)
                                   └─► periodic rollup ──► trellis.metric_rollup
                                                            (pruned to retention window)

logs/spans ──► `tracing` facade ──► optional OTLP export layer
```

## Metrics

### What "latency" means

Trellis has a natural clock already flowing in: the **source commit timestamp**
rides in on the replication `Commit` event (`commit_time_micros`, see
`trellis/src/intake/mod.rs`). Every propagated change can be timestamped at each
stage relative to that origin. Two families of measurement follow:

* **Per-transform latency** — time from a change becoming available at a
  transform's input to its output being applied. One histogram per transform.
* **End-to-end latency** — time from the *source* commit to the *final*
  transform's apply. For a DAG this is the whole-chain rollup, keyed by the
  **terminal transform** — settled in
  [ADR-0009](decisions/0009-observability-decisions.md#2-end-to-end-latency-keying-terminal-transform-only),
  not by source→sink pair, to keep cardinality low.

Recommended supporting counters/gauges so the histograms are interpretable:

* `changes_applied_total{transform}` — throughput denominator.
* `staging_segments{state}` — a cheap, system-level gauge counting segments
  by state (ties to [the staging ring](staging-and-claiming/02-the-staging-ring.md)).
  This replaces the per-transform `staging_ring_depth{transform}` gauge
  originally proposed here, which [ADR-0009](decisions/0009-observability-decisions.md#5-staging-ring-metrics-drop-the-per-transform-depth-gauge-add-a-cheap-segment-state-gauge)
  drops in favor of this lower-cost alternative.

Backfill progress is deliberately *not* a metric — it's the transform's
[lifecycle status](#backfill-status-and-the-xmin-caveat), a small enumerable
state rather than a counter.

### Quantiles via histograms, not summaries

**Recommendation: histograms.** Median and p99 are computed at query time from
exported bucket counts (`histogram_quantile` in PromQL), rather than
client-computed summary quantiles. Histograms **aggregate across instances**;
summaries do not. Since a Trellis deployment may run more than one engine
process against the same cluster, aggregatability matters. The cost is choosing
bucket boundaries up front — settled in
[ADR-0009](decisions/0009-observability-decisions.md#6-histogram-bucket-boundaries)
as a single global exponential set spanning ~10ms-60s (the expected
sub-second to tens-of-seconds propagation range), not configurable per
transform in this pass.

### Exposition: a mountable handler, not a bound port

The library stays HTTP-agnostic. It exposes a render function over its registry;
the operator serves the result from their own HTTP stack:

```rust
let body = trellis.metrics().render_prometheus(); // Prometheus text format
// operator serves `body` from their own axum/actix/hyper /metrics route
```

No port binding, no HTTP framework, no bind-address config; wiring costs the
operator a few lines.

**Implemented (issue #53).** [`Trellis::metrics`](../trellis/src/app.rs) (and
[`BlockingTrellis::metrics`](../trellis/src/blocking.rs), for callers without
a `tokio` runtime of their own) returns a
[`trellis::metrics::Metrics`](../trellis/src/metrics.rs) handle whose
[`render_prometheus`](../trellis/src/metrics.rs) method is exactly the
`String`-returning call sketched above — no HTTP framework or bound socket
inside the `trellis` crate itself, per ADR-0009 decision 1. A minimal
end-to-end example, an axum-style `/metrics` handler mounted alongside a
running engine:

```rust,no_run
# async fn example(trellis: std::sync::Arc<trellis::Trellis>) -> String {
// A route handler in the operator's own HTTP stack, closing over the
// running `Trellis` (or just calling `trellis::metrics::Metrics::new()`
// directly — the registry is process-wide, not scoped to one `Trellis`
// connection, so any in-process handle reaches the same data).
trellis.metrics().render_prometheus()
# }
```

`cli/src/commands/prometheus.rs` is a second, complete (if deliberately
minimal — no HTTP-parsing crate, see its module doc comment) example: a
`trellis prometheus [--bind <ADDR>]` subcommand that hand-rolls a small
TCP listener and answers every request with `Metrics::new().render_prometheus()`
as a `200 OK`, `Content-Type: text/plain; version=0.0.4; charset=utf-8`
response — worth reading as a template for wiring this into a real HTTP
stack, though note its own doc comment's caveat: run standalone (its only
mode), it renders *its own* process's registry, which stays empty unless
that same process is also running the engine. A real deployment mounts
`render_prometheus()` from inside the process actually running
[`Trellis`]/[`Client`] (`staging`/`drain_threads` set), not from a separate
scrape-only binary.

### Retention: Postgres rollup tables

Trellis keeps its own history in a **pruned Postgres table** it owns (working
name `trellis.metric_rollup`), written by a periodic rollup job and trimmed to a
configurable retention window:

```
metrics ──► rollup every N minutes ──► trellis.metric_rollup ──► prune > retention_window
```

Chosen over an in-memory ring (which a restart wipes) and an embedded on-disk
store (a new storage dependency) because it **reuses the Postgres schema Trellis
already owns**, survives restarts, is directly SQL-queryable for the future
"suggest optimizations" use case, and is naturally shared across engine
instances. The costs we accept: added write load, one more schema object, and a
prune job on the DB. Rollup interval and retention window are configurable;
defaults are settled in
[ADR-0009](decisions/0009-observability-decisions.md#7-rollup-interval-and-retention-issue-54):
5-minute rollup interval, 7-day retention, storing raw histogram buckets
(not pre-computed quantiles) so history stays re-aggregatable.

## Logs and traces

Logs go through the **`tracing`** facade with an optional **OTLP export layer**,
so operators who run an OpenTelemetry collector get compliant output and those
who don't still get structured local logs.

A change flowing source → hop → hop → apply *is* a trace. Settled in
[ADR-0009](decisions/0009-observability-decisions.md#3-traces-vs-flat-logs-spans-are-first-class):
we adopt **spans** as a first-class signal, modeling propagation as a
`tracing` span tree. This makes the pipeline's shape observable and carries
the per-hop latency data for free — the per-transform latency histogram is
*derived from* span durations captured during apply (downstream of fold,
where changes are already grouped by the transform(s) that consume them),
rather than instrumented independently. This gates issue #56's design
(span-based instrumentation of the propagation path).

## Transform status lifecycle

Rather than instrument the backfill with bespoke metrics and log events, every
transform carries an observable **status**. This is the same lever quarantine
already uses — [ADR-0003](decisions/0003-quarantine-storage-and-api.md) marks a
fused transform `quarantined` and resumes it by re-running the *same* backfill —
so backfill and quarantine are two arcs of one lifecycle:

```
(new transform)──► waiting_to_backfill ──► backfilling ──► live
                          ▲                                  │
                          │ (resume re-runs backfill)        │ (fuse trips)
                          └──────────── quarantined ◄─────────┘
```

* **`waiting_to_backfill`** — the transform is defined and its backfill marker
  is durable, but the pre-existing rows haven't been enumerated yet. This is
  where a transform sits while its transaction fence is unsettled (see the
  caveat below).
* **`backfilling`** — the pre-existing source rows are being enumerated and
  staged.
* **`live`** — backfill is complete; the transform is tracking live changes
  only. This is the steady state.
* **`quarantined`** — the fuse has tripped
  ([ADR-0003](decisions/0003-quarantine-storage-and-api.md)); resuming drops the
  transform back to `waiting_to_backfill` and re-runs the backfill.

### Backfill status and the `xmin` caveat

Adding a source table triggers a backfill of its pre-existing rows, gated on a
conservative transaction-fence settlement (`now.xmin > fence.xmax`,
`trellis/src/intake/publication.rs`). Because `xmin` is **cluster-global**, any
unrelated long-running transaction *anywhere in the cluster* pins it and holds
every waiting backfill in `waiting_to_backfill` until that transaction commits or
aborts.

This wait is **safe, not a fault**: the streaming apply loop and every already-
`live` transform are unaffected, and even the new table's *new* changes stream
through — only its *historical* rows are withheld until the fence settles. So we
deliberately do **not** emit a stall metric, a periodic warning log, or a
fail-loud timeout for it. The `waiting_to_backfill` status is the whole signal.

The remedy is documentation, not a signal: a transform stays in
`waiting_to_backfill` as long as a long-lived transaction pins the cluster's
`xmin`, and an operator clears it by clearing that transaction —
idle-in-transaction connections, long analytics queries, `pg_dump`, or workload
on another database sharing the cluster.

## Approved dependencies

None added to `Cargo.toml` yet (that's issue #51/#56's job, not this doc's),
but the choice itself is now approved — see
[ADR-0009](decisions/0009-observability-decisions.md#1-dependencies-metrics-facade-not-prometheus-directly)
for the full rationale, including why the `metrics` facade was chosen over
depending on the `prometheus` crate directly:

* `metrics` + `metrics-exporter-prometheus` for the in-process metrics
  registry and Prometheus text rendering. `metrics-exporter-prometheus`'s
  optional Hyper-listener feature will **not** be enabled — this stays a pure
  registry + text-encoder, matching "Exposition: a mountable handler, not a
  bound port" above.
* `tracing` + `tracing-opentelemetry` + `opentelemetry-otlp` for logs/traces.

Exact versions will be pinned when implementation starts.

## Open questions

All settled by [ADR-0009](decisions/0009-observability-decisions.md):

* **End-to-end keying for DAGs** — is end-to-end latency keyed by terminal
  transform, by source→sink pair, or both? Affects cardinality. **Settled:**
  [terminal transform only](decisions/0009-observability-decisions.md#2-end-to-end-latency-keying-terminal-transform-only).
* **Histogram buckets** — the initial boundary set, and whether they're
  configurable per transform. **Settled:**
  [a single global exponential set, ~10ms-60s, not configurable per transform](decisions/0009-observability-decisions.md#6-histogram-bucket-boundaries).
* **Traces vs. flat logs** — do we adopt spans/traces as a first-class signal
  (and derive latency from them), or keep logs flat and instrument metrics
  separately? **Settled:**
  [spans are first-class; per-transform latency derives from span durations](decisions/0009-observability-decisions.md#3-traces-vs-flat-logs-spans-are-first-class).
* **Rollup interval and retention window defaults**, and whether the rollup is
  raw histogram buckets or pre-computed quantiles (pre-computed quantiles are
  not re-aggregatable later). **Settled:**
  [5-minute interval, 7-day retention, raw buckets — both configurable](decisions/0009-observability-decisions.md#7-rollup-interval-and-retention-issue-54).
* **Where transform status is stored and read** — does the lifecycle status live
  alongside the [ADR-0003](decisions/0003-quarantine-storage-and-api.md)
  quarantine model or in its own transform-registry row, and is it exposed via
  the same client read that lists quarantined transforms? **Settled:**
  [the existing `transform_definitions.status` field — no new schema](decisions/0009-observability-decisions.md#4-transform-status-storage-no-new-field).

Additionally, the staging-ring supporting-gauge design (originally
`staging_ring_depth{transform}`, above) is
[settled](decisions/0009-observability-decisions.md#5-staging-ring-metrics-drop-the-per-transform-depth-gauge-add-a-cheap-segment-state-gauge)
in favor of a cheap `staging_segments{state}` gauge.

## Related

* [data-flow](data-flow.md) — the flow these metrics measure.
* [open-questions](open-questions.md#backfill-status-and-observability) — the
  pre-existing backfill-status/lag-telemetry question this doc subsumes.
* [#14](https://github.com/salesforce-misc/trellis/issues/14) — the motivating stall.
