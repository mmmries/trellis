# Observability

How an operator running Trellis sees what the pipeline is doing: how fast
changes are propagating, where they're stuck, and what work is pending or
blocked. This is the counterpart to [data-flow](data-flow.md) — that document
describes the flow; this one describes how we *measure* it.

This is a first design pass; settled decisions are stated as such, and open
ones are collected under [Open questions](#open-questions).

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
  live — the right-sized answer to the silent-stall problem in
  [#52](#backfill-status-and-the-xmin-caveat).

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
`engine/src/intake/mod.rs`). Every propagated change can be timestamped at each
stage relative to that origin. Two families of measurement follow:

* **Per-transform latency** — time from a change becoming available at a
  transform's input to its output being applied. One histogram per transform.
* **End-to-end latency** — time from the *source* commit to the *final*
  transform's apply. For a DAG this is the whole-chain rollup, keyed by the
  terminal transform (or by source→sink pair — see open questions).

Recommended supporting counters/gauges so the histograms are interpretable:

* `changes_applied_total{transform}` — throughput denominator.
* `staging_ring_depth{transform}` — backlog depth (ties to
  [the staging ring](staging-and-claiming/02-the-staging-ring.md)).

Backfill progress is deliberately *not* a metric — it's the transform's
[lifecycle status](#backfill-status-and-the-xmin-caveat), a small enumerable
state rather than a counter.

### Quantiles via histograms, not summaries

**Recommendation: histograms.** Median and p99 are computed at query time from
exported bucket counts (`histogram_quantile` in PromQL), rather than
client-computed summary quantiles. Histograms **aggregate across instances**;
summaries do not. Since a Trellis deployment may run more than one engine
process against the same cluster, aggregatability matters. The cost is choosing
bucket boundaries up front — we'll seed them from the expected sub-second to
tens-of-seconds propagation range and revisit.

### Exposition: a mountable handler, not a bound port

The library stays HTTP-agnostic. It exposes a render function over its registry;
the operator serves the result from their own HTTP stack:

```rust
let body = trellis.metrics().render_prometheus(); // Prometheus text format
// operator serves `body` from their own axum/actix/hyper /metrics route
```

No port binding, no HTTP framework, no bind-address config; wiring costs the
operator a few lines.

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
defaults TBD (see open questions).

## Logs and traces

Logs go through the **`tracing`** facade with an optional **OTLP export layer**,
so operators who run an OpenTelemetry collector get compliant output and those
who don't still get structured local logs.

Open framing question worth resolving early: a change flowing source → hop → hop
→ apply *is* a trace. Modeling propagation as **spans** would make the pipeline's
shape observable and could carry the per-hop latency data for free — potentially
letting the metrics histograms be *derived from* span durations rather than
instrumented separately. Whether we commit to spans/traces as a first-class
signal, or keep logs flat and instrument metrics independently, is open below.

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
`engine/src/intake/publication.rs`). Because `xmin` is **cluster-global**, any
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

## Proposed dependencies

None added yet — listed here as **proposals to approve**, per the no-auto-install
rule:

* Metrics registry + Prometheus text rendering (e.g. the `metrics` facade +
  `metrics-exporter-prometheus`, or the `prometheus` crate directly).
* `tracing` + an OTLP export layer (e.g. `tracing-opentelemetry` +
  `opentelemetry-otlp`) for logs/traces.

We'll pin exact crates and versions when we start implementation.

## Open questions

* **End-to-end keying for DAGs** — is end-to-end latency keyed by terminal
  transform, by source→sink pair, or both? Affects cardinality.
* **Histogram buckets** — the initial boundary set, and whether they're
  configurable per transform.
* **Traces vs. flat logs** — do we adopt spans/traces as a first-class signal
  (and derive latency from them), or keep logs flat and instrument metrics
  separately?
* **Rollup interval and retention window defaults**, and whether the rollup is
  raw histogram buckets or pre-computed quantiles (pre-computed quantiles are
  not re-aggregatable later).
* **Where transform status is stored and read** — does the lifecycle status live
  alongside the [ADR-0003](decisions/0003-quarantine-storage-and-api.md)
  quarantine model or in its own transform-registry row, and is it exposed via
  the same client read that lists quarantined transforms?

## Related

* [data-flow](data-flow.md) — the flow these metrics measure.
* [open-questions](open-questions.md#backfill-status-and-observability) — the
  pre-existing backfill-status/lag-telemetry question this doc subsumes.
* [#52](https://github.com/spiff-emu/trellis/issues/52) — the motivating stall.
