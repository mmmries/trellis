//! An internal facade over the `metrics`/`metrics-exporter-prometheus`
//! in-process registry (issue #51, `docs/decisions/0009-observability-decisions.md`).
//!
//! Call sites elsewhere in this crate (`staging::apply::compute`, today)
//! record through the plain functions below rather than reaching for the
//! `metrics` crate's own macros/types directly — so a future change to the
//! recording backend (a different facade, a second exporter, richer label
//! sets) stays contained to this one module instead of touching every call
//! site.
//!
//! **What this module does not do** (see the ADR): persist rollups to
//! Postgres — that's `crate::rollup` (issue #54), which reads this module's
//! [`snapshot`] but owns the write/prune SQL itself, keeping this module a
//! pure registry facade with no Postgres dependency of its own. This module
//! builds and populates the in-process registry, and exposes it two ways:
//! issue #53's [`Metrics::render_prometheus`] (Prometheus text exposition,
//! obtained through [`crate::app::Trellis::metrics`] or
//! [`crate::blocking::BlockingTrellis::metrics`], matching
//! `docs/observability.md`'s `trellis.metrics().render_prometheus()` sketch),
//! and issue #54's [`snapshot`] (structured rows, for
//! `crate::rollup`'s periodic Postgres write — see that function's doc
//! comment for why it's built by parsing [`PrometheusHandle::render`]'s text
//! output rather than a lower-level structured API).
//!
//! ## Recorder installation
//!
//! `metrics`'s macros (`counter!`/`histogram!`/`gauge!`) record against
//! whichever [`metrics::Recorder`] is currently installed as the process's
//! *global* recorder — a single, process-wide registry, not one per
//! [`crate::app::Trellis`] instance, matching `docs/observability.md`'s "one
//! in-process registry" design. [`ensure_installed`] lazily builds a
//! [`metrics_exporter_prometheus::PrometheusRecorder`] (recorder + text
//! encoder only — the exporter's optional Hyper-listener feature is not
//! enabled in `Cargo.toml`, so this never binds a socket) and installs it
//! the first time any recording function in this module runs. Installation
//! is idempotent and best-effort: if a global recorder is already installed
//! (a second call racing the [`OnceLock`], or — someday — an embedder
//! installing its own before this crate's first call), later attempts
//! simply lose and every macro call below still records into *this*
//! module's own handle instead of whatever won, since nothing else in this
//! process installs a recorder yet. A real multi-installer story is out of
//! scope for this issue.

use std::collections::{BTreeMap, HashMap};
use std::sync::OnceLock;
use std::time::Duration;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

/// ADR-0009 decision 6: exponential bucket boundaries spanning ~10ms-60s
/// (`docs/observability.md`'s "sub-second to tens-of-seconds propagation
/// range"), applied as a single global default to every histogram this
/// crate records — per-transform latency today; end-to-end latency (issue
/// #52) is expected to reuse the same set rather than introduce its own.
pub const LATENCY_BUCKETS: &[f64] = &[
    0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 60.0,
];

/// Per-transform hop latency: time from a change becoming available at a
/// transform's input to its output being applied (`docs/observability.md`'s
/// "What 'latency' means"). Labeled `transform` — the transform's target
/// table name, the same identifier `staging::quarantine::resume_column`'s
/// `transform` parameter and `ApplyError::ColumnNotPaused`/`DefinitionNotLive`
/// already use, not a separate "transform name" field (there isn't one —
/// see [`crate::defs::ast::TransformDef`]).
const TRANSFORM_LATENCY_METRIC: &str = "trellis_transform_latency_seconds";

/// Throughput denominator for [`TRANSFORM_LATENCY_METRIC`]
/// (`docs/observability.md`'s "Recommended supporting counters/gauges so
/// the histograms are interpretable"): one increment per applied change,
/// recorded at the same call site as the latency observation.
const CHANGES_APPLIED_METRIC: &str = "trellis_changes_applied_total";

/// End-to-end latency: time from the *source* commit to the *terminal*
/// transform's apply (`docs/observability.md`'s "What 'latency' means",
/// ADR-0009 decision 2). Labeled `transform` — same convention as
/// [`TRANSFORM_LATENCY_METRIC`] — but only ever recorded for a transform
/// whose target has no downstream reader of its own (a DAG sink), never for
/// an intermediate hop: keyed by terminal transform only, summed across
/// every source feeding it, not by source->sink pair (issue #52).
const END_TO_END_LATENCY_METRIC: &str = "trellis_end_to_end_latency_seconds";

/// ADR-0009 decision 5's cheap, system-level gauge: a count of `segments`
/// rows by [`crate::staging::SegmentState`], labeled `state`.
const STAGING_SEGMENTS_METRIC: &str = "trellis_staging_segments";

/// The process-wide recorder handle, built and installed on first use. See
/// the module doc comment's "Recorder installation" section.
fn handle() -> &'static PrometheusHandle {
    static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
    HANDLE.get_or_init(|| {
        let builder = PrometheusBuilder::new()
            .set_buckets(LATENCY_BUCKETS)
            .expect("LATENCY_BUCKETS is non-empty and every boundary is finite");
        let recorder = builder.build_recorder();
        let handle = recorder.handle();
        // Best-effort install — see the module doc comment. `build_recorder`
        // (rather than `install`/`install_recorder`) is used deliberately:
        // those two are only compiled under the exporter's `http-listener`
        // feature, which this crate does not enable (no bound socket).
        let _ = metrics::set_global_recorder(recorder);
        describe_metrics();
        handle
    })
}

/// Registers a `# HELP` description for every metric this module records,
/// once, right after [`handle`] installs the global recorder. Without this,
/// `metrics-exporter-prometheus` still renders a `# TYPE` line per series
/// (inferred from the macro used to record it — `histogram!`/`counter!`/
/// `gauge!`) but omits `# HELP` entirely, since it has no description to
/// put there; issue #53's exposition is meant to be self-documenting for an
/// operator reading a raw scrape, so every series gets one.
fn describe_metrics() {
    metrics::describe_histogram!(
        TRANSFORM_LATENCY_METRIC,
        metrics::Unit::Seconds,
        "Time from a change becoming available at a transform's input to its output being \
         applied, labeled by transform."
    );
    metrics::describe_counter!(
        CHANGES_APPLIED_METRIC,
        "Count of changes applied, labeled by transform — the throughput denominator for \
         trellis_transform_latency_seconds."
    );
    metrics::describe_histogram!(
        END_TO_END_LATENCY_METRIC,
        metrics::Unit::Seconds,
        "Time from the source commit to a terminal (sink) transform's apply, labeled by that \
         terminal transform and summed across every source feeding it."
    );
    metrics::describe_gauge!(
        STAGING_SEGMENTS_METRIC,
        "Count of staging ring segments, labeled by state (active/sealed/draining/drained)."
    );
}

/// Ensures the registry is installed. Every recording function below calls
/// this too, so callers never need to call it explicitly — it's exposed
/// purely so something that wants the registry ready before its first
/// observation (an embedder, a test) can force that at a known point.
pub fn ensure_installed() {
    let _ = handle();
}

/// Records one observation of [`TRANSFORM_LATENCY_METRIC`] for `transform`.
/// Called from [`crate::staging::apply::compute`] once per applied change
/// that carries an origin timestamp (`FoldedChange::src_changed` is `None`
/// for a bare recompute trigger with no source change behind it — nothing
/// to measure latency against, so callers skip this and call only
/// [`increment_changes_applied`] for such a change).
pub fn record_transform_latency(transform: &str, latency: Duration) {
    ensure_installed();
    metrics::histogram!(TRANSFORM_LATENCY_METRIC, "transform" => transform.to_string())
        .record(latency.as_secs_f64());
}

/// Records one observation of [`END_TO_END_LATENCY_METRIC`] for `transform`
/// — issue #52. Called from [`crate::staging::apply::compute`] once per
/// applied change whose *consuming* transform is terminal (no downstream
/// reader — see [`crate::defs::catalog::transforms_for_source`]) and that
/// carries an origin timestamp, mirroring
/// [`record_transform_latency`]'s `src_changed` gate exactly: the value
/// observed is the same `now - src_changed` duration, just gated to
/// terminal transforms and recorded under a different metric name. Reuses
/// [`LATENCY_BUCKETS`], the same global bucket set every histogram in this
/// module shares (ADR-0009 decision 6) — no separate boundary set for this
/// metric.
pub fn record_end_to_end_latency(transform: &str, latency: Duration) {
    ensure_installed();
    metrics::histogram!(END_TO_END_LATENCY_METRIC, "transform" => transform.to_string())
        .record(latency.as_secs_f64());
}

/// Increments [`CHANGES_APPLIED_METRIC`] by one for `transform`. Called
/// once per applied change, at the same call site as
/// [`record_transform_latency`] (when that change carries an origin
/// timestamp) so the two series stay consistent.
pub fn increment_changes_applied(transform: &str) {
    ensure_installed();
    metrics::counter!(CHANGES_APPLIED_METRIC, "transform" => transform.to_string()).increment(1);
}

/// Sets [`STAGING_SEGMENTS_METRIC`] for `state` to `count` — ADR-0009
/// decision 5's cheap segment-state gauge, refreshed on-demand (today: once
/// per [`crate::client`]'s maintenance tick) rather than incremented on the
/// hot append/fold path.
pub fn set_staging_segments(state: &str, count: u64) {
    ensure_installed();
    metrics::gauge!(STAGING_SEGMENTS_METRIC, "state" => state.to_string()).set(count as f64);
}

/// A handle onto this process's in-process metrics registry — the public,
/// embedder-facing entry point for Prometheus exposition (issue #53).
///
/// Obtained via [`crate::app::Trellis::metrics`] (or
/// [`crate::blocking::BlockingTrellis::metrics`]), matching
/// `docs/observability.md`'s `trellis.metrics().render_prometheus()` sketch.
/// Carries no fields: per the module doc comment's "Recorder installation"
/// section, recording happens against one process-wide global registry, not
/// one scoped to a particular `Trellis` connection, so there's no per-instance
/// state to hold. It's a named type rather than a bare free function so the
/// `trellis.metrics().render_prometheus()` method chain reads naturally and
/// so a future addition (another export format, say) has an obvious home;
/// [`Metrics::new`] is public too since some callers (this crate's own
/// integration tests, `cli/src/commands/prometheus.rs`) read the registry
/// without going through a full [`crate::app::Trellis`] connection.
#[derive(Debug, Clone, Copy)]
pub struct Metrics {
    _private: (),
}

impl Metrics {
    /// Ensures the registry is installed (see [`ensure_installed`]) and
    /// returns a handle onto it. Cheap and side-effect-free beyond that
    /// first-call installation — safe to call as often as wanted.
    pub fn new() -> Self {
        ensure_installed();
        Self { _private: () }
    }

    /// Renders the registry's current contents in Prometheus text exposition
    /// format (`# HELP`/`# TYPE` lines followed by each series' samples).
    ///
    /// Just a `String` — no HTTP framework, no bound socket (ADR-0009
    /// decision 1; `docs/observability.md`'s "Exposition: a mountable
    /// handler, not a bound port"). The operator serves the result from
    /// their own HTTP stack's `/metrics` route, e.g. with the `text/plain;
    /// version=0.0.4` content type Prometheus's exposition format expects:
    ///
    /// ```no_run
    /// # async fn example(trellis: &trellis::Trellis) {
    /// let body = trellis.metrics().render_prometheus();
    /// // ...serve `body` from an axum/actix/hyper (or hand-rolled, as
    /// // `cli/src/commands/prometheus.rs` does) `/metrics` route...
    /// # }
    /// ```
    pub fn render_prometheus(&self) -> String {
        handle().render()
    }

    /// A structured snapshot of the registry's current contents — issue
    /// #54's read path for `crate::rollup`'s periodic Postgres write. See
    /// the free function [`snapshot`] for why this parses
    /// [`Self::render_prometheus`]'s text output rather than reading some
    /// lower-level structured API.
    pub fn snapshot(&self) -> Vec<MetricSample> {
        snapshot()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------
// Structured snapshot (issue #54): read path for `crate::rollup`
// ---------------------------------------------------------------------

/// A metric's shape, mirroring the three kinds `metrics`/
/// `metrics-exporter-prometheus` themselves record (ADR-0009 decision 1),
/// and the same three strings `V25__metric_rollup.sql`'s `metric_kind`
/// check constraint accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

impl MetricKind {
    /// The lowercase text this kind persists as — the same string
    /// Prometheus's own `# TYPE` line already carries (parsed back out of
    /// exactly that line by [`snapshot`]), and what
    /// `V25__metric_rollup.sql`'s `metric_kind` column stores.
    pub fn as_sql(self) -> &'static str {
        match self {
            MetricKind::Counter => "counter",
            MetricKind::Gauge => "gauge",
            MetricKind::Histogram => "histogram",
        }
    }
}

/// One metric series' current reading, as read back out of the registry by
/// [`snapshot`] — one entry per distinct `(name, labels)` pair the registry
/// currently holds. `crate::rollup::write_snapshot` turns each of these
/// into one `metric_rollup` row.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricSample {
    /// The metric's name, e.g. `trellis_transform_latency_seconds` — for a
    /// [`MetricKind::Histogram`] sample, the *base* name (never carrying a
    /// `_bucket`/`_sum`/`_count` suffix; those three lines are folded back
    /// into `buckets`/`sum`/`count` below rather than kept as separate
    /// samples).
    pub name: String,
    pub kind: MetricKind,
    /// This series' label set as `(key, value)` pairs, sorted by key for
    /// deterministic output independent of
    /// `metrics-exporter-prometheus`'s own internal iteration order —
    /// `crate::rollup::write_snapshot` JSON-encodes them in this same
    /// order.
    pub labels: Vec<(String, String)>,
    /// Set for [`MetricKind::Counter`]/[`MetricKind::Gauge`] samples
    /// (a counter's cumulative total, a gauge's instantaneous reading);
    /// `None` for [`MetricKind::Histogram`] (see `buckets`/`sum`/`count`
    /// below instead).
    pub value: Option<f64>,
    /// Set for [`MetricKind::Histogram`] samples only: `(le, cumulative_count)`
    /// pairs in ascending `le` order, excluding the implicit `+Inf` bucket
    /// every Prometheus histogram carries (`count` below already *is* that
    /// `+Inf` cumulative count — see `V25__metric_rollup.sql`'s column doc
    /// comment for why storing it twice would be redundant). Empty for
    /// `Counter`/`Gauge`.
    pub buckets: Vec<(f64, u64)>,
    /// Set for [`MetricKind::Histogram`] samples only.
    pub sum: Option<f64>,
    /// Set for [`MetricKind::Histogram`] samples only.
    pub count: Option<u64>,
}

/// A structured snapshot of the registry's current contents — issue #54's
/// read path for `crate::rollup`'s periodic Postgres write.
///
/// **Why this parses rendered text rather than reading a structured API.**
/// [`PrometheusHandle`] (the handle `metrics-exporter-prometheus` hands back
/// for the recorder installed as this process's global recorder — see the
/// module doc comment's "Recorder installation" section) exposes exactly
/// four public methods: `render`/`render_to_write` (Prometheus text) and
/// `render_protobuf`/`render_protobuf_to_write` (Prometheus protobuf, gated
/// behind the exporter's `protobuf` Cargo feature, off by default and not
/// enabled in this crate's `Cargo.toml`). There is no third, lower-level
/// "hand back the raw counters/gauges/histograms as Rust values" method —
/// every render path only ever produces an already-*encoded* exposition
/// payload; the structured registry/distribution types the encoder reads
/// from internally (`metrics_util::registry::Registry`,
/// `metrics_exporter_prometheus`'s own private `Distribution` type) are not
/// part of the handle's public surface.
///
/// Two ways to get structured data back out, then, weighed against each
/// other:
///
/// 1. **Enable the `protobuf` feature and decode `render_protobuf()`.**
///    Genuinely structured — it decodes into `MetricFamily` protobuf
///    messages with real bucket/counter/gauge fields, no text parsing
///    needed. Rejected here because the feature pulls in
///    `prost`/`prost-types`/`prost-build` — a build-time `.proto`
///    compilation step — for what this crate has otherwise kept a
///    dependency-light, no-build-script pair of crates (ADR-0009 decision 1
///    is explicit about that minimalism: "neither adds an HTTP server to
///    the core `trellis` crate", the same spirit that keeps the exporter's
///    own Hyper-listener feature off). That's a heavier cost than one read
///    path justifies, especially since this crate only ever emits four
///    known, simple metric shapes.
/// 2. **Parse `render()`'s text output (chosen here).** `render()`'s output
///    is already fully specified, stable text (the Prometheus [exposition
///    format](https://github.com/prometheus/docs/blob/main/content/docs/instrumenting/exposition_formats.md#text-format-details))
///    generated deterministically, by this same dependency, from the exact
///    same underlying registry `render_protobuf` would read — and the only
///    shapes this crate's four recording functions
///    ([`record_transform_latency`], [`record_end_to_end_latency`],
///    [`increment_changes_applied`], [`set_staging_segments`]) ever produce
///    are a small, fixed grammar (`# TYPE`/`# HELP` comments, a counter/
///    gauge sample line, a histogram's `_bucket`/`_sum`/`_count` sample
///    lines), not the general Prometheus text format's full generality. No
///    new dependency, no build step, and it reuses the exact rendering
///    [`Metrics::render_prometheus`] already calls — one encode path serves
///    both readers, matching ADR-0009 decision 1's rationale for choosing
///    the `metrics` facade in the first place ("both `render_prometheus()`
///    and the rollup job can consume the registry through
///    `metrics`/`metrics-exporter-prometheus`'s own inspection surface,
///    rather than the rollup job reaching into [a lower-level crate's] own
///    concrete... types directly").
///
/// Because of (2), this is **not** a general Prometheus text parser — it
/// doesn't handle quantile summaries, native histograms, or every corner of
/// the exposition format's grammar, only the shapes this module's own
/// recording functions ever emit.
pub fn snapshot() -> Vec<MetricSample> {
    parse_prometheus_text(&Metrics::new().render_prometheus())
}

/// Accumulates one histogram series' `_bucket`/`_sum`/`_count` lines
/// (encountered in whatever order [`parse_prometheus_text`] walks the
/// rendered text) before it's turned into one [`MetricSample`].
#[derive(Default)]
struct HistogramAccum {
    buckets: Vec<(f64, u64)>,
    sum: Option<f64>,
    count: Option<u64>,
}

/// The actual text -> [`MetricSample`] parser behind [`snapshot`]. Kept as
/// a free function taking the text directly (rather than a method that
/// re-renders internally) so unit tests can feed it fixed text without
/// depending on this process's shared global registry.
fn parse_prometheus_text(text: &str) -> Vec<MetricSample> {
    // Pass 1: every `# TYPE <name> <kind>` line, keyed by the *base* metric
    // name exactly as Prometheus's own `# TYPE` comment names it (for a
    // histogram, that's the name *without* a `_bucket`/`_sum`/`_count`
    // suffix — those suffixes only ever show up on the sample lines below).
    let mut kinds: HashMap<&str, MetricKind> = HashMap::new();
    for line in text.lines() {
        let Some(rest) = line.strip_prefix("# TYPE ") else {
            continue;
        };
        let Some((name, kind_str)) = rest.rsplit_once(' ') else {
            continue;
        };
        let kind = match kind_str {
            "counter" => MetricKind::Counter,
            "gauge" => MetricKind::Gauge,
            "histogram" => MetricKind::Histogram,
            // A quantile summary or another kind this module never records
            // (see the doc comment's "not a general parser" caveat).
            _ => continue,
        };
        kinds.insert(name, kind);
    }

    let mut samples = Vec::new();
    let mut histograms: BTreeMap<(String, Vec<(String, String)>), HistogramAccum> = BTreeMap::new();

    // Pass 2: every sample line (comments and blank separator lines
    // skipped), matched back against the `kinds` map built above.
    for line in text.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((name_and_labels, value_str)) = line.rsplit_once(' ') else {
            continue;
        };
        let Ok(value) = value_str.parse::<f64>() else {
            continue;
        };
        let (full_name, mut labels) = match name_and_labels.split_once('{') {
            Some((name, rest)) => {
                let body = rest.strip_suffix('}').unwrap_or(rest);
                (name, parse_labels(body))
            }
            None => (name_and_labels, Vec::new()),
        };

        // Counter/gauge: the sample's own name matches a TYPE line exactly.
        if let Some(&kind) = kinds.get(full_name) {
            labels.sort();
            samples.push(MetricSample {
                name: full_name.to_string(),
                kind,
                labels,
                value: Some(value),
                buckets: Vec::new(),
                sum: None,
                count: None,
            });
            continue;
        }

        // Histogram: the sample's name is the TYPE-declared base name plus
        // one of the three suffixes `recorder.rs` always writes together.
        for suffix in ["_bucket", "_sum", "_count"] {
            let Some(base) = full_name.strip_suffix(suffix) else {
                continue;
            };
            if kinds.get(base) != Some(&MetricKind::Histogram) {
                continue;
            }

            if suffix == "_bucket" {
                let Some(le_pos) = labels.iter().position(|(k, _)| k == "le") else {
                    break;
                };
                let (_, le_str) = labels.remove(le_pos);
                // The implicit +Inf bucket's cumulative count is exactly
                // the `_count` line's value — see `V25__metric_rollup.sql`'s
                // column doc comment for why keeping both would be
                // redundant.
                if le_str == "+Inf" {
                    break;
                }
                let Ok(le) = le_str.parse::<f64>() else {
                    break;
                };
                labels.sort();
                histograms
                    .entry((base.to_string(), labels))
                    .or_default()
                    .buckets
                    .push((le, value as u64));
            } else {
                labels.sort();
                let entry = histograms.entry((base.to_string(), labels)).or_default();
                if suffix == "_sum" {
                    entry.sum = Some(value);
                } else {
                    entry.count = Some(value as u64);
                }
            }
            break;
        }
    }

    for ((name, labels), accum) in histograms {
        let mut buckets = accum.buckets;
        buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
        samples.push(MetricSample {
            name,
            kind: MetricKind::Histogram,
            labels,
            value: None,
            buckets,
            sum: accum.sum,
            count: accum.count,
        });
    }

    samples
}

/// Splits a Prometheus label-list body (the text between `{` and `}`, e.g.
/// `transform="orders",le="0.25"`) into `(key, value)` pairs, unescaping
/// each value per the exposition format's own escaping rules (backslash,
/// quote, and newline — the same three `sanitize_label_value` escapes when
/// `metrics-exporter-prometheus` writes them). Splits only on commas
/// outside a quoted value, so an escaped comma or quote embedded in a label
/// value (a `transform` label is a target table name, and Postgres allows a
/// quoted identifier to contain almost anything) doesn't corrupt the split.
fn parse_labels(body: &str) -> Vec<(String, String)> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut escape_next = false;
    for c in body.chars() {
        if escape_next {
            current.push(c);
            escape_next = false;
            continue;
        }
        match c {
            '\\' if in_quotes => {
                current.push(c);
                escape_next = true;
            }
            '"' => {
                in_quotes = !in_quotes;
                current.push(c);
            }
            ',' if !in_quotes => parts.push(std::mem::take(&mut current)),
            _ => current.push(c),
        }
    }
    if !current.is_empty() {
        parts.push(current);
    }

    parts
        .into_iter()
        .filter_map(|part| {
            let (key, quoted) = part.split_once('=')?;
            let inner = quoted.strip_prefix('"')?.strip_suffix('"')?;
            Some((key.to_string(), unescape_label_value(inner)))
        })
        .collect()
}

/// Reverses `sanitize_label_value`'s escaping: `\n` -> a real newline, and
/// `\"`/`\\` -> a literal `"`/`\` (any other escaped character is passed
/// through as-is defensively, though `sanitize_label_value` never produces
/// one).
fn unescape_label_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some(other) => out.push(other),
            None => out.push('\\'),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use regex::Regex;

    use super::*;

    #[test]
    fn transform_latency_and_changes_applied_are_recorded_and_render() {
        record_transform_latency("metrics_facade_test_target", Duration::from_millis(120));
        increment_changes_applied("metrics_facade_test_target");

        let rendered = Metrics::new().render_prometheus();
        assert!(
            rendered.contains("trellis_transform_latency_seconds"),
            "rendered output missing the latency histogram: {rendered}"
        );
        assert!(
            rendered.contains("trellis_changes_applied_total"),
            "rendered output missing the throughput counter: {rendered}"
        );
        assert!(
            rendered.contains("metrics_facade_test_target"),
            "rendered output missing the transform label: {rendered}"
        );
    }

    #[test]
    fn end_to_end_latency_is_recorded_and_renders_with_the_shared_bucket_set() {
        record_end_to_end_latency("metrics_facade_test_terminal", Duration::from_millis(250));

        let rendered = Metrics::new().render_prometheus();
        assert!(
            rendered.contains("trellis_end_to_end_latency_seconds"),
            "rendered output missing the end-to-end latency histogram: {rendered}"
        );
        assert!(
            rendered.contains("metrics_facade_test_terminal"),
            "rendered output missing the transform label: {rendered}"
        );
        // ADR-0009 decision 6: this histogram reuses LATENCY_BUCKETS, the
        // same global default the per-transform histogram uses — spot-check
        // one boundary shared by both rather than asserting the whole set,
        // since `render_prometheus` renders every histogram's buckets
        // interleaved.
        assert!(
            rendered.contains("le=\"0.25\""),
            "rendered output missing a LATENCY_BUCKETS boundary: {rendered}"
        );
    }

    #[test]
    fn staging_segments_gauge_is_recorded_and_renders() {
        set_staging_segments("metrics_facade_test_state", 3);

        let rendered = Metrics::new().render_prometheus();
        assert!(
            rendered.contains("trellis_staging_segments"),
            "rendered output missing the segment-state gauge: {rendered}"
        );
        assert!(
            rendered.contains("metrics_facade_test_state"),
            "rendered output missing the state label: {rendered}"
        );
    }

    /// Issue #53's acceptance criteria calls for "a snapshot test on the
    /// rendered body." A byte-exact snapshot isn't a good fit here: the
    /// registry is one process-wide global (see the module doc comment's
    /// "Recorder installation" section) shared by every test in this binary,
    /// so the exact set/order of series `render_prometheus()` returns
    /// depends on whichever other tests happened to run first in this
    /// process — not something this test controls or should pin to. Instead
    /// this asserts the *shape* is valid Prometheus text exposition format:
    /// every metric this crate records gets a `# HELP`/`# TYPE` pair, and
    /// every non-comment sample line parses as `name{labels} value`.
    #[test]
    fn render_prometheus_produces_a_valid_exposition_format_body() {
        record_transform_latency("metrics_shape_test_target", Duration::from_millis(42));
        increment_changes_applied("metrics_shape_test_target");
        record_end_to_end_latency("metrics_shape_test_target", Duration::from_millis(84));
        set_staging_segments("metrics_shape_test_state", 7);

        let rendered = Metrics::new().render_prometheus();

        for metric in [
            TRANSFORM_LATENCY_METRIC,
            CHANGES_APPLIED_METRIC,
            END_TO_END_LATENCY_METRIC,
            STAGING_SEGMENTS_METRIC,
        ] {
            assert!(
                rendered.contains(&format!("# HELP {metric} ")),
                "rendered output missing a HELP line for {metric}: {rendered}"
            );
            assert!(
                rendered.contains(&format!("# TYPE {metric} ")),
                "rendered output missing a TYPE line for {metric}: {rendered}"
            );
        }

        // Shape-check every non-comment, non-blank line against the
        // exposition format's sample-line grammar (metric name, optional
        // `{label="value", ...}` block, whitespace, a value) — loose enough
        // to tolerate metrics-exporter-prometheus's own label/bucket
        // ordering, strict enough to catch a gross regression (labels or
        // values landing somewhere they shouldn't).
        let sample_line =
            Regex::new(r#"^[a-zA-Z_:][a-zA-Z0-9_:]*(\{[^}]*\})?\s+\S+$"#).expect("valid regex");
        for line in rendered.lines() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            assert!(
                sample_line.is_match(line),
                "line does not look like a valid Prometheus sample: {line:?}"
            );
        }

        assert!(
            rendered.contains("metrics_shape_test_target"),
            "rendered output missing the transform label: {rendered}"
        );
        assert!(
            rendered.contains("metrics_shape_test_state"),
            "rendered output missing the state label: {rendered}"
        );
    }

    // -------------------------------------------------------------------
    // Structured snapshot (issue #54)
    // -------------------------------------------------------------------

    /// Feeds [`parse_prometheus_text`] hand-written, fixed exposition text
    /// (not the shared global registry, which every other test in this
    /// binary also records into) so this test controls the input exactly
    /// and can assert an exact [`MetricSample`] shape back out, covering
    /// all three kinds in one pass.
    #[test]
    fn parse_prometheus_text_recovers_counter_gauge_and_histogram_samples() {
        let text = "\
# HELP snapshot_test_counter a counter
# TYPE snapshot_test_counter counter
snapshot_test_counter{transform=\"orders\"} 7
# HELP snapshot_test_gauge a gauge
# TYPE snapshot_test_gauge gauge
snapshot_test_gauge{state=\"active\"} 3
# HELP snapshot_test_hist a histogram
# TYPE snapshot_test_hist histogram
snapshot_test_hist_bucket{transform=\"orders\",le=\"0.01\"} 0
snapshot_test_hist_bucket{transform=\"orders\",le=\"0.1\"} 2
snapshot_test_hist_bucket{transform=\"orders\",le=\"1\"} 5
snapshot_test_hist_bucket{transform=\"orders\",le=\"+Inf\"} 5
snapshot_test_hist_sum{transform=\"orders\"} 3.5
snapshot_test_hist_count{transform=\"orders\"} 5
";

        let samples = parse_prometheus_text(text);

        let counter = samples
            .iter()
            .find(|s| s.name == "snapshot_test_counter")
            .expect("counter sample present");
        assert_eq!(counter.kind, MetricKind::Counter);
        assert_eq!(
            counter.labels,
            vec![("transform".to_string(), "orders".to_string())]
        );
        assert_eq!(counter.value, Some(7.0));
        assert!(counter.buckets.is_empty());
        assert_eq!(counter.sum, None);
        assert_eq!(counter.count, None);

        let gauge = samples
            .iter()
            .find(|s| s.name == "snapshot_test_gauge")
            .expect("gauge sample present");
        assert_eq!(gauge.kind, MetricKind::Gauge);
        assert_eq!(
            gauge.labels,
            vec![("state".to_string(), "active".to_string())]
        );
        assert_eq!(gauge.value, Some(3.0));

        let hist = samples
            .iter()
            .find(|s| s.name == "snapshot_test_hist")
            .expect("histogram sample present");
        assert_eq!(hist.kind, MetricKind::Histogram);
        assert_eq!(
            hist.labels,
            vec![("transform".to_string(), "orders".to_string())]
        );
        assert_eq!(hist.value, None);
        // The +Inf bucket is dropped: its cumulative count is exactly
        // `count` below, so keeping it as a fourth bucket would be
        // redundant (V25__metric_rollup.sql's column doc comment).
        assert_eq!(hist.buckets, vec![(0.01, 0), (0.1, 2), (1.0, 5)]);
        assert_eq!(hist.sum, Some(3.5));
        assert_eq!(hist.count, Some(5));
    }

    /// [`parse_labels`] must split only on commas *outside* a quoted value
    /// and correctly unescape a value that itself contains an escaped
    /// comma, quote, backslash, and newline — the four characters
    /// `sanitize_label_value`/`sanitize_description` (in
    /// `metrics-exporter-prometheus`) ever escape, and a plausible real
    /// value here: a `transform` label is a target table name, and Postgres
    /// allows a quoted identifier to contain almost any of them.
    #[test]
    fn parse_labels_handles_escaped_commas_quotes_backslashes_and_newlines() {
        let body =
            r#"transform="weird, name with \"quotes\", a \\backslash, and a \nnewline",le="0.25""#;
        let labels = parse_labels(body);
        assert_eq!(
            labels,
            vec![
                (
                    "transform".to_string(),
                    "weird, name with \"quotes\", a \\backslash, and a \nnewline".to_string()
                ),
                ("le".to_string(), "0.25".to_string()),
            ]
        );
    }

    /// End-to-end wiring check: [`snapshot`] (which calls
    /// [`Metrics::render_prometheus`] under the hood, same as
    /// [`Metrics::snapshot`]) recovers real observations recorded through
    /// this module's own public recording functions — not just
    /// hand-written fixture text. Distinctive labels, per this file's other
    /// tests' convention, since the registry is one shared global.
    #[test]
    fn snapshot_recovers_real_observations_recorded_through_this_module() {
        record_transform_latency("metrics_snapshot_test_target", Duration::from_millis(120));
        increment_changes_applied("metrics_snapshot_test_target");
        set_staging_segments("metrics_snapshot_test_state", 4);

        let samples = snapshot();

        let hist = samples
            .iter()
            .find(|s| {
                s.name == "trellis_transform_latency_seconds"
                    && s.labels
                        == vec![(
                            "transform".to_string(),
                            "metrics_snapshot_test_target".to_string(),
                        )]
            })
            .unwrap_or_else(|| panic!("no per-transform latency sample in {samples:?}"));
        assert_eq!(hist.kind, MetricKind::Histogram);
        assert_eq!(
            hist.count,
            Some(1),
            "exactly one observation for this test's own label"
        );
        assert_eq!(hist.sum, Some(0.12), "0.12s matches the 120ms observation");
        assert!(
            !hist.buckets.is_empty(),
            "the shared LATENCY_BUCKETS set must show up as raw buckets"
        );
        // Buckets are cumulative and ascending — every later bucket's count
        // must be >= every earlier one's.
        for pair in hist.buckets.windows(2) {
            assert!(
                pair[0].0 < pair[1].0,
                "bucket bounds must be strictly ascending: {:?}",
                hist.buckets
            );
            assert!(
                pair[0].1 <= pair[1].1,
                "cumulative bucket counts must be non-decreasing: {:?}",
                hist.buckets
            );
        }

        let counter = samples
            .iter()
            .find(|s| {
                s.name == "trellis_changes_applied_total"
                    && s.labels
                        == vec![(
                            "transform".to_string(),
                            "metrics_snapshot_test_target".to_string(),
                        )]
            })
            .unwrap_or_else(|| panic!("no changes_applied sample in {samples:?}"));
        assert_eq!(counter.kind, MetricKind::Counter);
        assert_eq!(counter.value, Some(1.0));

        let gauge = samples
            .iter()
            .find(|s| {
                s.name == "trellis_staging_segments"
                    && s.labels
                        == vec![(
                            "state".to_string(),
                            "metrics_snapshot_test_state".to_string(),
                        )]
            })
            .unwrap_or_else(|| panic!("no staging_segments sample in {samples:?}"));
        assert_eq!(gauge.kind, MetricKind::Gauge);
        assert_eq!(gauge.value, Some(4.0));
    }
}
