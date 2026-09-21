//! Text-scraping helpers for `trellis::metrics::Metrics::render_prometheus()`
//! (issue #266's B0 prerequisite (d)): no bucket-boundary changes, no raw
//! sample recording, no new histograms — the issue's targets are stated at
//! exactly `trellis::metrics::LATENCY_BUCKETS`' existing boundaries so a
//! cumulative-bucket-fraction read off a scrape answers them with no
//! interpolation and no estimator. This module's `bucket_count`/`total_count`
//! shape mirrors `trellis/tests/end_to_end_latency.rs` and
//! `end_to_end_latency_aggregate.rs`'s own private helpers of the same name
//! (this crate can't import those — they're private to another crate's test
//! binaries — so this is a deliberate, small re-implementation, not a
//! divergent one).
//!
//! **Bucket label format**: confirmed by rendering a real histogram rather
//! than guessed — `metrics-exporter-prometheus` renders a whole-number `f64`
//! bucket bound with no trailing `.0` (`le="1"`, not `le="1.0"`), which is
//! why [`LE_MAX`] is `"1"` below.
//!
//! **Why diff against a baseline at all**: `Metrics`'s registry is a
//! process-wide global (`trellis::metrics::handle`, `OnceLock`), not
//! per-`Client`/per-`TestCluster`. A benchmark that starts more than one
//! `Client` in one process — e.g. a hop-depth ladder looping over several
//! depths — sees every prior depth's counts still sitting in the same
//! series. [`HistogramSnapshot::since`] subtracts a scrape taken right
//! before a measurement window from one taken right after, isolating that
//! window's own contribution regardless of what ran earlier in the same
//! process. Every histogram/counter here is cumulative (Prometheus convention:
//! bucket counts and `_count`/`_total` only ever increase), so a plain
//! subtraction is exact, not an approximation.

use std::collections::HashMap;

/// The `le` label string for T1's p50 boundary (`<= 250ms`).
pub const LE_P50: &str = "0.25";
/// The `le` label string for T1's p99 boundary (`<= 500ms`).
pub const LE_P99: &str = "0.5";
/// The `le` label string for T1's "reliably < 1s" boundary.
pub const LE_MAX: &str = "1";

/// The three T1 boundaries this module reads, in scrape order.
pub const T1_BOUNDS: [&str; 3] = [LE_P50, LE_P99, LE_MAX];

/// One histogram series' cumulative bucket counts (only the bounds asked
/// for — not every bound the histogram carries) plus its total sample
/// count, read from one `render_prometheus()` scrape for one
/// `metric{transform="..."}` series.
#[derive(Debug, Clone, Default)]
pub struct HistogramSnapshot {
    buckets: HashMap<String, u64>,
    pub count: u64,
}

impl HistogramSnapshot {
    /// Captures `metric{transform="transform"}`'s cumulative count in each
    /// of `bounds`, plus its `_count` total, from `rendered`. A bound this
    /// transform/metric pair has never observed anything under still
    /// captures as `0` (a present-but-zero bucket line), not absent — see
    /// [`bucket_count`].
    pub fn capture(rendered: &str, metric: &str, transform: &str, bounds: &[&str]) -> Self {
        let buckets = bounds
            .iter()
            .map(|le| {
                (
                    (*le).to_string(),
                    bucket_count(rendered, metric, transform, le),
                )
            })
            .collect();
        Self {
            buckets,
            count: total_count(rendered, metric, transform),
        }
    }

    /// This snapshot's contribution since `baseline` — see this module's
    /// doc comment on why a diff, not the raw scrape, is what a benchmark
    /// almost always wants.
    pub fn since(&self, baseline: &HistogramSnapshot) -> HistogramSnapshot {
        let buckets = self
            .buckets
            .iter()
            .map(|(le, count)| {
                let base = baseline.buckets.get(le).copied().unwrap_or(0);
                (le.clone(), count.saturating_sub(base))
            })
            .collect();
        HistogramSnapshot {
            buckets,
            count: self.count.saturating_sub(baseline.count),
        }
    }

    /// The cumulative count captured for bound `le` (`0` if `le` wasn't
    /// asked for at capture time, or was genuinely never observed).
    pub fn bucket(&self, le: &str) -> u64 {
        self.buckets.get(le).copied().unwrap_or(0)
    }
}

/// The cumulative count in one histogram bucket (`le="<bound>"`) for
/// `metric{transform="target"}`, `0` if the series/bucket line isn't present
/// at all (rather than panicking) — a scrape taken before anything has ever
/// been observed for a fresh transform name legitimately has no such line.
fn bucket_count(rendered: &str, metric: &str, transform: &str, le: &str) -> u64 {
    let bucket_metric = format!("{metric}_bucket");
    rendered
        .lines()
        .find(|line| {
            line.starts_with(&bucket_metric)
                && line.contains(&format!("transform=\"{transform}\""))
                && line.contains(&format!("le=\"{le}\""))
        })
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// The histogram's total sample count (`<metric>_count{transform="target"}`),
/// `0` if absent.
fn total_count(rendered: &str, metric: &str, transform: &str) -> u64 {
    let count_metric = format!("{metric}_count");
    rendered
        .lines()
        .find(|line| {
            line.starts_with(&count_metric) && line.contains(&format!("transform=\"{transform}\""))
        })
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// A plain counter's current value for one `metric{transform="target"}`
/// series (e.g. `trellis_changes_applied_total`) — same scrape convention,
/// no `_bucket`/`_count` suffix since a counter has neither. `0` if absent.
pub fn counter_value(rendered: &str, metric: &str, transform: &str) -> u64 {
    rendered
        .lines()
        .find(|line| {
            line.starts_with(metric) && line.contains(&format!("transform=\"{transform}\""))
        })
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// T1's three targets (issue #266), evaluated as cumulative-bucket-fraction
/// pass/fail against one histogram window. Deliberately reports fractions,
/// not an interpolated percentile — the issue's own "Two consequences to
/// accept up front" section: "'p99 ≤ 500 ms' is answerable; 'our p99 is
/// 380 ms' is not."
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct T1Evaluation {
    /// Total end-to-end observations in the window. `0` means nothing was
    /// observed at all — check `commits_issued`/`changes_applied` before
    /// trusting a pass here, per the issue's own sanity-check requirement.
    pub count: u64,
    /// `bucket(le=0.25) / count` — target: `>= 0.50`.
    pub p50_frac: f64,
    /// `bucket(le=0.5) / count` — target: `>= 0.99`.
    pub p99_frac: f64,
    /// `bucket(le=1) == count` — every observation landed under 1s.
    pub max_ok: bool,
    pub p50_pass: bool,
    pub p99_pass: bool,
}

impl T1Evaluation {
    pub fn evaluate(window: &HistogramSnapshot) -> Self {
        let count = window.count;
        if count == 0 {
            return Self {
                count: 0,
                p50_frac: 0.0,
                p99_frac: 0.0,
                max_ok: false,
                p50_pass: false,
                p99_pass: false,
            };
        }
        let p50_frac = window.bucket(LE_P50) as f64 / count as f64;
        let p99_frac = window.bucket(LE_P99) as f64 / count as f64;
        let max_ok = window.bucket(LE_MAX) == count;
        Self {
            count,
            p50_frac,
            p99_frac,
            max_ok,
            p50_pass: p50_frac >= 0.50,
            p99_pass: p99_frac >= 0.99,
        }
    }

    pub fn all_pass(&self) -> bool {
        self.count > 0 && self.p50_pass && self.p99_pass && self.max_ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "\
trellis_end_to_end_latency_seconds_bucket{transform=\"t\",le=\"0.25\"} 5\n\
trellis_end_to_end_latency_seconds_bucket{transform=\"t\",le=\"0.5\"} 9\n\
trellis_end_to_end_latency_seconds_bucket{transform=\"t\",le=\"1\"} 10\n\
trellis_end_to_end_latency_seconds_count{transform=\"t\"} 10\n\
trellis_changes_applied_total{transform=\"t\"} 10\n";

    #[test]
    fn captures_bucket_and_total_counts() {
        let snap = HistogramSnapshot::capture(
            SAMPLE,
            "trellis_end_to_end_latency_seconds",
            "t",
            &T1_BOUNDS,
        );
        assert_eq!(snap.count, 10);
        assert_eq!(snap.bucket(LE_P50), 5);
        assert_eq!(snap.bucket(LE_P99), 9);
        assert_eq!(snap.bucket(LE_MAX), 10);
        assert_eq!(
            counter_value(SAMPLE, "trellis_changes_applied_total", "t"),
            10
        );
    }

    #[test]
    fn since_subtracts_a_baseline_cumulative_scrape() {
        let baseline = HistogramSnapshot::capture(
            SAMPLE,
            "trellis_end_to_end_latency_seconds",
            "t",
            &T1_BOUNDS,
        );
        // A later scrape after 5 more observations, all under the p50 bound.
        let later: &str = "\
trellis_end_to_end_latency_seconds_bucket{transform=\"t\",le=\"0.25\"} 10\n\
trellis_end_to_end_latency_seconds_bucket{transform=\"t\",le=\"0.5\"} 14\n\
trellis_end_to_end_latency_seconds_bucket{transform=\"t\",le=\"1\"} 15\n\
trellis_end_to_end_latency_seconds_count{transform=\"t\"} 15\n";
        let after = HistogramSnapshot::capture(
            later,
            "trellis_end_to_end_latency_seconds",
            "t",
            &T1_BOUNDS,
        );
        let window = after.since(&baseline);
        assert_eq!(window.count, 5);
        assert_eq!(window.bucket(LE_P50), 5);
        assert_eq!(window.bucket(LE_P99), 5);
        assert_eq!(window.bucket(LE_MAX), 5);
    }

    #[test]
    fn t1_evaluation_matches_the_issues_boundary_fractions() {
        let snap = HistogramSnapshot::capture(
            SAMPLE,
            "trellis_end_to_end_latency_seconds",
            "t",
            &T1_BOUNDS,
        );
        let eval = T1Evaluation::evaluate(&snap);
        assert_eq!(eval.p50_frac, 0.5);
        assert!(eval.p50_pass, "5/10 == 0.50 satisfies the >= 0.50 boundary");
        assert!(eval.p99_frac > 0.5);
        assert!(eval.max_ok, "bucket(le=1) == count in this fixture");
    }

    #[test]
    fn zero_observations_never_reports_a_pass() {
        let empty = HistogramSnapshot::default();
        let eval = T1Evaluation::evaluate(&empty);
        assert_eq!(eval.count, 0);
        assert!(!eval.all_pass());
    }
}
