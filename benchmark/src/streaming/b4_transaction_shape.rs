//! B4 — transaction-shape sensitivity (issue #266): the same rows/sec
//! delivered as 1 row/txn, 100 rows/txn, 10k rows/txn — separating per-commit
//! cost (decode, `Commit` handling, watermark advance, `NOTIFY`) from
//! per-row cost. Holds `target_rows_per_sec` fixed and sweeps
//! `rows_per_commit`; [`super::single_hop_probe`] does the actual work,
//! deriving `commits_per_sec = target_rows_per_sec / rows_per_commit` for
//! each shape.
//!
//! Picking a `target_rows_per_sec` matters: it must sit comfortably under
//! B2's measured single-hop ceiling (~220-250k rows/sec on this box) so a
//! probe's `sustained: false` here means the *shape* hurt, not that the
//! rate itself was already past saturation regardless of shape.

use std::time::Duration;

use super::single_hop_probe::{ThroughputProbe, run_probe};

const APPLICATION_THREADS: usize = 8;

/// The three shapes the issue names explicitly.
pub const DEFAULT_SHAPES: &[usize] = &[1, 100, 10_000];

/// Runs one probe per `rows_per_commit` in `shapes`, all at the same
/// `target_rows_per_sec` — every probe's `commits_per_sec` differs
/// (`target_rows_per_sec / rows_per_commit`), which is the whole point:
/// 1 row/commit at a high target rate means a very high commit rate, and
/// this measures what that costs relative to a few large commits.
pub async fn run_sweep(
    shapes: &[usize],
    target_rows_per_sec: f64,
    offered_duration: Duration,
    grace: Duration,
) -> Vec<ThroughputProbe> {
    let mut probes = Vec::new();
    for &rows_per_commit in shapes {
        let commits_per_sec = target_rows_per_sec / rows_per_commit as f64;
        let probe = run_probe(
            rows_per_commit,
            commits_per_sec,
            APPLICATION_THREADS,
            offered_duration,
            grace,
        )
        .await;
        probes.push(probe);
    }
    probes
}
