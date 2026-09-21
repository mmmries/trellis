//! E2 — seal-cadence sweep (issue #266, tests H1): sweeps
//! `ClientOptions::maintenance_interval` against B1's depth ladder to
//! confirm (or refute) H1's predicted linear dependence on
//! `hops * maintenance_interval`, and to give a real number for what a
//! shorter tick would cost/buy before anyone recommends shipping one.
//!
//! **This module does not ship a fix.** Per the issue's own instruction:
//! "evaluate seal-on-demand as a design option... Do not ship a fix under
//! this issue — write it up as a follow-up with the measurement behind it."
//! This is purely the measurement: does a shorter `maintenance_interval`
//! actually buy the latency B1 is missing, roughly linearly, with no other
//! surprise? (Seal churn/WAL cost at a shorter interval is a *separate*
//! question this module doesn't answer — it only has a Prometheus-level
//! view, not WAL volume — see the issue's own caution not to just tune
//! `maintenance_interval` down until a number goes green.)

use std::time::Duration;

use super::b1_hop_ladder::{HopLatencyResult, run_depth};

/// Runs `run_depth` for every `(interval, depth)` pair, sweeping intervals
/// as the outer loop (so results group naturally by cadence, matching how
/// the issue frames the sweep) and depths as the inner loop.
pub async fn run_sweep(
    interval_candidates_ms: &[u64],
    depths: &[usize],
    commits_per_sec: f64,
    duration: Duration,
) -> Vec<HopLatencyResult> {
    let mut results = Vec::new();
    for &interval_ms in interval_candidates_ms {
        let interval = Duration::from_millis(interval_ms);
        for &depth in depths {
            results.push(run_depth(depth, commits_per_sec, duration, interval).await);
        }
    }
    results
}
