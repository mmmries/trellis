//! B2 — 1-1 throughput ramp (issue #266): single hop, ramp offered load
//! until the pipeline can't keep up, report the saturation rate plus e2e
//! latency at 25/50/80/95% of it. This is the T2 measurement (100k rows/sec
//! sustained, single-hop plain 1-1) and gives the load levels B3/B4/B5/B7/B8
//! should be run at.
//!
//! **What "sustained" means here is deliberately weaker than T2's own
//! definition.** T2 (issue #266) requires "held for >= 10 minutes with ring
//! depth and replication lag both flat." A ramp searching several candidate
//! rates at 10 minutes each is a multi-hour scenario before it's found
//! anything — impractical for the exploratory search this module does. Each
//! probe here instead offers load for a short, configurable window (default
//! 20s) and then checks whether the backlog it created (`rows offered -
//! rows applied`) fully drains within a bounded grace period. That is a
//! reasonable proxy — a rate whose backlog doesn't even drain in a
//! bounded grace period after a short offered window is certainly not
//! sustainable for 10 minutes — but it is not a substitute for a real T2
//! confirmation run: once this ramp identifies a candidate saturation rate,
//! confirming it holds for T2's actual 10-minute window is a separate,
//! longer run this module doesn't do automatically.
//!
//! Each probe offers its target rate through [`super::load::run_controlled_load`]
//! (paced, self-throttling — see [`super::single_hop_probe`]), not the
//! max-rate generator — a ramp needs to test *specific* rates, not "as fast
//! as this connection can go." At high enough target rates the single
//! generator connection can itself become the bottleneck before the engine
//! does (an `INSERT` taking longer than one tick period), in which case
//! `achieved_rows_per_sec` undershoots `target_rows_per_sec` and a
//! "sustained" verdict at that rate says more about the generator than the
//! engine — always read the two side by side, never `target_rows_per_sec`
//! alone.

use std::time::Duration;

use super::single_hop_probe::{ThroughputProbe, run_probe};

/// More drain parallelism than B1's latency ladder: B2 is measuring
/// throughput, where H4 (issue #266) expects `SEG_BUCKETS = 8` worth of
/// useful claim parallelism per batch.
const APPLICATION_THREADS: usize = 8;

/// A fixed generator cadence (issue #266's B4 axis holds transaction shape
/// constant while sweeping rate; this ramp holds it at a mid-sized batch —
/// neither B4's 1-row nor 10k-row extreme — and varies `rows_per_commit` to
/// hit the target rate instead, so the ramp isn't itself confounded by a
/// changing commit rate).
const RAMP_COMMITS_PER_SEC: f64 = 50.0;

/// Ramps through `candidate_rates` (ascending) until a probe fails to drain
/// its backlog within `grace`, or the list is exhausted. Returns every probe
/// run, in order — the caller reads the last `sustained: true` entry (if
/// any) as the candidate saturation rate.
/// Issue #268's X3: `seal_mode` selects stock timer-driven sealing or one of
/// the naive/gated demand-driven variants, compared against each other on
/// this exact knee. `main.rs`'s `throughput-ramp` CLI arm always passes this
/// explicitly (`trellis::SealMode::Timer` when `--seal-mode` isn't given, via
/// `parse_seal_mode`'s own default).
pub async fn run_ramp_with_seal_mode(
    candidate_rates: &[f64],
    offered_duration: Duration,
    grace: Duration,
    seal_mode: trellis::SealMode,
) -> Vec<ThroughputProbe> {
    let mut probes = Vec::new();
    for &target_rows_per_sec in candidate_rates {
        let rows_per_commit =
            ((target_rows_per_sec / RAMP_COMMITS_PER_SEC).round() as usize).max(1);
        let probe = run_probe(
            rows_per_commit,
            RAMP_COMMITS_PER_SEC,
            APPLICATION_THREADS,
            offered_duration,
            grace,
            seal_mode,
        )
        .await;
        let sustained = probe.sustained;
        probes.push(probe);
        if !sustained {
            break;
        }
    }
    probes
}
