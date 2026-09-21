//! Steady-state streaming benchmark harness (issue #266's B0): the
//! prerequisite everything else in the issue builds on. Unlike
//! `scenario.rs`/`scenario_relationship.rs`, which time
//! [`trellis::dev::defs::backfill_definition`] — the direct, ring-bypassing
//! build (ADR-0007) — this harness drives the real, product-facing path: a
//! real [`trellis::Client`] (CDC intake -> ring append -> seal -> claim ->
//! fold -> apply), fed by a controlled-rate load generator, measured through
//! the Prometheus histograms that already exist
//! (`trellis::metrics::Metrics::render_prometheus()`) rather than any new
//! instrumentation (the issue is explicit that no telemetry work is in
//! scope).
//!
//! Submodules:
//!
//! - [`chain`]: programmatic N-hop 1-1 transform chain builder, through the
//!   real `install_definition` front door.
//! - [`load`]: a controlled-offered-rate load generator (commits/sec,
//!   configurable rows/commit).
//! - [`metrics_scrape`]: text-scraping helpers for `render_prometheus()`
//!   output, plus the T1 boundary-fraction evaluation the issue specifies.
//! - [`b1_hop_ladder`]: B1, the hop-depth latency ladder — the direct test
//!   of H1 (the seal-cadence hypothesis).
//! - [`single_hop_probe`]: shared single-hop probe scaffolding B2 and B4
//!   both build on.
//! - [`b2_throughput_ramp`]: B2, the single-hop 1-1 throughput ramp.
//! - [`b4_transaction_shape`]: B4, transaction-shape sensitivity (same
//!   rows/sec at 1/100/10k rows per commit).
//! - [`e2_seal_cadence_sweep`]: E2, sweeps `maintenance_interval` against
//!   B1's depth ladder to measure whether a shorter seal cadence actually
//!   fixes what H1 predicts it does.
//! - [`e3_intake_ceiling`]: E3, the CDC-intake-alone throughput ceiling
//!   (tests H2) — the hard ceiling B2/B3 sit under.

pub mod b1_hop_ladder;
pub mod b2_throughput_ramp;
pub mod b4_transaction_shape;
pub mod chain;
pub mod e2_seal_cadence_sweep;
pub mod e3_intake_ceiling;
pub mod load;
pub mod metrics_scrape;
pub mod single_hop_probe;
