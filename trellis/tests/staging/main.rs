//! The staging ring and drain: apply, seal, quarantine, retire, holdups and latency.
//!
//! Run one module with `cargo test -p trellis --test staging <module>::`.

#[path = "../support/mod.rs"]
#[allow(dead_code, unused_imports)]
mod support;

mod apply;
mod apply_entrypoint;
mod attribution;
mod build_step_probe;
mod chained_live_hops;
mod column_quarantine;
mod drain_driver_seal_gate;
mod drain_holdups;
mod drain_paging;
mod end_to_end_latency;
mod end_to_end_latency_aggregate;
mod held_key_retry;
mod intermediate_hop_cdc;
mod origin_propagation;
mod pause_and_drop;
mod pipeline_helpers;
mod quarantine;
mod quarantine_recompute_null_group_key;
mod rederive_build;
mod release_held_key;
mod retire;
mod sealing;
mod staging_ring;
mod table_catch_ups;
mod target_mutation_seam;
mod tracing_instrumentation;
mod wide_rows;
mod work_wake_edges;
