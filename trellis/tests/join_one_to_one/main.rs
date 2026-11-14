//! Two-table 1-1 `JOIN` transforms.
//!
//! Run one module with `cargo test -p trellis --test join_one_to_one <module>::`.

#[path = "../support/mod.rs"]
#[allow(dead_code, unused_imports)]
mod support;

mod join_one_to_one_build;
mod join_one_to_one_fan_out;
mod join_one_to_one_interleavings;
mod join_one_to_one_lease;
mod join_one_to_one_over_targets;
mod join_one_to_one_producers;
mod join_one_to_one_quarantine;
mod join_one_to_one_readers;
mod join_one_to_one_recollation;
mod join_one_to_one_resume;
mod join_one_to_one_schema_changes;
mod join_one_to_one_self_check;
mod join_one_to_one_status;
