//! `JOIN ... GROUP BY` transforms (ADR-0019).
//!
//! Run one module with `cargo test -p trellis --test join <module>::`.

#[path = "../support/mod.rs"]
#[allow(dead_code, unused_imports)]
mod support;

mod join_interleavings;
mod join_over_targets;
mod join_schema_changes;
mod join_self_check;
mod join_status;
mod join_swapped_table;
