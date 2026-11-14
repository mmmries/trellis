//! `self_check` and the target audits.
//!
//! Run one module with `cargo test -p trellis --test self_check <module>::`.

#[path = "../support/mod.rs"]
#[allow(dead_code, unused_imports)]
mod support;

mod orphan_walk_collation;
mod self_check;
mod self_check_join_on_collation;
mod self_check_partitioned;
mod target_select_parity;
