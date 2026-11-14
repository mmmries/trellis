//! Star 1-1 `JOIN` transforms, first half (split from `join_star_b` only to balance the CI shards).
//!
//! Run one module with `cargo test -p trellis --test join_star_a <module>::`.

#[path = "../support/mod.rs"]
#[allow(dead_code, unused_imports)]
mod support;

mod join_star_build;
mod join_star_crossing;
mod join_star_define;
mod join_star_families;
mod join_star_fan_out;
mod join_star_harness;
mod join_star_self_check;
mod join_star_status;
mod join_star_truncate;
