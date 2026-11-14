//! Group-basis aggregates: build, maintenance, MIN/MAX, NUMERIC and self-check.
//!
//! Run one module with `cargo test -p trellis --test group_basis <module>::`.

#[path = "../support/mod.rs"]
#[allow(dead_code, unused_imports)]
mod support;

mod group_basis_alias_columns;
mod group_basis_build;
mod group_basis_chained;
mod group_basis_collation;
mod group_basis_interleavings;
mod group_basis_min_max;
mod group_basis_min_max_index;
mod group_basis_numeric_nan;
mod group_basis_numeric_scales;
mod group_basis_rebuild;
mod group_basis_resume;
mod group_basis_resume_after_source_key_change;
mod group_basis_self_check;
