//! 1-1 transforms over a single source.
//!
//! Run one module with `cargo test -p trellis --test one_to_one <module>::`.

#[path = "../support/mod.rs"]
#[allow(dead_code, unused_imports)]
mod support;

mod one_to_one_backfill_composite_nullable_group_key;
mod one_to_one_build_interleavings;
mod one_to_one_chained_off_nullable_aggregate_group_key;
mod one_to_one_composite_primary_key;
mod one_to_one_control_char_pk;
mod one_to_one_ledger_interleavings;
mod one_to_one_nullable_key;
mod one_to_one_render;
mod one_to_one_self_check_ring;
mod oneone_text_compare;
