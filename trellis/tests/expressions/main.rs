//! Expression evaluation: types, casts, literals, functions and key spaces.
//!
//! Run one module with `cargo test -p trellis --test expressions <module>::`.

#[path = "../support/mod.rs"]
#[allow(dead_code, unused_imports)]
mod support;

mod arithmetic_key_spaces;
mod bpchar_literal;
mod build_key_collation;
mod builtin_shadowing;
mod cast_aliases;
mod cast_identity;
mod date_key_spaces;
mod enum_literal_equality;
mod fold;
mod ledger_xmloption;
mod logic_key_spaces;
mod null_literal_key_spaces;
mod numeric_sums_upgrade;
mod plan_time_constant_quarantine;
mod regexp_pattern_probe;
mod strict_stored_values;
mod type_matrix;
mod unbounded_varchar_copies;
