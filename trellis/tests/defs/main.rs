//! Defining, altering and dumping transforms, and the define-time checks.
//!
//! Run one module with `cargo test -p trellis --test defs <module>::`.

#[path = "../support/mod.rs"]
#[allow(dead_code, unused_imports)]
mod support;

mod alter_transform;
mod bare_source_resolution;
mod define_adopt_target;
mod define_partitioned_source;
mod define_privileges;
mod definition_floor;
mod defs_aggregate_chained_composite_group_key;
mod defs_aggregate_chained_single_column_group_key;
mod defs_aggregate_field_named_like_source_column;
mod defs_ambiguous_bare_name;
mod defs_backfill;
mod defs_bit;
mod defs_boolean;
mod defs_bytea;
mod defs_catalog;
mod defs_collation_conflict;
mod defs_count_column;
mod defs_ddl;
mod defs_deleted_oneone_target_reduces_chained_aggregate;
mod defs_edges;
mod defs_enum;
mod defs_exact_integers;
mod defs_floats;
mod defs_grammar_version;
mod defs_index_key_cap;
mod defs_install_definition;
mod defs_join_define;
mod defs_join_one_to_one_define;
mod defs_jsonb;
mod defs_key_collation;
mod defs_netaddr;
mod defs_nodes;
mod defs_numeric_add;
mod defs_oracle;
mod defs_qualified_passthrough;
mod defs_reserved_word_columns;
mod defs_table_column_cap;
mod defs_temporal;
mod defs_text_functions;
mod defs_typed_literals;
mod defs_where;
mod dump_definitions_sql;
mod identifier_case_hint;
mod mixed_case_source;
mod qualified_target_names;
mod target_in_catalog_schema;
