-- The transform-definition catalog (issue #23): a versioned Postgres store
-- for 1-1 transform definitions and the per-source-table monotonic version
-- stage 05's version fence reads via FOR SHARE/FOR UPDATE.
--
-- `source_table_versions` holds one real, lockable row per source table
-- that has ever had a definition created against it; `version` is bumped
-- in the same transaction that inserts a new `transform_definitions` row.
create table if not exists source_table_versions (
    source_table text primary key,
    version bigint not null
);

-- Definitions are immutable in v1 (creation only, no update/delete) and are
-- stored as their original source text; see `trellis/src/defs/catalog.rs`
-- for why re-parsing on read was chosen over a serialized AST.
create table if not exists transform_definitions (
    id bigint primary key generated always as identity,
    target_table text not null unique,
    source_table text not null references source_table_versions (source_table),
    source_version bigint not null,
    definition_text text not null,
    created_at timestamptz not null default now()
);

create index if not exists transform_definitions_source_table_idx
    on transform_definitions (source_table);
