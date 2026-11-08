-- The joined tables of a JOIN definition past the first (ADR-0021).
--
-- A JOIN's joined tables are numbered by position from 1, in the order its
-- text joins them. Position 1 stays in `transform_definitions.join_table`
-- and `join_columns`, so a definition that joins one table has no rows here
-- and its catalog row is what it was before this table existed. Each later
-- position has one row: the qualified table, and its column types as
-- `join_columns` records them for position 1. `defs::catalog::JOINED_TABLES`
-- reads every position as one relation.
--
-- Runs again harmlessly (a test re-runs the migrations after V15).
--
-- Declares no catalog floor: an older release at the floor never wrote a
-- row here, and every definition it wrote joins at most one table, which
-- this release reads from `join_table` as before. A definition with more
-- joined tables declares the floor of the release that runs it.
create table if not exists definition_join_tables (
    transform_id bigint not null references transform_definitions (id) on delete cascade,
    position smallint not null check (position >= 2),
    table_name text not null,
    columns jsonb not null,
    primary key (transform_id, position),
    unique (transform_id, table_name)
);

create index if not exists definition_join_tables_table_name_idx
    on definition_join_tables (table_name);
