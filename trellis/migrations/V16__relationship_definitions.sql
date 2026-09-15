-- Relationship catalog (issue #26): a standalone-declared relationship
-- (ADR-0006) is stored the same way a transform definition is —
-- `definition_text` holds the original source text, re-parsed on read via
-- `trellis/src/defs/parser.rs`'s `parse_relationship` rather than a
-- serialized AST — see `trellis/src/defs/catalog.rs`'s module doc comment
-- for the tradeoff this mirrors.
--
-- `from_table`/`from_col`/`to_table`/`to_col`/`name` are denormalized out of
-- `definition_text` into their own columns, matching
-- `transform_definitions.target_table`/`source_table`: a definition's
-- identity and lookup keys are real columns a query (and a uniqueness
-- constraint) can hit directly, not something every reader has to re-parse
-- `definition_text` to discover.
--
-- No column here is validated against the live source schema: this table
-- only records what was declared, never issues DDL against `from_table`/
-- `to_table` (ADR-0005), and does not check that `from_col`/`to_col` exist
-- or are key-like — that's later validation-issue scope.
--
-- Naming (ADR-0006 "Naming and scope"): a relationship name is unique
-- **per from-table**, not global — `posts` may declare both `author` and
-- `editor` pointing at `users` as independent relationships, so the name
-- alone can repeat across different from-tables. The uniqueness constraint
-- below is `(from_table, name)`, matching that rule exactly; a bare `name`
-- unique constraint would reject that legitimate case.
create table if not exists relationship_definitions (
    id bigint primary key generated always as identity,
    name text not null,
    from_table text not null,
    from_col text not null,
    to_table text not null,
    to_col text not null,
    definition_text text not null,
    created_at timestamptz not null default now(),
    unique (from_table, name)
);

create index if not exists relationship_definitions_from_table_idx
    on relationship_definitions (from_table);

create index if not exists relationship_definitions_to_table_idx
    on relationship_definitions (to_table);
