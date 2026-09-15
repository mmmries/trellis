-- Relationship cardinality (issue #27, ADR-0006): whether a relationship's
-- to-side is guaranteed at most one row per from-row. Determined once, live
-- against `pg_catalog`, at `create_relationship` time — `to_col` is checked
-- for being the sole column of a `PRIMARY KEY` or `UNIQUE` index on
-- `to_table` — and persisted here rather than re-introspected on every read
-- (see `trellis/src/defs/model.rs`'s `RelationshipCardinality`), so a later
-- reference-time check (validating a bare-path vs. aggregate-wrapped use of
-- the relationship — deferred past issue #27) has a stable answer even if
-- the underlying index is later dropped, matching ADR-0005's "introspect
-- once, at definition time" pattern rather than trusting the live schema to
-- stay put.
alter table relationship_definitions
    add column if not exists cardinality text not null default 'many';

alter table relationship_definitions
    add constraint relationship_definitions_cardinality_check
        check (cardinality in ('one', 'many'));
