-- Persists the source-column type map a definition was validated against
-- (issue #23's `create_definition(source_columns)`) alongside the
-- definition row itself. Previously this map only existed transiently at
-- `create_definition` call time, so the physical apply path (stage 05's
-- `compute`) had nowhere to load it back from and defaulted every column to
-- Numeric — silently corrupting Text/Boolean calculated fields (issue #63's
-- write-path gap).
alter table transform_definitions
    add column if not exists source_columns jsonb not null default '{}'::jsonb;
