-- Transform lifecycle status (issue #55): `waiting_to_backfill` ->
-- `backfilling` -> `live`, plus `quarantined` — the state machine
-- `docs/transforms.md`'s "Status" section documents in prose. See
-- `trellis::defs::model::TransformStatus` for the Rust round-trip and
-- `defs::catalog::install_definition` for the one writer that transitions a
-- row through more than one value (`backfilling` -> `live`) today.
--
-- Defaulted to `live` for both existing and new rows: `install_definition`
-- today runs a definition's full backfill synchronously and, prior to this
-- migration, only ever persisted the definition row once that backfill had
-- already completed — so every row that exists as of this migration is, by
-- construction, already fully backfilled. `live` is also the correct default
-- for every other creation path (the ring-based `create_definition` stages
-- its enumeration in the same transaction as the row insert), leaving
-- `waiting_to_backfill`/`backfilling`/`quarantined` as states a row only
-- ever reaches via an explicit later transition, never a default.
alter table transform_definitions
    add column if not exists status text not null default 'live';

alter table transform_definitions
    add constraint transform_definitions_status_check
        check (status in ('waiting_to_backfill', 'backfilling', 'live', 'quarantined'));
