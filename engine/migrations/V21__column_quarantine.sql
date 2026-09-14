-- Column-level quarantine (issue #82/#49, docs/decisions/0003-quarantine-
-- storage-and-api.md's 2026-09-12 amendment): a single TRANSFORM can compute
-- several calculated columns, and a failure in one column's formula
-- shouldn't force every other healthy column into quarantine. This adds a
-- second, finer-grained fuse tier alongside the existing whole-key/whole-
-- transform one (`poison`/`key_deaths`, V13__quarantine.sql), which is left
-- completely unchanged: it remains the coarser tier for failures that
-- aren't attributable to one column (a key-shape/DDL failure that dooms a
-- whole row).
--
-- `poison.failures` (this migration's ADR-literal piece): a `jsonb` array,
-- one element per `(transform, column)` pair that failed while propagating a
-- poisoned source row, extending (not replacing) `last_error`. `column` is
-- null for a failure that isn't attributable to one calculated field, same
-- as `evict_key`'s own eviction (a key-shape/DDL failure that dooms the
-- whole row for that transform). Existing `last_error` values are migrated
-- into one such (transform-less, column-less) entry so no history is lost.
--
-- The GIN index is sized for the containment queries a `(transform, column)`
-- pair needs (`failures @> jsonb_build_array(jsonb_build_object('transform',
-- $1, 'column', $2))`). In practice (see `engine/src/staging/quarantine.rs`'s
-- module doc comment on this migration) the column-fuse's own per-row
-- bookkeeping and the "sample quarantined rows for transform.column" read use
-- the dedicated `column_failures` table below instead, precisely *because* a
-- row landing in `poison` at all means "globally excluded from folding"
-- (`poisoned_keys_among`) — which is correct for a whole-key eviction but
-- would be wrong for a column-only failure (decision: a paused column
-- freezes its value; it must not evict the row from every other column/
-- transform that still computes cleanly). `poison.failures`/this index stay
-- as literal ADR-0003 storage and back today's whole-key eviction path
-- (`evict_key`, updated to append an entry here too), not the new per-column
-- path.
alter table poison
    add column if not exists failures jsonb not null default '[]'::jsonb;

update poison
set failures = jsonb_build_array(
    jsonb_build_object(
        'transform', null,
        'column', null,
        'error', last_error,
        'failed_at', poisoned_at
    )
)
where failures = '[]'::jsonb;

create index if not exists poison_failures_gin
    on poison using gin (failures jsonb_path_ops);

-- The column-status table (ADR-0003 amendment, "Column status table"): a
-- small, dense table recording each transform's currently-paused columns —
-- "is anything paused right now" is a cheap read over a handful of rows. A
-- transform with no rows here has every column live.
--
-- `local_fuse` distinguishes *why* a row is paused, which
-- `staging::quarantine::resume_column`'s cascade-aware resume needs:
-- `true` means this specific (transform, column) pair's own fuse tripped
-- (too many of its own rows failed); `false` means it's paused *only*
-- because an upstream column it reads was paused (decision #5: cascade the
-- pause to dependent readers). A column can be both at once (its own fuse
-- trips independently of, or in addition to, a cascade) — `local_fuse`
-- tracks that half; `column_pause_cascades` below tracks the other. Resuming
-- an upstream column must not un-pause a downstream one that still has a
-- local reason of its own to stay paused, or another live cascade edge.
create table if not exists column_status (
    transform_table text not null references transform_definitions (target_table),
    column_name text not null,
    paused_at timestamptz not null default now(),
    last_error text,
    local_fuse boolean not null default false,
    primary key (transform_table, column_name)
);

-- The column-fuse's incrementally-maintained counter (mirrors `key_deaths`
-- for the row-level fuse, for the same write-amplification reasons: a live
-- aggregate/GIN query over per-row failure detail on every batch would cost
-- far more than a single indexed upsert). Counts *distinct poisoned rows*,
-- not failure events — see `column_failures` below, whose insert is what
-- actually decides whether a given occurrence increments this counter.
create table if not exists column_deaths (
    transform_table text not null,
    column_name text not null,
    deaths integer not null default 0,
    last_error text,
    last_death_at timestamptz,
    primary key (transform_table, column_name)
);

-- The column-fuse's own per-row failure detail — deliberately a table of its
-- own rather than reusing `poison.failures` for this (see this migration's
-- header comment): a row landing here has *not* been evicted from folding,
-- it has simply contributed one failure toward its column's fuse count.
-- Primary-keyed on `(transform_table, column_name, src_table, key)` so a
-- retried probe of the same already-recorded key is a no-op insert (`on
-- conflict do nothing`) rather than a second charge — this is what makes
-- `column_deaths` count distinct rows rather than raw retry attempts, the
-- same distinction `docs/decisions/0003-quarantine-storage-and-api.md`'s
-- "Fuse" section draws ("a large fraction of one column's rows fail," not "one
-- row failed many times" — the latter is exactly what the existing row-level
-- fuse already handles). Serves as the "sample quarantined rows" read's data
-- source for a `transform.column` target — deliberately *not* cleared when
-- the fuse trips (unlike `poison.failures` in the ADR's literal proposal):
-- clearing it right then would erase the very evidence a caller most wants
-- right after a pause. Cleared instead when the column is resumed
-- (`staging::quarantine::resume_column`), once it's eligible to start
-- accumulating a fresh set.
create table if not exists column_failures (
    transform_table text not null,
    column_name text not null,
    src_table text not null,
    key text not null,
    error text not null,
    failed_at timestamptz not null default now(),
    primary key (transform_table, column_name, src_table, key)
);

-- Cascade edges (decision #5): when `(upstream_transform, upstream_column)`
-- pauses, every `(downstream_transform, downstream_column)` whose formula
-- reads it (a chained 1-1 transform, or a relationship-enriched field —
-- see `defs::catalog::column_dependents`) must pause too, rather than
-- silently consume a frozen/stale value with no signal. Recorded so resume
-- can tell "this downstream column is paused *only* because of this upstream
-- pause" from "it also has its own independent reason" (`column_status.local_fuse`,
-- or another remaining cascade edge) before un-pausing it.
create table if not exists column_pause_cascades (
    downstream_transform text not null,
    downstream_column text not null,
    upstream_transform text not null,
    upstream_column text not null,
    primary key (downstream_transform, downstream_column, upstream_transform, upstream_column)
);

create index if not exists column_pause_cascades_upstream_idx
    on column_pause_cascades (upstream_transform, upstream_column);
