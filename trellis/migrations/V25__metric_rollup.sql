-- Self-retained metric history (issue #54, epic #49; docs/observability.md's
-- "Retention: Postgres rollup tables" section; ADR-0009 decision 7,
-- docs/decisions/0009-observability-decisions.md#7-rollup-interval-and-retention-issue-54).
--
-- A periodic job (`trellis::rollup`, wired into `client::maintenance_loop`)
-- reads `trellis::metrics`'s in-process registry (the same registry issue
-- #53's `render_prometheus()` exposes) and writes one snapshot row per
-- distinct metric series into `metric_rollup` below, every
-- `rollup_interval` (default 5 minutes). A second job prunes rows older
-- than `retention_window` (default 7 days). Both knobs are configurable —
-- see `ClientOptions::rollup_interval`/`ClientOptions::rollup_retention`.
--
-- Unqualified table/index names, like every other migration in this
-- directory: this file runs with `search_path` already pinned to the
-- Trellis-managed schema (`crate::pool`/`crate::migrate`), not the separate,
-- independently-configurable transform *target* schema
-- (`Config::target_schema`) transform output tables live under. The doc's
-- informal `trellis.metric_rollup` naming refers to that managed schema,
-- whatever it's actually configured as (`TRELLIS_SCHEMA`, default
-- `trellis`) — this migration doesn't hardcode the literal name `trellis`
-- anywhere, matching every other table here.
--
-- One table, not three: unlike the quarantine tracks (`V13__quarantine.sql`)
-- or column-status tracks (`V21__column_quarantine.sql`), a metric
-- observation is a single self-contained fact (a name, a kind, a label set,
-- and a value shape depending on that kind) with no cross-referencing
-- lifecycle of its own — nothing here is ever updated in place, only
-- inserted and later pruned, so there's no separate "current state" table to
-- split off from a "history" table the way poison/poison_held or
-- column_status/column_failures split theirs.
--
-- Column shape, one row per (rolled_up_at, metric_name, labels):
--   `rolled_up_at`   — the wall-clock instant this snapshot was taken (the
--                      rollup job's own tick time, not a bucketed window
--                      start/end — the job runs on a fixed interval, so the
--                      tick time already carries that information).
--   `metric_name`    — e.g. `trellis_transform_latency_seconds`,
--                      `trellis_changes_applied_total`,
--                      `trellis_staging_segments` — the same names
--                      `crate::metrics` records under and `render_prometheus`
--                      exposes, so a row here is traceable back to exactly
--                      the live series it snapshotted.
--   `metric_kind`    — `counter` | `gauge` | `histogram`, mirroring the
--                      `metrics`/`metrics-exporter-prometheus` crate's own
--                      three kinds (`docs/decisions/0009-observability-decisions.md`
--                      decision 1) — check-constrained the same way
--                      `transform_definitions.status` is
--                      (`V19__transform_status.sql`) rather than left as
--                      free text.
--   `labels`         — every metric this crate records carries a different,
--                      small label set (`transform` for the two latency
--                      histograms and the throughput counter, `state` for
--                      the segment gauge — `trellis/src/metrics.rs`), and
--                      nothing here ever needs to query or index into one
--                      label key specifically (that's future work, the
--                      "suggest optimizations" query API this issue
--                      explicitly excludes) — so a jsonb object (e.g.
--                      `{"transform": "order_totals"}`) is the simplest fit,
--                      not a normalized `metric_labels` join table (which
--                      would need its own key/value rows and a join to
--                      reconstruct one observation, for no read this issue
--                      needs yet) and not one nullable column per
--                      known-today label key (which would need a migration
--                      every time a future metric adds a new label).
--                      Defaults to `{}` for a label-less series, so every
--                      row's `labels` is always a valid jsonb object, never
--                      null, keeping any future `labels ->> 'x'` read
--                      total rather than needing a null-check first.
--   `value`          — the current reading, for `counter`/`gauge` rows only
--                      (a counter's cumulative total since process start, a
--                      gauge's instantaneous value at `rolled_up_at`) — null
--                      for `histogram` rows, which use the four columns
--                      below instead.
--   `bucket_bounds`,
--   `bucket_counts`  — the histogram's raw bucket boundaries (`le`, ascending,
--                      excluding the implicit `+Inf` bucket every Prometheus
--                      histogram carries — `histogram_count` below already
--                      *is* that `+Inf` cumulative count, so storing it a
--                      second time as a bucket would be redundant) and each
--                      boundary's own cumulative count, same length and
--                      order so `bucket_bounds[i]`/`bucket_counts[i]` pair up
--                      positionally — deliberately the *raw* per-bucket
--                      counts, not a pre-computed quantile (ADR-0009
--                      decision 7's central point: a stored p50/p99 can't be
--                      re-aggregated across rollup periods or across engine
--                      instances after the fact, since you can't average two
--                      p99s into a valid p99, while raw buckets can be
--                      summed and re-queried with `histogram_quantile`-style
--                      math at any later time and slice). Null for
--                      `counter`/`gauge` rows.
--   `histogram_sum`,
--   `histogram_count`  — the histogram's running sum and total observation
--                      count (the same two fields every Prometheus histogram
--                      exposes alongside its buckets, needed to compute a
--                      mean or to weight buckets across a merge). Null for
--                      `counter`/`gauge` rows.
--
-- No `updated_at`/upsert story: every rollup tick inserts fresh rows rather
-- than updating a prior one in place, since the whole point (per ADR-0009
-- decision 7 and `docs/observability.md`'s "self-retained history") is
-- keeping a time series of snapshots, not the latest reading only — that's
-- already what the live in-process registry (and `render_prometheus`) is
-- for.
-- Kind-dependent nullability, enforced the same way `V11__truncate_op.sql`
-- conditions `old_image`/`new_image` on its own discriminator column
-- (`op`): a `counter`/`gauge` row carries `value` and nothing else; a
-- `histogram` row carries the four histogram columns and no `value`. Without
-- this, the DB would silently accept a row whose shape contradicts its own
-- `metric_kind` (e.g. `histogram` with `bucket_bounds is null`) — only the
-- Rust write path would be preventing that, not the schema.
create table if not exists metric_rollup (
    id bigint generated always as identity primary key,
    rolled_up_at timestamptz not null default now(),
    metric_name text not null,
    metric_kind text not null check (metric_kind in ('counter', 'gauge', 'histogram')),
    labels jsonb not null default '{}'::jsonb,
    value double precision,
    bucket_bounds double precision[],
    bucket_counts bigint[],
    histogram_sum double precision,
    histogram_count bigint,
    constraint metric_rollup_kind_shape check (
        case metric_kind
            when 'histogram' then
                value is null
                and bucket_bounds is not null
                and bucket_counts is not null
                and histogram_sum is not null
                and histogram_count is not null
                and array_length(bucket_bounds, 1) = array_length(bucket_counts, 1)
            else
                value is not null
                and bucket_bounds is null
                and bucket_counts is null
                and histogram_sum is null
                and histogram_count is null
        end
    )
);

-- The prune job's own access path (`delete from metric_rollup where
-- rolled_up_at < $1`, `trellis::rollup::prune`) — a leading column on
-- `rolled_up_at` alone (not a composite starting with `metric_name`) is what
-- a range-only predicate like that needs to seek rather than scan. Also
-- serves the future "history for a time window" query the design doc's
-- "suggest optimizations" use case will eventually want, without this issue
-- having to build that read path itself.
create index if not exists metric_rollup_rolled_up_at_idx on metric_rollup (rolled_up_at);
