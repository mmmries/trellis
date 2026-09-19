-- Worker registry (issue #144; docs/decisions/0010-embeddable-clients.md,
-- decision 3). One row per live `Client` process/connection running with
-- `drain_threads > 0` (application workers) — see
-- `trellis::staging::worker_registry` and `Trellis::has_live_drain_workers`.
--
-- Why this can't just be `seg_claims`/`drainers`: `seg_claims`
-- (V7__claims_and_buckets.sql) only ever holds a row while a worker is
-- mid-batch, so a worker that is running, healthy, and simply idle leaves no
-- trace there — an empty claim table is indistinguishable from an empty
-- fleet, which is exactly the silent-stall misconfiguration this table
-- exists to make detectable (every connection at `drain_threads: 0`, every
-- transform stuck in `waiting_to_backfill` forever, nothing erroring). The
-- `drainers` table (also V7) is a different, narrower thing: the claim-time
-- share denominator for bucket sizing, upserted only from inside a live
-- drain iteration and never cleaned up on shutdown — its own doc comment
-- calls it "deliberately minimal" for that one purpose, and reusing it here
-- would tie this feature's correctness to a table that was never meant to
-- answer "is anyone draining at all."
--
-- Staleness deliberately has no independent TTL column or config knob here:
-- `trellis::staging::worker_registry::has_live_workers` compares `last_seen`
-- against the same `reclaim_ttl` `trellis::staging::liveness::reclaim_stale`
-- already uses for a stale claim, at read time — see that module's doc
-- comment for why a design that instead needed a delete-based reclaim pass
-- to have already run would be wrong in precisely the fleet this table
-- matters most for (one with no live worker anywhere to run one).
create table if not exists worker_registry (
    worker_id text primary key,
    registered_at timestamptz not null default now(),
    last_seen timestamptz not null default now()
);
