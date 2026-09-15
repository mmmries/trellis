-- Buckets and multi-worker claims (issue #14, stage 04's second half). See
-- docs/staging-and-claiming/04-claiming-and-the-fold.md, "Partitioning a
-- batch across workers" and "The claim is a cursor".

-- One row per (batch, bucket) a worker holds. Exclusivity is the primary
-- key, not a lock: `INSERT ... ON CONFLICT (seg_seq, bucket) DO NOTHING`
-- (trellis::staging::claim::claim) is what makes two overlapping claims land
-- disjointly — no bucket is ever held twice. No index beyond the PK: every
-- read here is scoped by seg_seq (often plus claimed_by), which the PK's
-- leading column already serves.
--
-- Nothing here deletes or marks a claim done — completion (issue #11,
-- blocked on aggregate transform-defs) is a later stage's job. A claimed
-- batch sits in 'draining' with rows in this table until #11 closes the
-- loop; that is expected, not a leak.
-- `on delete cascade`: a retired segment's claims are meaningless, so #13's
-- retirement pass can drop a drained `segments` row without first clearing
-- `seg_claims` by hand (and without an FK violation). #11 completion deletes
-- its claims explicitly before touching `segments`, so the cascade is only a
-- backstop for the retirement path.
create table if not exists seg_claims (
    seg_seq bigint not null references segments (seg_seq) on delete cascade,
    bucket smallint not null,
    claimed_by text not null,
    claimed_at timestamptz not null default now(),
    primary key (seg_seq, bucket)
);

-- One row per live worker: the share denominator a claim divides its
-- batch's free buckets by (trellis::staging::claim::count_live_drainers).
-- Deliberately minimal — full heartbeat cadence, reclaim TTL, and the
-- pause lease are issue #15's job; this table is only the registration +
-- count today's claim-time share sizing needs.
create table if not exists drainers (
    drainer_id text primary key,
    last_seen timestamptz not null default now()
);

-- How many buckets a batch was split into, decided once at seal time from
-- configuration and batch size alone (trellis::staging::claim::SEG_BUCKETS/
-- MIN_ROWS_TO_SPLIT) and fixed for the batch's whole life: changing it, or
-- recomputing `route % bucket_count`, mid-drain would move rows between
-- buckets — a double apply or lost work (see doc 04, "Partitioning a batch
-- across workers"). Default 1 matches the unsplit behaviour every segment
-- inserted before this column existed already has. The existing
-- `bucket_mask` column is unrelated and stays unused by this migration —
-- issue #11 is expected to add or repurpose a `drained_mask` for
-- completion tracking, which is not this migration's job.
alter table segments add column if not exists bucket_count smallint not null default 1;
