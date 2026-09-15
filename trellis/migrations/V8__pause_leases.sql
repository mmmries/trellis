-- The fleet-wide pause lease (issue #15, stage 04's third piece). See
-- docs/staging-and-claiming/04-claiming-and-the-fold.md, "An orthogonal
-- gate: the pause lease".
--
-- A heartbeated *lease*, not a latch: `expires_at` in the future means the
-- lease is active, and a holder that dies simply stops heartbeating and lets
-- it lapse. That is the whole point of a lease over a latch here — a latch
-- (held until explicitly released) can be wedged forever by a pauser that
-- crashes mid-pause, which would permanently stall every worker's claim.
-- `trellis::staging::liveness::heartbeat_pause_lease` is what refreshes
-- `expires_at`; its `WHERE expires_at > now()` guard is load-bearing and
-- lives in Rust, not here, so a heartbeat that arrives after expiry cannot
-- resurrect a lapsed lease.
create table if not exists pause_leases (
    lease_id text primary key,
    holder text not null,
    acquired_at timestamptz not null default now(),
    expires_at timestamptz not null
);
