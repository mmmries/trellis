-- The durable watermark intake (issue #7, stage 01) advances in the same
-- transaction as the stage — the linchpin at
-- docs/staging-and-claiming/01-intake-and-lsn-confirmation.md. One row per
-- replication slot.
--
-- Why a table here rather than a slot's own `confirmed_flush_lsn`: the
-- guarantee is "the slot's confirmed position never exceeds durably-staged
-- work," and the only way to make that atomic is one transaction committing
-- both halves together. Postgres's own slot bookkeeping advances via the
-- Standby Status Update, a separate round-trip the consumer sends only after
-- this row is durably committed (see `trellis::intake`) — this table is the
-- durable half; the protocol message is the acknowledgment that lets the
-- server reclaim WAL.
--
-- A row is inserted once, by whatever first uses a slot's name (in practice
-- `initial_snapshot_handshake`, seeded at the slot's own consistent point),
-- and only UPDATEd after (see the linchpin's `confirmed_lsn < :end_lsn`
-- guard). The row therefore always exists once a slot is in use — "missing
-- row = not converged" is not the mechanism stage 07 can rely on. Instead:
-- `confirmed_lsn` always leads applied state (seeded ahead of the initial
-- backfill's fold, and ahead of the fold in steady state too), so
-- convergence must be judged by whether pending staged work through a given
-- position has been folded and applied — never by `confirmed_lsn` alone.
create table if not exists replication_progress (
    slot_name text primary key,
    confirmed_lsn pg_lsn not null
);
