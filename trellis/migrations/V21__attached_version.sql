-- The binary version that last ran the takeover work on this instance
-- (ADR-0023, rule 3), written in the transaction that runs it. A background
-- process of another version runs the work before it takes over.
--
-- Runs again harmlessly (a test re-runs the migrations after V15).
--
-- Declares no catalog floor: a nullable column an older release never reads.
alter table trellis_instance add column if not exists attached_version text;
