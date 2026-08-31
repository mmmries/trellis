-- Trellis's first real migration. It exists to prove the runner applies an
-- ordered, idempotent SQL file end to end; the transform/staging schema
-- itself lands in later work.
--
-- This file is applied with `search_path` already pointed at the
-- Trellis-managed schema (see `crate::pool` and `crate::migrate`), so table
-- names here are intentionally unqualified. That schema is the instance's
-- identity: one named schema per instance, configurable so several can share
-- a cluster (see `docs/instance-identity.md`).
create table if not exists engine_bootstrap (
    id integer primary key generated always as identity,
    bootstrapped_at timestamptz not null default now()
);
