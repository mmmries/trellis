-- Instance identity marker (issue #28). Every Trellis instance owns exactly
-- one named schema (see docs/instance-identity.md); this table is the
-- durable record, inside that schema, of *which* instance owns it and in
-- what shape. `trellis::identity` reads and seeds it (before this migration
-- runs, on a fresh attach) to decide whether attaching to a schema is a
-- clean initialization, a no-op re-attach to the same instance, or a
-- conflict with something else that must be refused.
--
-- `singleton` enforces the single-row invariant: it's the primary key, can
-- only ever be `true` (the `check` forbids inserting `false`), so a second
-- row would collide with the first on the primary key rather than silently
-- coexisting.
create table if not exists trellis_instance (
    singleton boolean primary key default true check (singleton),
    schema_name text not null,
    instance_format_version integer not null
);
