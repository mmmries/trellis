# Instance Identity

Every Trellis instance owns exactly one **named PostgreSQL schema**, and all of
its internal state lives there. That state includes the [staging
ring](staging-and-claiming/02-the-staging-ring.md), the migration ledger, and —
as the project grows — the transform catalog and any other bookkeeping Trellis
manages on the user's behalf. Nothing Trellis owns lives in `public` or is
scattered across unqualified names.

## Why a named schema

Tying an instance to a single schema gives us two things:

* **A clean footprint.** Everything Trellis creates is qualified by one schema,
  so an install can be inspected, backed up, or dropped as a unit without
  touching the application tables that share the database.
* **Multiple instances per cluster.** Because the schema name is configurable,
  several independent Trellis instances can coexist in one database cluster —
  even one database — without colliding. Each instance is identified by, and
  isolated within, its own schema.

## Default and configuration

By default Trellis qualifies its records under a schema named `trellis`. The
name is configurable via the `TRELLIS_SCHEMA` environment variable, so an
operator running more than one instance in the same database gives each a
distinct schema.

The schema is created if it does not already exist when migrations run, and the
connection pool pins `search_path` to it on every connection so Trellis's own
SQL never needs to qualify names by hand. Resolution and defaulting live in
`trellis/src/config.rs` (`DEFAULT_SCHEMA`); the `search_path` seam lives in
`trellis/src/pool.rs`. `Config::schema()` exposes the resolved schema, and
`Config`'s `Display` impl gives a one-line summary suitable for logging.

A configured name that isn't usable as a schema is rejected before it ever
reaches SQL: `config::validate_schema_name` rejects an empty or all-whitespace
name, a name containing a NUL byte, and a name longer than Postgres's
63-byte `NAMEDATALEN` limit (Postgres would otherwise silently truncate it,
which could make two configured names collide without anyone noticing).
Beyond that the character set is intentionally unrestricted — every place
the name reaches SQL goes through `pool::quote_ident` as a quoted
identifier, so Postgres itself decides what's acceptable. `Config`'s `dsn`
and `schema` fields are private; every constructor (`Config::resolve` and
`Config::from_dsn` both delegate to `Config::with_schema`) validates the
schema, so there is no path to a `Config` carrying an unvalidated name.

## Detecting a foreign or incompatible schema

Sharing a name isn't enough to prove two attaches are the same instance —
the schema could have been recreated, restored under the wrong name, or
never have been Trellis's to begin with. Each instance therefore writes a
single-row identity marker, `trellis_instance` (added in migration V9),
recording the schema name it was created under and an
`instance_format_version` (see `trellis::identity::INSTANCE_FORMAT_VERSION`).

Before the migration runner ever touches the schema, `trellis::identity::prepare_attach`
(called from `trellis::migrate`) checks it:

* **Fresh, or ours mid-migration** (schema doesn't exist yet, exists but is
  empty, or exists with Trellis's own migration ledger —
  `refinery_schema_history` — but no marker yet) — safe to (re)take.
  The "ours mid-migration" case matters for crash recovery: `migrate` isn't
  one transaction (refinery commits each migration separately, and the
  marker seed is a separate statement after), so a process death between
  the runner creating `trellis_instance` and the seed committing leaves a
  schema with our tables and ledger but an empty marker table. Because the
  ledger's presence is what distinguishes "ours, unfinished" from
  "genuinely foreign," this state is treated exactly like a fresh attach,
  not refused.
* **Same instance** (marker present, schema name matches, format version
  understood) — a clean, idempotent no-op.
* **Refused** — a mismatched schema name in the marker, a format version
  newer than this build understands, or a pre-existing schema with foreign
  objects and *neither* a marker *nor* a migration ledger. Each of these
  surfaces as a typed `Error::IncompatibleInstance` rather than silently
  proceeding.

After the runner has ensured `trellis_instance` exists, `trellis::identity::seed_marker`
always runs — an `insert ... on conflict (singleton) do nothing`, not a
plain insert gated on "is this fresh." That makes seeding idempotent across
three cases that would otherwise need separate handling: a clean re-attach
(the row already matches, nothing changes), the crash-recovery case above
(the row genuinely doesn't exist yet, so it's inserted), and two processes
racing to migrate the same brand-new schema for the first time (the loser's
insert becomes a no-op instead of a unique-violation error).

`trellis::identity::Identity::resolved` reports the identity (schema +
format version) this build would attach as, without touching the database.
