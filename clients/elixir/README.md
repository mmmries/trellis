# Trellis for Elixir

Embedded Trellis for Elixir apps: a [Rustler](https://github.com/rusterlium/rustler)
NIF over the `trellis` crate's `BlockingTrellis`, so a Phoenix app can define
and run streaming transforms without deploying a separate service. The design
is [ADR-0010](https://github.com/salesforce-misc/trellis/blob/main/docs/decisions/0010-embeddable-clients.md).

## Installation

The Hex package is `trellis_pg` (Hex's `trellis` is taken); the modules are
`Trellis.*`, and the OTP application is `:trellis_pg`.

```elixir
def deps do
  [{:trellis_pg, "~> 0.3.0"}]
end
```

Installing needs no Rust: compiling the package downloads the NIF built for
the machine from the release's GitHub page and checks it against the SHA-256
the package ships. Builds exist for

- Linux, glibc 2.17 or newer: `x86_64` and `aarch64`
- Linux, musl (Alpine): `x86_64` and `aarch64`
- macOS: Apple silicon and Intel

### Supported versions

Elixir 1.19 and later, on the OTP releases Elixir supports for it (26 to 29
today); `ecto_sql` 3.13+ and `plug` 1.16+ where used; the PostgreSQL
versions [Trellis supports](../../docs/embedding.md#supported-postgresql-versions). CI runs the suite on Elixir 1.19 / OTP 26 and
Elixir 1.20 / OTP 29.

On any other target, or to build it yourself, build the NIF from source. It
needs a Rust toolchain (the version in the repository's `rust-toolchain.toml`)
and Rustler in the host's own deps:

```elixir
{:rustler, "~> 0.38.0", runtime: false}
```

```sh
TRELLIS_PG_BUILD=1 mix deps.compile trellis_pg --force
```

The choice is made when `trellis_pg` compiles, so `--force` it after changing
the variable. `config :rustler_precompiled, :force_build, trellis_pg: true`
does the same from config. Cargo fetches the `trellis` crate from this
repository at the released commit.

The NIF is built without the `trellis` crate's `otlp` feature, so neither
path pulls in an OpenTelemetry or gRPC stack. The engine's log lines reach
`Logger` through `Trellis.LogBridge`.

## Usage

The surface mirrors the Rust crate's `BlockingTrellis`. Every function has a bang variant.

Connect directly or through a session-mode pooler, never a transaction-mode one ([recommendations](../../docs/recommendations.md#connect-directly-or-through-a-session-mode-pooler)).

- `connect/1`, `migrate/1`, `config/1`, `shutdown/1`: the handle's lifecycle.
- `apply/2`: any statement of Trellis's grammar (`TRANSFORM`,
  `PAUSE TRANSFORM`, `RESUME TRANSFORM`, `DROP TRANSFORM`,
  `ALTER TRANSFORM`), returning what it did as a tagged
  value (`{:transform_defined, target_table}`, `:paused`,
  `{:resumed, addresses}`, ...). `define/2` is an `apply/2` that refuses
  anything but a `TRANSFORM`.
- `status/2`, `definitions/1`: the catalog reads. `status/2` returns a
  `Trellis.Status` (with nested `Trellis.DrainFailure`,
  `Trellis.SourceIndexGap`, `Trellis.ExtremeIndexGap`,
  `Trellis.CorrectingNode`, `Trellis.VersionHandover`,
  `Trellis.BelowCatalogFloor`, `Trellis.RefusedStep`, `Trellis.JoinStatus`
  and the like) whose fields are named and defined in the
  [status reference](../../docs/status-reference.md).
- `quarantined/1`, `quarantine_status/2`, `sample_quarantined/3`,
  `poisoned_since/2`: the quarantine reads. Resuming is a statement.
  `release_key/4` releases one key a transform holds, once its cause is
  fixed.
- `request_backfill/2`, `detect_capture_disruptions/1` (pauses now the readers
  of every table whose Trellis capture was changed outside Trellis, and
  returns the tables), `has_live_drain_workers/1`,
  `has_live_staging_worker/1`, `watermark_token/1`, `await_converged/3`.
- `self_check/3` and `self_check_job/2`: check a target against a fresh
  recompute from its source, as a background job. `self_check/3` starts it and
  returns a `Trellis.SelfCheckJob` (`id`, `target`, `mode`, `state`,
  `rows_compared`, `report`, `error`) at once; a drain worker runs it, and
  `self_check_job/2` reads it back by `id` until `state` is `:done`
  (`:queued` and `:running` are in progress; `:failed` and `:cancelled` carry
  an `error`), when `report` is a `Trellis.SelfCheckReport` (`outcome`,
  `divergences`, `rows_compared`, `truncated`, ...). A target whose job is
  still unfinished gets that job back; the next call replaces a finished
  one, and `self_check_job/2` returns `nil` for the replaced job or one whose
  transform was dropped. With no drain worker anywhere in the fleet a job
  stays `:queued`.
- `start_link/1`, as `{Trellis, options}` in a supervision tree: a process
  that owns the handle, so every function above also takes its name.
- `Trellis.Migration`: `define`, `apply` and `status` in an Ecto migration.
- `schema_dump/1`, `filter_schema_dump/2`, `mix trellis.dump`,
  `mix trellis.load` and `Trellis.StructureDump`: the definitions in the
  structure file, restored when it loads (see [Schema dumps](#schema-dumps)).
- `Trellis.Metrics.render_prometheus/0`: the process-wide metrics registry
  as Prometheus text, for a `/metrics` route the host already serves (the
  binding opens no port). It needs no handle. Every series carries the
  `trellis_instance` label (`<database>/<catalog schema>`, as in the log
  lines), so handles in one VM stay apart. With `:plug` in your deps,
  `Trellis.Metrics.Plug` serves it (`forward "/metrics/trellis", Trellis.Metrics.Plug`),
  and `Trellis.Metrics.Appender` (`plug Trellis.Metrics.Appender, path: "/metrics"`,
  ahead of `PromEx.Plug` or another exporter plug) appends it to that
  exporter's response. The status gauges come with it; see the "Alerting"
  table in `docs/observability.md`.
- `Trellis.LogBridge`: started with the `:trellis_pg` application, it forwards
  the engine's log lines to `Logger` (`domain: [:trellis]`, the Rust module
  as `:target` metadata). Every line names its instance in the text, as
  `trellis_instance=<database>/<catalog schema>`, so handles in one VM stay
  apart. Set `config :trellis_pg, log_level: :info` to choose the most
  verbose level forwarded (default: `Logger.level()`, read once at start),
  or `config :trellis_pg, log_bridge: false` to forward nothing. A
  `tracing` subscriber installed by another NIF never sees the engine's
  lines, since each native library links its own `tracing`; see
  `Trellis.LogBridge`. A line can be dropped if the bridge falls behind,
  and a warning says how many; logging never blocks or crashes the engine.

```elixir
# A deploy's migration step: the defaults run nothing in the background.
{:ok, migrator} = Trellis.connect(url: "postgres://localhost/app")
# TLS options go in the URL (docs/embedding.md, "TLS to Postgres"):
#   url: "postgres://db.example.com/app?sslmode=verify-full&sslrootcert=/etc/ssl/ca.pem"
:ok = Trellis.migrate(migrator)
:ok = Trellis.shutdown(migrator)

# The app, once migrated. The staging worker reads the catalog as it starts,
# so a `staging: true` handle can't connect before `migrate/1` has run.
{:ok, trellis} = Trellis.connect(url: "postgres://localhost/app", staging: true, drain_threads: 2)
{:ok, _definition} = Trellis.define(trellis, "TRANSFORM widget_prices FROM widgets SELECT price AS price")
{:ok, %Trellis.Status{status: :live}} = Trellis.status(trellis, "widget_prices")

# A column that paused on bad data: find the rows, fix them, resume it.
{:ok, [%Trellis.QuarantineEntry{target: "widget_prices.price"} | _]} = Trellis.quarantined(trellis)
{:ok, %Trellis.SamplePage{samples: rows, next_cursor: cursor}} =
  Trellis.sample_quarantined(trellis, "widget_prices.price", limit: 50)
{:ok, {:resumed, ["widget_prices.price"]}} =
  Trellis.apply(trellis, "RESUME TRANSFORM widget_prices.price")

:ok = Trellis.shutdown(trellis)
```

`connect/1`'s defaults (`staging: false, drain_threads: 0`) run nothing in
the background. For each instance (catalog schema), some connection in the
fleet must set `staging: true` and some connection must run drain threads, or
no definition ever reaches `:live`. Any number of connections may set
`staging: true`: one takes the producer lock and leads, the rest stand by and
take over when it goes away, so a rolling deploy needs no operator step and
the application boots even when the old worker is still running (see
[Rolling deploys](../../docs/embedding.md#rolling-deploys)).

### Resources

At the defaults, a handle holds up to 20 Postgres connections (its call pool,
opened as calls need them and closed after 60 seconds idle) and 3 threads. A
worker handle (`staging: true, drain_threads: N`) also owns a background pool
of up to `2N + 6` connections (`2N + 5` without staging, `2` with staging
alone), closed after 60 seconds idle, and two connections that stay open: the
staging session and one `LISTEN` shared by its drain threads. It holds 6
threads: drain threads are tasks on its runtime, not OS threads. Its memory is
bounded by row counts, not bytes: about `N` × 100,000 changed rows' images at
its peak. `:max_call_pool_size` lowers the 20 (the background pool is sized by
the handle's roles, not by an option), and `:worker_threads` sets the threads.
Every handle (each `{Trellis, name: ...}` child) costs this again. The full
accounting is in
[`docs/embedding.md`](../../docs/embedding.md#what-a-handle-uses).

### Conventions

- Non-bang functions return `{:ok, value}` (or `:ok`) or
  `{:error, %Trellis.Error{code: code, error_id: error_id, message: message}}`;
  bang variants return the value or raise that error. `code` is one of a
  closed set of atoms, with `:unknown` for a code newer than the binding.
  `error_id` is the engine's per-error code (`"T0042"`), which the message
  starts with, or `nil` for an error the binding raises itself.
- No atom is ever built from a string the database returned: every atom in a
  result comes from a closed set the NIF allocates when it loads.
- Times are `DateTime`s. They cross the NIF as epoch microseconds.
- A quarantine target is an address string, `"transform"` or
  `"transform.column"`, exactly as `quarantined/1` reports it.
- `sample_quarantined/3`'s cursor and `watermark_token/1`'s token are
  opaque: pass back what the previous call returned.
- A call on a handle holds no scheduler while it waits. The NIF submits the
  call on a normal scheduler and returns a reference at once; the engine
  sends the result to the calling process, which waits for it in a `receive`.
  The function is synchronous to its caller, a process killed during a call
  exits at once, and a call stuck on the database holds no scheduler or
  thread of its own.
  `connect/1` and `shutdown/1` run on dirty IO schedulers, as do the NIFs
  that touch no handle; `Trellis.Metrics.render_prometheus/0` runs on a
  dirty CPU scheduler.
- Every call returns within 30 seconds, counted from when it is submitted
  (a database that can't answer in time gives `{:error, %Trellis.Error{code:
  :timeout}}`; the server stops what the call started). Calls run in
  parallel, on one handle and across processes: a call stuck on a lock
  doesn't delay another process's `status/2`. A process's own calls stay in
  order, because it waits for each reply; calls from different processes
  have no order.
- `shutdown/1` doesn't wait for calls in flight: it cancels them, and each
  of their callers gets a `:validation` error.

## In a Phoenix app

Let the application's supervisor own the handle. `{Trellis, options}` starts
a process that connects with `connect/1`'s options as it starts, hands the
handle to the callers that make calls through its name, and shuts the handle
down when the supervisor stops it:

```elixir
# config/runtime.exs
# A direct or session-mode URL, not the Repo's pooled DATABASE_URL.
database_url = System.fetch_env!("TRELLIS_DATABASE_URL")

config :my_app, MyApp.Trellis,
  name: MyApp.Trellis,
  url: database_url,
  # Only the fleet's one worker node runs the background work.
  staging: System.get_env("TRELLIS_WORKER") == "true",
  drain_threads: if(System.get_env("TRELLIS_WORKER") == "true", do: 2, else: 0)
# The worker is one more copy of the app, e.g. a Procfile line:
#   worker: TRELLIS_WORKER=true mix run --no-halt

# lib/my_app/application.ex
children = [
  MyApp.Repo,
  {Trellis, Application.fetch_env!(:my_app, MyApp.Trellis)},
  MyAppWeb.Endpoint
]

# Anywhere in the app: the name stands in for the handle.
{:ok, status} = Trellis.status(MyApp.Trellis, "widget_prices")
```

Each call runs in the process that makes it, in parallel with every other,
so no process queues behind another's call. The caller has to be on the same
node: a handle works only on the node that connected it. `shutdown/1` refuses
the name; the supervisor stops it, which cancels the calls still in flight.

Define transforms in Ecto migrations with `Trellis.Migration`. Trellis never
joins a migration's transaction, so a migration that uses it must set
`@disable_ddl_transaction true` (the helpers raise before applying anything
if a transaction is open), and must define `up/0` and `down/0`:

```elixir
defmodule MyApp.Repo.Migrations.DefineWidgetPrices do
  use Ecto.Migration
  use Trellis.Migration

  @disable_ddl_transaction true

  def up do
    define "TRANSFORM widget_prices FROM widgets SELECT price AS price"
  end

  def down do
    apply "PAUSE TRANSFORM widget_prices"
    apply "DROP TRANSFORM widget_prices"
  end
end
```

Ecto doesn't start the application to migrate, so each helper connects a
handle of its own, from the repo's `:trellis` configuration
(`config :my_app, MyApp.Repo, trellis: [url: database_url]`), and runs
`migrate/1` on it first. A define isn't idempotent, and the helpers don't
make it so: Ecto's `schema_migrations` runs each migration once. See
`Trellis.Migration` for the details, and for an optional guard.

Add `import_deps: [:trellis_pg]` to the app's `.formatter.exs` to keep
`define` and `apply` free of parentheses, like Ecto's own commands.

### Upgrading the package

A release that changes `trellis_pg`'s version can't also define, alter,
pause, resume or drop a transform. While a worker of the old version runs,
those calls refuse with `VersionHandoverError`
([rolling deploys](../../docs/embedding.md#rolling-deploys)), so a deploy that
migrates before the old worker stops fails at its first Trellis migration.
Ship the version change by itself, and the migrations in the next release.

`mix trellis.release_check` fails a build that combines the two:

```sh
mix trellis.release_check --base origin/main
```

It compares the working tree with its merge base against the base, and exits
1 when `trellis_pg`'s version in `mix.lock` changed and a migration added
since then (in each `:ecto_repos` repo's migrations directory, or each
`--migrations-path`) mentions `Trellis`. It reads git and files only: it
needs no database and starts no application. `BASE=origin/main` as an
argument, or in the environment, sets the base too.

- **On a pull request**, the base is the branch it targets, and the job needs
  the history back to the merge base (`actions/checkout` with
  `fetch-depth: 0`).
- **For a release of several merged pull requests**, the base is the last
  deployed ref.
- It sees migrations only. A state-changing call from a script or a console
  is in no file it reads; the engine still refuses it.

## Testing

The recipe: Trellis off in the test environment, targets filled by hand.

```elixir
# config/test.exs
config :my_app, MyApp.Repo,
  # ...the generated settings...
  # The migration helpers connect here, so the test repo needs its own url,
  # partition suffix included.
  trellis: [url: "postgres://localhost/my_app_test#{System.get_env("MIX_TEST_PARTITION")}"]

config :trellis_pg, enabled: false, allow_target_writes: true
```

Off, `{Trellis, options}` starts nothing and nothing runs in the background,
but the `Trellis.Migration` helpers still run. Phoenix's default `test` alias
(`ecto.create`, `ecto.migrate`) builds every target, and a new define reaches
the test database on the next `mix test`. With no worker, definitions stay
`waiting_to_backfill` and sources get no capture triggers, so test writes
capture nothing and targets stay empty. `Trellis.enabled?/0` reports the
setting. `allow_target_writes` lets a test write target schemas (see below).

`Trellis.TestSupport.refresh_target/2` fills a target from its definition's
own query (upstream targets first, one transaction). It needs
`allow_target_writes: true`. Inside the SQL sandbox:

```elixir
test "revenue by region" do
  Repo.insert!(%Order{region: "west", total: 40})
  Repo.insert!(%Order{region: "west", total: 60})
  Trellis.TestSupport.refresh_target(MyApp.Repo, "sales_by_region")
  assert %{total: 100} = Repo.get_by!(SalesByRegion, region: "west")
end
```

Or write the target rows by hand, in the sandbox, with `allow_target_writes`.

- A `GROUP BY` or `JOIN` define needs a source index on its columns
  ([T0134](../../docs/errors.md#t0134-sourceindexmissing)), so the migration
  creates one first:

  ```elixir
  def up do
    create index(:orders, [:region])
    define "TRANSFORM sales_by_region FROM orders SELECT region AS region, SUM(total) AS total GROUP BY region"
  end
  ```

- Give `async: true` tests distinct group keys. Two tests refreshing the same
  group each insert it into the target's unique index, and the second waits
  for the first's sandbox transaction to end, which serializes them.
- Drop a test database a worker has run on (`MIX_ENV=test mix ecto.drop`).
  It keeps its capture triggers and live definitions after Trellis is turned
  off, so every test write pays the capture cost.

### Trellis on in tests

To test the real pipeline, run the worker and wait for it, in its own
database (another name or `MIX_TEST_PARTITION`), never the off suite's. Its
fixtures and the target rows the worker writes stay committed: in the off
suite's database they break exact counts, and every `refresh_target` of
those targets locks the same rows, which serializes async tests.

```elixir
setup_all do
  trellis = Trellis.connect!(url: url, staging: true, drain_threads: 2)
  # Fixtures outside the sandbox, so they commit.
  Ecto.Adapters.SQL.Sandbox.unboxed_run(MyApp.Repo, fn -> load_fixtures() end)
  token = Trellis.watermark_token!(trellis)
  :ok = Trellis.await_converged!(trellis, token, 30_000)
  on_exit(fn -> Trellis.shutdown(trellis) end)
  %{trellis: trellis}
end
```

Trellis sees a write when it commits, so the fixtures that targets must
reflect load outside the sandbox transaction (in `setup_all`, or with the
sandbox in `:auto` mode), and one `await_converged!` waits for them.

### Bulk loads with triggers disabled

A loader that disables triggers for the load (`ALTER TABLE ... DISABLE
TRIGGER ALL`, `pg_restore --disable-triggers`) disables Trellis's capture
triggers too, and `ENABLE TRIGGER ALL` leaves them in a mode Trellis treats as
a disruption. The rows it wrote never reached a target, so Trellis pauses
every definition that reads the table (`capture_failure` has `kind:
:disruption`) instead of letting a test read silently wrong data, and waits
for a `RESUME`. `Trellis.TestSupport` does that `RESUME` for a suite, then
waits for the rebuilt targets to catch up:

```elixir
setup do
  # ... load the fixtures with triggers disabled, then re-enabled ...
  {:ok, _resumed} = Trellis.TestSupport.resume_capture_disruptions(trellis)
  :ok
end
```

`resume_capture_disruptions/2` first pauses the readers of any disrupted
table itself (`Trellis.detect_capture_disruptions/1`), so it doesn't wait for
the staging worker's next pass; resumes only the definitions paused with kind
`:disruption`; waits until each is `:live` and every write committed so far
has reached its targets; and returns `{:ok, resumed}` (`[]` when nothing was
disrupted, which costs one catalog read per captured table). A transform an
operator paused, or one paused by a schema change or another failure, stays
paused. A `RESUME` the engine refuses (a table that is gone, a table define
would now refuse) leaves that definition paused; the rest are still resumed,
and the call then returns `{:error, {:resume_refused, refused}}`, a map of
target to reason. `resume_capture_disruptions!/2` returns the targets and
raises `Trellis.TestSupport.ResumeRefused` (or `Trellis.Error`). The
`:timeout_ms` option (default 30 000) bounds the wait. The handle needs its
staging worker and a drain worker (`staging: true, drain_threads: 1`),
because the resume rebuilds through them.

## Target schemas are read-only

Only Trellis writes a target. Mark an Ecto schema over one, and add the guard to the repo:

```elixir
defmodule MyApp.EventCount do
  use Ecto.Schema
  use Trellis.Target
  # schema "event_counts" do ... end
end

defmodule MyApp.Repo do
  use Ecto.Repo, otp_app: :my_app, adapter: Ecto.Adapters.Postgres
  use Trellis.Repo # after `use Ecto.Repo`
end
```

`insert`, `update`, `delete`, `insert_or_update` (and bang forms), `insert_all`, `update_all`
and `delete_all` on a marked schema raise `Trellis.ReadOnlyTarget` before any SQL is sent;
`Ecto.Multi` goes through them. Raw SQL and writes that name no schema
(`insert_all("table", ...)`, `from("table")`) go around the guard: a `SELECT`-only grant
is the strong answer (`docs/recommendations.md`).

`config :trellis_pg, allow_target_writes: true` lifts it (default `false`, read at each
write, independent of `enabled`).

## Schema dumps

`mix trellis.dump` writes the structure file `mix ecto.dump` writes, with
the definitions in it the way Ecto puts `schema_migrations` in it, and
`mix trellis.load` loads it. Make them what `mix ecto.dump` and
`mix ecto.load` run, by aliases in `mix.exs`:

```elixir
defp aliases do
  ["ecto.dump": "trellis.dump", "ecto.load": "trellis.load"]
end
```

The file has the application's tables, the target tables (with their
indexes and grants), the `schema_migrations` rows, and one row per
definition: its source, target and `JOIN` table, its text as written, its
grammar version and its floor. Trellis's own schema, the capture triggers on
source tables and the 1-1 targets' ledgers are not in it, so regenerating
the file changes it only where a definition changed. `mix trellis.load` runs
`mix ecto.load` and then `Trellis.migrate/1` for each repo with a `:trellis`
configuration, which creates Trellis's tables and restores each definition
into its dumped target. Each is restored unbuilt, and the worker builds it
from the source rows; a paused or quarantined transform is restored
unpaused, because a pause is runtime state. A file loaded another way
(`psql -f`) needs `Trellis.migrate/1` after it. How the restore works, and
what it refuses, is in
[`docs/embedding.md`](../../docs/embedding.md#schema-dumps).

`mix ecto.dump` can't pass `pg_dump` extra arguments, so for a repo with a
`:trellis` configuration (as for `Trellis.Migration`) `mix trellis.dump`
runs `pg_dump` itself, with `ecto.dump`'s arguments (`-r`, `-d`, `--prefix`,
`-q`) and `Trellis.schema_dump/1`'s, which leave Trellis's schema and the
ledgers out. With `--prefix`, the instance's target schema is dumped too. It
takes the capture triggers out of the output (`Trellis.filter_schema_dump/2`),
and reads the definitions before and after the `pg_dump`, redoing the dump,
up to three times, when a define or drop committed in between, so the
file's DDL and definitions agree. A repo with no `:trellis` configuration
is dumped by `mix ecto.dump`.

`mix trellis.dump` also dumps a repo's `:migration_repo`, as `mix ecto.dump`
does, before the Trellis repo's file, and with `--prefix` it reads
`schema_migrations` from the schemas named only (not the target schema it adds
to the dump). The repo and its `:trellis` url have to name the same database,
by host, port and database name spelled alike (`localhost` is not
`127.0.0.1`; a repo connecting over a socket directory, or naming no
host, matches a local host), or `mix trellis.dump` and `mix trellis.load`
refuse, before anything is written or loaded. A dump that fails once
`pg_dump` has written the file removes it, since its raw `pg_dump` output
would load without the definitions; a file no `pg_dump` wrote to is left as
it was. `mix trellis.load` restores nothing for a repo `mix ecto.load`
left alone (`--skip-if-loaded` on a loaded database, or a "no" to its
prompt).

`pg_dump` 17 writes `CREATE SCHEMA public;` when it dumps `public` by name,
which stops `mix ecto.load` into a database `mix ecto.create` made; the task
writes `CREATE SCHEMA IF NOT EXISTS public;` instead.

A structure file written by an earlier release, which carries the Trellis
schema and the definitions' catalog rows, still loads.
`Trellis.StructureDump.append/2` writes that format onto a structure file
someone else dumped with the Trellis schema in it (reading the definitions
once, so it can't see a define racing that dump), and
`Trellis.dump_definitions_sql/1` returns its SQL.

## Layout

- `lib/`: the public `Trellis` module and its structs, the process that
  owns a supervised handle (`lib/trellis/owner.ex`), and `Trellis.Migration`, which
  is compiled only when the host depends on `ecto_sql`.
- `lib/trellis/test_support.ex`: `Trellis.TestSupport`, the test helper above.
- `native/trellis_nif/`: the NIF crate, a member of the repository's Cargo
  workspace. The release makes the packaged copy stand alone (see
  Releasing). Plain-data conversion and error codes come from
  `clients/embed` (`trellis-embed`), shared with the Ruby binding.

## Development

The Erlang and Elixir versions are pinned in `.tool-versions`. From this
directory:

```sh
cargo build -p testkit --bin trellis-testkit   # the suite's throwaway Postgres
mix deps.get
mix test
```

An app that takes `trellis_pg` as a `path:` dependency (to test a change here
in a scratch app) compiles it in `:prod`, which downloads the NIF. Add
`{:rustler, "~> 0.38.0", runtime: false}` to the app's deps and compile with
`TRELLIS_PG_BUILD=1` to build it from source.

In this repository (`:dev` and `:test`) the NIF always builds from source,
never downloads. `mix test` compiles it through Cargo into the workspace's
`target/`, and the suite spawns `trellis-testkit` (from `PATH`, else the
workspace's debug build) for a Postgres cluster that is torn down when the
test run exits. It needs the Postgres server binaries on `PATH`, like the
Rust tests.

`test/trellis/parity_test.exs` runs `clients/parity/cases.json`, the fixture
the Ruby suite runs too, so the two bindings can't drift apart unnoticed
(see `clients/parity/README.md`).

## Releasing

`trellis_pg` is released with the rest of Trellis, from one `v<version>` tag
([docs/releasing.md](../../docs/releasing.md)). `@version` in `mix.exs` and
the NIF crate's version (`native/trellis_nif/Cargo.toml`) are the release's.
`.github/workflows/release.yml` builds the NIF for each target above and
attaches them to the tag's GitHub release, where `lib/trellis/native.ex`
downloads them from. The package is then published to Hex by hand with
`.github/scripts/publish-packages.sh`, which writes
`checksum-Elixir.Trellis.Native.exs` from the NIFs that release serves.

`.github/scripts/build-elixir-nif.sh <target> <out-dir>` builds one NIF the
way the workflow does. Linux targets need `cargo-zigbuild` and Zig
(`pip install cargo-zigbuild ziglang`).
