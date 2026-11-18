defmodule Trellis.TestSupport do
  @moduledoc """
  Helpers for a test suite that loads fixtures or bulk data into tables
  Trellis captures. Call them from test code only.

  A fixture loader that disables triggers for the load (Rails-style
  `disable_referential_integrity`, `pg_restore --disable-triggers`, a
  hand-written `ALTER TABLE ... DISABLE TRIGGER ALL`) also disables
  Trellis's capture triggers, so the rows it writes never reach a target,
  and `ENABLE TRIGGER ALL` leaves them in a mode the staging worker treats
  as a disruption of capture. Trellis then pauses every definition that
  reads the table (a `Trellis.CaptureFailure` of kind `:disruption` on its
  status) rather than let a test read targets that are silently wrong. A
  test suite's answer is a `RESUME` after the load, which rebuilds the
  targets from the tables as they are now: `resume_capture_disruptions/2`.

      setup do
        Postgrex.query!(pg, "ALTER TABLE orders DISABLE TRIGGER ALL", [])
        # ... load the fixtures ...
        Postgrex.query!(pg, "ALTER TABLE orders ENABLE TRIGGER ALL", [])

        {:ok, _resumed} = Trellis.TestSupport.resume_capture_disruptions(trellis)
        :ok
      end

  It is cheap when nothing was disrupted (one catalog read per captured
  table), so running it in every test's `setup` is fine.

  ## Ecto's SQL sandbox

  Trellis sees a write when it commits. A test that writes inside a sandbox
  transaction (the default `:manual` or `:shared` mode) rolls it back, so
  nothing reaches a target. Load the fixtures that targets must reflect
  outside the sandbox (in `setup_all`, or with the sandbox in `:auto` mode),
  then call the helper once.
  """

  alias Trellis.{CaptureFailure, Error, Status}

  # `refresh_target/3` runs its SQL over `ecto_sql`, an optional dependency
  # a host that calls it has.
  @compile {:no_warn_undefined, Ecto.Adapters.SQL}

  @default_schema "trellis"

  defmodule ResumeRefused do
    @moduledoc """
    Raised by `Trellis.TestSupport.resume_capture_disruptions!/2` when a
    `RESUME` is refused or a resumed definition pauses again. `refused` maps
    each such target (`"schema.table"`) to the reason. Those definitions stay
    paused; every other disrupted definition was resumed, and waited for,
    before this is raised.
    """
    defexception [:refused, :resumed]

    @impl true
    def message(%{refused: refused}) do
      why = Enum.map_join(refused, "; ", fn {target, why} -> "#{target}: #{why}" end)

      "could not resume #{map_size(refused)} definition(s) paused by a capture disruption " <>
        "(they stay paused): " <> why
    end
  end

  @poll_ms 50

  @doc """
  Resumes every definition of `trellis` that is paused because Trellis's
  capture of a table it reads was changed outside Trellis, waits until each
  is `:live` again, then waits until every write committed so far has
  reached its targets (`Trellis.await_converged/3`), so a test that follows
  reads exact targets. Returns `{:ok, resumed}` with the resumed targets
  (`"schema.table"`; `[]` when nothing was disrupted).

  It runs a disruption check of its own first
  (`Trellis.detect_capture_disruptions/1`), so a disruption the staging
  worker has not seen yet is paused and resumed here rather than a pass
  later.

  It resumes only a `Trellis.CaptureFailure` of kind `:disruption`. A
  definition an operator paused (`PAUSE TRANSFORM`), or one paused by a
  schema change, a drain halt or a failing build, stays paused. A definition
  paused because an upstream one was resumes with its upstream.

  A `RESUME` the engine refuses (for example, the table a definition read is
  missing, or a table define would now refuse) leaves that definition
  paused. The others are still resumed and waited for; then
  `{:error, {:resume_refused, refused}}` is returned, `refused` mapping each
  target to the reason. Any other engine error, or a definition that is not
  `:live` within `:timeout_ms` (default 30 000; a `:timeout` error), is
  `{:error, %Trellis.Error{}}`.

  The instance needs its staging worker and a drain worker running somewhere
  (a test setup usually connects with `staging: true, drain_threads: 1`):
  resuming rebuilds through them.
  """
  @spec resume_capture_disruptions(Trellis.trellis(), timeout_ms: non_neg_integer()) ::
          {:ok, [String.t()]}
          | {:error, {:resume_refused, %{String.t() => String.t()}}}
          | {:error, Error.t()}
  def resume_capture_disruptions(trellis, opts \\ []) do
    deadline = now() + Keyword.get(opts, :timeout_ms, 30_000)

    with {:ok, _tables} <- Trellis.detect_capture_disruptions(trellis),
         {:ok, targets} <- disrupted(trellis),
         {:ok, resumed, refused} <- resume_all(trellis, targets),
         {:ok, refused} <- await_all_live(trellis, resumed, deadline, refused),
         :ok <- converge(trellis, deadline) do
      if map_size(refused) == 0,
        do: {:ok, resumed},
        else: {:error, {:resume_refused, refused}}
    end
  end

  @doc """
  Like `resume_capture_disruptions/2`, but returns the resumed targets and
  raises `Trellis.Error`, or `Trellis.TestSupport.ResumeRefused` when a
  resume is refused.
  """
  @spec resume_capture_disruptions!(Trellis.trellis(), timeout_ms: non_neg_integer()) :: [
          String.t()
        ]
  def resume_capture_disruptions!(trellis, opts \\ []) do
    case resume_capture_disruptions(trellis, opts) do
      {:ok, resumed} -> resumed
      {:error, {:resume_refused, refused}} -> raise ResumeRefused, refused: refused
      {:error, %Error{} = error} -> raise error
    end
  end

  @doc """
  Fills the target `target` (`"table"` or `"schema.table"`, an atom, or an
  Ecto schema module) from its definition's own query: `DELETE` then
  `INSERT ... SELECT` in one transaction on `repo`, after refreshing each
  target it reads that has a definition of its own. For a suite that runs
  with Trellis off (`config :trellis_pg, enabled: false`), where nothing
  maintains a target.

  Reads the definition from Trellis's catalog (the definitions a migration
  or `mix trellis.load` recorded), or from a restore queue a structure file
  loaded without `Trellis.migrate/1` left. Returns `:ok`.

  Raises `Trellis.ReadOnlyTarget` unless
  `config :trellis_pg, allow_target_writes` is `true`, and `ArgumentError`
  when no definition is recorded for `target`, its name is ambiguous, or
  the target is no table, and `Trellis.Error` when the definition computes
  nothing for one of the target's columns. A raise writes nothing.

  Options: `:schemas`, the schemas holding Trellis's tables (default
  `["trellis"]`).
  """
  @spec refresh_target(module(), String.t() | atom(), schemas: [String.t()]) :: :ok
  def refresh_target(repo, target, opts \\ []) do
    unless Trellis.Target.writes_allowed?() do
      raise Trellis.ReadOnlyTarget,
        schema: nil,
        write: :refresh_target,
        message:
          "refresh_target writes a Trellis target, which only Trellis writes: refused. " <>
            "Tests write it with `config :trellis_pg, allow_target_writes: true` (config/test.exs)."
    end

    schemas = Keyword.get(opts, :schemas, [@default_schema])
    name = target_name(target)

    {:ok, :ok} =
      repo.transaction(fn ->
        definitions = recorded_definitions(repo, schemas)
        refresh_one(repo, find_definition(definitions, name, schemas), definitions, [])
      end)

    :ok
  end

  # An Ecto schema's source, qualified by its `@schema_prefix` when it has one.
  defp target_name(target) when is_atom(target) do
    if Code.ensure_loaded?(target) and function_exported?(target, :__schema__, 1) do
      case target.__schema__(:prefix) do
        nil -> target.__schema__(:source)
        prefix -> "#{prefix}.#{target.__schema__(:source)}"
      end
    else
      Atom.to_string(target)
    end
  end

  defp target_name(name) when is_binary(name), do: name

  # Refreshes each target `definition` reads that has a definition of its
  # own, then `definition`'s. `path` holds the targets being refreshed below,
  # so a read cycle can't recurse forever.
  defp refresh_one(repo, definition, definitions, path) do
    for table <- definition.reads,
        upstream = Enum.find(definitions, &(&1.target == table)),
        table not in path do
      refresh_one(repo, upstream, definitions, [definition.target | path])
    end

    [schema, table] = String.split(definition.target, ".", parts: 2)
    quoted = quote_ident(schema) <> "." <> quote_ident(table)

    columns =
      sql!(
        repo,
        "select a.attname::text from pg_catalog.pg_attribute a " <>
          "where a.attrelid = pg_catalog.to_regclass($1) " <>
          "and a.attnum > 0 and not a.attisdropped order by a.attnum",
        [quoted]
      )
      |> List.flatten()

    if columns == [],
      do:
        raise(
          ArgumentError,
          "the target #{definition.target} of a recorded definition is no table"
        )

    select =
      case Trellis.Native.target_select_sql(definition.text, definition.grammar_version, columns) do
        {:ok, select} -> select
        {:error, error} -> raise Error.from_native(error)
      end

    column_list = Enum.map_join(columns, ", ", &quote_ident/1)
    sql!(repo, "delete from #{quoted}", [])
    sql!(repo, "insert into #{quoted} (#{column_list}) #{select}", [])
    :ok
  end

  # Every definition recorded in `schemas`: a schema load's restore queue
  # where there is one, else a live instance's catalog.
  defp recorded_definitions(repo, schemas) do
    Enum.flat_map(schemas, fn schema ->
      quoted = quote_ident(schema)

      cond do
        regclass?(repo, "#{quoted}.restore_queue") -> queued_definitions(repo, quoted)
        regclass?(repo, "#{quoted}.transform_definitions") -> cataloged_definitions(repo, quoted)
        true -> []
      end
    end)
  end

  defp queued_definitions(repo, quoted_schema) do
    queue = "#{quoted_schema}.restore_queue"

    [[later]] =
      sql!(
        repo,
        "select count(*) from pg_catalog.pg_attribute where attrelid = $1::text::regclass " <>
          "and attname = 'later_join_tables' and not attisdropped",
        [queue]
      )

    later = if later > 0, do: "later_join_tables", else: "'{}'::text[]"

    definitions(
      repo,
      "select target_table, source_table, array_remove(array[join_table], null) || #{later}, " <>
        "definition_text, grammar_version from #{queue}"
    )
  end

  defp cataloged_definitions(repo, quoted_schema) do
    definitions(
      repo,
      "select d.target_table, d.source_table, " <>
        "array(select j.table_name from (select 1 as position, d.join_table as table_name " <>
        "where d.join_table is not null union all select f.position, f.table_name " <>
        "from #{quoted_schema}.definition_join_tables f where f.transform_id = d.id) j " <>
        "order by j.position), d.definition_text, d.grammar_version " <>
        "from #{quoted_schema}.transform_definitions d"
    )
  end

  defp definitions(repo, sql) do
    for [target, source, joined, text, version] <-
          sql!(
            repo,
            "select target_table, source_table, joined, definition_text, grammar_version " <>
              "from (#{sql}) as d(target_table, source_table, joined, definition_text, grammar_version)",
            []
          ) do
      %{target: target, reads: [source | joined], text: text, grammar_version: version}
    end
  end

  defp find_definition(definitions, name, schemas) do
    matches =
      Enum.filter(definitions, fn %{target: target} ->
        if String.contains?(name, "."),
          do: target == name,
          else: List.last(String.split(target, ".", parts: 2)) == name
      end)

    case matches do
      [one] ->
        one

      [] ->
        raise ArgumentError,
              "no Trellis definition is recorded for the target #{name} in the schemas " <>
                Enum.join(schemas, ", ")

      many ->
        raise ArgumentError,
              "#{name} names more than one target (#{Enum.map_join(many, ", ", & &1.target)}): " <>
                "qualify it as schema.table"
    end
  end

  defp regclass?(repo, name),
    do: sql!(repo, "select pg_catalog.to_regclass($1)::text", [name]) != [[nil]]

  defp sql!(repo, sql, params), do: Ecto.Adapters.SQL.query!(repo, sql, params).rows

  defp quote_ident(name), do: ~s("#{String.replace(name, ~s("), ~s(""))}")

  # The targets of the definitions paused with a `:disruption`, oldest first.
  defp disrupted(trellis) do
    with {:ok, definitions} <- Trellis.definitions(trellis) do
      definitions
      |> Enum.filter(&(&1.status == :paused))
      |> Enum.reduce_while({:ok, []}, fn definition, {:ok, acc} ->
        case Trellis.status(trellis, definition.target_table) do
          {:ok, %Status{capture_failure: %CaptureFailure{kind: :disruption}}} ->
            {:cont, {:ok, [definition.target_table | acc]}}

          {:ok, _other} ->
            {:cont, {:ok, acc}}

          {:error, _} = error ->
            {:halt, error}
        end
      end)
      |> case do
        {:ok, acc} -> {:ok, Enum.reverse(acc)}
        error -> error
      end
    end
  end

  defp resume_all(trellis, targets) do
    Enum.reduce_while(targets, {:ok, [], %{}}, fn target, {:ok, resumed, refused} ->
      case Trellis.apply(trellis, "RESUME TRANSFORM #{target}") do
        {:ok, _applied} ->
          {:cont, {:ok, resumed ++ [target], refused}}

        {:error, %Error{code: code, message: message}}
        when code in [:validation, :not_found, :conflict] ->
          {:cont, {:ok, resumed, Map.put(refused, target, message)}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
  end

  defp await_all_live(trellis, targets, deadline, refused) do
    Enum.reduce_while(targets, {:ok, refused}, fn target, {:ok, refused} ->
      case await_live(trellis, target, deadline) do
        :ok -> {:cont, {:ok, refused}}
        {:refused, why} -> {:cont, {:ok, Map.put(refused, target, why)}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp await_live(trellis, target, deadline) do
    case Trellis.status(trellis, target) do
      {:ok, %Status{status: :live}} ->
        :ok

      {:ok,
       %Status{status: :paused, capture_failure: %CaptureFailure{kind: kind, error: why}} =
           status}
      when kind != :disruption ->
        # Paused again by something else: its rebuild failed. An error that
        # starts "resuming:" is the resume still under way (the staging
        # worker re-types Trellis's columns first, or waits for an upstream
        # definition), and is waited for. (Paused again by a disruption is
        # waited out, and the timeout reports it.)
        if String.starts_with?(why, "resuming:"),
          do: poll(trellis, target, deadline, status),
          else: {:refused, why}

      {:ok, status} ->
        poll(trellis, target, deadline, status)

      {:error, _} = error ->
        error
    end
  end

  defp poll(trellis, target, deadline, status) do
    if now() > deadline do
      {:error,
       %Error{
         code: :timeout,
         message: "#{target} was not :live in time after its resume; last saw #{inspect(status)}"
       }}
    else
      Process.sleep(@poll_ms)
      await_live(trellis, target, deadline)
    end
  end

  # Everything committed so far reaches its targets, in as many waits as the
  # 30-second call limit needs.
  defp converge(trellis, deadline) do
    with {:ok, token} <- Trellis.watermark_token(trellis), do: converge(trellis, token, deadline)
  end

  defp converge(trellis, token, deadline) do
    remaining = deadline - now()

    if remaining <= 0 do
      {:error, %Error{code: :timeout, message: "targets did not converge in time"}}
    else
      case Trellis.await_converged(trellis, token, min(remaining, 30_000)) do
        {:error, %Error{code: :timeout}} = timeout ->
          if now() >= deadline, do: timeout, else: converge(trellis, token, deadline)

        other ->
          other
      end
    end
  end

  defp now, do: System.monotonic_time(:millisecond)
end
