if Code.ensure_loaded?(Ecto.Repo) do
  defmodule Trellis.Repo do
    @moduledoc """
    Makes an Ecto repo refuse writes to schemas marked with
    `Trellis.Target`. Put it after `use Ecto.Repo`:

        defmodule MyApp.Repo do
          use Ecto.Repo, otp_app: :my_app, adapter: Ecto.Adapters.Postgres
          use Trellis.Repo
        end

    It wraps the repo's write functions (`insert`, `update`, `delete`,
    `insert_or_update`, their bang forms, `insert_all`, `update_all` and
    `delete_all`) and raises `Trellis.ReadOnlyTarget` before any SQL is sent,
    unless `config :trellis_pg, allow_target_writes` is `true`. `Ecto.Multi`
    calls these functions, so it is covered. See `Trellis.Target`.

    Ecto has no callback that sees a struct write (`prepare_query/3` sees
    only queries, and a changeset's `prepare_changes` is opt-in per
    changeset), so the repo's own functions are the one place every write
    passes. Repo-wide `read_only: true` is too coarse: it refuses every
    schema.
    """

    @struct_writes [:insert, :update, :delete, :insert_or_update]

    defmacro __using__(_opts) do
      struct_writes = @struct_writes

      quote bind_quoted: [struct_writes: struct_writes] do
        unless Module.defines?(__MODULE__, {:insert, 2}) and
                 Module.defines?(__MODULE__, {:update_all, 3}) do
          raise ArgumentError,
                "use Trellis.Repo goes after `use Ecto.Repo` in #{inspect(__MODULE__)}"
        end

        for name <- struct_writes, bang <- [name, :"#{name}!"] do
          defoverridable [{bang, 1}, {bang, 2}]

          def unquote(bang)(subject, opts \\ []) do
            Trellis.Target.guard!(subject, unquote(bang))
            super(subject, opts)
          end
        end

        defoverridable insert_all: 2,
                       insert_all: 3,
                       update_all: 2,
                       update_all: 3,
                       delete_all: 1,
                       delete_all: 2

        def insert_all(schema_or_source, entries, opts \\ []) do
          Trellis.Target.guard!(schema_or_source, :insert_all)
          super(schema_or_source, entries, opts)
        end

        def update_all(queryable, updates, opts \\ []) do
          Trellis.Target.guard!(queryable, :update_all)
          super(queryable, updates, opts)
        end

        def delete_all(queryable, opts \\ []) do
          Trellis.Target.guard!(queryable, :delete_all)
          super(queryable, opts)
        end
      end
    end
  end
end
