defmodule Trellis.Target do
  @moduledoc """
  Marks an Ecto schema as a view of a Trellis target table: only Trellis
  writes a target, so a repo with `use Trellis.Repo` refuses every write
  through the schema with `Trellis.ReadOnlyTarget`. Reads, queries and
  associations are untouched.

      defmodule MyApp.EventCount do
        use Ecto.Schema
        use Trellis.Target

        @primary_key false
        schema "event_counts" do
          field :account_id, :integer, primary_key: true
          field :total, :integer
        end
      end

  `config :trellis_pg, allow_target_writes: true` lifts the guard, read at
  each write. It defaults to `false` and is independent of
  `config :trellis_pg, enabled`: turning the engine off does not make a
  target writable. A test suite sets both in `config/test.exs`:

      config :trellis_pg, enabled: false, allow_target_writes: true

  The guard covers the repo's `insert`, `update`, `delete`, `insert_or_update`
  (and bang forms), `insert_all`, `update_all` and `delete_all`, so
  `Ecto.Multi` too. Writes that name no schema go around it: SQL sent with
  `Ecto.Adapters.SQL.query/3`, and a schemaless `insert_all("table", ...)`.
  That is the database grant's job (`docs/recommendations.md`,
  "Application roles read targets, and nothing else").
  """

  defmacro __using__(_opts) do
    quote do
      @doc false
      def __trellis_target__, do: true
    end
  end

  @doc "Whether `allow_target_writes` is true."
  @spec writes_allowed?() :: boolean()
  def writes_allowed?, do: Application.get_env(:trellis_pg, :allow_target_writes, false) == true

  @doc "Whether `schema` is a module marked with `use Trellis.Target`."
  @spec target?(term()) :: boolean()
  def target?(schema) when is_atom(schema) and not is_nil(schema) do
    Code.ensure_loaded?(schema) and function_exported?(schema, :__trellis_target__, 0)
  end

  def target?(_other), do: false

  @doc false
  # Raises ReadOnlyTarget when `subject` names a target schema and writes
  # aren't allowed. `subject` is what a repo write takes: a schema module,
  # a struct, a changeset, a {source, schema} tuple or a query.
  @spec guard!(term(), atom()) :: :ok
  def guard!(subject, write) do
    schema = schema_of(subject)

    if target?(schema) and not writes_allowed?() do
      raise Trellis.ReadOnlyTarget, schema: schema, write: write
    end

    :ok
  end

  defp schema_of(%{__struct__: Ecto.Changeset, data: data}), do: schema_of(data)
  defp schema_of({_source, schema}) when is_atom(schema), do: schema
  defp schema_of(schema) when is_atom(schema), do: schema

  defp schema_of(%{__struct__: struct} = value) do
    if struct == Ecto.Query or function_exported?(struct, :__schema__, 1) do
      queryable_schema(value, struct)
    else
      struct
    end
  end

  defp schema_of(_other), do: nil

  # A schema struct is its own schema; a query's written table is its FROM.
  defp queryable_schema(value, Ecto.Query) do
    case value.from do
      %{source: {_source, schema}} -> schema
      _ -> nil
    end
  end

  defp queryable_schema(_value, struct), do: struct
end
