defmodule Trellis.ReadOnlyTarget do
  @moduledoc """
  Raised for a write to an Ecto schema marked with `use Trellis.Target`,
  through a repo that has `use Trellis.Repo`, while
  `config :trellis_pg, allow_target_writes` is not `true`.

  A client-side error: it carries no Trellis error code.
  """

  defexception [:schema, :write, :message]

  @impl true
  def exception(opts) do
    schema = Keyword.fetch!(opts, :schema)
    write = Keyword.fetch!(opts, :write)

    %__MODULE__{
      schema: schema,
      write: write,
      message:
        "#{inspect(schema)} is a schema over a Trellis target, which only Trellis writes: " <>
          "Repo.#{write} refused. Tests write it with " <>
          "`config :trellis_pg, allow_target_writes: true` (config/test.exs)."
    }
  end
end
