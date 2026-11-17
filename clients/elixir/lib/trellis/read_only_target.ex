defmodule Trellis.ReadOnlyTarget do
  @moduledoc """
  Raised for a write to an Ecto schema marked with `use Trellis.Target`,
  through a repo that has `use Trellis.Repo`, while
  `config :trellis_pg, allow_target_writes` is not `true`, and by
  `Trellis.TestSupport.refresh_target/3` then (`schema` is `nil`, `write` is
  `:refresh_target`).

  A client-side error: it carries no Trellis error code.
  """

  defexception [:schema, :write, :message]

  @impl true
  def exception(opts) do
    schema = Keyword.get(opts, :schema)
    write = Keyword.fetch!(opts, :write)

    %__MODULE__{
      schema: schema,
      write: write,
      message:
        opts[:message] ||
          "#{inspect(schema)} is a schema over a Trellis target, which only Trellis writes: " <>
            "Repo.#{write} refused. Tests write it with " <>
            "`config :trellis_pg, allow_target_writes: true` (config/test.exs)."
    }
  end
end
