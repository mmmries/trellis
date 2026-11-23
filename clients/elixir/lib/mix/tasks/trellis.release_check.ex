defmodule Mix.Tasks.Trellis.ReleaseCheck do
  use Mix.Task

  @shortdoc "Fails if this release changes trellis_pg's version and adds a Trellis migration"

  @moduledoc """
  Fails when a release both changes `trellis_pg`'s version in `mix.lock` and
  adds a migration that mentions `Trellis`. While a worker of the old version
  runs, a define, ALTER, PAUSE, RESUME or DROP from the new version refuses
  with `VersionHandoverError`
  ([rolling deploys](../../docs/embedding.md#rolling-deploys)), so a deploy
  that migrates before the old worker stops fails at the first Trellis
  migration. Ship the version change by itself, and the migrations in the
  next release.

      mix trellis.release_check --base origin/main
      mix trellis.release_check BASE=origin/main
      BASE=origin/main mix trellis.release_check

  It compares the working tree with its merge base against the base ref and
  exits 1 for a release that combines the two, or when the check can't run (no
  base, a ref the clone lacks, a shallow clone). It reads git and files only,
  so it needs no database and starts no application. See
  `Trellis.ReleaseCheck`.

  ## Options

    * `--base REF` - the git ref to compare with: the branch a pull request
      targets, or the last deployed ref for a release of several merged pull
      requests. Also read from a `BASE=REF` argument or the `BASE`
      environment variable, in that order.
    * `--migrations-path PATH` - a migrations directory, relative to the
      project (repeatable). Default: `priv/*/migrations`, every repo's
      conventional directory.
    * `--lockfile PATH` - default `mix.lock`.
  """

  @switches [base: :string, migrations_path: :keep, lockfile: :string]

  @impl Mix.Task
  def run(args) do
    {opts, rest} = OptionParser.parse!(args, strict: @switches)
    base = opts[:base] || base_argument(rest) || System.get_env("BASE")

    call_opts =
      [root: File.cwd!(), base: base]
      |> put_paths(Keyword.get_values(opts, :migrations_path))
      |> put_lockfile(opts[:lockfile])

    case Trellis.ReleaseCheck.call(call_opts) do
      {:ok, result} ->
        if Trellis.ReleaseCheck.ok?(result) do
          Mix.shell().info(Trellis.ReleaseCheck.message(result))
        else
          Mix.raise(Trellis.ReleaseCheck.message(result))
        end

      {:error, reason} ->
        Mix.raise("trellis.release_check: #{reason}")
    end
  end

  defp base_argument(rest) do
    Enum.find_value(rest, fn
      "BASE=" <> ref -> ref
      _ -> nil
    end)
  end

  defp put_paths(opts, []), do: opts
  defp put_paths(opts, paths), do: Keyword.put(opts, :migrations_paths, paths)

  defp put_lockfile(opts, nil), do: opts
  defp put_lockfile(opts, file), do: Keyword.put(opts, :lockfile, file)
end
