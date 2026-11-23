defmodule Mix.Tasks.Trellis.ReleaseCheck do
  use Mix.Task

  @shortdoc "Fails if this release changes trellis_pg's version and adds a Trellis migration"

  @moduledoc """
  Fails when a release both changes `trellis_pg`'s version in `mix.lock` and
  adds a migration that mentions `Trellis`. While a worker of the old version
  runs, a define, ALTER, PAUSE, RESUME or DROP from the new version refuses
  with `VersionHandoverError` (see `docs/embedding.md`, "Rolling deploys"),
  so a deploy that migrates before the old worker stops fails at the first
  Trellis migration. Ship the version change by itself, and the migrations in the
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
      project (repeatable). Default: each of the app's `:ecto_repos`
      migrations directory (its `:priv`, default `priv/<repo>`, plus
      `migrations`), or `priv/*/migrations` for an app without
      `:ecto_repos`.
    * `--lockfile PATH` - default the project's `:lockfile` (`mix.lock`;
      `../../mix.lock` in an umbrella child, where the task runs, like
      Ecto's).
  """

  @switches [base: :string, migrations_path: :keep, lockfile: :string]

  @impl Mix.Task
  def run(args) do
    {opts, rest} = OptionParser.parse!(args, strict: @switches)
    base = opts[:base] || base_argument(rest) || System.get_env("BASE")

    call_opts =
      [root: File.cwd!(), base: base]
      |> put_paths(paths(Keyword.get_values(opts, :migrations_path)))
      |> Keyword.put(:lockfile, opts[:lockfile] || Mix.Project.config()[:lockfile] || "mix.lock")

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

  defp paths([]) do
    Mix.Task.run("loadconfig")
    ecto_migrations_paths(Mix.Project.config()[:app]) || []
  end

  defp paths(paths), do: paths

  # Each repo in `app`'s `:ecto_repos`: its `:priv` (Ecto's default
  # `priv/<repo>`) plus `migrations`, as `mix ecto.migrate` reads them.
  # Config only, so nothing is compiled or started. nil without repos.
  @doc false
  def ecto_migrations_paths(nil), do: nil

  def ecto_migrations_paths(app) do
    case Application.get_env(app, :ecto_repos, []) do
      [] ->
        nil

      repos ->
        for repo <- repos do
          priv =
            Keyword.get(Application.get_env(app, repo, []), :priv) ||
              "priv/#{repo |> Module.split() |> List.last() |> Macro.underscore()}"

          Path.join(priv, "migrations")
        end
    end
  end

  defp put_paths(opts, []), do: opts
  defp put_paths(opts, paths), do: Keyword.put(opts, :migrations_paths, paths)
end
