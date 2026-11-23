defmodule Trellis.ReleaseCheck do
  @moduledoc """
  A static check for CI: does this release both change the `trellis_pg`
  dependency's version and add a migration that uses Trellis?

  Such a release fails where migrations run before the old processes stop
  (a release command, a pre-deploy job): while a worker of the old version is
  live, a define, ALTER, PAUSE, RESUME or DROP from the new version refuses
  with `VersionHandoverError`
  ([rolling deploys](../../docs/embedding.md#rolling-deploys)). The remedy is
  to ship the version change by itself and the migrations in the next
  release, and this check says so on the pull request instead of in the
  deploy.

      case Trellis.ReleaseCheck.call(root: File.cwd!(), base: "origin/main") do
        {:ok, %Trellis.ReleaseCheck{} = result} -> Trellis.ReleaseCheck.message(result)
        {:error, reason} -> reason
      end

  `mix trellis.release_check --base origin/main` runs it.

  It reads git and files only: no database, no Trellis handle and no NIF. It
  compares the working tree under `root` with its merge base against `base`,
  so a branch that is behind `base` is judged by what the branch itself
  changes, and uncommitted work counts.

  It sees migrations only. A state-changing call from a deploy script or a
  console isn't in any file it reads; the engine's refusal covers those.
  """

  @package "trellis_pg"

  @enforce_keys [:base, :old_version, :new_version, :migrations]
  defstruct [:base, :old_version, :new_version, :migrations]

  @typedoc """
  What the check found. `base` is the merge-base commit it compared with, the
  versions are `trellis_pg`'s in `mix.lock` there and in the working tree
  (`nil` where the lockfile doesn't list the package), and `migrations` are
  the migrations added since `base` that mention Trellis, relative to `root`.
  """
  @type t :: %__MODULE__{
          base: String.t(),
          old_version: String.t() | nil,
          new_version: String.t() | nil,
          migrations: [String.t()]
        }

  @doc """
  Runs the check for the app at `:root` against the git ref `:base`: the
  branch a pull request targets, or the last deployed ref for a release of
  several merged pull requests.

  `:migrations_paths` (default `[":(glob)priv/*/migrations/**"]`, git pathspecs, so
  every repo's conventional directory) and `:lockfile` (default `"mix.lock"`)
  are relative to `:root`; absolute paths under it work too.

  Returns `{:ok, result}`, or `{:error, message}` when the check can't run.
  """
  @spec call(keyword()) :: {:ok, t()} | {:error, String.t()}
  def call(opts) do
    root = opts |> Keyword.fetch!(:root) |> Path.expand()
    base = (opts[:base] || "") |> to_string() |> String.trim()
    paths = Keyword.get(opts, :migrations_paths, [":(glob)priv/*/migrations/**"])
    lockfile = opts |> Keyword.get(:lockfile, "mix.lock") |> relative(root)

    cond do
      base == "" ->
        {:error,
         "set BASE to the git ref this release is compared with: the branch a pull " <>
           "request targets (BASE=origin/main), or the last deployed ref"}

      # Never an option to git, whatever the environment variable held.
      String.starts_with?(base, "-") ->
        {:error, "#{inspect(base)} is not a git ref"}

      true ->
        with {:ok, merge_base} <- merge_base(root, base),
             {:ok, migrations} <-
               added_trellis_migrations(root, merge_base, Enum.map(paths, &relative(&1, root))) do
          old = locked_version(committed(root, merge_base, lockfile))

          new = locked_version(read(Path.join(root, lockfile)))

          {:ok,
           %__MODULE__{
             base: merge_base,
             old_version: old,
             new_version: new,
             migrations: migrations
           }}
        end
    end
  end

  @doc """
  Any difference counts, a patch release and a downgrade too: Trellis
  versions match exactly, so every change of release is a handover. A release
  that adds the package has no old worker to hand over from.
  """
  @spec version_changed?(t()) :: boolean()
  def version_changed?(%__MODULE__{old_version: old, new_version: new}),
    do: old != nil and new != nil and old != new

  @spec ok?(t()) :: boolean()
  def ok?(%__MODULE__{} = result), do: not (version_changed?(result) and result.migrations != [])

  @doc "The verdict as the text `mix trellis.release_check` prints."
  @spec message(t()) :: String.t()
  def message(%__MODULE__{} = result) do
    if ok?(result), do: ok_message(result), else: failure_message(result)
  end

  defp ok_message(%__MODULE__{} = r) do
    version =
      if version_changed?(r) do
        "#{@package} changes from #{r.old_version} to #{r.new_version}"
      else
        "#{@package} #{r.new_version || "is not in the lockfile"}, unchanged"
      end

    "trellis.release_check: ok (#{version}; #{length(r.migrations)} Trellis migration(s) " <>
      "added since #{String.slice(r.base, 0, 12)})"
  end

  defp failure_message(%__MODULE__{} = r) do
    paths = Enum.map_join(r.migrations, "\n", &"  #{&1}")

    """
    trellis.release_check: this release changes #{@package} from #{r.old_version} to #{r.new_version} and adds Trellis migrations:
    #{paths}
    While a worker of #{r.old_version} is still running, #{r.new_version} may not define, alter, pause, resume
    or drop a transform (VersionHandoverError), so a deploy that migrates before the old
    worker stops fails at these migrations. Ship the #{@package} change by itself, then these
    migrations in the next release.\
    """
  end

  defp merge_base(root, base) do
    with {:ok, _} <- git(root, ["rev-parse", "--is-inside-work-tree"], :capture),
         {:ok, _} <- verify(root, base),
         {:ok, out} <- merge_base_commit(root, base) do
      {:ok, String.trim(out)}
    end
  end

  defp verify(root, base) do
    case git_raw(root, ["rev-parse", "--verify", "--quiet", "#{base}^{commit}"], :parse) do
      {_, 0} -> {:ok, :known}
      _ -> {:error, "#{base} is not a commit this clone knows: fetch it first"}
    end
  end

  defp merge_base_commit(root, base) do
    case git_raw(root, ["merge-base", base, "HEAD"], :parse) do
      {out, 0} ->
        {:ok, out}

      _ ->
        {:error,
         "this checkout has no merge base with #{base}: fetch more history " <>
           "(a shallow clone has none; with actions/checkout, set fetch-depth: 0)"}
    end
  end

  # `path`'s text at `commit`, or nil where the commit has no such file.
  defp committed(root, commit, path) do
    with {_, 0} <-
           git_raw(root, ["rev-parse", "--verify", "--quiet", "#{commit}:./#{path}"], :parse),
         {:ok, text} <- git(root, ["show", "#{commit}:./#{path}"]) do
      text
    else
      _ -> nil
    end
  end

  # trellis_pg's version in a lockfile's text: its `mix.lock` entry. nil for
  # no text or no entry.
  defp locked_version(nil), do: nil

  defp locked_version(text) do
    case Regex.run(~r/^\s*"#{@package}": \{:hex, :#{@package}, "([^"]+)"/m, text) do
      [_, version] -> version
      nil -> nil
    end
  end

  # The migrations under `paths` that the working tree has and `merge_base`
  # hasn't (committed, staged or untracked), and that mention Trellis: the
  # Trellis.Migration helpers, or a direct Trellis call. An edited old
  # migration doesn't run again, so only added (and renamed) files count.
  defp added_trellis_migrations(root, merge_base, paths) do
    with {:ok, added} <-
           git(root, [
             "diff",
             "--name-only",
             "-z",
             "--diff-filter=AR",
             "--relative",
             merge_base,
             "--" | paths
           ]),
         {:ok, untracked} <-
           git(root, ["ls-files", "-z", "--others", "--exclude-standard", "--" | paths]) do
      migrations =
        (String.split(added, "\0", trim: true) ++ String.split(untracked, "\0", trim: true))
        |> Enum.uniq()
        |> Enum.filter(fn path ->
          String.ends_with?(path, ".exs") and
            Regex.match?(~r/\bTrellis\b/, read(Path.join(root, path)) || "")
        end)
        |> Enum.sort()

      {:ok, migrations}
    end
  end

  defp read(path) do
    case File.read(path) do
      {:ok, text} -> text
      {:error, _} -> nil
    end
  end

  defp relative(path, root) do
    if Path.type(path) == :absolute, do: Path.relative_to(path, root), else: path
  end

  # git's stdout, run in `root`; a failing command is an error. Output that
  # is parsed (`:parse`) leaves git's stderr on the console: System.cmd can't
  # capture it apart, and a warning (core.autocrlf's, an ambiguous refname)
  # mixed into it would corrupt the paths or the commit. Output that isn't
  # parsed captures stderr, so the error carries git's message.
  defp git(root, args, mode \\ :parse) do
    case git_raw(root, args, mode) do
      {out, 0} ->
        {:ok, out}

      {_, status} when mode == :parse ->
        {:error, "git #{hd(args)} failed in #{root} (exit #{status}; git's message is above)"}

      {err, _} ->
        {:error, "git #{hd(args)} failed in #{root}: #{String.trim(err)}"}
    end
  end

  defp git_raw(root, args, mode) do
    System.cmd("git", args, cd: root, stderr_to_stdout: mode == :capture)
  rescue
    e in ErlangError -> {"can't run git in #{root}: #{Exception.message(e)}", 127}
  end
end
