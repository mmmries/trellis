defmodule Trellis.ReleaseCheckTest do
  # Trellis.ReleaseCheck against throwaway git repositories: it reads git and
  # files only, so no test here touches the cluster.
  use ExUnit.Case, async: true

  alias Trellis.ReleaseCheck

  @trellis_migration """
  defmodule MyApp.Repo.Migrations.DefineOrderTotals do
    use Ecto.Migration
    use Trellis.Migration

    def up, do: define("TRANSFORM order_totals FROM orders SELECT price + tax AS total")

    def down do
      apply("PAUSE TRANSFORM order_totals")
      apply("DROP TRANSFORM order_totals")
    end
  end
  """
  @plain_migration """
  defmodule MyApp.Repo.Migrations.AddNoteToOrders do
    use Ecto.Migration
    def change, do: alter(table(:orders), do: add(:note, :text))
  end
  """
  @define "priv/repo/migrations/20260201000000_define_order_totals.exs"

  @moduletag :tmp_dir

  setup %{tmp_dir: root} do
    git(root, ["init", "--quiet", "--initial-branch=main"])
    git(root, ["config", "user.email", "test@example.com"])
    git(root, ["config", "user.name", "Test"])
    write(root, "mix.lock", lockfile("0.2.0"))

    write(
      root,
      "priv/repo/migrations/20260101000000_create_orders.exs",
      "defmodule CreateOrders do\nend\n"
    )

    commit(root, "the deployed app")
    git(root, ["checkout", "--quiet", "-b", "feature"])
    %{root: root}
  end

  test "a version change with a Trellis migration fails and says what to do", %{root: root} do
    write(root, "mix.lock", lockfile("0.3.0"))
    write(root, @define, @trellis_migration)
    commit(root, "bump and define")

    result = check(root)
    refute ReleaseCheck.ok?(result)
    assert {result.old_version, result.new_version} == {"0.2.0", "0.3.0"}
    assert result.migrations == [@define]
    message = ReleaseCheck.message(result)
    assert message =~ "changes trellis_pg from 0.2.0 to 0.3.0 and adds Trellis migrations:"
    assert message =~ "  #{@define}"
    assert message =~ "Ship the trellis_pg change by itself"
  end

  test "a version change alone passes", %{root: root} do
    write(root, "mix.lock", lockfile("0.3.0"))
    write(root, "priv/repo/migrations/20260201000000_add_note_to_orders.exs", @plain_migration)
    commit(root, "bump, and a migration that doesn't use Trellis")

    result = check(root)
    assert ReleaseCheck.ok?(result)
    assert result.migrations == []

    assert ReleaseCheck.message(result) =~
             "ok (trellis_pg changes from 0.2.0 to 0.3.0; 0 Trellis migration(s) added"
  end

  test "a Trellis migration alone passes", %{root: root} do
    write(root, @define, @trellis_migration)
    commit(root, "define")

    result = check(root)
    assert ReleaseCheck.ok?(result)
    assert length(result.migrations) == 1

    assert ReleaseCheck.message(result) =~
             "ok (trellis_pg 0.2.0, unchanged; 1 Trellis migration(s) added"
  end

  # Adopting Trellis: there is no old worker to hand over from.
  test "a release that adds the package and its first transform passes", %{root: root} do
    git(root, ["checkout", "--quiet", "main"])

    write(
      root,
      "mix.lock",
      "%{\n  \"ecto\": {:hex, :ecto, \"3.13.0\", \"abc\", [:mix], [], \"hexpm\", \"def\"},\n}\n"
    )

    commit(root, "before Trellis")
    git(root, ["checkout", "--quiet", "-b", "adopt"])
    write(root, "mix.lock", lockfile("0.3.0"))
    write(root, @define, @trellis_migration)
    commit(root, "adopt Trellis")

    result = check(root)
    assert ReleaseCheck.ok?(result)
    assert result.old_version == nil
    assert result.new_version == "0.3.0"
  end

  test "a downgrade counts as a version change", %{root: root} do
    write(root, "mix.lock", lockfile("0.1.1"))
    write(root, @define, @trellis_migration)
    commit(root, "downgrade and define")

    refute ReleaseCheck.ok?(check(root))
  end

  # A developer runs it before committing.
  test "uncommitted and untracked work counts", %{root: root} do
    write(root, "mix.lock", lockfile("0.3.0"))
    write(root, @define, @trellis_migration)

    result = check(root)
    refute ReleaseCheck.ok?(result)
    assert result.migrations == [@define]
  end

  # The branch is judged by what it changes, not by what `base` gained since
  # the branch left it.
  test "a branch behind its base is compared with the merge base", %{root: root} do
    write(root, @define, @trellis_migration)
    commit(root, "define, on the branch")
    git(root, ["checkout", "--quiet", "main"])
    write(root, "mix.lock", lockfile("0.3.0"))
    commit(root, "bump, on main, after the branch left")
    git(root, ["checkout", "--quiet", "feature"])

    result = check(root)
    assert ReleaseCheck.ok?(result)
    assert {result.old_version, result.new_version} == {"0.2.0", "0.2.0"}
  end

  # The same two commits as one release: BASE is the last deployed ref.
  test "a release of several commits is checked against the deployed ref", %{root: root} do
    git(root, ["tag", "deployed"])
    write(root, "mix.lock", lockfile("0.3.0"))
    commit(root, "bump")
    write(root, @define, @trellis_migration)
    commit(root, "define, in a later commit")

    refute ReleaseCheck.ok?(check(root, base: "deployed"))
    assert ReleaseCheck.ok?(check(root, base: "HEAD~1"))
  end

  test "an edited old Trellis migration does not count", %{root: root} do
    git(root, ["checkout", "--quiet", "main"])
    write(root, "priv/repo/migrations/20260115000000_define_order_totals.exs", @trellis_migration)
    commit(root, "a transform defined long ago")
    git(root, ["checkout", "--quiet", "-B", "feature"])
    write(root, "mix.lock", lockfile("0.3.0"))

    write(
      root,
      "priv/repo/migrations/20260115000000_define_order_totals.exs",
      @trellis_migration <> "# reformatted\n"
    )

    commit(root, "bump, and touch an old migration")

    result = check(root)
    assert ReleaseCheck.ok?(result)
    assert result.migrations == []
  end

  test "a migration that calls Trellis without the helpers counts", %{root: root} do
    write(root, "mix.lock", lockfile("0.3.0"))

    write(root, "priv/repo/migrations/20260201000000_pause_order_totals.exs", """
    defmodule MyApp.Repo.Migrations.PauseOrderTotals do
      use Ecto.Migration
      def up, do: Trellis.apply(MyApp.Trellis, "PAUSE TRANSFORM order_totals")
    end
    """)

    commit(root, "bump and pause")

    refute ReleaseCheck.ok?(check(root))
  end

  test "every repo's migrations directory is read by default", %{root: root} do
    write(root, "mix.lock", lockfile("0.3.0"))

    write(
      root,
      "priv/reports_repo/migrations/20260201000000_define_order_totals.exs",
      @trellis_migration
    )

    commit(root, "bump, and define in the second repo's migrations")

    result = check(root)
    refute ReleaseCheck.ok?(result)

    assert result.migrations == [
             "priv/reports_repo/migrations/20260201000000_define_order_totals.exs"
           ]

    # An explicit list (relative or absolute) replaces the default.
    assert ReleaseCheck.ok?(check(root, migrations_paths: ["priv/repo/migrations"]))

    refute ReleaseCheck.ok?(
             check(root,
               migrations_paths: [
                 "priv/repo/migrations",
                 Path.join(root, "priv/reports_repo/migrations")
               ]
             )
           )
  end

  # An app that isn't at the top of its repository (an umbrella child).
  test "an app in a subdirectory of the repository", %{root: root} do
    File.mkdir_p!(Path.join(root, "apps/shop"))
    git(root, ["mv", "mix.lock", "apps/shop/mix.lock"])
    git(root, ["mv", "priv", "apps/shop/priv"])
    commit(root, "move the app")
    git(root, ["checkout", "--quiet", "-B", "main"])
    git(root, ["checkout", "--quiet", "-b", "feature-in-subdirectory"])
    write(root, "apps/shop/mix.lock", lockfile("0.3.0"))
    write(root, "apps/shop/#{@define}", @trellis_migration)
    write(root, @define, @trellis_migration)
    commit(root, "bump and define")

    assert {:ok, result} = ReleaseCheck.call(root: Path.join(root, "apps/shop"), base: "main")
    refute ReleaseCheck.ok?(result)
    assert result.migrations == [@define]
  end

  # git writes warnings to stderr; none of them may reach the output the
  # check parses. core.autocrlf warns about an edited LF file in the working
  # tree, and the warning would otherwise prefix the first added path.
  test "a git warning doesn't hide an added migration", %{root: root} do
    write(root, "mix.lock", lockfile("0.3.0"))
    write(root, @define, @trellis_migration)
    commit(root, "bump and define")
    git(root, ["config", "core.autocrlf", "true"])

    write(
      root,
      "priv/repo/migrations/20260101000000_create_orders.exs",
      "defmodule CreateOrders do\n  # edited\nend\n"
    )

    assert git_stderr(root, ["diff", "--name-only", "main"]) =~ "LF will be replaced by CRLF"

    result = check(root)
    assert result.migrations == [@define]
    refute ReleaseCheck.ok?(result)
  end

  # A tag and a branch both named `main`: git warns that the name is
  # ambiguous while it resolves it.
  test "an ambiguous base still yields its merge base", %{root: root} do
    git(root, ["tag", "main", "main"])
    write(root, "mix.lock", lockfile("0.3.0"))
    write(root, @define, @trellis_migration)
    commit(root, "bump and define")

    assert git_stderr(root, ["merge-base", "main", "HEAD"]) =~ "ambiguous"

    result = check(root)
    assert result.base =~ ~r/\A[0-9a-f]{40}\z/
    refute ReleaseCheck.ok?(result)
  end

  # An umbrella child: its lockfile is the umbrella's, `../../mix.lock`.
  test "a lockfile above the app's directory", %{root: root} do
    File.mkdir_p!(Path.join(root, "apps/shop"))
    git(root, ["mv", "priv", "apps/shop/priv"])
    commit(root, "an umbrella")
    git(root, ["checkout", "--quiet", "-B", "main"])
    git(root, ["checkout", "--quiet", "-b", "umbrella-feature"])
    write(root, "mix.lock", lockfile("0.3.0"))
    write(root, "apps/shop/#{@define}", @trellis_migration)
    commit(root, "bump and define")

    assert {:ok, result} =
             ReleaseCheck.call(
               root: Path.join(root, "apps/shop"),
               base: "main",
               lockfile: "../../mix.lock"
             )

    assert {result.old_version, result.new_version} == {"0.2.0", "0.3.0"}
    assert result.migrations == [@define]
    refute ReleaseCheck.ok?(result)
  end

  test "no base is an error that says what to pass", %{root: root} do
    for base <- ["", "  ", nil] do
      assert {:error, message} = ReleaseCheck.call(root: root, base: base)
      assert message =~ "set BASE to the git ref"
    end
  end

  test "a base that would read as a git option is refused", %{root: root} do
    assert {:error, ~s("--all" is not a git ref)} = ReleaseCheck.call(root: root, base: "--all")
  end

  test "an unknown base is an error", %{root: root} do
    assert {:error, "origin/main is not a commit this clone knows: fetch it first"} =
             ReleaseCheck.call(root: root, base: "origin/main")
  end

  # A shallow clone: the base is known, the history that joins it isn't.
  test "no merge base is an error that names the shallow clone", %{root: root} do
    git(root, ["checkout", "--quiet", "--orphan", "unrelated"])
    commit(root, "a history of its own")

    assert {:error, message} = check_result(root)
    assert message =~ "no merge base with main"
    assert message =~ "fetch-depth: 0"
  end

  test "outside a git checkout is an error" do
    dir = Path.join(System.tmp_dir!(), "not-a-repository-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)

    assert {:error, message} = ReleaseCheck.call(root: dir, base: "main")
    assert message =~ "git rev-parse failed"
  end

  # What git writes to stderr for `args` in `root`: a test's precondition.
  defp git_stderr(root, args) do
    {stdout, 0} = System.cmd("git", args, cd: root)
    {both, 0} = System.cmd("git", args, cd: root, stderr_to_stdout: true)
    String.replace(both, stdout, "")
  end

  defp check(root, opts \\ []) do
    {:ok, result} = check_result(root, opts)
    result
  end

  defp check_result(root, opts \\ []) do
    ReleaseCheck.call(Keyword.merge([root: root, base: "main"], opts))
  end

  # A mix.lock as Mix writes it for the package.
  defp lockfile(version) do
    """
    %{
      "ecto": {:hex, :ecto, "3.13.0", "abc", [:mix], [], "hexpm", "def"},
      "trellis_pg": {:hex, :trellis_pg, "#{version}", "0123", [:mix], [{:rustler_precompiled, "~> 0.9.0", [hex: :rustler_precompiled, repo: "hexpm", optional: false]}], "hexpm", "4567"},
    }
    """
  end

  defp write(root, path, content) do
    path = Path.join(root, path)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, content)
  end

  defp commit(root, message) do
    git(root, ["add", "--all"])
    git(root, ["commit", "--quiet", "--message", message])
  end

  defp git(root, args) do
    {out, status} = System.cmd("git", args, cd: root, stderr_to_stdout: true)
    assert status == 0, "git #{Enum.join(args, " ")} failed: #{out}"
    out
  end
end
