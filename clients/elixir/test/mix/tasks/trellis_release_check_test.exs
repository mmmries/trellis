defmodule Mix.Tasks.Trellis.ReleaseCheckTest do
  # The task reads the current directory, which is the process's, so this
  # module isn't async. The check's own cases are in
  # test/trellis/release_check_test.exs.
  use ExUnit.Case, async: false

  alias Mix.Tasks.Trellis.ReleaseCheck, as: Task

  @moduletag :tmp_dir
  @define "priv/repo/migrations/20260201000000_define_order_totals.exs"

  setup %{tmp_dir: root} do
    git(root, ["init", "--quiet", "--initial-branch=main"])
    git(root, ["config", "user.email", "test@example.com"])
    git(root, ["config", "user.name", "Test"])
    write(root, "mix.lock", lockfile("0.2.0"))
    git(root, ["add", "--all"])
    git(root, ["commit", "--quiet", "--message", "the deployed app"])
    git(root, ["checkout", "--quiet", "-b", "feature"])

    cwd = File.cwd!()
    File.cd!(root)
    Mix.shell(Mix.Shell.Process)
    previous = System.get_env("BASE")
    System.delete_env("BASE")

    on_exit(fn ->
      File.cd!(cwd)
      Mix.shell(Mix.Shell.IO)
      if previous, do: System.put_env("BASE", previous), else: System.delete_env("BASE")
    end)

    %{root: root}
  end

  test "prints the verdict for a passing release" do
    Task.run(["--base", "main"])
    assert_received {:mix_shell, :info, [message]}
    assert message =~ "trellis.release_check: ok (trellis_pg 0.2.0, unchanged; 0 Trellis"
  end

  test "BASE comes from a BASE=<ref> argument or the environment" do
    Task.run(["BASE=main"])
    assert_received {:mix_shell, :info, [_]}

    System.put_env("BASE", "main")
    Task.run([])
    assert_received {:mix_shell, :info, [_]}
  end

  test "raises for a release that bumps the package and adds a Trellis migration", %{root: root} do
    write(root, "mix.lock", lockfile("0.3.0"))
    write(root, @define, "defmodule M do\n  use Trellis.Migration\nend\n")

    error = assert_raise Mix.Error, fn -> Task.run(["--base", "main"]) end

    assert error.message =~
             "trellis.release_check: this release changes trellis_pg from 0.2.0 to 0.3.0"

    assert error.message =~ "  #{@define}"
  end

  test "raises when the check can't run" do
    error = assert_raise Mix.Error, fn -> Task.run([]) end
    assert error.message =~ "trellis.release_check: set BASE to the git ref"
  end

  test "reads the migrations of each repo in :ecto_repos", %{root: root} do
    on_exit(fn ->
      Application.delete_env(:trellis_pg, :ecto_repos)
      Application.delete_env(:trellis_pg, MyApp.ReportsRepo)
    end)

    # A repo whose :priv is outside priv/*: only its configuration finds it.
    Application.put_env(:trellis_pg, :ecto_repos, [MyApp.Repo, MyApp.ReportsRepo])
    Application.put_env(:trellis_pg, MyApp.ReportsRepo, priv: "db/reports")
    write(root, "mix.lock", lockfile("0.3.0"))
    write(root, "db/reports/migrations/20260201000000_define.exs", "use Trellis.Migration\n")

    error = assert_raise Mix.Error, fn -> Task.run(["--base", "main"]) end
    assert error.message =~ "  db/reports/migrations/20260201000000_define.exs"

    # --migrations-path replaces the configured list.
    Task.run(["--base", "main", "--migrations-path", "priv/repo/migrations"])
    assert_received {:mix_shell, :info, [_]}
  end

  test "an app's repos give Ecto's migrations directories" do
    on_exit(fn ->
      Application.delete_env(:release_check_app, :ecto_repos)
      Application.delete_env(:release_check_app, MyApp.Other)
    end)

    assert Task.ecto_migrations_paths(nil) == nil
    assert Task.ecto_migrations_paths(:release_check_app) == nil

    Application.put_env(:release_check_app, :ecto_repos, [MyApp.Repo, MyApp.Other])
    Application.put_env(:release_check_app, MyApp.Other, priv: "priv/tenants")

    assert Task.ecto_migrations_paths(:release_check_app) ==
             ["priv/repo/migrations", "priv/tenants/migrations"]
  end

  defp lockfile(version) do
    ~s(%{\n  "trellis_pg": {:hex, :trellis_pg, "#{version}", "0123", [:mix], [], "hexpm", "4567"},\n}\n)
  end

  defp write(root, path, content) do
    path = Path.join(root, path)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, content)
  end

  defp git(root, args), do: {_, 0} = System.cmd("git", args, cd: root, stderr_to_stdout: true)
end
