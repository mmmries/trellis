defmodule Trellis.EnabledTest do
  # `config :trellis_pg, enabled: false`: Trellis is off, and the host runs as
  # if it were not installed. Each off behaviour, against a cluster of its
  # own for the ones that touch a database.
  use ExUnit.Case, async: false

  alias Trellis.{TestCluster, TestRepo}

  @moduletag :tmp_dir

  defmodule DefineWidgets do
    use Ecto.Migration
    use Trellis.Migration

    @disable_ddl_transaction true

    def up do
      create table(:off_widgets) do
        add :price, :integer
      end

      define "TRANSFORM off_widget_prices FROM off_widgets SELECT price AS price"
      apply "PAUSE TRANSFORM off_widget_prices"
      nil = status("off_widget_prices")
    end

    def down, do: :ok
  end

  defp turn_off do
    Application.put_env(:trellis_pg, :enabled, false)
    on_exit(fn -> Application.delete_env(:trellis_pg, :enabled) end)
  end

  test "Trellis is on unless config turns it off" do
    assert Trellis.enabled?()

    turn_off()
    refute Trellis.enabled?()

    Application.put_env(:trellis_pg, :enabled, true)
    assert Trellis.enabled?()
  end

  test "the child spec starts nothing and connects nothing where Trellis is off" do
    turn_off()
    options = [name: :off_trellis, url: "postgres://nobody@127.0.0.1:1/none"]

    assert :ignore = Trellis.start_link(options)

    {:ok, supervisor} = Supervisor.start_link([{Trellis, options}], strategy: :one_for_one)
    assert [{:off_trellis, :undefined, _, _}] = Supervisor.which_children(supervisor)
    assert Process.whereis(:off_trellis) == nil
    Supervisor.stop(supervisor)
  end

  test "the log bridge is not started where Trellis is off" do
    assert Trellis.Application.children() == [Trellis.LogBridge]

    turn_off()
    assert Trellis.Application.children() == []
  end

  describe "with a database" do
    setup %{tmp_dir: dir} do
      cluster = TestCluster.private!()
      {:ok, _} = Application.ensure_all_started(:ecto_sql)
      {:ok, _} = Application.ensure_all_started(:postgrex)

      # The :trellis url names a database that doesn't exist: nothing may
      # connect to it, or be refused for it.
      Application.put_env(:trellis_pg, TestRepo,
        socket_dir: cluster["host"],
        port: cluster["port"],
        username: cluster["user"],
        database: cluster["dbname"],
        pool_size: 2,
        log: false,
        dump_prefixes: ["public"],
        trellis: [url: String.replace(cluster["dsn"], cluster["dbname"], "elsewhere")]
      )

      Application.put_env(:trellis_pg, :enabled, false)

      on_exit(fn ->
        Application.delete_env(:trellis_pg, :enabled)
        Application.delete_env(:trellis_pg, TestRepo)
      end)

      start_supervised!(TestRepo)
      %{cluster: cluster, pg: TestCluster.postgrex!(cluster), dir: dir}
    end

    test "the migration helpers skip, and the migration still records as run", %{pg: pg} do
      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert :ok = Ecto.Migrator.up(TestRepo, 1, DefineWidgets, log: false)
        end)

      assert log =~ ~s[status("off_widget_prices") returns nil]
      assert log =~ "Trellis is off"
      assert log =~ "skipped TRANSFORM off_widget_prices"

      # The host's own commands ran; Trellis's did not, and it was never set up.
      assert table?(pg, "off_widgets")
      refute table?(pg, "off_widget_prices")
      refute schema?(pg, "trellis")
      assert %{rows: [[1]]} = Postgrex.query!(pg, "select count(*) from schema_migrations", [])
    end

    test "dump and load are ecto.dump and ecto.load, and leave targets plain",
         %{cluster: cluster, pg: pg, dir: dir} do
      Postgrex.query!(pg, "create table off_prices (id integer primary key, price integer)", [])
      path = Path.join(dir, "structure.sql")

      Mix.Tasks.Trellis.Dump.run(~w(-r Trellis.TestRepo --dump-path #{path} --quiet))

      text = File.read!(path)
      assert text =~ "CREATE TABLE public.off_prices"
      refute text =~ "Trellis definitions"
      refute text =~ "restore_queue"

      database = "off_loaded"
      Postgrex.query!(pg, "create database #{database}", [])
      stop_supervised!(TestRepo)

      config = Application.fetch_env!(:trellis_pg, TestRepo)

      Application.put_env(
        :trellis_pg,
        TestRepo,
        Keyword.merge(config, database: database, dump_path: path)
      )

      Mix.Tasks.Trellis.Load.run(~w(-r Trellis.TestRepo --quiet))

      loaded = TestCluster.postgrex!(Map.put(cluster, "dbname", database))

      assert table?(loaded, "public.off_prices")
      refute schema?(loaded, "trellis")

      GenServer.stop(loaded)
    end
  end

  defp schema?(pg, name) do
    %{rows: [[found]]} =
      Postgrex.query!(pg, "select exists (select from pg_namespace where nspname = $1)", [name])

    found
  end

  defp table?(pg, name) do
    %{rows: [[found]]} = Postgrex.query!(pg, "select to_regclass($1) is not null", [name])
    found
  end
end
