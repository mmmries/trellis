defmodule Trellis.EnabledTest do
  # `config :trellis_pg, enabled: false`: Trellis's runtime is off, and its
  # migration helpers and schema tasks run as when it's on. Each off
  # behaviour, against a cluster of its own for the ones that touch a
  # database.
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
    end

    def down do
      apply "PAUSE TRANSFORM off_widget_prices"
      apply "DROP TRANSFORM off_widget_prices"
      drop table(:off_widgets)
    end
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

      Application.put_env(:trellis_pg, TestRepo,
        socket_dir: cluster["host"],
        port: cluster["port"],
        username: cluster["user"],
        database: cluster["dbname"],
        pool_size: 2,
        log: false,
        dump_prefixes: ["public"],
        trellis: [url: cluster["dsn"]]
      )

      Application.put_env(:trellis_pg, :enabled, false)

      on_exit(fn ->
        Application.delete_env(:trellis_pg, :enabled)
        Application.delete_env(:trellis_pg, TestRepo)
      end)

      start_supervised!(TestRepo)
      %{cluster: cluster, pg: TestCluster.postgrex!(cluster), dir: dir}
    end

    test "the migration helpers run: a define creates its target and nothing captures",
         %{pg: pg} do
      assert :ok = Ecto.Migrator.up(TestRepo, 1, DefineWidgets, log: false)

      assert table?(pg, "off_widget_prices")

      assert %{rows: [["waiting_to_backfill"]]} =
               Postgrex.query!(
                 pg,
                 "select status from trellis.transform_definitions " <>
                   "where target_table = 'public.off_widget_prices'",
                 []
               )

      # Nothing installed a capture trigger, so a write stages nothing.
      assert triggers(pg, "off_widgets") == []
      Postgrex.query!(pg, "insert into off_widgets (price) values (3)", [])
      assert %{rows: [[0]]} = Postgrex.query!(pg, "select count(*) from off_widget_prices", [])

      assert :ok = Ecto.Migrator.down(TestRepo, 1, DefineWidgets, log: false)
      refute table?(pg, "off_widget_prices")

      assert %{rows: [[0]]} =
               Postgrex.query!(pg, "select count(*) from trellis.transform_definitions", [])
    end

    test "dump and load run as when Trellis is on", %{cluster: cluster, pg: pg, dir: dir} do
      assert :ok = Ecto.Migrator.up(TestRepo, 1, DefineWidgets, log: false)
      path = Path.join(dir, "structure.sql")

      Mix.Tasks.Trellis.Dump.run(~w(-r Trellis.TestRepo --dump-path #{path} --quiet))

      text = File.read!(path)
      assert text =~ "CREATE TABLE public.off_widgets"
      assert text =~ "TRANSFORM off_widget_prices"

      database = "off_loaded"
      Postgrex.query!(pg, "create database #{database}", [])
      stop_supervised!(TestRepo)

      config = Application.fetch_env!(:trellis_pg, TestRepo)
      loaded_cluster = Map.put(cluster, "dbname", database)

      Application.put_env(
        :trellis_pg,
        TestRepo,
        Keyword.merge(config,
          database: database,
          dump_path: path,
          trellis: [url: String.replace(cluster["dsn"], cluster["dbname"], database)]
        )
      )

      Mix.Tasks.Trellis.Load.run(~w(-r Trellis.TestRepo --quiet))

      loaded = TestCluster.postgrex!(loaded_cluster)

      assert %{rows: [["public.off_widget_prices"]]} =
               Postgrex.query!(
                 loaded,
                 "select target_table from trellis.transform_definitions",
                 []
               )

      assert triggers(loaded, "off_widgets") == []

      GenServer.stop(loaded)
    end
  end

  defp triggers(pg, table) do
    %{rows: rows} =
      Postgrex.query!(
        pg,
        "select tgname from pg_trigger where tgrelid = $1::text::regclass and not tgisinternal",
        [table]
      )

    rows
  end

  defp table?(pg, name) do
    %{rows: [[found]]} = Postgrex.query!(pg, "select to_regclass($1) is not null", [name])
    found
  end
end
