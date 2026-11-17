defmodule Trellis.TargetTest do
  # `use Trellis.Target` and `use Trellis.Repo`: a marked schema refuses
  # every repo write unless `allow_target_writes` is true.
  use ExUnit.Case, async: false

  import Ecto.Query

  alias Trellis.{GuardedTestRepo, ReadOnlyTarget, TestCluster}

  defmodule Counter do
    use Ecto.Schema
    use Trellis.Target

    @primary_key {:id, :integer, autogenerate: false}
    schema "guard_counters" do
      field(:total, :integer)
    end
  end

  defmodule Plain do
    use Ecto.Schema

    @primary_key {:id, :integer, autogenerate: false}
    schema "guard_plain" do
      field(:total, :integer)
      # Not a real relation: lets a plain parent write a target child.
      has_many(:counters, Counter, foreign_key: :total, references: :id)
    end
  end

  setup_all do
    cluster = TestCluster.private!()
    {:ok, _} = Application.ensure_all_started(:ecto_sql)
    {:ok, _} = Application.ensure_all_started(:postgrex)

    Application.put_env(:trellis_pg, GuardedTestRepo,
      socket_dir: cluster["host"],
      port: cluster["port"],
      username: cluster["user"],
      database: cluster["dbname"],
      pool_size: 2,
      log: false
    )

    start_supervised!(GuardedTestRepo)

    for t <- ~w(guard_counters guard_plain) do
      GuardedTestRepo.query!("create table #{t} (id integer primary key, total integer)")
    end

    on_exit(fn -> Application.delete_env(:trellis_pg, GuardedTestRepo) end)
    :ok
  end

  setup do
    Application.delete_env(:trellis_pg, :allow_target_writes)
    on_exit(fn -> Application.delete_env(:trellis_pg, :allow_target_writes) end)
    GuardedTestRepo.query!("truncate guard_counters, guard_plain")
    GuardedTestRepo.query!("insert into guard_counters values (1, 10)")
    :ok
  end

  defp count, do: GuardedTestRepo.aggregate(Counter, :count)

  defp writes do
    row = %Counter{id: 1, total: 10}
    change = Ecto.Changeset.change(row, total: 11)

    [
      insert: fn -> GuardedTestRepo.insert(%Counter{id: 2, total: 1}) end,
      insert!: fn -> GuardedTestRepo.insert!(%Counter{id: 2, total: 1}) end,
      update: fn -> GuardedTestRepo.update(change) end,
      update!: fn -> GuardedTestRepo.update!(change) end,
      delete: fn -> GuardedTestRepo.delete(row) end,
      delete!: fn -> GuardedTestRepo.delete!(row) end,
      insert_or_update: fn -> GuardedTestRepo.insert_or_update(change) end,
      insert_or_update!: fn -> GuardedTestRepo.insert_or_update!(change) end,
      insert_opts: fn -> GuardedTestRepo.insert(%Counter{id: 2, total: 1}, returning: true) end,
      delete_opts!: fn -> GuardedTestRepo.delete!(row, stale_error_field: :id) end,
      insert_all: fn -> GuardedTestRepo.insert_all(Counter, [%{id: 3, total: 3}]) end,
      insert_all_tuple: fn ->
        GuardedTestRepo.insert_all({"guard_counters", Counter}, [%{id: 3, total: 3}], [])
      end,
      update_all_source_tuple: fn ->
        GuardedTestRepo.update_all(from(c in {"guard_counters", Counter}), set: [total: 0])
      end,
      update_all: fn -> GuardedTestRepo.update_all(Counter, set: [total: 0]) end,
      update_all_query: fn ->
        GuardedTestRepo.update_all(from(c in Counter, where: c.id == 1), set: [total: 0])
      end,
      delete_all: fn -> GuardedTestRepo.delete_all(Counter) end,
      delete_all_query: fn -> GuardedTestRepo.delete_all(from(c in Counter, where: c.id == 1)) end,
      child_of_plain_parent: fn ->
        GuardedTestRepo.insert(%Plain{id: 1, counters: [%Counter{id: 6}]})
      end,
      multi_update_all: fn ->
        Ecto.Multi.new()
        |> Ecto.Multi.update_all(:rows, Counter, set: [total: 0])
        |> GuardedTestRepo.transaction()
      end,
      multi: fn ->
        Ecto.Multi.new()
        |> Ecto.Multi.insert(:row, %Counter{id: 4, total: 1})
        |> GuardedTestRepo.transaction()
      end
    ]
  end

  test "every write path refuses a target schema, sending nothing" do
    for {name, write} <- writes() do
      assert_raise ReadOnlyTarget, ~r/Trellis target/, fn -> write.() end

      assert {name, GuardedTestRepo.all(from(c in Counter, select: {c.id, c.total}))} ==
               {name, [{1, 10}]}
    end
  end

  test "the error names the schema and the write, and has no T-code" do
    e = assert_raise ReadOnlyTarget, fn -> GuardedTestRepo.delete(%Counter{id: 1}) end
    assert e.schema == Counter
    assert e.write == :delete
    refute e.message =~ ~r/T\d{4}/
  end

  test "allow_target_writes: true lets each write through" do
    Application.put_env(:trellis_pg, :allow_target_writes, true)

    assert {:ok, _} = GuardedTestRepo.insert(%Counter{id: 2, total: 1})
    assert %Counter{} = GuardedTestRepo.insert!(%Counter{id: 5, total: 1})

    assert {:ok, _} =
             GuardedTestRepo.update(Ecto.Changeset.change(%Counter{id: 1, total: 10}, total: 11))

    assert {1, _} = GuardedTestRepo.insert_all(Counter, [%{id: 3, total: 3}])
    assert {_, _} = GuardedTestRepo.update_all(Counter, set: [total: 0])
    assert {:ok, _} = GuardedTestRepo.delete(%Counter{id: 2})

    assert {:ok, _} =
             Ecto.Multi.new()
             |> Ecto.Multi.insert(:row, %Counter{id: 4, total: 1})
             |> GuardedTestRepo.transaction()

    assert {_, _} = GuardedTestRepo.delete_all(Counter)
    assert count() == 0
  end

  test "the setting is read at each write" do
    Application.put_env(:trellis_pg, :allow_target_writes, true)
    assert {:ok, _} = GuardedTestRepo.insert(%Counter{id: 2, total: 1})
    Application.put_env(:trellis_pg, :allow_target_writes, false)
    assert_raise ReadOnlyTarget, fn -> GuardedTestRepo.insert(%Counter{id: 3, total: 1}) end
  end

  test "enabled: false does not lift the guard" do
    Application.put_env(:trellis_pg, :enabled, false)
    on_exit(fn -> Application.delete_env(:trellis_pg, :enabled) end)
    assert_raise ReadOnlyTarget, fn -> GuardedTestRepo.insert(%Counter{id: 2, total: 1}) end
  end

  test "reads and unmarked schemas are untouched" do
    assert count() == 1
    assert %Counter{total: 10} = GuardedTestRepo.get!(Counter, 1)
    assert {:ok, _} = GuardedTestRepo.insert(%Plain{id: 1, total: 1})
    assert {1, _} = GuardedTestRepo.update_all(Plain, set: [total: 2])
    assert {1, _} = GuardedTestRepo.delete_all(Plain)
    assert {1, _} = GuardedTestRepo.insert_all("guard_plain", [%{id: 9, total: 1}])
  end

  test "queries that name no schema pass the guard without crashing" do
    assert :ok = Trellis.Target.guard!(from(c in "guard_counters"), :update_all)
    assert :ok = Trellis.Target.guard!(from(s in subquery(from(c in Counter))), :update_all)
    assert :ok = Trellis.Target.guard!("guard_counters", :insert_all)
    assert :ok = Trellis.Target.guard!({"guard_plain", Plain}, :insert_all)
    assert {1, _} = GuardedTestRepo.update_all(from(c in "guard_counters"), set: [total: 0])
  end

  test "use Trellis.Repo before use Ecto.Repo is refused at compile time" do
    assert_raise ArgumentError, ~r/after `use Ecto.Repo`/, fn ->
      Code.compile_string("defmodule Trellis.BadRepo do use Trellis.Repo end")
    end
  end
end
