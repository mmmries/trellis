defmodule Trellis.RefreshTargetTest do
  # Trellis.TestSupport.refresh_target/3 refills a target from its
  # definition's own query, upstream targets first. Mirrors the Ruby
  # `refresh_trellis_target` tests.
  use ExUnit.Case, async: false

  import Trellis.Eventually

  alias Trellis.{GuardedTestRepo, ReadOnlyTarget, Status, TestCluster, TestSupport}

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
    on_exit(fn -> Application.delete_env(:trellis_pg, GuardedTestRepo) end)

    migrator = Trellis.connect!(url: cluster["dsn"])
    :ok = Trellis.migrate!(migrator)
    :ok = Trellis.shutdown!(migrator)

    trellis = Trellis.connect!(url: cluster["dsn"], staging: true, drain_threads: 1)
    on_exit(fn -> Trellis.shutdown(trellis) end)

    GuardedTestRepo.query!(
      "create table rt_events (id integer primary key, g integer not null, n integer not null)"
    )

    GuardedTestRepo.query!(
      "insert into rt_events select i, i % 3, i from generate_series(1, 10) i"
    )

    Trellis.apply!(
      trellis,
      "TRANSFORM rt_sums FROM rt_events GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g, SUM(n) AS total"
    )

    await_live(trellis, "rt_sums")
    Trellis.apply!(trellis, "TRANSFORM rt_copy FROM rt_sums SELECT total AS total")
    await_live(trellis, "rt_copy")
    {:ok, trellis: trellis}
  end

  setup do
    Application.put_env(:trellis_pg, :allow_target_writes, true)
    on_exit(fn -> Application.delete_env(:trellis_pg, :allow_target_writes) end)
    :ok
  end

  defp rows(table),
    do: GuardedTestRepo.query!("select * from #{table} order by 1, 2").rows

  test "a wiped chain is refilled, upstream first, to what the engine maintains" do
    sums = rows("rt_sums")
    copy = rows("rt_copy")
    assert sums != [] and copy != []

    GuardedTestRepo.query!("delete from rt_copy")
    GuardedTestRepo.query!("delete from rt_sums")

    assert :ok = TestSupport.refresh_target(GuardedTestRepo, "rt_copy")
    assert rows("rt_sums") == sums
    assert rows("rt_copy") == copy
  end

  test "an unqualified or qualified name works; an unknown one raises" do
    assert :ok = TestSupport.refresh_target(GuardedTestRepo, "public.rt_sums")

    assert_raise ArgumentError, ~r/no Trellis definition is recorded for the target nope/, fn ->
      TestSupport.refresh_target(GuardedTestRepo, "nope")
    end
  end

  test "it refuses unless allow_target_writes is true" do
    Application.delete_env(:trellis_pg, :allow_target_writes)
    before = rows("rt_sums")

    assert_raise ReadOnlyTarget, ~r/allow_target_writes/, fn ->
      TestSupport.refresh_target(GuardedTestRepo, "rt_sums")
    end

    assert rows("rt_sums") == before
  end

  defp await_live(trellis, target) do
    eventually("#{target} to reach :live", fn ->
      case Trellis.status!(trellis, target) do
        %Status{status: :live} = status -> {:done, status}
        status -> {:waiting, status}
      end
    end)
  end
end
