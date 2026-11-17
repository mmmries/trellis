defmodule Trellis.RefreshTargetTest do
  # Trellis.TestSupport.refresh_target/3 refills a target from its
  # definition's own query, upstream targets first. Mirrors the Ruby
  # `refresh_trellis_target` tests. No polling: the definitions are read from
  # a restore queue written here, or from a catalog no worker builds from.
  use ExUnit.Case, async: false

  alias Trellis.{GuardedTestRepo, ReadOnlyTarget, TestCluster, TestSupport}

  defmodule OtherCounts do
    @moduledoc false
    use Ecto.Schema
    @schema_prefix "rq_other"
    schema("rq_counts", do: field(:n, :integer))
  end

  @queue "rq_trellis"

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

    # The way an off environment's schema load leaves a database: sources,
    # targets as plain tables, and the definitions in a restore queue.
    Enum.each(
      [
        "create table rq_orders (id integer primary key, customer_id integer, amount integer)",
        "create table rq_counts (customer_id integer, n bigint, total bigint, " <>
          "unique nulls not distinct (customer_id))",
        "create table rq_doubled (customer_id integer, doubled bigint, " <>
          "unique nulls not distinct (customer_id))",
        "create table rq_view (customer_id integer, n bigint, doubled bigint, " <>
          "unique nulls not distinct (customer_id))",
        "create schema rq_other",
        "create table rq_other.rq_counts (customer_id integer, n bigint, " <>
          "unique nulls not distinct (customer_id))",
        "create table rq_bad (customer_id integer, n bigint, extra integer, " <>
          "unique nulls not distinct (customer_id))",
        "create schema #{@queue}",
        "create table #{@queue}.restore_queue (ordinal integer primary key, " <>
          "source_table text not null, target_table text not null, join_table text, " <>
          "definition_text text not null, grammar_version integer not null, " <>
          "floor_version text not null)"
      ],
      &GuardedTestRepo.query!/1
    )

    queue(
      "public.rq_orders",
      "public.rq_counts",
      nil,
      "TRANSFORM rq_counts FROM rq_orders GROUP BY customer_id " <>
        "SELECT customer_id AS customer_id, COUNT(*) AS n, SUM(amount) AS total"
    )

    queue(
      "public.rq_counts",
      "public.rq_doubled",
      nil,
      "TRANSFORM rq_doubled FROM rq_counts SELECT n * 2 AS doubled"
    )

    # A diamond: rq_view reads rq_counts directly and through rq_doubled.
    queue(
      "public.rq_counts",
      "public.rq_view",
      "public.rq_doubled",
      "TRANSFORM rq_view FROM rq_counts JOIN rq_doubled " <>
        "ON rq_counts.customer_id = rq_doubled.customer_id " <>
        "SELECT rq_counts.n AS n, rq_doubled.doubled AS doubled"
    )

    queue(
      "public.rq_orders",
      "rq_other.rq_counts",
      nil,
      "TRANSFORM rq_other.rq_counts FROM rq_orders GROUP BY customer_id " <>
        "SELECT customer_id AS customer_id, COUNT(*) AS n"
    )

    # Its target has a column the definition computes nothing for.
    queue(
      "public.rq_counts",
      "public.rq_bad",
      nil,
      "TRANSFORM rq_bad FROM rq_counts GROUP BY customer_id SELECT customer_id AS customer_id, COUNT(*) AS n"
    )

    %{cluster: cluster}
  end

  setup do
    Application.put_env(:trellis_pg, :allow_target_writes, true)
    on_exit(fn -> Application.delete_env(:trellis_pg, :allow_target_writes) end)
    :ok
  end

  test "a chain with a diamond is refilled upstream first, from uncommitted rows, and rolls back" do
    {:error, :rolled_back} =
      GuardedTestRepo.transaction(fn ->
        sql("insert into rq_orders values (1, 8, 3), (2, 8, 1), (3, 9, 4)")
        # Rows stubbed by hand, which a refresh in the wrong order would read.
        sql("insert into rq_counts values (7, 99, 99)")
        sql("insert into rq_doubled values (7, 99)")

        assert :ok = refresh("rq_view")
        assert rows("rq_counts") == [[8, 2, 4], [9, 1, 4]]
        assert rows("rq_doubled") == [[8, 4], [9, 2]]
        assert rows("rq_view") == [[8, 2, 4], [9, 1, 2]]
        GuardedTestRepo.rollback(:rolled_back)
      end)

    assert rows("rq_orders") == []
    assert rows("rq_view") == []
  end

  test "a name resolves unqualified, qualified, as an atom or as a prefixed Ecto schema" do
    {:error, :rolled_back} =
      GuardedTestRepo.transaction(fn ->
        sql("insert into rq_orders values (1, 8, 3)")
        assert :ok = refresh(:rq_doubled)
        assert rows("rq_doubled") == [[8, 2]]
        assert :ok = refresh("public.rq_counts")
        assert :ok = refresh(OtherCounts)
        assert rows("rq_other.rq_counts") == [[8, 1]]
        GuardedTestRepo.rollback(:rolled_back)
      end)
  end

  test "an unknown or ambiguous name raises" do
    assert_raise ArgumentError,
                 ~r/no Trellis definition is recorded for the target nope: .* rq_trellis.restore_queue/,
                 fn -> refresh("nope") end

    assert_raise ArgumentError,
                 ~r/rq_counts names more than one target \(public.rq_counts, rq_other.rq_counts\)/,
                 fn -> refresh("rq_counts") end

    # The default Trellis schema holds no queue here.
    assert_raise ArgumentError, ~r/ trellis.restore_queue/, fn ->
      TestSupport.refresh_target(GuardedTestRepo, "rq_view")
    end
  end

  test "it refuses unless allow_target_writes is true, leaving the target as it was" do
    Application.delete_env(:trellis_pg, :allow_target_writes)
    sql("insert into rq_orders values (1, 8, 3)")
    sql("insert into rq_doubled values (7, 99)")

    try do
      assert_raise ReadOnlyTarget, ~r/allow_target_writes: true/, fn -> refresh("rq_doubled") end
      assert rows("rq_doubled") == [[7, 99]]
    after
      sql("delete from rq_doubled")
      sql("delete from rq_orders")
    end
  end

  test "a refresh that fails part way writes nothing, upstream included" do
    sql("insert into rq_orders values (1, 8, 3)")
    sql("insert into rq_counts values (7, 99, 99)")

    try do
      assert_raise Trellis.Error, ~r/extra/, fn -> refresh("rq_bad") end
      assert rows("rq_counts") == [[7, 99, 99]]
    after
      sql("delete from rq_counts")
      sql("delete from rq_orders")
    end
  end

  test "a live instance's catalog is read when there is no restore queue", %{cluster: cluster} do
    # No staging or drain worker: the define records the definition and
    # creates its target, and nothing else writes it. (A chained define
    # needs its upstream live, so the chains are the restore queue's tests.)
    trellis = Trellis.connect!(url: cluster["dsn"])
    :ok = Trellis.migrate!(trellis)

    try do
      sql("create table rc_groups (id integer primary key, label text not null)")

      sql(
        "create table rc_events (id integer primary key, g integer not null, n integer not null)"
      )

      sql("create index on rc_events (g)")
      sql("insert into rc_groups select i, 'g' || i from generate_series(0, 2) i")
      sql("insert into rc_events select i, i % 3, i from generate_series(1, 10) i")

      Trellis.apply!(
        trellis,
        "TRANSFORM rc_sums FROM rc_events JOIN rc_groups ON rc_events.g = rc_groups.id " <>
          "GROUP BY rc_groups.label SELECT rc_groups.label AS label, SUM(rc_events.n) AS total"
      )

      assert :ok = TestSupport.refresh_target(GuardedTestRepo, "rc_sums")

      assert sql("select label, total from rc_sums order by label").rows ==
               [["g0", 18], ["g1", 22], ["g2", 15]]
    after
      Trellis.shutdown!(trellis)
    end
  end

  defp refresh(target), do: TestSupport.refresh_target(GuardedTestRepo, target, schemas: [@queue])

  defp sql(text), do: GuardedTestRepo.query!(text)

  defp rows(table), do: sql("select * from #{table} order by 1").rows

  defp queue(source, target, join, text) do
    GuardedTestRepo.query!(
      "insert into #{@queue}.restore_queue " <>
        "select count(*) + 1, $1, $2, $3, $4, 2, '0.2.0' from #{@queue}.restore_queue",
      [source, target, join, text]
    )
  end
end
