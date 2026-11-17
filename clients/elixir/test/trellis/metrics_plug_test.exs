defmodule Trellis.MetricsPlugTest do
  use ExUnit.Case, async: true

  import Plug.Conn
  import Plug.Test

  alias Trellis.Metrics.{Appender, Plug}, warn: false

  @prom "text/plain; version=0.0.4"

  # Stands in for PromEx / TelemetryMetricsPrometheus: its own series on
  # /metrics, anything else elsewhere.
  defp exporter(conn, opts) do
    conn
    |> put_resp_content_type(Keyword.get(opts, :content_type, @prom))
    |> then(fn c ->
      Enum.reduce(Keyword.get(opts, :headers, []), c, fn {k, v}, c -> put_resp_header(c, k, v) end)
    end)
    |> send_resp(Keyword.get(opts, :status, 200), Keyword.get(opts, :body, "app_metric 1\n"))
  end

  defp through_appender(conn, appender_opts \\ [], exporter_opts \\ []) do
    conn
    |> Appender.call(Appender.init([render: &trellis_body/0] ++ appender_opts))
    |> exporter(exporter_opts)
  end

  # The registry is empty without a handle, so the appended text is stubbed.
  defp trellis_body, do: "trellis_instance_up{trellis_instance=\"db/trellis\"} 1\n"

  describe "Trellis.Metrics.Plug" do
    test "serves the registry in the Prometheus content type" do
      conn = conn(:get, "/anything") |> Plug.call(Plug.init([]))
      assert conn.status == 200
      assert [ct] = get_resp_header(conn, "content-type")
      assert ct =~ "text/plain; version=0.0.4"
      assert conn.resp_body =~ ~r/\A(# (HELP|TYPE) trellis_|trellis_|\z)/
    end

    test "answers HEAD with the headers and no body" do
      conn = conn(:head, "/metrics") |> Plug.call(Plug.init([]))
      assert conn.status == 200
      assert get_resp_header(conn, "content-type") != []
    end

    test "refuses other methods with 405 and an allow header" do
      for method <- [:post, :put, :delete] do
        conn = conn(method, "/metrics") |> Plug.call(Plug.init([]))
        assert conn.status == 405
        assert get_resp_header(conn, "allow") == ["GET, HEAD"]
      end
    end
  end

  describe "Trellis.Metrics.Appender" do
    test "appends the engine's text to the exporter's 200 on its path" do
      conn = conn(:get, "/metrics") |> through_appender()
      assert conn.status == 200
      assert conn.resp_body == "app_metric 1\n" <> trellis_body()
    end

    test "adds a newline when the exporter's body lacks one" do
      conn = conn(:get, "/metrics") |> through_appender([], body: "app_metric 1")
      assert conn.resp_body == "app_metric 1\n" <> trellis_body()
    end

    test "matches the configured path, with or without slashes" do
      conn = conn(:get, "/prom") |> through_appender(path: "/prom")
      assert conn.resp_body == "app_metric 1\n" <> trellis_body()

      conn = conn(:get, "/metrics") |> through_appender(path: "/prom")
      assert conn.resp_body == "app_metric 1\n"
    end

    test "appends nothing when the registry is empty" do
      conn =
        conn(:get, "/metrics")
        |> Appender.call(Appender.init(render: fn -> "" end))
        |> exporter([])

      assert conn.resp_body == "app_metric 1\n"
    end

    test "defaults to the engine's registry" do
      conn = conn(:get, "/metrics") |> Appender.call(Appender.init([])) |> exporter([])
      assert conn.resp_body == "app_metric 1\n" <> Trellis.Metrics.render_prometheus()
    end

    test "leaves other paths and methods alone" do
      assert (conn(:get, "/other") |> through_appender()).resp_body == "app_metric 1\n"
      assert (conn(:post, "/metrics") |> through_appender()).resp_body == "app_metric 1\n"
      assert (conn(:get, "/metrics/extra") |> through_appender()).resp_body == "app_metric 1\n"
    end

    test "appends to HEAD as it would to GET" do
      conn = conn(:head, "/metrics") |> through_appender()
      assert conn.status == 200
    end

    test "leaves non-200 responses alone" do
      conn = conn(:get, "/metrics") |> through_appender([], status: 503, body: "down")
      assert conn.resp_body == "down"
    end

    test "leaves non-text/plain responses alone" do
      conn =
        conn(:get, "/metrics")
        |> through_appender([], content_type: "application/json", body: "{}")

      assert conn.resp_body == "{}"
    end

    test "leaves compressed responses alone, appends to an identity one" do
      conn =
        conn(:get, "/metrics")
        |> through_appender([], headers: [{"content-encoding", "gzip"}])

      assert conn.resp_body == "app_metric 1\n"

      conn =
        conn(:get, "/metrics")
        |> through_appender([], headers: [{"content-encoding", "identity"}])

      assert conn.resp_body == "app_metric 1\n" <> trellis_body()
    end

    test "leaves chunked responses alone" do
      conn =
        conn(:get, "/metrics")
        |> Appender.call(Appender.init(render: &trellis_body/0))
        |> put_resp_content_type(@prom)
        |> send_chunked(200)

      {:ok, conn} = chunk(conn, "app_metric 1\n")
      assert conn.state == :chunked
      assert conn.resp_body == "app_metric 1\n"
    end

    test "does not append twice when the plug runs twice" do
      conn =
        conn(:get, "/metrics")
        |> Appender.call(Appender.init(render: &trellis_body/0))
        |> Appender.call(Appender.init(render: &trellis_body/0))
        |> exporter([])

      assert conn.resp_body == "app_metric 1\n" <> trellis_body()
    end

    test "matches the remaining path when mounted by forward" do
      conn = %{conn(:get, "/ops/metrics") | path_info: ["metrics"], script_name: ["ops"]}
      conn = conn |> through_appender()
      assert conn.resp_body == "app_metric 1\n" <> trellis_body()
    end
  end
end
