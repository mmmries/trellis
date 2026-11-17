if Code.ensure_loaded?(Plug) do
  defmodule Trellis.Metrics.Appender do
    @moduledoc """
    A Plug that appends `Trellis.Metrics.render_prometheus/0` to the Prometheus
    text another exporter's plug in the host's pipeline (`PromEx.Plug`, or one
    serving `TelemetryMetricsPrometheus.Core.scrape/1`) serves at `:path`, so
    one scrape returns both. `TelemetryMetricsPrometheus`'s own server takes
    no plugs: scrape `Trellis.Metrics.Plug` as a second target instead.
    Compiled only when `:plug` is a dependency of the host.

    It registers a `Plug.Conn.register_before_send/2` callback, so it must run
    before the exporter's plug in the pipeline:

        plug Trellis.Metrics.Appender, path: "/metrics"
        plug PromEx.Plug, prom_ex_module: MyApp.PromEx

    A `GET` or `HEAD` of `:path` (default `"/metrics"`, matched on the
    request's remaining path segments) is left to the exporter. Only a 200
    whose body is set in memory, whose content type is `text/plain` and that
    has no `content-encoding` other than `identity` is changed: a newline is
    added if the body lacks one and the Trellis text follows. Everything else
    passes through as the exporter sent it: other paths and methods, other
    statuses, compressed, chunked and file responses, and an empty registry.
    Appending twice on one request, as when the plug is in two pipelines,
    happens once.

    It does not dedupe, and the text format allows each metric name once, so
    don't name an app metric like one of the engine's `trellis_*` series.

    The `:render` option, a zero-arity function returning the text to append,
    exists for tests; it defaults to `Trellis.Metrics.render_prometheus/0`.
    """

    @behaviour Plug

    @private_key :trellis_metrics_appended

    @impl Plug
    def init(opts) do
      {opts |> Keyword.get(:path, "/metrics") |> segments(),
       Keyword.get(opts, :render, &Trellis.Metrics.render_prometheus/0)}
    end

    @impl Plug
    def call(%Plug.Conn{method: method, path_info: path} = conn, {segments, render})
        when method in ["GET", "HEAD"] and path == segments do
      if conn.private[@private_key] do
        conn
      else
        conn
        |> Plug.Conn.put_private(@private_key, true)
        |> Plug.Conn.register_before_send(&append(&1, render))
      end
    end

    def call(conn, _opts), do: conn

    defp segments(path), do: String.split(path, "/", trim: true)

    defp append(%Plug.Conn{status: 200, state: :set, resp_body: body} = conn, render)
         when is_binary(body) or is_list(body) do
      if appendable?(conn) do
        case render.() do
          "" ->
            conn

          trellis ->
            text = IO.iodata_to_binary(body)
            sep = if text == "" or String.ends_with?(text, "\n"), do: "", else: "\n"
            %{conn | resp_body: [text, sep, trellis]}
        end
      else
        conn
      end
    end

    defp append(conn, _render), do: conn

    defp appendable?(conn) do
      text_plain?(Plug.Conn.get_resp_header(conn, "content-type")) and
        Enum.all?(Plug.Conn.get_resp_header(conn, "content-encoding"), &identity?/1)
    end

    defp text_plain?([value | _]),
      do: value |> String.downcase() |> String.starts_with?("text/plain")

    defp text_plain?([]), do: false

    defp identity?(value), do: String.downcase(value) == "identity"
  end
end
