if Code.ensure_loaded?(Plug) do
  defmodule Trellis.Metrics.Plug do
    @moduledoc """
    A Plug answering `GET` and `HEAD` with `Trellis.Metrics.render_prometheus/0`,
    in the content type Prometheus expects, wherever it is mounted; every other
    method is a 405. Compiled only when `:plug` is a dependency of the host.

    Unauthenticated, as a Prometheus target is: guard it if the port is public.

        # Phoenix router
        forward "/metrics/trellis", Trellis.Metrics.Plug

        # Plug.Router
        forward "/metrics/trellis", to: Trellis.Metrics.Plug

    To add the engine's series to an exporter the host already serves, see
    `Trellis.Metrics.Appender`.
    """

    @behaviour Plug

    import Plug.Conn

    @content_type "text/plain; version=0.0.4"

    @impl Plug
    def init(opts), do: opts

    @impl Plug
    def call(%Plug.Conn{method: method} = conn, _opts) when method in ["GET", "HEAD"] do
      conn
      |> put_resp_content_type(@content_type)
      |> send_resp(200, Trellis.Metrics.render_prometheus())
    end

    def call(conn, _opts) do
      conn
      |> put_resp_header("allow", "GET, HEAD")
      |> send_resp(405, "")
    end
  end
end
