# frozen_string_literal: true

module Trellis
  # The engine's metrics, in Prometheus text exposition format: the Rust
  # crate's trellis::Metrics#render_prometheus
  # (docs/decisions/0009-observability-decisions.md).
  #
  # The registry is process-wide: one body holds the series of every Trellis
  # handle in this process, so it needs no handle and works before
  # Trellis.connect and after Trellis.shutdown. Every series carries a
  # trellis_instance="<database>/<catalog schema>" label, so instances in one
  # process stay apart. trellis_instance_up is 1 while an instance has a
  # running handle and 0 after its last one shuts down; the other gauges of a
  # stopped instance disappear after 60 seconds. See docs/observability.md.
  #
  # The binding opens no port. Trellis::Metrics::RackApp serves the text from
  # a route of its own; Trellis::Metrics::Middleware appends it to the body of
  # an exporter's route the app already has. Most series are set only in the
  # process running the staging or drain work, so scrape that one (README,
  # Metrics).
  #
  # A process forked while its parent had an engine running may have
  # inherited the registry's lock held, so rendering there raises
  # Trellis::ForkedHandleError, as Trellis.connect does.
  module Metrics
    # Renders the registry's current contents: # HELP and # TYPE lines, then
    # each series' samples.
    def self.render_prometheus
      Native.render_prometheus
    end

    # A Rack app answering GET and HEAD with render_prometheus, in the
    # content type Prometheus expects, at whatever path it is mounted; every
    # other method is a 405. It needs no Rack gem. Unauthenticated, as a
    # Prometheus target is: guard it if the port is public.
    #
    #   mount Trellis::Metrics::RackApp, at: "/metrics/trellis"   # Rails
    #   map("/metrics/trellis") { run Trellis::Metrics::RackApp } # config.ru
    class RackApp
      CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

      def self.call(env)
        new.call(env)
      end

      def call(env)
        case env["REQUEST_METHOD"]
        when "GET"
          body = Metrics.render_prometheus
          [200, headers(body), [body]]
        when "HEAD"
          [200, headers(Metrics.render_prometheus), []]
        else
          [405, { "allow" => "GET, HEAD", "content-length" => "0" }, []]
        end
      end

      private

      def headers(body)
        { "content-type" => CONTENT_TYPE, "content-length" => body.bytesize.to_s }
      end
    end

    # Rack middleware that appends render_prometheus to the Prometheus text
    # another exporter (prometheus-client's Prometheus::Middleware::Exporter,
    # Yabeda's exporter) serves at `path`, so one scrape returns both. It
    # needs no exporter gem and no Rack gem: it wraps any Rack app, and must
    # sit outside the exporter it appends to.
    #
    #   config.middleware.insert_before Prometheus::Middleware::Exporter,
    #                                   Trellis::Metrics::Middleware, path: "/metrics"
    #
    # A GET or HEAD of `path` (PATH_INFO, as the exporters match it) is
    # answered by the wrapped app. Only a 200 with a text/plain body that is
    # neither compressed nor chunk-framed (no Content-Encoding or
    # Transfer-Encoding other than identity) is changed: it is buffered, a
    # newline is added if it lacks one, the Trellis text follows, and
    # Content-Length is the new size. Everything else passes through as
    # the wrapped app returned it: other paths and methods, other statuses,
    # compressed bodies (put Rack::Deflater outside this middleware), bodies
    # with no each (Rack 3 streaming), an empty registry, and a process
    # forked while its parent had an engine running (render_prometheus
    # raises Trellis::ForkedHandleError there; the exporter's own series are
    # still served). HEAD is answered as the GET would be, without the body.
    #
    # It does not dedupe: Prometheus rejects a scrape that declares one
    # metric name twice, so an app metric named like one of the engine's
    # trellis_* series breaks the scrape.
    class Middleware
      def initialize(app, path: "/metrics")
        @app = app
        @path = path
      end

      def call(env)
        method = env["REQUEST_METHOD"]
        return @app.call(env) unless (method == "GET" || method == "HEAD") && env["PATH_INFO"] == @path

        # A HEAD is forwarded as the GET it stands for, so the wrapped app's
        # Content-Length is the body's real size; the body is dropped below.
        status, headers, body = @app.call(method == "HEAD" ? env.merge("REQUEST_METHOD" => "GET") : env)
        status, headers, body = append(status, headers, body)
        if method == "HEAD"
          body.close if body.respond_to?(:close)
          body = []
        end
        [status, headers, body]
      end

      private

      def append(status, headers, body)
        return [status, headers, body] unless appendable?(status, headers, body)

        trellis = begin
          Metrics.render_prometheus
        rescue ForkedHandleError
          return [status, headers, body]
        end
        return [status, headers, body] if trellis.empty?

        text = read(body)
        text << "\n" unless text.empty? || text.end_with?("\n")
        text << trellis.b
        headers = headers.reject { |name, _| name.casecmp?("content-length") }
        headers["content-length"] = text.bytesize.to_s
        [status, headers, [text]]
      end

      def appendable?(status, headers, body)
        status == 200 && body.respond_to?(:each) &&
          header(headers, "content-type").to_s.start_with?("text/plain") &&
          %w[content-encoding transfer-encoding].all? { |name| [nil, "identity"].include?(header(headers, name)) }
      end

      def header(headers, name)
        headers.each { |key, value| return value.to_s.downcase if key.casecmp?(name) }
        nil
      end

      # The whole body as bytes; the wrapped body is closed, as the server
      # would have.
      def read(body)
        text = String.new(encoding: Encoding::BINARY)
        body.each { |chunk| text << chunk.b }
        text
      ensure
        body.close if body.respond_to?(:close)
      end
    end
  end
end
