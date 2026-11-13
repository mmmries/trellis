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
  # a route the app already has, in Rails
  #
  #   # config/routes.rb
  #   mount Trellis::Metrics::RackApp, at: "/metrics/trellis"
  #
  # or in any Rack server.
  #
  # The series that matter (the ring gauges, the latency histograms, the
  # applied counters) are set in the process that runs the staging and drain
  # work (staging: true, drain_threads above 0). A process with neither holds
  # only trellis_instance_up, so scrape the process that runs the worker. In a
  # forking server each worker process has its own registry, holding only its
  # own handles' series.
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
    # content type Prometheus expects, at whatever path it is mounted. It
    # needs no Rack gem: any server that calls an object with an environment
    # Hash will do. It renders the registry of the process it runs in (see
    # Metrics).
    #
    #   mount Trellis::Metrics::RackApp, at: "/metrics/trellis"   # Rails
    #   map("/metrics/trellis") { run Trellis::Metrics::RackApp } # config.ru
    #
    # Every other method is a 405. The mount is unauthenticated, as a
    # Prometheus target is: put it behind the app's own constraint if the
    # port is public.
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
  end
end
