# frozen_string_literal: true

require "rack/builder"
require "rack/lint"
require "minitest/mock"
require "rack/mock_request"
require "prometheus/client"
require "prometheus/middleware/exporter"
require "test_helper"

# Trellis::Metrics::Middleware, wrapped around prometheus-client's real
# exporter (Yabeda's exporter subclasses it) and around fake apps for the
# responses the real one never gives.
class MetricsMiddlewareTest < Minitest::Test
  include TrellisTestCase

  TEXT = "text/plain; version=0.0.4; charset=utf-8"
  SAMPLE_OR_COMMENT = /\A(# (HELP|TYPE) \w+ .+|\w+(\{[^}]*\})? \S+)\z/

  # A body that counts its closes and yields its chunks one at a time.
  class ChunkedBody
    attr_reader :closes

    def initialize(*chunks)
      @chunks = chunks
      @closes = 0
    end

    def each(&)
      @chunks.each(&)
    end

    def close
      @closes += 1
    end
  end

  def test_appends_the_engine_series_to_the_exporters_body
    Trellis::Instance.connect(url: TestCluster.dsn, schema: "metrics_mw")
    response = scrape(exporter_stack)

    assert_equal 200, response.status
    assert_equal Prometheus::Client::Formats::Text::CONTENT_TYPE, response.headers["content-type"]
    assert_equal response.body.bytesize.to_s, response.headers["content-length"]
    assert response.body.start_with?("# TYPE app_jobs_total counter\n"), response.body
    assert_match(%r{^app_jobs_total 3.0$}, response.body)
    assert_match(%r{^trellis_instance_up\{trellis_instance="[^"/]+/metrics_mw"\} 1$}, response.body)
    response.body.each_line(chomp: true) { |line| assert_match SAMPLE_OR_COMMENT, line }
    families = response.body.scan(/^# TYPE (\w+) /).flatten
    assert_equal families.uniq, families, "a metric family is declared twice"
  end

  def test_head_answers_with_the_length_of_the_get_and_no_body
    Trellis::Instance.connect(url: TestCluster.dsn, schema: "metrics_mw_head")
    stack = exporter_stack
    get = scrape(stack)
    head = scrape(stack, method: "HEAD")

    assert_equal 200, head.status
    assert_equal "", head.body
    assert_equal get.headers["content-length"], head.headers["content-length"]
  end

  def test_only_a_get_or_head_of_the_path_is_changed
    app = fake(200, { "content-type" => TEXT }, ["app 1\n"])
    middleware = Trellis::Metrics::Middleware.new(app, path: "/metrics")
    with_trellis("trellis_x 1\n") do
      assert_equal "app 1\ntrellis_x 1\n", scrape(middleware, path: "/metrics").body
      assert_equal "app 1\n", scrape(middleware, path: "/other").body
      assert_equal "app 1\n", scrape(middleware, path: "/metrics/").body
      assert_equal "app 1\n", scrape(middleware, path: "/metrics", method: "POST").body
    end
  end

  # A server (or Rails' router) answers a HEAD with the GET's headers and no
  # body, so the wrapped app's Content-Length is the one to add to.
  def test_head_is_answered_as_the_get_would_be_even_when_the_wrapped_app_answers_it_bodiless
    app = lambda do |env|
      head = env["REQUEST_METHOD"] == "HEAD"
      [200, { "content-type" => TEXT, "content-length" => "4" }, head ? [] : ["a 1\n"]]
    end
    with_trellis("t 1\n") do
      head = scrape(Trellis::Metrics::Middleware.new(app), method: "HEAD")

      assert_equal "", head.body
      assert_equal "8", head.headers["content-length"]
    end
  end

  def test_the_path_defaults_to_metrics
    middleware = Trellis::Metrics::Middleware.new(fake(200, { "content-type" => TEXT }, ["a 1\n"]))
    with_trellis("t 1\n") { assert_equal "a 1\nt 1\n", scrape(middleware, path: "/metrics").body }
  end

  def test_a_response_other_than_200_passes_through
    # The exporter's own 406, for a client that accepts no text format.
    stack = exporter_stack
    with_trellis("trellis_x 1\n") do
      response = Rack::MockRequest.new(Rack::Lint.new(stack)).get("/metrics", "HTTP_ACCEPT" => "application/json")

      assert_equal 406, response.status
      assert_equal "Supported media types: text/plain", response.body
    end
  end

  def test_a_200_that_is_not_text_passes_through
    app = fake(200, { "content-type" => "application/json" }, ["{}"])
    with_trellis("trellis_x 1\n") do
      assert_same_response app, "{}"
    end
  end

  def test_a_compressed_body_passes_through_unchanged
    gzipped = "\x1F\x8B compressed".b
    app = fake(200, { "content-type" => TEXT, "content-encoding" => "gzip",
                      "content-length" => gzipped.bytesize.to_s }, [gzipped])
    with_trellis("trellis_x 1\n") do
      response = scrape(Trellis::Metrics::Middleware.new(app), path: "/metrics")

      assert_equal gzipped, response.body.b
      assert_equal gzipped.bytesize.to_s, response.headers["content-length"]
      assert_equal "gzip", response.headers["content-encoding"]
    end
  end

  def test_a_body_with_its_own_chunk_framing_passes_through_unchanged
    app = fake(200, { "content-type" => TEXT, "transfer-encoding" => "chunked" }, ["3\r\na 1\r\n0\r\n\r\n"])
    with_trellis("trellis_x 1\n") do
      assert_equal "3\r\na 1\r\n0\r\n\r\n", scrape(Trellis::Metrics::Middleware.new(app), path: "/metrics").body
    end
  end

  def test_a_streaming_body_is_read_through_each_and_closed
    body = ChunkedBody.new("# TYPE a counter\n", "a 1\n", "b 2") # no final newline
    app = fake(200, { "content-type" => TEXT }, body)
    with_trellis("trellis_x 1\n") do
      response = scrape(Trellis::Metrics::Middleware.new(app), path: "/metrics")

      assert_equal "# TYPE a counter\na 1\nb 2\ntrellis_x 1\n", response.body
      assert_equal response.body.bytesize.to_s, response.headers["content-length"]
      assert_equal 1, body.closes
    end
  end

  def test_no_newline_is_added_when_the_exporters_body_ends_with_one
    app = fake(200, { "content-type" => TEXT }, ["a 1\n"])
    with_trellis("trellis_x 1\n") do
      assert_equal "a 1\ntrellis_x 1\n", scrape(Trellis::Metrics::Middleware.new(app), path: "/metrics").body
    end
  end

  def test_a_body_that_is_called_rather_than_iterated_passes_through
    streaming = proc { |stream| stream.write("a 1\n") && stream.close }
    app = fake(200, { "content-type" => TEXT }, streaming)
    with_trellis("trellis_x 1\n") do
      _status, _headers, body = Trellis::Metrics::Middleware.new(app).call(env("/metrics"))

      assert_same streaming, body
    end
  end

  def test_an_empty_registry_leaves_the_response_as_it_was
    headers = { "content-type" => TEXT, "content-length" => "4" }
    body = ["a 1\n"]
    with_trellis("") do
      assert_equal [200, headers, body].map(&:object_id),
                   Trellis::Metrics::Middleware.new(fake(200, headers, body)).call(env("/metrics")).map(&:object_id)
    end
  end

  # The registry can't be rendered in a process forked while its parent had
  # an engine running: the exporter's own scrape must still succeed.
  def test_a_forked_childs_error_leaves_the_exporters_response_alone
    app = fake(200, { "content-type" => TEXT, "content-length" => "4" }, ["a 1\n"])
    raises = -> { raise Trellis::ForkedHandleError, "forked" }
    Trellis::Metrics.stub(:render_prometheus, raises) do
      response = scrape(Trellis::Metrics::Middleware.new(app), path: "/metrics")

      assert_equal 200, response.status
      assert_equal "a 1\n", response.body
      assert_equal "4", response.headers["content-length"]
    end
  end

  def test_other_errors_are_not_swallowed
    app = fake(200, { "content-type" => TEXT }, ["a 1\n"])
    Trellis::Metrics.stub(:render_prometheus, -> { raise "boom" }) do
      assert_raises(RuntimeError) { Trellis::Metrics::Middleware.new(app).call(env("/metrics")) }
    end
  end

  # The exporter's bytes and the engine's (a transform name in a label) are
  # joined as bytes, and Content-Length counts bytes.
  def test_non_ascii_text_from_both_sides_is_joined_by_bytes
    app = fake(200, { "content-type" => TEXT }, ChunkedBody.new("app{k=\"na\u00EFve\"} 1\n", "app{k=\"se\u00F1or\"} 2\n"))
    with_trellis("trellis_x{transform=\"caf\u00E9\"} 1\n") do
      response = scrape(Trellis::Metrics::Middleware.new(app), path: "/metrics")

      expected = "app{k=\"na\u00EFve\"} 1\napp{k=\"se\u00F1or\"} 2\ntrellis_x{transform=\"caf\u00E9\"} 1\n"
      assert_equal expected.b, response.body.b
      assert_equal expected.bytesize.to_s, response.headers["content-length"]
    end
  end

  # Rack 2 and hand-written apps spell headers as they like.
  def test_header_names_are_matched_without_regard_to_case
    app = fake(200, { "Content-Type" => TEXT, "Content-Length" => "4" }, ["a 1\n"])
    with_trellis("t 1\n") do
      _status, headers, body = Trellis::Metrics::Middleware.new(app).call(env("/metrics"))

      assert_equal({ "Content-Type" => TEXT, "content-length" => "8" }, headers)
      assert_equal ["a 1\nt 1\n"], body
    end
  end

  def test_it_takes_its_place_in_a_rack_builder_stack
    registry = Prometheus::Client::Registry.new
    app = Rack::Builder.new do
      use Trellis::Metrics::Middleware, path: "/m"
      use Prometheus::Middleware::Exporter, registry: registry, path: "/m"
      run ->(_env) { [404, {}, []] }
    end
    with_trellis("trellis_x 1\n") do
      assert_equal "trellis_x 1\n", scrape(app, path: "/m").body
      assert_equal 404, scrape(app, path: "/elsewhere").status
    end
  end

  private

  def exporter_stack
    registry = Prometheus::Client::Registry.new
    registry.counter(:app_jobs_total, docstring: "Jobs.").increment(by: 3)
    exporter = Prometheus::Middleware::Exporter.new(->(_env) { [404, {}, []] }, registry: registry)
    Trellis::Metrics::Middleware.new(exporter)
  end

  def fake(status, headers, body)
    ->(_env) { [status, headers, body] }
  end

  def with_trellis(text, &)
    Trellis::Metrics.stub(:render_prometheus, text, &)
  end

  def assert_same_response(app, expected_body)
    response = scrape(Trellis::Metrics::Middleware.new(app), path: "/metrics")

    assert_equal 200, response.status
    assert_equal expected_body, response.body
  end

  def env(path)
    Rack::MockRequest.env_for(path)
  end

  # Rack::Lint fails the request on any breach of the Rack spec.
  def scrape(app, path: "/metrics", method: "GET")
    Rack::MockRequest.new(Rack::Lint.new(app)).request(method, path)
  end
end
