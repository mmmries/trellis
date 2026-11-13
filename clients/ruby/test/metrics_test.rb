# frozen_string_literal: true

require "rack/builder"
require "rack/lint"
require "minitest/mock"
require "rack/mock_request"
require "test_helper"

class MetricsTest < Minitest::Test
  include TrellisTestCase

  EXPOSITION_LINE = /\A(# (HELP|TYPE) trellis_\w+ .+|trellis_\w+(\{[^}]*\})? \S+)\z/

  # The registry is process-wide, so rendering needs no handle. What it holds
  # depends on what earlier tests in this process recorded, so this only
  # checks the text's shape.
  def test_renders_the_process_wide_registry_as_prometheus_text_without_a_handle
    refute Trellis.connected?
    body = Trellis::Metrics.render_prometheus
    assert_kind_of String, body

    body.each_line(chomp: true).reject(&:empty?).each do |line|
      assert_match EXPOSITION_LINE, line, "not a Prometheus exposition line: #{line.inspect}"
    end
  end

  # trellis_instance_up is rendered from the running handles themselves, so
  # it is there as soon as connect returns, and stays at 0 after shutdown.
  def test_an_instance_is_up_while_its_handle_runs_and_down_after_shutdown
    instance = Trellis::Instance.connect(url: TestCluster.dsn, schema: "metrics_up")
    assert_equal 1, instance_up("metrics_up")

    instance.shutdown
    assert_equal 0, instance_up("metrics_up")
  end

  def test_the_rack_app_serves_the_registry_of_a_live_handle_as_prometheus_text
    Trellis::Instance.connect(url: TestCluster.dsn, schema: "metrics_rack")
    response = scrape(Trellis::Metrics::RackApp)

    assert_equal 200, response.status
    assert_equal "text/plain; version=0.0.4; charset=utf-8", response.headers["content-type"]
    assert_equal response.body.bytesize.to_s, response.headers["content-length"]
    assert_match(%r{^trellis_instance_up\{trellis_instance="[^"/]+/metrics_rack"\} 1$}, response.body)
    response.body.each_line(chomp: true).reject(&:empty?).each do |line|
      assert_match EXPOSITION_LINE, line
    end
  end

  def test_the_rack_app_answers_head_with_headers_alone
    Trellis::Instance.connect(url: TestCluster.dsn, schema: "metrics_rack_head")
    head = scrape(Trellis::Metrics::RackApp, method: "HEAD")

    assert_equal 200, head.status
    assert_equal "", head.body
    assert_equal Trellis::Metrics::RackApp::CONTENT_TYPE, head.headers["content-type"]
    assert_operator Integer(head.headers["content-length"]), :>, 0
  end

  def test_the_rack_app_refuses_other_methods
    response = scrape(Trellis::Metrics::RackApp, method: "POST")

    assert_equal 405, response.status
    assert_equal "GET, HEAD", response.headers["allow"]
    assert_equal "", response.body
  end

  def test_the_rack_app_mounts_at_a_path_like_rails_mount_does
    app = Rack::Builder.new do
      map("/metrics/trellis") { run Trellis::Metrics::RackApp }
    end

    assert_equal 200, scrape(app, path: "/metrics/trellis").status
    assert_equal 404, scrape(app, path: "/elsewhere").status
  end

  # A label value (a transform name) can hold non-ASCII text.
  def test_the_rack_app_counts_the_body_in_bytes
    Trellis::Metrics.stub(:render_prometheus, "trellis_changes_applied_total{transform=\"café\"} 1\n") do
      response = scrape(Trellis::Metrics::RackApp)

      assert_equal response.body.bytesize.to_s, response.headers["content-length"]
      refute_equal response.body.length.to_s, response.headers["content-length"]
    end
  end

  private

  # Rack::Lint fails the request on any breach of the Rack spec.
  def scrape(app, path: "/", method: "GET")
    Rack::MockRequest.new(Rack::Lint.new(app)).request(method, path)
  end

  def instance_up(schema)
    sample = Trellis::Metrics.render_prometheus[
      %r{^trellis_instance_up\{trellis_instance="[^"/]+/#{schema}"\} (\d+)$}, 1
    ]
    refute_nil sample, "no trellis_instance_up series for #{schema}"
    Integer(sample)
  end
end
