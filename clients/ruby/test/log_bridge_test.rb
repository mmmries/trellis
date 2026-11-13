# frozen_string_literal: true

require "json"
require "rbconfig"
require "test_helper"
require_relative "support/recording_logger"

# Trellis::LogBridge: the engine's log lines reach a Logger. Every test drains
# with drain_once, which waits out the bridge's own thread, so none waits for
# a tick.
class LogBridgeTest < Minitest::Test
  include TrellisTestCase

  # The engine logs this at info, and it needs no background work: dropping a
  # transform that doesn't exist is a no-op.
  NO_SUCH = /drop is a no-op; no such definition trellis_instance=\S+\/\S+ transform=%s\z/

  def teardown
    Trellis.logger = nil
    super
  end

  def drop_line(name)
    Regexp.new(format(NO_SUCH.source, name))
  end

  def test_an_engine_line_reaches_the_logger_with_its_level_target_and_fields
    logger = RecordingLogger.new
    Trellis.logger = logger
    Trellis.connect(url: TestCluster.dsn)
    Trellis.apply("DROP TRANSFORM no_such_transform_one")
    Trellis::LogBridge.current.drain_once

    line = logger.matching(drop_line("no_such_transform_one")).fetch(0)
    assert_equal 1, line.severity, "info"
    assert_equal "trellis::defs::lifecycle", line.progname
    assert_same logger, Trellis.logger
  end

  def test_the_level_filters_at_the_engine_and_can_change
    logger = RecordingLogger.new
    Trellis.logger = logger
    Trellis.log_level = :warn
    Trellis.connect(url: TestCluster.dsn)
    Trellis.apply("DROP TRANSFORM no_such_transform_two")
    Trellis::LogBridge.current.drain_once
    assert_empty logger.matching(/no_such_transform_two/)

    Trellis.log_level = "info"
    Trellis.apply("DROP TRANSFORM no_such_transform_three")
    Trellis::LogBridge.current.drain_once
    assert_equal 1, logger.matching(drop_line("no_such_transform_three")).size
  end

  def test_the_loggers_own_level_is_the_default_filter
    logger = RecordingLogger.new(2)
    Trellis.logger = logger
    Trellis.connect(url: TestCluster.dsn)
    Trellis.apply("DROP TRANSFORM no_such_transform_four")
    Trellis::LogBridge.current.drain_once
    assert_empty logger.matching(/no_such_transform_four/)
  end

  def test_a_level_is_a_logger_level_and_anything_else_is_refused
    filter = Trellis::LogBridge.method(:filter)
    assert_equal %w[debug info warn error off off], (0..5).map { |severity| filter.call(severity) }
    assert_equal %w[debug info warn error off off],
                 %i[debug info warn error fatal unknown].map { |level| filter.call(level) }
    assert_equal "info", filter.call("INFO")
    [:loud, "", nil, 1.5].each do |level|
      assert_raises(Trellis::ValidationError, level.inspect) { filter.call(level) }
    end
    assert_raises(Trellis::ValidationError) { Trellis.log_level = :info }
    assert_raises(Trellis::ValidationError) { Trellis::LogBridge.new(Object.new) }
  end

  # Every level the extension can send maps to the Logger severity of its
  # name, and trace to DEBUG.
  def test_every_engine_level_has_a_severity
    require "logger"
    assert_equal Trellis::LogBridge::SEVERITIES.keys.sort, Trellis::Native.log_levels.sort
    assert_equal({ error: Logger::ERROR, warn: Logger::WARN, info: Logger::INFO, debug: Logger::DEBUG,
                   trace: Logger::DEBUG }, Trellis::LogBridge::SEVERITIES)
  end

  def test_a_full_queue_is_reported_as_one_warning
    logger = RecordingLogger.new
    bridge = Trellis::LogBridge.new(logger)
    batches = [[[[:info, "a::b", "kept"]], 7], [[], 0]]
    take = ->(max) { assert_equal Trellis::LogBridge::BATCH, max; batches.shift || [[], 0] }
    Trellis::Native.stub(:take_log_records, take) { bridge.drain_once }

    assert_equal [[1, "a::b", "kept"], [2, "trellis", "Trellis dropped 7 log lines: the Logger bridge fell behind"]],
                 logger.lines.map { |l| [l.severity, l.progname, l.message] }.sort_by(&:first)
  end

  def test_drain_once_says_whether_more_may_be_queued
    logger = RecordingLogger.new
    bridge = Trellis::LogBridge.new(logger)
    full = Array.new(Trellis::LogBridge::BATCH) { [:info, "a", "x"] }
    Trellis::Native.stub(:take_log_records, [full, 0]) do
      assert bridge.drain_once, "every batch was full"
    end
    assert_equal Trellis::LogBridge::MAX_BATCHES * Trellis::LogBridge::BATCH, logger.lines.size
    Trellis::Native.stub(:take_log_records, [[], 0]) { refute bridge.drain_once }
  end

  def test_a_logger_that_raises_does_not_end_the_thread
    logger = Class.new(RecordingLogger) do
      def add(*)
        raise IOError, "closed stream"
      end
    end.new
    bridge = Trellis::LogBridge.new(logger)
    Trellis::Native.stub(:take_log_records, [[[:info, "a", "x"]], 0]) do
      _, err = capture_io { bridge.send(:drain_safely) }
      assert_includes err, "IOError: closed stream"
      # The same failure is warned about once.
      _, err = capture_io { bridge.send(:drain_safely) }
      assert_empty err
    end
  end

  def test_setting_the_logger_replaces_the_bridge_and_nil_stops_it
    first = Trellis::LogBridge.install(RecordingLogger.new)
    assert first.running?
    second = Trellis::LogBridge.install(RecordingLogger.new)
    refute first.running?
    assert second.running?
    Trellis.logger = nil
    refute second.running?
    assert_nil Trellis.logger
  end

  # The thread doesn't survive fork: a child starts its own when a handle
  # connects there, and logs from it, but not the lines the parent had
  # queued and not yet drained.
  def test_a_forked_child_logs_after_it_connects
    logger = RecordingLogger.new
    Trellis.logger = logger
    bridge = Trellis::LogBridge.current
    # Only this test drains the parent's queue from here, so parent_marker
    # is still queued when the child forks.
    bridge.instance_variable_get(:@thread).kill.join
    Trellis.connect(url: TestCluster.dsn)
    Trellis.apply("DROP TRANSFORM parent_marker")
    Trellis.shutdown
    out_r, out_w = IO.pipe

    child = fork do
      out_r.close
      seen = { before: bridge.running? }
      Trellis.connect(url: TestCluster.dsn)
      seen[:after] = bridge.running?
      Trellis.apply("DROP TRANSFORM child_marker")
      bridge.drain_once
      seen[:lines] = logger.lines.map(&:message)
      out_w.puts JSON.generate(seen)
      Trellis.shutdown
    ensure
      out_w.close
      # Never the parent's at_exit hooks (minitest's would rerun the suite).
      exit!(0)
    end
    out_w.close
    status = wait_for_child(child, seconds: 60)
    report = out_r.read
    assert status.success?, report
    seen = JSON.parse(report)

    assert_equal false, seen.fetch("before"), "the thread didn't cross the fork"
    assert_equal true, seen.fetch("after"), "connecting started it"
    assert seen.fetch("lines").any? { |m| m.match?(drop_line("child_marker")) }, report
    refute(seen.fetch("lines").any? { |m| m.include?("parent_marker") },
           "the child logged a line its parent queued: #{report}")
    bridge.drain_once
    assert_equal 1, logger.matching(drop_line("parent_marker")).size, "the parent logs its own line"
  ensure
    out_r&.close
  end

  def test_a_logger_that_raises_can_be_replaced
    logger = Class.new(RecordingLogger) do
      def add(*)
        raise IOError, "closed stream"
      end
    end.new
    Trellis.logger = logger
    replacement = RecordingLogger.new
    _, err = capture_io do
      Trellis::Native.stub(:take_log_records, [[[:info, "a", "x"]], 0]) { Trellis.logger = replacement }
    end
    assert_includes err, "IOError: closed stream"
    assert_same replacement, Trellis.logger
  end

  # What a process logs just before it exits still arrives: the bridge stops
  # and drains after the instances shut down.
  def test_lines_logged_at_exit_are_forwarded_before_the_process_ends
    script = <<~RUBY
      require "trellis/pg"
      require_relative #{File.expand_path("support/recording_logger.rb", __dir__).inspect}
      logger = RecordingLogger.new
      at_exit { puts logger.lines.map(&:message) } # registered first, so it runs last
      Trellis.logger = logger
      Trellis.connect(url: ARGV.fetch(0))
      Trellis.apply("DROP TRANSFORM exit_marker")
    RUBY
    out_r, out_w = IO.pipe
    pid = Process.spawn(RbConfig.ruby, "-I", File.expand_path("../lib", __dir__), "-e", script,
                        TestCluster.dsn, out: out_w, err: out_w, pgroup: true)
    out_w.close
    status = wait_for_child(pid, seconds: 60, group: true)
    output = out_r.read
    assert status.success?, output
    assert output.lines(chomp: true).any? { |line| line.match?(drop_line("exit_marker")) }, output
  ensure
    out_r&.close
  end
end
