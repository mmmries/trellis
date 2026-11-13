# frozen_string_literal: true

module Trellis
  # Forwards the engine's log lines to a Logger: Trellis.logger= installs
  # trellis-embed's pull-based tracing subscriber in this extension's native
  # library, and a Ruby thread drains its bounded queue into the Logger every
  # 100 ms. Engine threads only append to the queue; nothing calls into the
  # VM from them. Levels, drops, fork and exit behaviour: "Logs" in the
  # README.
  class LogBridge
    # Seconds between drains.
    INTERVAL = 0.1
    # Records per call into the extension: one call holds the GVL for no
    # longer than this many lines take to copy.
    BATCH = 500
    # Batches per drain_once: enough to empty a full queue (10,000 lines).
    MAX_BATCHES = 21

    # The engine's level words, as Logger's severity integers (written out,
    # so the gem needs no `require "logger"`, a bundled gem since Ruby 4.0).
    # Its trace has no Logger equivalent.
    SEVERITIES = { error: 3, warn: 2, info: 1, debug: 0, trace: 0 }.freeze

    # A Logger level, as the most verbose engine level worth forwarding. The
    # engine has no level above error.
    FILTERS = { debug: "debug", info: "info", warn: "warn", error: "error", fatal: "off", unknown: "off" }.freeze

    @lock = Mutex.new
    @current = nil

    class << self
      # The bridge Trellis.logger= installed, or nil.
      attr_reader :current

      # Replaces the process's bridge with one for `logger` (stopping the
      # previous one), or with none if `logger` is nil: the engine's lines then
      # go nowhere. `level` is the most verbose level forwarded, a Logger
      # level (:info, "warn", 0); the default is logger.level.
      # Returns the new bridge.
      def install(logger, level: nil)
        @lock.synchronize do
          previous = @current
          previous&.stop
          @current = nil
          if logger.nil?
            Native.set_log_level("off") if previous
            return nil
          end
          @current = new(logger, level: level).start
        end
      end

      # Starts the bridge's thread again in a process forked since it started.
      # What Instance.connect calls before it connects.
      def ensure_running
        @lock.synchronize { @current }&.ensure_running
      end

      # Stops the current bridge, draining what is left. Instance's at_exit
      # hook calls it once every instance is down.
      def stop_current
        @lock.synchronize { @current }&.stop
      end

      # A Logger level (Integer or word) as the engine's level filter word.
      # Raises ValidationError for anything else.
      def filter(level)
        case level
        when Integer then FILTERS.values.fetch(level.clamp(0, FILTERS.size - 1))
        when Symbol, String
          FILTERS.fetch(level.to_s.downcase.to_sym) { invalid_level!(level) }
        else invalid_level!(level)
        end
      end

      private

      def invalid_level!(level)
        raise ValidationError,
              "a log level is one of #{FILTERS.keys.map(&:inspect).join(', ')} or a Logger severity " \
              "integer, got: #{level.inspect}"
      end
    end

    attr_reader :logger

    def initialize(logger, level: nil)
      unless logger.respond_to?(:add)
        raise ValidationError, "the logger must respond to add (a Logger), got: #{logger.inspect}"
      end

      @logger = logger
      @filter = self.class.filter(level || logger.level)
      @drain_lock = Mutex.new
      @state = Mutex.new
      @pid = nil
      @thread = nil
      @stopped = false
      @last_error = nil
    end

    # Installs the subscriber in the native library (once per process, however
    # many bridges) and starts the thread. Raises ConflictError if another
    # subscriber in that library was installed first.
    def start
      Native.install_log_bridge(@filter)
      ensure_running
      self
    end

    # Starts the thread if this process hasn't (it was forked since). A
    # process forked from the one that started the bridge begins with an empty
    # queue: the lines the parent hadn't drained yet are the parent's to log.
    def ensure_running
      @state.synchronize do
        return self if @stopped || (@pid == Process.pid && @thread&.alive?)

        discard_queue if @pid && @pid != Process.pid
        @pid = Process.pid
        @thread = Thread.new { run }
        @thread.name = "trellis-log-bridge"
        @thread.report_on_exception = false
      end
      self
    end

    # Whether this process's thread is running.
    def running?
      @state.synchronize { @pid == Process.pid && !!@thread&.alive? }
    end

    # Changes the most verbose level forwarded: a Logger level, as for
    # install.
    def level=(level)
      @filter = self.class.filter(level)
      Native.set_log_level(@filter)
    end

    # Forwards every line queued so far to the Logger before returning,
    # rather than on the next tick. The thread calls it; tests and a host
    # that wants the engine's last lines logged now can too. Returns true if
    # lines may remain, because the queue held more than one call forwards.
    def drain_once
      @drain_lock.synchronize do
        MAX_BATCHES.times do
          records, dropped = Native.take_log_records(BATCH)
          if dropped.positive?
            logger.add(SEVERITIES.fetch(:warn), "Trellis dropped #{dropped} log lines: the Logger bridge fell behind", "trellis")
          end
          records.each { |level, target, message| logger.add(SEVERITIES.fetch(level), message, target) }
          return false if records.size < BATCH
        end
        true
      end
    end

    # Stops the thread and forwards what is left. Does nothing in a process
    # forked since the bridge started and not started again: the queue it
    # inherited isn't its own to log. A stopped bridge stays stopped.
    def stop
      thread = @state.synchronize do
        @stopped = true
        @thread if @pid == Process.pid
      end
      return unless thread

      begin
        thread.wakeup
      rescue ThreadError
        # Already finished.
      end
      thread.join
      drain_safely
    end

    private

    def run
      until @stopped
        sleep INTERVAL
        drain_safely
      end
    end

    # A Logger that raises (a closed stream) mustn't end the thread, or make
    # the next drain's lines vanish: warn once per distinct failure, drop
    # nothing it hasn't already taken, and carry on.
    def drain_safely
      nil while drain_once
      @last_error = nil
    rescue StandardError => e
      failure = "#{e.class}: #{e.message}"
      warn "trellis: forwarding the engine's log lines failed: #{failure}" unless failure == @last_error
      @last_error = failure
    end

    def discard_queue
      nil while Native.take_log_records(BATCH).first.size == BATCH
    end
  end
end
