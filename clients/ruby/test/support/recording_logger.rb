# frozen_string_literal: true

# The part of Logger that Trellis::LogBridge calls, recording what it is
# given, so a test needs neither the logger gem nor an IO.
class RecordingLogger
  Line = Struct.new(:severity, :progname, :message)

  attr_accessor :level
  attr_reader :lines

  def initialize(level = 0)
    @level = level
    @lines = []
    @lock = Mutex.new
  end

  def add(severity, message = nil, progname = nil)
    return true if severity < level

    @lock.synchronize { @lines << Line.new(severity, progname, message) }
    true
  end

  # The lines whose message matches `pattern`.
  def matching(pattern)
    @lock.synchronize { @lines.select { |line| line.message.match?(pattern) } }
  end
end
