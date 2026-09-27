# frozen_string_literal: true

# Run by fork_test.rb: `ruby -I lib fork_worker.rb DSN`. A parent that
# connects and forks a worker, the way a preloading, forking server would.
# Both processes exit normally, so their at_exit hooks run and their handles
# are freed as Ruby exits.

require "trellis"

$stdout.sync = true
dsn = ARGV.fetch(0)
Trellis.connect(url: dsn)

child = fork do
  begin
    Trellis.status("no_such_target")
    puts "child: status returned"
  rescue Trellis::ForkedHandleError => e
    puts "child: #{e.class}"
  end
  puts "child: connected? #{Trellis.connected?}"

  # Connecting after fork replaces the inherited handle, which is left alone.
  Trellis.connect(url: dsn)
  puts "child: own handle status #{Trellis.status('no_such_target').inspect}"
  puts "child: connected? #{Trellis.connected?}"
  # Exits without shutting down: the at_exit hook does it.
end

Process.wait(child)
puts "child exit: #{$?.exitstatus}"
puts "parent: status #{Trellis.status('no_such_target').inspect}"
# Exits without shutting down: the at_exit hook does it.
