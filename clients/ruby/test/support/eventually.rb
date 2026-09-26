# frozen_string_literal: true

# Polling for the tests that wait on the engine's background workers.
module Eventually
  CONVERGE_SECONDS = 30

  # Calls the block until it returns something truthy and returns that, or
  # fails the test naming what it waited for and the last value it saw.
  def eventually(what, seconds: CONVERGE_SECONDS)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds
    loop do
      seen = yield
      return seen if seen
      if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
        flunk "waited #{seconds}s for #{what}; last saw #{seen.inspect}"
      end

      sleep 0.05
    end
  end
end
