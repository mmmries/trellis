# frozen_string_literal: true

require "rbconfig"
require "test_helper"

# ADR-0010 decision 3: a handle does not survive fork. Rust threads don't
# cross it, so a child's calls on the handle it inherited would wait forever
# for a reply; instead every one raises Trellis::ForkedHandleError at once.
class ForkTest < Minitest::Test
  include TrellisTestCase

  def test_a_forked_child_s_calls_raise_rather_than_hang
    Trellis.connect(url: TestCluster.dsn)
    parent = Process.pid
    out_r, out_w = IO.pipe

    child = fork do
      out_r.close
      {
        "status" => -> { Trellis.status("no_such_target") },
        "migrate" => -> { Trellis.migrate },
        "define" => -> { Trellis.define("TRANSFORM t FROM no_such_source SELECT a AS a") },
        "shutdown" => -> { Trellis.shutdown }
      }.each do |name, call|
        call.call
        out_w.puts "#{name}: returned"
      rescue StandardError => e
        out_w.puts "#{name}: #{e.class}: #{e.message}"
      end
      out_w.puts "connected?: #{Trellis.connected?}"
    ensure
      out_w.close
      # Never the parent's at_exit hooks (minitest's would rerun the suite).
      exit!(0)
    end
    out_w.close

    status = wait_for_child(child, seconds: 30)
    output = out_r.read
    assert status.success?, output

    lines = output.lines(chomp: true)
    %w[status migrate define shutdown].each do |name|
      assert_includes lines,
                      "#{name}: Trellis::ForkedHandleError: this Trellis handle was connected by " \
                      "process #{parent}, and this is process #{child}: a handle does not survive " \
                      "fork, so call Trellis.connect in this process (after forking: Puma's " \
                      "on_worker_boot, Passenger's starting_worker_process)",
                      output
    end
    assert_includes lines, "connected?: false"

    # The parent's handle is untouched.
    assert Trellis.connected?
    assert_nil Trellis.status("no_such_target")
  ensure
    out_r&.close
  end

  # The whole life of a forking server's worker, in a script of its own so
  # both processes exit normally, running their at_exit hooks and freeing
  # their handles: the child finds the inherited handle refused, connects
  # its own, uses it, and exits; the parent's handle is unaffected.
  def test_a_forked_child_connects_its_own_handle_and_both_exit_cleanly
    script = File.expand_path("support/fork_worker.rb", __dir__)
    lib = File.expand_path("../lib", __dir__)
    out_r, out_w = IO.pipe
    pid = Process.spawn(RbConfig.ruby, "-I", lib, script, TestCluster.dsn,
                        out: out_w, err: out_w, pgroup: true)
    out_w.close

    status = wait_for_child(pid, seconds: 60, group: true)
    output = out_r.read
    assert status.success?, output
    assert_equal <<~OUT, output
      child: Trellis::ForkedHandleError
      child: connected? false
      child: own handle status nil
      child: connected? true
      child exit: 0
      parent: status nil
    OUT
  ensure
    out_r&.close
  end
end
