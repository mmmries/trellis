# frozen_string_literal: true

require "json"
require "pg"

# One throwaway Postgres for the whole suite, held by `trellis-testkit`
# (testkit/src/bin/trellis-testkit.rs). Its stdin is a pipe from this
# process: TestCluster.stop closes it at exit, and if this process dies
# instead the pipe closes all the same. Either way testkit stops the server
# and deletes its directory.
module TestCluster
  WORKSPACE = File.expand_path("../../../..", __dir__)

  class << self
    # The cluster's details: "dsn" (a libpq key=value string, which both
    # Trellis.connect and PG.connect take), and "host", "port", "user",
    # "dbname", "log_file".
    attr_reader :info

    # Starts the cluster and migrates Trellis's schema into its database, the
    # way a deploy's migration step would: on a handle that runs nothing in
    # the background.
    def start
      @testkit = IO.popen([executable], "r+")
      unless @testkit.wait_readable(120)
        raise "trellis-testkit reported no cluster within 120s"
      end

      line = @testkit.gets or raise "trellis-testkit exited before reporting a cluster"
      @info = JSON.parse(line)

      Trellis.connect(url: dsn)
      Trellis.migrate
      Trellis.shutdown
    end

    # Closes testkit's stdin and waits for it to tear the cluster down.
    def stop
      @testkit&.close
    end

    def dsn
      info.fetch("dsn")
    end

    # A new `pg` connection to the cluster's database. Close it when done.
    def pg
      PG.connect(dsn)
    end

    private

    # CI puts trellis-testkit on PATH; locally, the workspace's debug build.
    def executable
      on_path = ENV.fetch("PATH", "").split(File::PATH_SEPARATOR)
                   .map { |dir| File.join(dir, "trellis-testkit") }
                   .find { |path| File.executable?(path) }
      local = File.join(WORKSPACE, "target", "debug", "trellis-testkit")
      on_path || (File.executable?(local) && local) ||
        raise("trellis-testkit not found: run `cargo build -p testkit --bin trellis-testkit`")
    end
  end
end
