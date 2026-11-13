# frozen_string_literal: true

# A minimal Rails app, booted in a process of its own by rails_test.rb, one
# scenario per run: ARGV[0] names it, and TRELLIS_TEST_DSN is the cluster.
# Prints one JSON line of what it saw.
$LOAD_PATH.unshift File.expand_path("../../lib", __dir__)
require "json"
require "stringio"
require "tmpdir"

# An app without ActiveRecord: the bundle has the gem, so this shadows it
# with a file whose require fails the way it would if the app's Gemfile
# didn't list it.
WITHOUT_ACTIVE_RECORD = ARGV.fetch(0) == "rake_without_active_record"
if WITHOUT_ACTIVE_RECORD
  shadow = Dir.mktmpdir("no-active-record")
  File.write(File.join(shadow, "active_record.rb"),
             'raise LoadError, "cannot load such file -- active_record"')
  $LOAD_PATH.unshift shadow
end

# ActiveRecord's own connection, never opened: with no config/database.yml,
# loading ActiveRecord::Base needs one configured.
ENV["DATABASE_URL"] = "postgresql://localhost/unused"
require "logger"
require "rails"
require "active_record/railtie" unless WITHOUT_ACTIVE_RECORD
# After Rails, as Bundler.require in config/application.rb loads it.
require "trellis/pg"

class BootApp < Rails::Application
  config.root = __dir__
  config.eager_load = false
  config.logger = Logger.new(nil)
  config.secret_key_base = "test"
  # Without load_defaults, Rails 8.0 warns while booting about 8.1's `to_time`
  # behavior (8.1 has it and deprecates the setting).
  config.active_support.to_time_preserves_timezone = :zone if Rails.gem_version < Gem::Version.new("8.1")
  config.trellis.connect = { "url" => ENV.fetch("TRELLIS_TEST_DSN"), "target_schema" => "boot_targets" }
end

seen = {}
case ARGV.fetch(0)
when "boot"
  BootApp.initialize!
  seen[:connected] = Trellis.connected?
  seen[:target_schema] = Trellis.config.target_schema
  # The Railtie requires the migration helpers and Trellis::Target as
  # ActiveRecord loads.
  ActiveRecord::Base
  seen[:migration_after_active_record] = defined?(Trellis::Migration) && defined?(Trellis::Target) ? true : false
when "logger", "logger_level", "logger_off"
  # The engine's log lines go to Rails.logger, unless the app opts out.
  io = StringIO.new
  BootApp.config.logger = Logger.new(io)
  BootApp.config.trellis.log_level = :warn if ARGV.fetch(0) == "logger_level"
  BootApp.config.trellis.log_bridge = false if ARGV.fetch(0) == "logger_off"
  BootApp.initialize!
  Trellis.apply("DROP TRANSFORM no_such_transform")
  bridge = Trellis::LogBridge.current
  bridge&.drain_once
  seen[:connected] = Trellis.connected?
  seen[:bridge] = !bridge.nil?
  seen[:rails_logger] = bridge&.logger.equal?(Rails.logger)
  seen[:logged] = io.string.lines.grep(/drop is a no-op.*trellis_instance=\S+\/\S+ transform=no_such_transform/).size
when "logger_off_in", "logger_default_off_in"
  # Trellis is off in this environment (named, or the test default): no
  # bridge starts.
  BootApp.config.trellis.off_in = [Rails.env] if ARGV.fetch(0) == "logger_off_in"
  BootApp.initialize!
  seen[:bridge] = !Trellis::LogBridge.current.nil?
  seen[:connected] = Trellis.connected?
when "unset"
  BootApp.config.trellis.connect = nil
  BootApp.initialize!
  seen[:connected] = Trellis.connected?
when "connect_on_boot_false"
  BootApp.config.trellis.connect_on_boot = false
  BootApp.initialize!
  seen[:connected] = Trellis.connected?
  Trellis::Railtie.connect
  seen[:connected_after_connect] = Trellis.connected?
when "rake", "rake_without_active_record"
  # What `rails db:migrate` does: a rake run loads the tasks and runs one
  # that needs the app, whose `environment` prerequisite initializes it (by
  # requiring config/environment.rb, which this app hasn't got).
  require "rake"
  Rake.with_application do |rake|
    rake.init("rails", ["probe"])
    BootApp.load_tasks
    Rake::Task["environment"].enhance { BootApp.initialize! }
    Rake::Task.define_task(probe: :environment) do
      seen[:connected] = Trellis.connected?
      Rake::Task["trellis:migrate"].invoke
      seen[:connected_after_migrate] = Trellis.connected?
      # The scenario's premise: nothing loaded ActiveRecord.
      seen[:active_record] = defined?(::ActiveRecord) ? true : false if WITHOUT_ACTIVE_RECORD
    end
    rake.top_level
  end
when "tasks_loaded_before_boot"
  # What `rails test` does: it loads the tasks to run test:prepare, and
  # only then, outside any task, boots the app.
  require "rake"
  Rake.with_application do |rake|
    rake.init("rails", ["test:prepare"])
    BootApp.load_tasks
    Rake::Task.define_task("test:prepare")
    rake.top_level
  end
  BootApp.initialize!
  seen[:connected] = Trellis.connected?
when "puma_hooks"
  # A preloading server: the app boots in the parent, which shuts its handle
  # down before forking (before_fork), and each worker connects its own
  # (before_worker_boot).
  BootApp.initialize!
  Trellis.shutdown
  reader, writer = IO.pipe
  pid = fork do
    reader.close
    Trellis::Railtie.connect
    writer.puts JSON.generate(connected: Trellis.connected?, status: Trellis.status("no_such_target"))
    Trellis.shutdown
    exit!(0)
  rescue StandardError => e
    writer.puts JSON.generate(error: e.class.name)
    exit!(1)
  end
  writer.close
  seen[:child] = reader.wait_readable(60) ? JSON.parse(reader.gets || "null") : "no report within 60s"
  Process.kill(:KILL, pid) if seen[:child].is_a?(String)
  Process.waitpid(pid)
  seen[:parent_connected] = Trellis.connected?
when "named_instances"
  # config.trellis.instances connect with the default instance at boot.
  BootApp.config.trellis.instances = { reports: { url: ENV.fetch("TRELLIS_TEST_DSN"), schema: "boot_reports" } }
  BootApp.initialize!
  seen[:connected] = Trellis.connected?
  seen[:named_schema] = Trellis::Railtie.named_instance(:reports).config.schema
  seen[:named_class] = Trellis::Railtie.named_instance("reports").class.name
  begin
    Trellis::Railtie.named_instance(:nope)
  rescue Trellis::ValidationError => e
    seen[:unknown_instance] = e.message
  end
  reports = Trellis::Railtie.named_instance(:reports)
  Trellis::Instance.shutdown_all
  seen[:connected_after_shutdown_all] = [Trellis.connected?, reports.connected?]
when "named_instances_only"
  BootApp.config.trellis.connect = nil
  BootApp.config.trellis.instances = { reports: { url: ENV.fetch("TRELLIS_TEST_DSN"), schema: "boot_reports" } }
  BootApp.initialize!
  seen[:connected] = Trellis.connected?
  reports = Trellis::Railtie.named_instance(:reports)
  seen[:named_connected] = reports.connected?
  # A second connect raises, as Trellis.connect does for the default
  # instance, rather than opening a second handle beside the first.
  begin
    Trellis::Railtie.connect
  rescue Trellis::ValidationError => e
    seen[:second_connect] = e.message
  end
  seen[:same_instance] = Trellis::Railtie.named_instance(:reports).equal?(reports)
  seen[:instances_held] = Trellis::Instance.connected.size
when "named_instances_rake"
  # `trellis:migrate` migrates every configured instance on handles of its
  # own, and leaves none connected.
  BootApp.config.trellis.instances = { reports: { url: ENV.fetch("TRELLIS_TEST_DSN"), schema: "boot_reports" } }
  require "rake"
  Rake.with_application do |rake|
    rake.init("rails", ["probe"])
    BootApp.load_tasks
    Rake::Task["environment"].enhance { BootApp.initialize! }
    Rake::Task.define_task(probe: :environment) do
      Rake::Task["trellis:migrate"].invoke
      seen[:connected_after_migrate] = Trellis.connected?
      seen[:named_after_migrate] = Trellis::Railtie.instance_options.keys
      seen[:with_handle] = Trellis::Railtie.with_handle(:reports) { |trellis| trellis.config.schema }
      seen[:with_handle_unknown] = begin
        Trellis::Railtie.with_handle(:nope) { nil }
      rescue Trellis::ValidationError => e
        e.message
      end
    end
    rake.top_level
  end
when "failed_connect_rolls_back"
  # A named instance that can't connect doesn't leave the default one (or an
  # earlier named one) connected.
  BootApp.config.trellis.connect_on_boot = false
  BootApp.config.trellis.instances = {
    first: { url: ENV.fetch("TRELLIS_TEST_DSN"), schema: "boot_reports" },
    broken: { url: ENV.fetch("TRELLIS_TEST_DSN"), schema: "boot_reports", worker_threads: 0 }
  }
  BootApp.initialize!
  begin
    Trellis::Railtie.connect
  rescue Trellis::ValidationError => e
    seen[:error] = e.message
  end
  seen[:connected] = Trellis.connected?
  begin
    Trellis::Railtie.named_instance(:first)
    seen[:first_connected] = true
  rescue Trellis::ValidationError
    seen[:first_connected] = false
  end
when "puma_hooks_named"
  # The same hooks with a named instance: shutdown_all in the parent, and
  # each worker connects every instance of its own.
  BootApp.config.trellis.instances = { reports: { url: ENV.fetch("TRELLIS_TEST_DSN"), schema: "boot_reports" } }
  BootApp.initialize!
  reports = Trellis::Railtie.named_instance(:reports)
  reports.migrate
  Trellis::Instance.shutdown_all
  reader, writer = IO.pipe
  pid = fork do
    reader.close
    Trellis::Railtie.connect
    writer.puts JSON.generate(connected: Trellis.connected?,
                              named: Trellis::Railtie.named_instance(:reports).status("no_such_target"))
    Trellis::Instance.shutdown_all
    exit!(0)
  rescue StandardError => e
    writer.puts JSON.generate(error: e.class.name)
    exit!(1)
  end
  writer.close
  seen[:child] = reader.wait_readable(60) ? JSON.parse(reader.gets || "null") : "no report within 60s"
  Process.kill(:KILL, pid) if seen[:child].is_a?(String)
  Process.waitpid(pid)
  seen[:parent_connected] = [Trellis.connected?, reports.connected?]
when "off_in", "off_in_elsewhere", "default_off_in", "opt_in_off_in"
  # config.trellis.off_in: Trellis is off in this app's own environment, in
  # another one only, in the default (the test environment), or in none.
  case ARGV.fetch(0)
  when "off_in" then BootApp.config.trellis.off_in = [Rails.env]
  when "off_in_elsewhere" then BootApp.config.trellis.off_in = ["elsewhere"]
  when "opt_in_off_in" then BootApp.config.trellis.off_in = []
  end
  stderr = $stderr
  $stderr = StringIO.new
  begin
    BootApp.initialize!
    seen[:boot_warning] = $stderr.string
    $stderr = StringIO.new
    seen[:enabled] = Trellis.enabled?
    seen[:connected] = Trellis.connected?
    Trellis::Railtie.connect
    seen[:warning] = $stderr.string
  rescue Trellis::ValidationError => e
    seen[:warning] = e.message
  ensure
    $stderr = stderr
  end
  seen[:connected_after_connect] = Trellis.connected?
when "off_in_rake"
  # `rails trellis:migrate` in an off environment migrates nothing.
  BootApp.config.trellis.off_in = [Rails.env]
  require "rake"
  Rake.with_application do |rake|
    rake.init("rails", ["probe"])
    BootApp.load_tasks
    Rake::Task["environment"].enhance { BootApp.initialize! }
    Rake::Task.define_task(probe: :environment) do
      stderr = $stderr
      $stderr = StringIO.new
      begin
        Rake::Task["trellis:migrate"].invoke
        seen[:warning] = $stderr.string
      ensure
        $stderr = stderr
      end
    end
    rake.top_level
  end
when "release_check"
  # `rails trellis:release_check BASE=<ref>`, in an app that is a git
  # repository of its own, whose second database keeps its migrations in a
  # path of its own. The task has no `environment` prerequisite, so nothing
  # here initializes the app.
  require "rake"
  require "open3"
  require "stringio"
  root = Dir.mktmpdir("release-check-app")
  git = lambda do |*args|
    out, status = Open3.capture2e("git", *args, chdir: root)
    abort "git #{args.first} failed: #{out}" unless status.success?
  end
  lock = lambda do |version|
    File.write(File.join(root, "Gemfile.lock"), "GEM\n  specs:\n    trellis-pg (#{version}-x86_64-linux)\n")
  end
  FileUtils.mkdir_p([File.join(root, "config"), File.join(root, "db/reports_migrate")])
  File.write(File.join(root, "Gemfile"), "")
  File.write(File.join(root, "config/database.yml"), <<~YAML)
    production:
      primary:
        adapter: postgresql
      reports:
        adapter: postgresql
        migrations_paths: db/reports_migrate
  YAML
  lock.call("0.2.0")
  git.call("init", "--quiet", "--initial-branch=main")
  git.call("add", "--all")
  git.call("-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "--quiet", "--message", "deployed")
  File.write(File.join(root, "db/reports_migrate/20260201000000_define_order_totals.rb"),
             "class DefineOrderTotals < ActiveRecord::Migration[8.1]\n  include Trellis::Migration\nend\n")
  # As `bundle exec` has them in a real app: the app's own Gemfile and
  # lockfile, not this suite's.
  ENV["BUNDLE_GEMFILE"] = File.join(root, "Gemfile")
  ENV["BUNDLE_LOCKFILE"] = File.join(root, "Gemfile.lock")
  BootApp.config.root = root

  Rake.with_application do |rake|
    rake.init("rails", ["trellis:release_check"])
    BootApp.load_tasks
    task = Rake::Task["trellis:release_check"]
    run = lambda do |base|
      ENV["BASE"] = base
      task.reenable
      stdout = $stdout
      stderr = $stderr
      $stdout = StringIO.new
      $stderr = StringIO.new
      status = begin
        task.invoke
        0
      rescue SystemExit => e
        e.status
      end
      { status: status, stdout: $stdout.string, stderr: $stderr.string }
    ensure
      $stdout = stdout
      $stderr = stderr
    end
    seen[:no_base] = run.call(nil)
    seen[:unchanged_version] = run.call("main")
    lock.call("0.3.0")
    seen[:changed_version] = run.call("main")
  end
  seen[:initialized] = BootApp.initialized?
else
  abort "unknown scenario: #{ARGV[0]}"
end
puts JSON.generate(seen)
Trellis::Instance.shutdown_all
