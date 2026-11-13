# frozen_string_literal: true

require "rails/railtie"

module Trellis
  # Connects this process's handle as a Rails app boots, from the app's
  # `config.trellis`. `require "trellis/pg"` loads it when Rails is loaded
  # first, which is what Bundler.require in config/application.rb does.
  #
  #   # config/application.rb, or config/environments/*.rb
  #   config.trellis.connect = {
  #     url: ENV.fetch("DATABASE_URL"),
  #     staging: ENV["TRELLIS_WORKER"] == "1",
  #     drain_threads: ENV["TRELLIS_WORKER"] == "1" ? 2 : 0
  #   }
  #
  # config.trellis.connect takes Trellis.connect's options, and is the one
  # place the Rails integration reads the default instance's from: the boot
  # connect, the migration helpers (Trellis::Migration) and `rails
  # trellis:migrate` all use it. Leave it nil (the default) and the Railtie
  # does nothing for the default instance.
  #
  # An app that runs more instances (a second catalog schema, or another
  # database) names the extras in config.trellis.instances, each with
  # Trellis::Instance.connect's options. The Railtie connects them with the
  # default instance, `rails trellis:migrate` migrates them, and
  # Trellis::Railtie.named_instance(:reports) returns one:
  #
  #   config.trellis.instances = {
  #     reports: { url: ENV.fetch("REPORTS_DATABASE_URL"), schema: "reports" }
  #   }
  #
  # With `config.active_record.schema_format = :sql`, the same config puts
  # the definitions in `structure.sql` (Trellis::SchemaDump). The database
  # they dump into is config.trellis.dump_database, "primary" by default.
  #
  # `config.trellis.off_in` names the environments Trellis is off in. The
  # default is `%w[test]`: a test suite's transactions never commit, so it
  # can't see a target change. `off_in = []` turns Trellis on in tests. It
  # is read from any environment, so a `db:prepare` or `db:test:prepare` run
  # from development leaves Trellis out of the test database too. There,
  # nothing connects at boot, Railtie.connect and `trellis:migrate` do
  # nothing, the schema load and dump hooks leave the target tables as plain
  # ones, and the migration helpers refuse (or skip, migrating that database
  # from an environment Trellis is on in).
  # Trellis.enabled? is the predicate; "Testing" in the README has the rest.
  #
  # When either is set, the app's handles connect in an after_initialize hook,
  # except in these cases:
  #
  # - An app booted by a rake task (`rails db:migrate`, `rails db:create`,
  #   ...): like Ecto, which migrates without starting the app, those get no
  #   boot handle. The database may not exist yet, and a migration run must
  #   not start the worker's background work. The migration helpers connect
  #   a handle of their own; a task of your own that calls Trellis calls
  #   Trellis::Railtie.connect. It's the task that counts, not the command:
  #   `rails test` loads the tasks to run test:prepare, then boots the app
  #   outside any task, so it gets its handle (but `bin/rails db:test:prepare
  #   test` boots it inside the first task, so it doesn't).
  # - Trellis is off in this environment (config.trellis.off_in).
  # - config.trellis.connect_on_boot = false, for a server that forks with
  #   no hook to shut the handle down first (see below).
  #
  # The engine's log lines go to Rails.logger (Trellis::LogBridge), at
  # Rails.logger's level or config.trellis.log_level (:debug, :info, :warn,
  # :error), unless config.trellis.log_bridge = false or Trellis is off in
  # this environment (nothing connects, so there are none). Trellis.logger =
  # in a config/initializers file sends them elsewhere.
  #
  # A handle doesn't survive fork. A preforking server that
  # loads the app before forking (Puma's preload_app!, on by default with
  # more than one worker) must shut the boot handle down before it forks,
  # and connect one in each worker:
  #
  #   # config/puma.rb
  #   before_fork { Trellis::Instance.shutdown_all }
  #   before_worker_boot { Trellis::Railtie.connect } # on_worker_boot before Puma 7
  #
  # With Puma's fork_worker, worker 0 forks the others, so it needs
  # before_worker_fork and after_worker_fork hooks as well: see "Forking
  # servers" in the README.
  class Railtie < ::Rails::Railtie
    config.trellis = ActiveSupport::OrderedOptions.new
    config.trellis.connect = nil
    config.trellis.instances = {}
    config.trellis.connect_on_boot = true
    config.trellis.dump_database = "primary"
    config.trellis.off_in = %w[test]
    config.trellis.log_bridge = true
    config.trellis.log_level = nil

    rake_tasks do
      namespace :trellis do
        desc "Create or upgrade Trellis's own tables, for config.trellis.connect and config.trellis.instances"
        task migrate: :environment do
          Trellis::Railtie.migrate_all
        end

        # No :environment: the check reads git and files, so it runs in a CI
        # job with no database and none of the app's initializers.
        desc "Fail if this release changes trellis-pg's version and adds a Trellis migration (BASE=<git ref>)"
        task :release_check do
          Trellis::Railtie.release_check(ENV.fetch("BASE", nil))
        end
      end
    end

    initializer "trellis.logger" do |app|
      settings = app.config.trellis
      next unless settings.log_bridge && ::Rails.logger && Trellis::Railtie.enabled?

      Trellis::LogBridge.install(::Rails.logger, level: settings.log_level)
    end

    initializer "trellis.migration" do
      ActiveSupport.on_load(:active_record) do
        require "trellis/migration"
        require "trellis/schema_dump"
        require "trellis/target"
      end
    end

    config.after_initialize { Trellis::Railtie.instance.send(:boot) }

    # The instances Railtie.connect opened for config.trellis.instances,
    # by name.
    @instances = {}

    class << self
      # Connects this process's instances from the config: the default
      # instance from config.trellis.connect (when set), and each of
      # config.trellis.instances. In a forked worker (Puma's
      # before_worker_boot, Unicorn's after_fork, Passenger's
      # starting_worker_process), or a rake task of your own. Warns and
      # connects nothing in an environment Trellis is off in. Raises
      # ValidationError if neither is set, or if a named instance is already
      # connected (as Trellis.connect does for the default one), and whatever
      # Trellis.connect and Trellis::Instance.connect raise; the instances
      # this call had connected are shut down first, so a failure leaves
      # nothing half connected.
      def connect
        return skipped("Trellis::Railtie.connect connects nothing") unless enabled?

        default = connect_options
        named = instance_options
        if default.nil? && named.empty?
          raise ValidationError,
                "config.trellis.connect is not set: give it Trellis.connect's options"
        end
        named.each_key do |name|
          next unless @instances[name]&.connected?

          raise ValidationError,
                "the instance named #{name.inspect} is already connected: call " \
                "Trellis::Instance.shutdown_all first"
        end

        opened = []
        begin
          if default
            Trellis.connect(**default)
            opened << -> { Trellis.shutdown }
          end
          named.each do |name, options|
            instance = Trellis::Instance.connect(**options)
            @instances[name] = instance
            opened << -> { @instances.delete(name)&.shutdown }
          end
        rescue Exception # rubocop:disable Lint/RescueException
          opened.reverse_each { |undo| ignoring_errors(&undo) }
          raise
        end
        nil
      end

      # The instance config.trellis.instances names `name`, connected by
      # Railtie.connect. Raises ValidationError if there is none.
      def named_instance(name)
        instance = @instances[name.to_sym]
        return instance if instance&.connected?

        raise ValidationError,
              "no connected instance named #{name.inspect}: name it in config.trellis.instances " \
              "and call Trellis::Railtie.connect"
      end

      # Runs the block with an instance, running its migrate first, and
      # returns the block's value. With no name, the default instance,
      # yielded as well as reachable as the Trellis module; with a name, that
      # instance of config.trellis.instances. If the instance isn't connected
      # (a rake task gets no boot handle), connects one for the length of
      # the block from its config, with staging: false and drain_threads: 0 so
      # it starts no background work, and shuts it down after. What `rails
      # trellis:migrate` and Trellis::Migration's helpers run on, for a script
      # or rake task of your own that needs Trellis the way a migration does.
      # Needs no ActiveRecord.
      #
      # Raises what the block raises. If the shutdown fails too, that's a
      # warning, so it doesn't hide the block's error; after a block that
      # returned, the shutdown's error is raised. Raises ValidationError if
      # the instance isn't connected and its options aren't configured.
      def with_handle(name = nil)
        instance = name ? @instances[name.to_sym] : (Trellis.default_instance if Trellis.connected?)
        if instance&.connected?
          instance.migrate
          return yield(instance)
        end

        options = name ? named_options(name) : required_connect_options
        options = options.merge(staging: false, drain_threads: 0)
        instance = name ? Trellis::Instance.connect(**options) : connect_default(options)
        primary = nil
        begin
          instance.migrate
          yield instance
        rescue Exception => e # any exception, Interrupt too: it's re-raised
          primary = e
          raise
        ensure
          shutdown_after(name, instance, primary)
        end
      end

      # Runs Trellis.migrate on the default instance (when
      # config.trellis.connect is set) and on each of
      # config.trellis.instances, each as with_handle does. Raises
      # ValidationError if neither is set.
      def migrate_all
        return skipped("trellis:migrate migrates nothing") unless enabled?

        default = connect_options
        named = instance_options
        if default.nil? && named.empty?
          raise ValidationError,
                "config.trellis.connect is not set: give it Trellis.connect's options"
        end

        with_handle { nil } if default
        named.each_key { |name| with_handle(name) { nil } }
        nil
      end

      # Runs Trellis::ReleaseCheck for this app against the git ref `base`,
      # over every migrations path the app has (db/migrate, and each
      # database's migrations_paths): prints its verdict, and exits 1 when
      # the release both changes trellis-pg's version and adds a Trellis
      # migration, or when the check can't run. What `rails
      # trellis:release_check BASE=<ref>` runs.
      def release_check(base)
        require "trellis/release_check"
        result = ReleaseCheck.call(root: ::Rails.root, base: base,
                                   migrations_paths: migrations_paths, lockfile: lockfile)
        abort result.message unless result.ok?
        puts result.message
      rescue ReleaseCheck::Error => e
        abort "trellis:release_check: #{e.message}"
      end

      # Whether Trellis is off in `environment`, however it is spelled.
      def off_in?(environment)
        off_in.include?(environment.to_s)
      end

      # Whether Trellis is on in `environment` (this process's, by default):
      # false when config.trellis.off_in names it. Trellis.enabled? is the
      # shortcut for code that doesn't need the Railtie.
      def enabled?(environment = ::Rails.env)
        !off_in?(environment)
      end

      # A sentence saying Trellis is off in `environment`, and `what` that
      # means was not done.
      def off_message(what, environment = ::Rails.env)
        "Trellis is off in the #{environment} environment (config.trellis.off_in): #{what}"
      end

      # config.trellis.connect, as Trellis.connect's keyword options, or nil
      # when it isn't set (or there's no Rails app).
      def connect_options
        app = ::Rails.respond_to?(:application) && ::Rails.application or return nil
        options = app.config.trellis.connect or return nil

        keyword_options("config.trellis.connect", options)
      end

      # config.trellis.instances, as each name's Trellis::Instance.connect
      # keyword options; empty when it isn't set (or there's no Rails app).
      def instance_options
        app = ::Rails.respond_to?(:application) && ::Rails.application or return {}
        instances = app.config.trellis.instances
        return {} if instances.nil? || instances.empty?

        unless instances.respond_to?(:to_h)
          raise ValidationError,
                "config.trellis.instances must be a Hash of names to Trellis::Instance.connect's " \
                "options, got: #{instances.inspect}"
        end

        instances.to_h.to_h do |name, options|
          [name.to_sym, keyword_options("config.trellis.instances[#{name.inspect}]", options)]
        end
      end

      private

      # The environments config.trellis.off_in names, as strings; empty when
      # it isn't set (or there's no Rails app).
      def off_in
        app = ::Rails.respond_to?(:application) && ::Rails.application or return []

        Array(app.config.trellis.off_in).map(&:to_s)
      end

      # Where the app keeps migrations: db/migrate, and the migrations_paths
      # of every database in config/database.yml, read without connecting.
      # An app whose database config can't be read here (no database.yml, a
      # missing environment variable) is checked on db/migrate alone.
      def migrations_paths
        app = ::Rails.application
        collect = lambda do |node|
          next [] unless node.is_a?(Hash)

          node.flat_map { |key, value| key.to_s == "migrations_paths" ? Array(value) : collect.call(value) }
        end
        configured = begin
          collect.call(app.config.database_configuration)
        rescue StandardError
          []
        end
        (app.paths["db/migrate"].to_a + configured).map(&:to_s).uniq
      end

      # The lockfile Bundler resolved the app with (it follows BUNDLE_GEMFILE,
      # and a gems.rb has a gems.locked), or Gemfile.lock in an app that
      # isn't running under Bundler.
      def lockfile
        defined?(::Bundler) ? ::Bundler.default_lockfile : "Gemfile.lock"
      rescue StandardError
        "Gemfile.lock"
      end

      def keyword_options(what, options)
        unless options.respond_to?(:to_h)
          raise ValidationError,
                "#{what} must be a Hash of Trellis.connect's options, got: #{options.inspect}"
        end

        options.to_h.transform_keys(&:to_sym)
      end

      def required_connect_options
        connect_options or
          raise ValidationError, "config.trellis.connect is not set: give it Trellis.connect's options"
      end

      def named_options(name)
        instance_options.fetch(name.to_sym) do
          raise ValidationError, "config.trellis.instances has no #{name.inspect}"
        end
      end

      def connect_default(options)
        Trellis.connect(**options)
        Trellis.default_instance
      end

      def skipped(what)
        warn "trellis: #{off_message(what)}"
        nil
      end

      def ignoring_errors
        yield
      rescue StandardError => e
        warn "trellis: shutting down after a failed connect failed: #{e.class}: #{e.message}"
      end

      # Shuts with_handle's own instance down. With a primary error on its
      # way out, a failed shutdown is only a warning: raising would replace
      # it.
      def shutdown_after(name, instance, primary)
        name ? instance.shutdown : Trellis.shutdown
      rescue StandardError => e
        raise unless primary

        warn "trellis: shutting down after #{primary.class} failed: #{e.class}: #{e.message}"
      end
    end

    private

    def boot
      return if rake_running? || !self.class.enabled?

      settings = ::Rails.application.config.trellis
      return unless settings.connect_on_boot && (settings.connect || !self.class.instance_options.empty?)

      self.class.connect
    end

    # Whether the app is booting inside a rake run (`rails db:migrate`,
    # `rake db:create`), not just in a process that loaded the tasks:
    # `rails test` loads them to run test:prepare, and only then, outside
    # any task, boots the app.
    def rake_running?
      defined?(::Rake.application) && ::Rake.application.top_level_tasks.any?
    end
  end
end
