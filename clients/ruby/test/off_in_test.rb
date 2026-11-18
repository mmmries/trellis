# frozen_string_literal: true

require "fileutils"
require "minitest/mock"
require "tmpdir"
require "test_helper"
require_relative "support/rails_app"
require "trellis/schema_dump"

# config.trellis.off_in: the environments Trellis's runtime is off in, so a
# test suite runs no worker. The predicate and the boot connect need no
# database; the schema load, the dump and the migration helpers, which run
# there as they do where it's on, run Rails' own tasks in-process over
# databases of the test cluster, the way schema_dump_test.rb does.
# (`parallelize` is simulated by the call Rails makes in each forked
# worker.)
class OffInTest < Minitest::Test
  include TrellisTestCase

  Tasks = ActiveRecord::Tasks::DatabaseTasks

  @serial = 0
  def self.next_id = (@serial += 1)

  def setup
    super
    @dir = Dir.mktmpdir("trellis-off-in")
    @admin = TestCluster.pg
    @id = self.class.next_id
    @databases = []
    @saved = { db_dir: Tasks.db_dir, schema_format: ActiveRecord.schema_format, env: Tasks.env,
               configurations: ActiveRecord::Base.configurations }
    Tasks.db_dir = @dir
    ActiveRecord.schema_format = :sql
    # Rails' load records itself in the app's own database.
    use_app_database
    # The suite's app has Trellis on everywhere (support/rails_app.rb); these
    # tests run the Railtie's default.
    TrellisTestApp.config.trellis.off_in = DEFAULT_OFF_IN
  end

  def teardown
    TrellisTestApp.config.trellis.connect = nil
    TrellisTestApp.config.trellis.off_in = []
    Tasks.db_dir = @saved.fetch(:db_dir)
    Tasks.env = @saved.fetch(:env)
    ActiveRecord.schema_format = @saved.fetch(:schema_format)
    ActiveRecord::Base.configurations = @saved.fetch(:configurations)
    @databases.each { |name| @admin.exec("drop database if exists #{@admin.escape_identifier(name)} with (force)") }
    @admin.close
    FileUtils.rm_rf(@dir)
    super
  end

  def test_trellis_is_off_in_the_test_environment_by_default
    assert_equal %w[test], DEFAULT_OFF_IN
    assert Trellis::Railtie.off_in?("test")
    refute Trellis::Railtie.enabled?("test")
    assert Trellis::Railtie.enabled?("development")
    assert Trellis::Railtie.enabled?("production")
  end

  # An app that runs Trellis in its tests leaves `test` out of off_in.
  def test_an_empty_off_in_turns_trellis_on_in_the_test_environment
    TrellisTestApp.config.trellis.off_in = []
    refute Trellis::Railtie.off_in?("test")
    in_environment("test") { assert Trellis.enabled? }
  end

  def test_off_in_names_environments_as_strings_or_symbols
    TrellisTestApp.config.trellis.off_in = [:test, "staging"]
    assert Trellis::Railtie.off_in?("test")
    assert Trellis::Railtie.off_in?(:staging)
    refute Trellis::Railtie.off_in?("development")
    assert Trellis::Railtie.enabled?("development")
    refute Trellis::Railtie.enabled?("test")
  end

  def test_enabled_follows_this_processs_rails_environment
    in_environment("development") { assert Trellis.enabled? }
    in_environment("test") { refute Trellis.enabled? }
  end

  def test_connect_connects_nothing_and_migrate_all_migrates_in_an_off_environment
    database = database("mall")
    TrellisTestApp.config.trellis.connect = { url: dsn(database) }
    in_environment("test") do
      _, warned = capture_io { assert_nil Trellis::Railtie.connect }
      assert_includes warned, "Trellis is off in the test environment (config.trellis.off_in)"
      assert_includes warned, "connects nothing"
      refute Trellis.connected?

      _, warned = capture_io { assert_nil Trellis::Railtie.migrate_all }
      assert_empty warned
      refute Trellis.connected?
      refute_nil to_regclass(database, "trellis.transform_definitions")
    end
    # The same app and config, in an environment it is on in.
    in_environment("development") do
      Trellis::Railtie.connect
      assert Trellis.connected?
    end
  end

  # Run in the off environment itself (`RAILS_ENV=test bin/rails
  # db:migrate`) or from an environment Trellis is on in (`db:prepare` in
  # development migrates the test database too), the helpers run against the
  # migrating database, whatever database config.trellis.connect names. No
  # worker builds the definition, and nothing installs a capture trigger.
  def test_the_migration_helpers_run_against_an_off_database
    klass = Class.new(ActiveRecord::Migration[ActiveRecord::Migration.current_version]) do
      include Trellis::Migration

      disable_ddl_transaction!

      def up
        create_table(:of_migrated) { |t| t.integer :price }
        define "TRANSFORM of_migrated_copy FROM of_migrated SELECT price AS price"
        define "TRANSFORM of_migrated_twice FROM of_migrated_copy SELECT price * 2 AS twice"
      end

      def down
        apply "PAUSE TRANSFORM of_migrated_twice"
        apply "DROP TRANSFORM of_migrated_twice"
        apply "PAUSE TRANSFORM of_migrated_copy"
        apply "DROP TRANSFORM of_migrated_copy"
        drop_table :of_migrated
      end
    end
    ActiveRecord::Migration.verbose = false
    elsewhere = database("migr_elsewhere")
    TrellisTestApp.config.trellis.connect = { url: dsn(elsewhere) }

    %w[test development].each do |running|
      database = database("migr_#{running}")
      ActiveRecord::Base.establish_connection(db_config(database))
      Tasks.env = running
      klass.new.exec_migration(ActiveRecord::Base.lease_connection, :up)

      # The chained define (of_migrated_twice reads of_migrated_copy) too.
      assert_equal "waiting_to_backfill,waiting_to_backfill",
                   scalar(database, "select string_agg(status, ',') from trellis.transform_definitions")
      assert_equal "0", capture_triggers(database)
      status = Trellis::Railtie.working_on(db_config(database)) { klass.new.status("of_migrated_copy") }
      refute_nil status

      klass.new.exec_migration(ActiveRecord::Base.lease_connection, :down)
      assert_nil to_regclass(database, "of_migrated_copy")
      assert_equal "0", scalar(database, "select count(*) from trellis.transform_definitions")
    ensure
      use_app_database
    end
    # The database config.trellis.connect names never got Trellis.
    assert_nil to_regclass(elsewhere, "trellis.transform_definitions")
  end

  # The load of an off environment's database restores every definition, as
  # a load where Trellis is on does, and no worker builds it: nothing
  # installs a capture trigger, and the restore queue is gone.
  def test_a_schema_load_into_an_off_database_restores_the_definitions_unbuilt
    source = source_database
    structure = dump(source)
    assert_includes structure, "CREATE TABLE public.of_totals_#{@id}"
    assert_includes structure, "-- Trellis definitions"

    off = database("off")
    # Not the loaded database: the restore connects to that one itself.
    TrellisTestApp.config.trellis.connect = { url: dsn(database("off_elsewhere")) }
    Tasks.env = "test"
    Tasks.load_schema(db_config(off), :sql, File.join(@dir, "structure.sql"))
    assert_includes tables(off, "public"), "of_totals_#{@id}"
    assert_equal "waiting_to_backfill",
                 scalar(off, "select status from trellis.transform_definitions " \
                             "where target_table = 'public.of_totals_#{@id}'")
    assert_equal "0", capture_triggers(off)
    assert_nil to_regclass(off, "trellis.restore_queue")
    # Rails 8.1's fixture check casts `conrelid::regclass` unqualified, and
    # fails on Trellis's own foreign keys unless Trellis's schema is on the
    # search path; 8.0's doesn't.
    if ActiveRecord.version >= Gem::Version.new("8.1")
      error = assert_raises(ActiveRecord::StatementInvalid) { check_foreign_keys(off) }
      assert_match(/does not exist/, error.message)
    end
    check_foreign_keys(off, schema_search_path: "public,trellis")
  end

  # `db:prepare` in development loads the test database with development's
  # config.trellis.connect, and `db:test:prepare` loads it from development
  # too: the restore connects to the test database.
  def test_a_load_of_an_off_environments_database_from_another_environment_restores_into_it
    dump(source_database)
    test = database("envtest")
    dev = database("envdev")
    TrellisTestApp.config.trellis.connect = { url: dsn(dev) }
    Tasks.env = "development"
    Tasks.load_schema(db_config(test), :sql, File.join(@dir, "structure.sql"))
    assert_equal "waiting_to_backfill", scalar(test, "select status from trellis.transform_definitions")
    assert_includes tables(test, "public"), "of_totals_#{@id}"
    assert_nil to_regclass(dev, "trellis.transform_definitions")
  end

  # `parallelize(workers: n)` makes a database per worker (`<name>_<i>`, or
  # `<name>-<i>` before Rails 8.1) and loads the schema into it in each forked
  # worker. config.trellis.connect names the one database, so with Trellis on
  # the load is refused; with it off, the restore connects to the worker's.
  def test_a_parallel_worker_database_loads_when_trellis_is_off
    dump(source_database)
    name = "of_app_#{@id}_#{Process.pid}"
    worker = "#{name}#{ActiveRecord.version >= Gem::Version.new("8.1") ? "_" : "-"}0"
    @databases << worker
    info = TestCluster.info
    base = { "adapter" => "postgresql", "host" => info.fetch("host"), "port" => info.fetch("port"),
             "username" => info.fetch("user"), "schema_search_path" => "public", "database" => name }
    ActiveRecord::Base.configurations = { "development" => { "primary" => base },
                                          "test" => { "primary" => base } }
    TrellisTestApp.config.trellis.connect = { url: dsn(name) }
    Tasks.env = "test" # `rails test`

    TrellisTestApp.config.trellis.off_in = [] # Trellis on in the test environment
    error = assert_raises(Trellis::ValidationError) do
      ActiveRecord::TestDatabases.create_and_load_schema(0, env_name: "test")
    end
    assert_includes error.message, "config.trellis.dump_database"
    # Rails recorded the load before the restore refused, and renamed the
    # config: start over.
    @admin.exec("drop database #{worker} with (force)")
    ActiveRecord::Base.configurations = { "development" => { "primary" => base },
                                          "test" => { "primary" => base } }

    TrellisTestApp.config.trellis.off_in = DEFAULT_OFF_IN
    ActiveRecord::TestDatabases.create_and_load_schema(0, env_name: "test")
    assert_includes tables(worker, "public"), "of_totals_#{@id}"
    assert_equal "waiting_to_backfill", scalar(worker, "select status from trellis.transform_definitions")
  end

  # The dump file is the environment Trellis is on in: a `db:prepare` there
  # also dumps the test database, which would only overwrite it.
  def test_a_dump_of_an_off_database_from_an_on_environment_is_skipped
    source = source_database
    TrellisTestApp.config.trellis.connect = { url: dsn(source) }
    Tasks.env = "development"
    path = File.join(@dir, "structure.sql")
    File.write(path, "-- the development dump\n")
    Tasks.dump_schema(db_config(source))
    assert_equal "-- the development dump\n", File.read(path)

    # From the off environment itself, it dumps as where Trellis is on,
    # reading the definitions from the dumped database.
    TrellisTestApp.config.trellis.connect = { url: dsn(database("dump_elsewhere")) }
    Tasks.env = "test"
    config = db_config(source)
    Tasks.with_temporary_connection(config) { Tasks.dump_schema(config) }
    structure = File.read(path)
    assert_includes structure, "CREATE TABLE public.of_totals_#{@id}"
    assert_includes structure, "-- Trellis definitions"
    refute_includes structure, "CREATE TABLE trellis."
  end

  private

  def in_environment(name, &)
    Rails.stub(:env, ActiveSupport::StringInquirer.new(name), &)
  end

  # A database with a foreign key and a 1-1 transform over it, defined by Trellis.
  def source_database
    @source ||= begin
      name = database("src")
      pg = PG.connect(dsn(name))
      pg.exec("create table of_customers_#{@id} (id bigint primary key)")
      pg.exec("create table of_orders_#{@id} (id bigint primary key, customer_id bigint " \
              "references of_customers_#{@id} (id), price bigint)")
      pg.close
      TrellisTestApp.config.trellis.connect = { url: dsn(name) }
      Trellis::Railtie.with_handle do |trellis|
        trellis.define("TRANSFORM of_totals_#{@id} FROM of_orders_#{@id} SELECT price AS price")
      end
      TrellisTestApp.config.trellis.connect = nil
      name
    end
  end

  # Dumps `database` as structure.sql, with Trellis on (these databases are
  # named "test"), and returns the file.
  def dump(database)
    TrellisTestApp.config.trellis.connect = { url: dsn(database) }
    TrellisTestApp.config.trellis.off_in = []
    # On the database's own pool, as Rails' callers dump: 8.0's dump_schema
    # reads schema_migrations from the current one.
    config = db_config(database)
    Tasks.with_temporary_connection(config) { Tasks.dump_schema(config) }
    File.read(File.join(@dir, "structure.sql"))
  ensure
    TrellisTestApp.config.trellis.connect = nil
    TrellisTestApp.config.trellis.off_in = DEFAULT_OFF_IN
  end

  def check_foreign_keys(database, **options)
    ActiveRecord::Base.establish_connection(db_config(database, **options))
    ActiveRecord::Base.lease_connection.check_all_foreign_keys_valid!
  ensure
    use_app_database
  end

  def use_app_database
    ActiveRecord::Base.establish_connection(db_config(TestCluster.info.fetch("dbname")))
  end

  def database(role)
    name = "of_#{role}_#{@id}_#{Process.pid}"
    @admin.exec("create database #{name}")
    @databases << name
    name
  end

  def dsn(database)
    TestCluster.dsn.sub(/dbname=\S+/, "dbname=#{database}")
  end

  def db_config(database, **options)
    info = TestCluster.info
    hash = { adapter: "postgresql", database:, host: info.fetch("host"), port: info.fetch("port"),
             username: info.fetch("user"), schema_search_path: "public", **options }
    ActiveRecord::DatabaseConfigurations::HashConfig.new("test", "primary", hash)
  end

  # The triggers on tables outside Trellis's own schema, as a count: a
  # capture trigger is the only kind Trellis installs there.
  def capture_triggers(database)
    scalar(database, "select count(*) from pg_trigger t join pg_class c on c.oid = t.tgrelid " \
                     "join pg_namespace n on n.oid = c.relnamespace where not t.tgisinternal and n.nspname <> 'trellis'")
  end

  def scalar(database, sql)
    pg = PG.connect(dsn(database))
    pg.exec(sql).getvalue(0, 0)
  ensure
    pg&.close
  end

  def to_regclass(database, name)
    pg = PG.connect(dsn(database))
    pg.exec_params("select to_regclass($1)", [name]).getvalue(0, 0)
  ensure
    pg&.close
  end

  def tables(database, schema)
    pg = PG.connect(dsn(database))
    pg.exec_params("select tablename from pg_tables where schemaname = $1", [schema]).map { |row| row.fetch("tablename") }
  ensure
    pg&.close
  end
end
