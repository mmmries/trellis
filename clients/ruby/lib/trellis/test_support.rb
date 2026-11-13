# frozen_string_literal: true

require_relative "pg"

module Trellis
  # Helpers for a test suite that loads fixtures or bulk data into tables
  # Trellis captures. Not loaded by `require "trellis/pg"`:
  #
  #   require "trellis/test_support"
  #
  # A fixture loader that disables triggers for the load (Rails'
  # `disable_referential_integrity`, `pg_restore --disable-triggers`, a
  # hand-written `ALTER TABLE ... DISABLE TRIGGER ALL`) also disables
  # Trellis's capture triggers, so the rows it writes never reach a target,
  # and `ENABLE TRIGGER ALL` leaves them in a mode the staging worker treats
  # as a disruption of capture. Trellis then pauses every definition that
  # reads the table (a CaptureFailure of kind :disruption on its status)
  # rather than let a test read targets that are silently wrong. A test
  # suite's answer is a RESUME after the load, which rebuilds the targets
  # from the tables as they are now:
  #
  #   Trellis::TestSupport.resume_capture_disruptions
  #
  # Include the module to call it as a method of the test or its class:
  #
  #   # test/test_helper.rb of a Rails app
  #   class ActiveSupport::TestCase
  #     include Trellis::TestSupport
  #     fixtures :all
  #     # Rails loads the fixtures in its own setup, before this block runs.
  #     setup { resume_capture_disruptions }
  #   end
  #
  # The call is cheap when nothing was disrupted (one catalog read per
  # captured table), so running it before every test is fine.
  #
  # Where Trellis is off (config.trellis.off_in), refresh_trellis_target fills
  # a target from its definition's own query, inside the test's transaction:
  #
  #   3.times { Event.create!(account: accounts(:one)) }
  #   refresh_trellis_target :event_counts
  #   assert_equal 3, accounts(:one).event_count.events_count
  module TestSupport
    # Raised when a RESUME is refused or a resumed definition pauses again.
    # `refused` maps each such target (as "schema.table") to the reason: the
    # engine's error for a refused RESUME, or the new CaptureFailure's
    # sentence. Those definitions stay paused. Every other disrupted
    # definition was resumed, and waited for, before this is raised.
    class ResumeRefused < ValidationError
      attr_reader :refused

      def initialize(refused)
        @refused = refused.freeze
        super("could not resume #{refused.size} definition(s) paused by a capture disruption " \
              "(they stay paused): " + refused.map { |target, why| "#{target}: #{why}" }.join("; "))
      end
    end

    POLL_SECONDS = 0.05
    # The schema Trellis keeps an instance's tables in, unless its connect
    # options name another.
    DEFAULT_SCHEMA = "trellis"
    # How a CaptureFailure's error starts while a resume is still under way.
    RESUMING = "resuming:"

    module_function

    # Resumes every definition of `instance` (the default instance if none
    # is given) that is paused because Trellis's capture of a table it reads
    # was changed outside Trellis, waits until each is :live again, then
    # waits until every write committed so far has reached its targets
    # (await_converged), so a test that follows reads exact targets. Returns
    # the resumed targets, as "schema.table" strings (empty when nothing was
    # disrupted).
    #
    # It runs a disruption check of its own first (Instance#detect_capture_disruptions),
    # so a disruption the staging worker has not seen yet is paused and
    # resumed here rather than a pass later.
    #
    # It resumes only the CaptureFailure kind :disruption. A definition an
    # operator paused (PAUSE TRANSFORM), one paused by a schema change, a
    # drain halt or a failing build stays paused. A definition paused because
    # an upstream one was resumes with its upstream.
    #
    # A RESUME the engine refuses (for example, the table a definition read
    # is missing, or a table define would now refuse) leaves that definition
    # paused. The others are still resumed and waited for; then
    # ResumeRefused, listing every refusal, is raised. A definition that
    # does not reach :live within `timeout_ms` raises Trellis::TimeoutError.
    #
    # The instance needs its staging worker and a drain worker running
    # somewhere (the usual test setup connects with `staging: true,
    # drain_threads: 1`): resuming rebuilds through them.
    def resume_capture_disruptions(instance = Trellis.default_instance, timeout_ms: 30_000)
      deadline = monotonic + (timeout_ms / 1000.0)
      instance.detect_capture_disruptions

      refused = {}
      resumed = []
      disrupted(instance).each do |target|
        instance.apply("RESUME TRANSFORM #{target}")
        resumed << target
      rescue ValidationError, NotFoundError, ConflictError => e
        refused[target] = e.message
      end

      resumed.each { |target| await_live(instance, target, deadline, refused) }
      converge(instance, deadline)
      raise ResumeRefused, refused unless refused.empty?

      resumed - refused.keys
    end

    # The targets of `instance`'s definitions that are paused with a
    # CaptureFailure of kind :disruption, oldest first.
    def disrupted(instance)
      instance.definitions.select { |definition| definition.status == :paused }.filter_map do |definition|
        status = instance.status(definition.target_table)
        definition.target_table if status&.capture_failure&.kind == :disruption
      end
    end

    # Fills `target` (a table name, "schema.table", or a model class) from
    # its definition, as a fresh build would compute it from the tables it
    # reads as they stand: `DELETE FROM` the target, then `INSERT INTO` it
    # the rows of the SELECT the engine renders from the definition
    # (Trellis::Native.target_select_sql). It runs on `connection`, the
    # test's own, so it sees the test's uncommitted rows and rolls back with
    # the test. It doesn't exercise capture, the drain or any pause. A table
    # the definition names without a schema resolves on the connection's
    # search_path. Returns the number of rows written.
    #
    # The definition is the one the schema load left in the restore queue
    # (`trellis.restore_queue`, or the schema config.trellis.connect or
    # config.trellis.instances names). A definition that reads another
    # transform's target refreshes that target first.
    #
    # Raises Trellis::NotFoundError when the target has no definition, and
    # Trellis::ValidationError when Trellis is on in this environment
    # (Trellis.enabled?), where the engine maintains the target and would
    # overwrite what this writes. `trellis_on: true` writes anyway, reading
    # the definition from the instance's catalog when there is no queue.
    def refresh_trellis_target(target, connection: active_record_connection, trellis_on: false)
      if Trellis.enabled? && !trellis_on
        raise ValidationError,
              "refresh_trellis_target fills a target where Trellis is off (config.trellis.off_in), and " \
              "Trellis is on here: the engine maintains #{target_name(target)}. Pass trellis_on: true " \
              "to write it anyway"
      end

      definitions = recorded_definitions(connection)
      refresh_target(connection, find_definition(definitions, target_name(target)), definitions, [])
    end

    # Refreshes each target `definition` reads that has a definition of its
    # own, then `definition`'s. `path` holds the targets being refreshed
    # below, which a stored definition can't read back.
    def refresh_target(connection, definition, definitions, path)
      target = definition.fetch(:target)
      definition.fetch(:reads).each do |table|
        upstream = definitions.find { |d| d.fetch(:target) == table }
        refresh_target(connection, upstream, definitions, path + [target]) if upstream && !path.include?(table)
      end

      schema, table = target.split(".", 2)
      quoted = "#{connection.quote_column_name(schema)}.#{connection.quote_column_name(table)}"
      columns = connection.select_values(
        "select a.attname::text from pg_catalog.pg_attribute a " \
        "where a.attrelid = pg_catalog.to_regclass(#{connection.quote(quoted)}) " \
        "and a.attnum > 0 and not a.attisdropped order by a.attnum"
      )
      raise NotFoundError, "the target #{target} of a recorded definition is no table" if columns.empty?

      select = Trellis::Native.target_select_sql(definition.fetch(:text), definition.fetch(:grammar_version),
                                                 columns)
      column_list = columns.map { |column| connection.quote_column_name(column) }.join(", ")
      connection.exec_update("delete from #{quoted}")
      connection.exec_update("insert into #{quoted} (#{column_list}) #{select}")
    end

    # Every definition recorded in the database, as hashes of :target and
    # :reads ("schema.table" names, as the dump resolved them), :text and
    # :grammar_version: a schema load's restore queue where there is one,
    # and otherwise an instance's catalog.
    def recorded_definitions(connection)
      trellis_schemas.flat_map do |schema|
        quoted = connection.quote_column_name(schema)
        if regclass?(connection, "#{quoted}.restore_queue")
          queued_definitions(connection, quoted)
        elsif regclass?(connection, "#{quoted}.transform_definitions")
          cataloged_definitions(connection, quoted)
        else
          []
        end
      end
    end

    def queued_definitions(connection, quoted_schema)
      queue = "#{quoted_schema}.restore_queue"
      # A dump with a definition that joins more than two tables has one more
      # column (trellis::dump::RESTORE_QUEUE).
      later = connection.select_value(
        "select count(*) from pg_catalog.pg_attribute where attrelid = '#{queue}'::regclass " \
        "and attname = 'later_join_tables' and not attisdropped"
      ).to_i.positive? ? "later_join_tables" : "'{}'::text[]"
      definitions(connection, "select target_table, source_table, " \
                              "array_remove(array[join_table], null) || #{later}, definition_text, " \
                              "grammar_version from #{queue}")
    end

    def cataloged_definitions(connection, quoted_schema)
      definitions(connection, "select d.target_table, d.source_table, " \
                              "array(select j.table_name from (select 1 as position, d.join_table as table_name " \
                              "where d.join_table is not null union all select f.position, f.table_name " \
                              "from #{quoted_schema}.definition_join_tables f where f.transform_id = d.id) j " \
                              "order by j.position), d.definition_text, d.grammar_version " \
                              "from #{quoted_schema}.transform_definitions d")
    end

    def definitions(connection, sql)
      connection.select_rows("select target_table, source_table || joined, definition_text, grammar_version " \
                             "from (#{sql}) as d(target_table, source_table, joined, definition_text, " \
                             "grammar_version)").map do |target, reads, text, version|
        { target:, reads: decode_text_array(connection, reads), text:, grammar_version: Integer(version) }
      end
    end

    # The definition of `name` ("table" or "schema.table") among `definitions`.
    def find_definition(definitions, name)
      matches = definitions.select do |definition|
        target = definition.fetch(:target)
        name.include?(".") ? target == name : target.split(".", 2).last == name
      end
      case matches.size
      when 1 then matches.first
      when 0
        raise NotFoundError,
              "no Trellis definition is recorded for the target #{name}: refresh_trellis_target reads " \
              "the definitions the schema load left in #{trellis_schemas.map { |s| "#{s}.restore_queue" }.join(', ')}"
      else
        raise ValidationError,
              "#{name} names more than one target (#{matches.map { |d| d.fetch(:target) }.join(', ')}): " \
              "qualify it as schema.table"
      end
    end

    # The schemas Trellis keeps its tables in: each configured instance's.
    def trellis_schemas
      options = []
      if defined?(Trellis::Railtie)
        options << Trellis::Railtie.connect_options
        options.concat(Trellis::Railtie.instance_options.values)
      end
      schemas = options.compact.map { |option| option.fetch(:schema, DEFAULT_SCHEMA).to_s }
      schemas.empty? ? [DEFAULT_SCHEMA] : schemas.uniq
    end

    def target_name(target)
      target.respond_to?(:table_name) ? target.table_name.to_s : target.to_s
    end

    def regclass?(connection, name)
      !connection.select_value("select pg_catalog.to_regclass(#{connection.quote(name)})::text").nil?
    end

    def decode_text_array(connection, value)
      return value if value.is_a?(Array)

      connection.select_values("select unnest(#{connection.quote(value)}::text[])")
    end

    def active_record_connection
      base = ::ActiveRecord::Base
      base.respond_to?(:lease_connection) ? base.lease_connection : base.connection
    end

    # Waits for `target` to be :live. A resumed definition that pauses
    # again (its rebuild failed) is recorded in `refused` instead of waited
    # for.
    def await_live(instance, target, deadline, refused)
      loop do
        status = instance.status(target)
        return if status&.status == :live

        if status&.status == :paused
          failure = status.capture_failure
          # Paused again by something else: its rebuild failed. A failure
          # whose error starts "resuming:" is the resume still under way (the
          # staging worker re-types Trellis's columns first, or waits for an
          # upstream definition), and is waited for. (Paused again by a
          # disruption is waited out: the next resume is the caller's, and
          # the timeout reports it.)
          if failure && failure.kind != :disruption && !failure.error.start_with?(RESUMING)
            refused[target] = failure.error
            return
          end
        end
        raise TimeoutError, "#{target} was not :live in time after its resume; last saw #{status.inspect}" if monotonic > deadline

        sleep POLL_SECONDS
      end
    end

    # Waits for everything committed so far to reach its targets, in as many
    # waits as the 30-second call limit needs.
    def converge(instance, deadline)
      token = instance.watermark_token
      loop do
        remaining_ms = ((deadline - monotonic) * 1000).to_i
        raise TimeoutError, "targets did not converge in time" if remaining_ms <= 0

        begin
          return instance.await_converged(token, timeout_ms: [remaining_ms, 30_000].min)
        rescue TimeoutError
          raise if monotonic >= deadline
        end
      end
    end

    def monotonic
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end
