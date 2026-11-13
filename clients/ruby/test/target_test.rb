# frozen_string_literal: true

require "test_helper"
require_relative "support/rails_app"
require "trellis/target"

# Trellis::Target: a model over a target refuses every ActiveRecord write
# where Trellis is on, and writes like any model where it is off
# (config.trellis.off_in). Each write path has a test of each half. The
# targets are real ones, defined by Trellis in a database of this file's
# own; no worker runs, so the rows each test starts from are written by hand.
class TargetTest < Minitest::Test
  include TrellisTestCase

  DATABASE = "target_test_#{Process.pid}".freeze
  ROWS = [[1, "click", 3], [1, "view", 1], [2, "click", 7]].freeze

  class Account < ActiveRecord::Base
    self.table_name = "tg_accounts"
    has_many :event_counts, class_name: "TargetTest::EventCount"
  end

  # A GROUP BY target: no primary key, a unique constraint on the group key.
  class EventCount < ActiveRecord::Base
    include Trellis::Target
    self.table_name = "tg_event_counts"
    self.primary_key = %i[account_id kind]
    belongs_to :account, class_name: "TargetTest::Account"
  end

  # A 1-1 target: the source's primary key, which ActiveRecord finds itself.
  class EventCopy < ActiveRecord::Base
    include Trellis::Target
    self.table_name = "tg_event_copies"
  end

  # A subclass of a target model is a target model.
  class ClickCount < EventCount; end

  class << self
    # The database, created and its targets defined once for the file.
    def database
      @database ||= begin
        admin = TestCluster.pg
        admin.exec("create database #{DATABASE}")
        Minitest.after_run do
          admin.exec("drop database if exists #{DATABASE} with (force)")
          admin.close
        end
        pg = PG.connect(dsn)
        pg.exec("create table tg_accounts (id bigint primary key)")
        pg.exec("insert into tg_accounts values (1), (2)")
        pg.exec("create table tg_events (id bigint primary key, account_id bigint not null " \
                "references tg_accounts (id), kind text not null, at timestamp not null)")
        pg.close
        instance = Trellis::Instance.connect(url: dsn)
        instance.migrate
        instance.define("TRANSFORM tg_event_counts FROM tg_events GROUP BY account_id, kind " \
                        "WITHOUT SOURCE INDEX SELECT COUNT(*) AS n")
        instance.define("TRANSFORM tg_event_copies FROM tg_events SELECT kind AS kind, at AS updated_at")
        instance.shutdown
        DATABASE
      end
    end

    def dsn
      TestCluster.dsn.sub(/dbname=\S+/, "dbname=#{DATABASE}")
    end
  end

  def setup
    super
    self.class.database
    info = TestCluster.info
    ActiveRecord::Base.establish_connection(
      adapter: "postgresql", database: DATABASE, host: info.fetch("host"), port: info.fetch("port"),
      username: info.fetch("user")
    )
    @pg = PG.connect(self.class.dsn)
    @pg.exec("delete from tg_event_counts")
    @pg.exec("delete from tg_event_copies")
    ROWS.each { |row| @pg.exec_params("insert into tg_event_counts values ($1, $2, $3)", row) }
    @pg.exec("insert into tg_event_copies (id, kind, updated_at) values (10, 'click', '2000-01-01')")
    TrellisTestApp.config.trellis.off_in = %w[test]
  end

  def teardown
    TrellisTestApp.config.trellis.off_in = []
    @pg&.close
    ActiveRecord::Base.connection_pool.disconnect!
    super
  end

  # Every ActiveRecord write path, each changing the target's rows.
  WRITES = {
    create!: -> { EventCount.create!(account_id: 2, kind: "view", n: 1) },
    new_save: -> { EventCount.new(account_id: 2, kind: "view", n: 1).save },
    update!: -> { first.update!(n: 4) },
    attribute_save!: -> { first.tap { |row| row.n = 4 }.save! },
    update_columns: -> { first.update_columns(n: 4) },
    update_column: -> { first.update_column(:n, 4) },
    increment!: -> { first.increment!(:n) },
    decrement!: -> { first.decrement!(:n) },
    touch: -> { EventCopy.find(10).touch },
    delete: -> { first.delete },
    destroy: -> { first.destroy },
    insert: -> { EventCount.insert({ account_id: 2, kind: "view", n: 1 }) },
    insert!: -> { EventCount.insert!({ account_id: 2, kind: "view", n: 1 }) },
    insert_all: -> { EventCount.insert_all([{ account_id: 2, kind: "view", n: 1 }]) },
    insert_all!: -> { EventCount.insert_all!([{ account_id: 2, kind: "view", n: 1 }]) },
    upsert: -> { EventCount.upsert({ account_id: 1, kind: "click", n: 9 }) },
    upsert_all: -> { EventCount.upsert_all([{ account_id: 1, kind: "click", n: 9 }]) },
    update_all: -> { EventCount.where(account_id: 1).update_all(n: 0) },
    update_counters: -> { EventCount.update_counters([1, "click"], n: 1) },
    delete_all: -> { EventCount.where(account_id: 1).delete_all },
    unscoped_delete_all: -> { EventCount.unscoped.delete_all },
    delete_by: -> { EventCount.delete_by(account_id: 2) },
    destroy_all: -> { EventCount.where(account_id: 2).destroy_all },
    destroy_by: -> { EventCount.destroy_by(account_id: 2) },
    find_or_create_by!: -> { EventCount.find_or_create_by!(account_id: 2, kind: "view") },
    touch_all: -> { EventCopy.touch_all },
    association_create!: -> { Account.find(2).event_counts.create!(kind: "view", n: 1) },
    association_push: -> { Account.find(2).event_counts << EventCount.new(kind: "view", n: 1) },
    association_delete_all: -> { Account.find(1).event_counts.delete_all },
    association_where_update_all: -> { Account.find(1).event_counts.where(kind: "view").update_all(n: 0) },
    subclass_update_all: -> { ClickCount.update_all(n: 0) },
    subclass_create!: -> { ClickCount.create!(account_id: 2, kind: "view", n: 1) },
    one_to_one_update!: -> { EventCopy.find(10).update!(kind: "view") }
  }.freeze

  def self.first = EventCount.find([1, "click"])

  WRITES.each do |name, write|
    define_method(:"test_#{name}_is_refused_where_trellis_is_on") do
      before = rows
      error = in_environment("development") { assert_raises(Trellis::ReadOnlyTarget) { self.class.instance_exec(&write) } }
      assert_kind_of ActiveRecord::ReadOnlyRecord, error
      assert_match(/model over the Trellis target tg_event_(counts|copies), which only Trellis writes/, error.message)
      assert_includes error.message, Trellis::Target::DOCS
      assert_equal before, rows, "#{name} changed the target"
    end

    define_method(:"test_#{name}_writes_where_trellis_is_off") do
      before = rows
      in_environment("test") { self.class.instance_exec(&write) }
      refute_equal before, rows, "#{name} wrote nothing"
    end
  end

  def test_allow_writes_lets_the_block_write_where_trellis_is_on
    in_environment("development") do
      assert Trellis.enabled?
      refute Trellis::Target.writable?
      value = Trellis::Target.allow_writes do
        assert Trellis::Target.writable?
        EventCount.insert_all([{ account_id: 2, kind: "view", n: 1 }])
        :done
      end
      assert_equal :done, value
      refute Trellis::Target.writable?
      assert_raises(Trellis::ReadOnlyTarget) { EventCount.where(account_id: 2).delete_all }
    end
    assert_includes rows, ["2", "view", "1"]
  end

  def test_allow_writes_restores_the_guard_when_the_block_raises
    in_environment("development") do
      assert_raises(RuntimeError) { Trellis::Target.allow_writes { raise "boom" } }
      refute Trellis::Target.writable?
    end
  end

  # The guard overrides ActiveRecord internals; an ActiveRecord that moved
  # one refuses the include rather than leaving writes unguarded.
  def test_an_activerecord_without_the_guarded_internals_refuses_the_include
    assert_empty Trellis::Target.missing_internals
    Trellis::Target.stub(:missing_internals, %i[_insert_record]) do
      error = assert_raises(NotImplementedError) { Class.new(ActiveRecord::Base) { include Trellis::Target } }
      assert_match(/ActiveRecord #{ActiveRecord.version}, which has no _insert_record/, error.message)
    end
  end

  def test_reads_work_where_trellis_is_on
    in_environment("development") do
      assert_equal 3, EventCount.count
      assert_equal 11, EventCount.sum(:n)
      assert_equal [[1, "click"], [2, "click"]], EventCount.where(kind: "click").order(:account_id).pluck(:account_id, :kind)
      assert_equal "click", EventCopy.find(10).kind
    end
  end

  def test_the_declared_composite_key_finds_and_identifies_rows
    in_environment("development") do
      assert_equal %w[account_id kind], EventCount.primary_key
      row = EventCount.find([1, "view"])
      assert_equal 1, row.n
      assert_equal [1, "view"], row.id
      assert_equal "1", row.reload.n.to_s
    end
  end

  def test_a_one_to_one_target_needs_no_key_declaration
    assert_equal "id", EventCopy.primary_key
  end

  def test_associations_work_both_ways
    in_environment("development") do
      count = EventCount.find([2, "click"])
      assert_equal Account.find(2), count.account
      assert_equal [%w[click 3], %w[view 1]],
                   Account.find(1).event_counts.order(:kind).map { |row| [row.kind, row.n.to_s] }
      assert_equal 2, Account.includes(:event_counts).find(1).event_counts.size
    end
  end

  # Fixtures and other SQL sent through the connection go around the model:
  # that is the database grant's job.
  def test_sql_through_the_connection_is_not_guarded
    in_environment("development") do
      EventCount.lease_connection.execute("delete from tg_event_counts where account_id = 2")
    end
    refute_includes rows.map(&:first), "2"
  end

  private

  def rows
    @pg.exec("select account_id::text, kind, n::text from tg_event_counts order by 1, 2").values +
      @pg.exec("select id::text, kind, updated_at::text from tg_event_copies order by 1").values
  end

  def in_environment(name, &)
    Rails.stub(:env, ActiveSupport::StringInquirer.new(name), &)
  end
end
