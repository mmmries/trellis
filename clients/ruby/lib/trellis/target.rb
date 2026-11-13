# frozen_string_literal: true

require "active_record"
require_relative "pg"

module Trellis
  # Raised for a write to a model over a Trellis target where Trellis is on.
  # A ReadOnlyRecord, so code that rescues ActiveRecord's read-only error
  # rescues this one too.
  class ReadOnlyTarget < ActiveRecord::ReadOnlyRecord; end

  # An ActiveRecord model over a target table: only Trellis writes a target,
  # so every write ActiveRecord would send for the model raises
  # Trellis::ReadOnlyTarget. Reads, scopes and associations are untouched.
  #
  #   class EventCount < ApplicationRecord
  #     include Trellis::Target
  #     self.primary_key = [:account_id, :kind] # a GROUP BY target's key
  #     belongs_to :account
  #   end
  #
  # Where Trellis is off (config.trellis.off_in, so Trellis.enabled? is
  # false) a target is an ordinary table that tests fill, and the model
  # writes like any other. Trellis::Target.allow_writes does the same for a
  # block, for a suite that runs with Trellis on.
  #
  # The guard sits where ActiveRecord builds each write: the record writes
  # (save, update, update_columns, touch, increment!, delete, destroy) all
  # reach the class's _insert_record, _update_record or _delete_record, and
  # the bulk ones (insert_all, upsert_all, update_all, update_counters,
  # touch_all, delete_all, delete_by) the relation's insert_all, insert_all!,
  # upsert_all, update_all or delete_all, whether called on the class, a
  # scope or an association. SQL sent through the connection (execute,
  # fixtures) goes around it: that is the database grant's job
  # (docs/recommendations.md, "Application roles read targets, and nothing
  # else").
  module Target
    extend ActiveSupport::Concern

    DOCS = "https://github.com/salesforce-misc/trellis/tree/main/clients/ruby#targets-as-models"
    RELATION_WRITES = %i[insert_all insert_all! upsert_all update_all delete_all].freeze
    ALLOWED = :trellis_target_writes_allowed

    class << self
      # Lets this thread write target models for the block, where Trellis
      # is on: a test that stubs a target's rows by hand. Returns the
      # block's value.
      def allow_writes
        state = ActiveSupport::IsolatedExecutionState
        allowed = state[ALLOWED]
        state[ALLOWED] = true
        yield
      ensure
        state[ALLOWED] = allowed
      end

      # Whether target models write here: Trellis is off in this
      # environment, or inside allow_writes.
      def writable?
        !Trellis.enabled? || ActiveSupport::IsolatedExecutionState[ALLOWED] == true
      end

      # Raises ReadOnlyTarget unless writable?. `write` names it: the
      # relation method, or the statement a record write sends.
      def guard!(model, write)
        return if writable?

        raise ReadOnlyTarget,
              "#{model.name} is a model over the Trellis target #{model.table_name}, which only Trellis " \
              "writes: #{write} refused. Tests write it with Trellis off (config.trellis.off_in) or in " \
              "Trellis::Target.allow_writes; see #{DOCS}"
      end

      # The ActiveRecord internals the guard overrides that this
      # ActiveRecord lacks. They are :nodoc:, so a release may move one, and
      # the guard would then let that write through: including the concern
      # raises instead.
      def missing_internals
        records = %i[_insert_record _update_record _delete_record].reject do |write|
          ActiveRecord::Base.respond_to?(write, true)
        end
        relations = RELATION_WRITES.reject { |write| ActiveRecord::Relation.method_defined?(write) }
        delegation = [[ActiveRecord::Delegation, :delegated_classes], [ActiveRecord::Base, :relation_delegate_class]]
                     .reject { |owner, method| owner.respond_to?(method, true) }.map(&:last)
        records + relations + delegation
      end

      # Every relation of `model` (Model.where, Model.all, association
      # scopes) is an instance of one of its relation classes.
      def guard_relations(model)
        ActiveRecord::Delegation.delegated_classes.each do |relation|
          model.relation_delegate_class(relation).prepend(RelationGuard)
        end
      end
    end

    module RelationGuard
      RELATION_WRITES.each do |write|
        define_method(write) do |*args, **options, &block|
          Target.guard!(model, write)
          super(*args, **options, &block)
        end
      end
    end

    included do
      missing = Target.missing_internals
      unless missing.empty?
        raise NotImplementedError,
              "Trellis::Target can't guard #{name} on ActiveRecord #{ActiveRecord.version}, which has no " \
              "#{missing.join(", ")}: writes to it would go through unrefused"
      end

      Target.guard_relations(self)
    end

    class_methods do
      # A subclass (single-table inheritance) gets relation classes of its
      # own.
      def inherited(subclass)
        super
        Target.guard_relations(subclass)
      end

      def _insert_record(...)
        Target.guard!(self, "INSERT")
        super
      end

      def _update_record(...)
        Target.guard!(self, "UPDATE")
        super
      end

      def _delete_record(...)
        Target.guard!(self, "DELETE")
        super
      end
    end
  end
end
