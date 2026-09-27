# frozen_string_literal: true

module Trellis
  # A registered transform definition, as Trellis.define returns it.
  #
  # - id: the definition's id.
  # - target_table, source_table: fully qualified, "schema.table".
  # - source_version: the source table's version it was validated against.
  # - status: a symbol (:waiting_to_backfill, :backfilling, :live, ...), from
  #   a closed set: never one made from a string the database returned.
  # - source_columns: each source column's name => its type's name
  #   ("integer", "numeric", "text", ...).
  Definition = Data.define(:id, :target_table, :source_table, :source_version, :status,
                           :source_columns)

  # A definition's status, as Trellis.status returns it. backfill_failure is
  # nil unless its source table's backfill keeps failing.
  Status = Data.define(:status, :backfill_failure) do
    def self.from_native(hash)
      failure = hash[:backfill_failure]
      new(status: hash[:status], backfill_failure: failure && BackfillFailure.from_native(failure))
    end
  end

  # Why a definition's source table isn't backfilling: the backfill marker is
  # parked on source_table after `attempts` failures, the latest being
  # last_error, and won't be retried before next_attempt_at (a Time).
  BackfillFailure = Data.define(:source_table, :attempts, :last_error, :next_attempt_at) do
    def self.from_native(hash)
      new(
        source_table: hash[:source_table],
        attempts: hash[:attempts],
        last_error: hash[:last_error],
        next_attempt_at: Time.at(0, hash[:next_attempt_at_micros], :microsecond, in: "UTC")
      )
    end
  end
end
