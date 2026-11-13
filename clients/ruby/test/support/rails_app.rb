# frozen_string_literal: true

# A Rails app in the test process, never initialized, so the Rails
# integration has a config.trellis to read. test_helper required "trellis/pg"
# before Rails was loaded, as an app whose Gemfile lists trellis-pg ahead of
# rails would, so the Railtie is required here by hand.
require "logger"
require "rails"
require "active_record"
require "trellis/railtie"
require "trellis/migration"

class TrellisTestApp < Rails::Application
  config.root = File.expand_path("../..", __dir__)
  config.eager_load = false
  config.logger = Logger.new(nil)
end

# What the Railtie ships with (Trellis off in the test environment), before
# the line below.
DEFAULT_OFF_IN = TrellisTestApp.config.trellis.off_in.dup.freeze

# This suite runs Trellis on in every environment: its schema, dump and
# migration tests build databases under a "test" config name. Only
# off_in_test.rb switches the default back on.
TrellisTestApp.config.trellis.off_in = []

ActiveRecord::Migration.verbose = false
