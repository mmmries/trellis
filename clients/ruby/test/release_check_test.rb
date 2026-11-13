# frozen_string_literal: true

require "open3"
require "tmpdir"
require "test_helper"
require "trellis/release_check"

# Trellis::ReleaseCheck against throwaway git repositories: it reads git and
# files only, so no test here touches the cluster. The rake task that runs it
# in a Rails app is in rails_test.rb.
class ReleaseCheckTest < Minitest::Test
  TRELLIS_MIGRATION = <<~RUBY
    class DefineOrderTotals < ActiveRecord::Migration[8.1]
      include Trellis::Migration
      disable_ddl_transaction!

      def up = define("TRANSFORM order_totals FROM orders SELECT price + tax AS total")

      def down
        apply "PAUSE TRANSFORM order_totals"
        apply "DROP TRANSFORM order_totals"
      end
    end
  RUBY
  PLAIN_MIGRATION = <<~RUBY
    class AddNoteToOrders < ActiveRecord::Migration[8.1]
      def change = add_column(:orders, :note, :text)
    end
  RUBY

  def setup
    @root = Dir.mktmpdir("release-check")
    git "init", "--quiet", "--initial-branch=main"
    git "config", "user.email", "test@example.com"
    git "config", "user.name", "Test"
    write "Gemfile.lock", lockfile("0.2.0")
    write "db/migrate/20260101000000_create_orders.rb", "class CreateOrders < ActiveRecord::Migration[8.1]; end\n"
    commit "the deployed app"
    git "checkout", "--quiet", "-b", "feature"
  end

  def teardown
    FileUtils.remove_entry(@root)
  end

  def test_a_version_change_with_a_trellis_migration_fails_and_says_what_to_do
    write "Gemfile.lock", lockfile("0.3.0")
    write "db/migrate/20260201000000_define_order_totals.rb", TRELLIS_MIGRATION
    commit "bump and define"

    result = check
    refute result.ok?
    assert_equal %w[0.2.0 0.3.0], [result.old_version, result.new_version]
    assert_equal ["db/migrate/20260201000000_define_order_totals.rb"], result.migrations
    assert_match "changes trellis-pg from 0.2.0 to 0.3.0 and adds Trellis migrations:", result.message
    assert_match "  db/migrate/20260201000000_define_order_totals.rb", result.message
    assert_match "Ship the trellis-pg change by itself", result.message
  end

  def test_a_version_change_alone_passes
    write "Gemfile.lock", lockfile("0.3.0")
    write "db/migrate/20260201000000_add_note_to_orders.rb", PLAIN_MIGRATION
    commit "bump, and a migration that doesn't use Trellis"

    result = check
    assert result.ok?
    assert_empty result.migrations
    assert_match "ok (trellis-pg changes from 0.2.0 to 0.3.0; 0 Trellis migration(s) added", result.message
  end

  def test_a_trellis_migration_alone_passes
    write "db/migrate/20260201000000_define_order_totals.rb", TRELLIS_MIGRATION
    commit "define"

    result = check
    assert result.ok?
    assert_equal 1, result.migrations.size
    assert_match "ok (trellis-pg 0.2.0, unchanged; 1 Trellis migration(s) added", result.message
  end

  # Adopting Trellis: there is no old worker to hand over from.
  def test_a_release_that_adds_the_gem_and_its_first_transform_passes
    git "checkout", "--quiet", "main"
    write "Gemfile.lock", "GEM\n  specs:\n    rake (13.3.0)\n"
    commit "before Trellis"
    git "checkout", "--quiet", "-b", "adopt"
    write "Gemfile.lock", lockfile("0.3.0")
    write "db/migrate/20260201000000_define_order_totals.rb", TRELLIS_MIGRATION
    commit "adopt Trellis"

    result = check
    assert result.ok?
    assert_nil result.old_version
    assert_equal "0.3.0", result.new_version
  end

  # A platform gem's lockfile entry carries its platform; a source gem's
  # doesn't. The same version either way is no change.
  def test_a_platform_entry_and_a_source_entry_of_one_version_are_the_same_version
    write "Gemfile.lock", lockfile("0.2.0", platforms: [])
    write "db/migrate/20260201000000_define_order_totals.rb", TRELLIS_MIGRATION
    commit "lock the source gem only"

    result = check
    assert_equal %w[0.2.0 0.2.0], [result.old_version, result.new_version]
    assert result.ok?
  end

  def test_a_downgrade_counts_as_a_version_change
    write "Gemfile.lock", lockfile("0.1.1")
    write "db/migrate/20260201000000_define_order_totals.rb", TRELLIS_MIGRATION
    commit "downgrade and define"

    refute check.ok?
  end

  # A developer runs it before committing.
  def test_uncommitted_and_untracked_work_counts
    write "Gemfile.lock", lockfile("0.3.0")
    write "db/migrate/20260201000000_define_order_totals.rb", TRELLIS_MIGRATION

    result = check
    refute result.ok?
    assert_equal ["db/migrate/20260201000000_define_order_totals.rb"], result.migrations
  end

  # The branch is judged by what it changes, not by what `base` gained since
  # the branch left it.
  def test_a_branch_behind_its_base_is_compared_with_the_merge_base
    write "db/migrate/20260201000000_define_order_totals.rb", TRELLIS_MIGRATION
    commit "define, on the branch"
    git "checkout", "--quiet", "main"
    write "Gemfile.lock", lockfile("0.3.0")
    commit "bump, on main, after the branch left"
    git "checkout", "--quiet", "feature"

    result = check
    assert result.ok?
    assert_equal %w[0.2.0 0.2.0], [result.old_version, result.new_version]
  end

  # The same two commits as one release: BASE is the last deployed ref.
  def test_a_release_of_several_commits_is_checked_against_the_deployed_ref
    git "tag", "deployed"
    write "Gemfile.lock", lockfile("0.3.0")
    commit "bump"
    write "db/migrate/20260201000000_define_order_totals.rb", TRELLIS_MIGRATION
    commit "define, in a later commit"

    refute check(base: "deployed").ok?
    assert check(base: "HEAD~1").ok?
  end

  def test_an_edited_old_trellis_migration_does_not_count
    git "checkout", "--quiet", "main"
    write "db/migrate/20260115000000_define_order_totals.rb", TRELLIS_MIGRATION
    commit "a transform defined long ago"
    git "checkout", "--quiet", "-B", "feature"
    write "Gemfile.lock", lockfile("0.3.0")
    write "db/migrate/20260115000000_define_order_totals.rb", "#{TRELLIS_MIGRATION}# reformatted\n"
    commit "bump, and touch an old migration"

    result = check
    assert result.ok?
    assert_empty result.migrations
  end

  def test_a_migration_that_calls_trellis_without_the_helpers_counts
    write "Gemfile.lock", lockfile("0.3.0")
    write "db/migrate/20260201000000_drop_order_totals.rb", <<~RUBY
      class DropOrderTotals < ActiveRecord::Migration[8.1]
        disable_ddl_transaction!
        def up = Trellis::Railtie.with_handle { |trellis| trellis.apply("PAUSE TRANSFORM order_totals") }
      end
    RUBY
    commit "bump and pause"

    refute check.ok?
  end

  def test_every_migrations_path_is_read
    write "Gemfile.lock", lockfile("0.3.0")
    write "db/reports_migrate/20260201000000_define_order_totals.rb", TRELLIS_MIGRATION
    commit "bump, and define in the second database's migrations"

    assert check.ok?
    result = check(migrations_paths: ["db/migrate", File.join(@root, "db/reports_migrate")])
    refute result.ok?
    assert_equal ["db/reports_migrate/20260201000000_define_order_totals.rb"], result.migrations
  end

  # A Rails app that isn't at the top of its repository.
  def test_an_app_in_a_subdirectory_of_the_repository
    FileUtils.mkdir_p(File.join(@root, "apps/shop"))
    git "mv", "Gemfile.lock", "apps/shop/Gemfile.lock"
    git "mv", "db", "apps/shop/db"
    commit "move the app"
    git "checkout", "--quiet", "-B", "main"
    git "checkout", "--quiet", "-b", "feature-in-subdirectory"
    write "apps/shop/Gemfile.lock", lockfile("0.3.0")
    write "apps/shop/db/migrate/20260201000000_define_order_totals.rb", TRELLIS_MIGRATION
    write "db/migrate/20260201000000_define_order_totals.rb", TRELLIS_MIGRATION
    commit "bump and define"

    result = Trellis::ReleaseCheck.call(root: File.join(@root, "apps/shop"), base: "main")
    refute result.ok?
    assert_equal ["db/migrate/20260201000000_define_order_totals.rb"], result.migrations
  end

  def test_no_base_is_an_error_that_says_what_to_pass
    ["", "  ", nil].each do |base|
      error = assert_raises(Trellis::ReleaseCheck::Error) { check(base: base) }
      assert_match "set BASE to the git ref", error.message
    end
  end

  def test_a_base_that_would_read_as_a_git_option_is_refused
    error = assert_raises(Trellis::ReleaseCheck::Error) { check(base: "--all") }
    assert_equal '"--all" is not a git ref', error.message
  end

  def test_an_unknown_base_is_an_error
    error = assert_raises(Trellis::ReleaseCheck::Error) { check(base: "origin/main") }
    assert_equal "origin/main is not a commit this clone knows: fetch it first", error.message
  end

  # A shallow clone: the base is known, the history that joins it isn't.
  def test_no_merge_base_is_an_error_that_names_the_shallow_clone
    git "checkout", "--quiet", "--orphan", "unrelated"
    commit "a history of its own"

    error = assert_raises(Trellis::ReleaseCheck::Error) { check }
    assert_match "no merge base with main", error.message
    assert_match "fetch-depth: 0", error.message
  end

  def test_outside_a_git_checkout_is_an_error
    Dir.mktmpdir("not-a-repository") do |dir|
      error = assert_raises(Trellis::ReleaseCheck::Error) do
        Trellis::ReleaseCheck.call(root: dir, base: "main")
      end
      assert_match "git rev-parse failed", error.message
    end
  end

  private

  def check(base: "main", **)
    Trellis::ReleaseCheck.call(root: @root, base: base, **)
  end

  # A lockfile as Bundler writes it for the gem: the source gem's entry,
  # with its dependency, and one entry per platform gem.
  def lockfile(version, platforms: %w[arm64-darwin x86_64-linux])
    entries = ["    trellis-pg (#{version})", "      rb_sys (~> 0.9.130)"]
    entries += platforms.map { |platform| "    trellis-pg (#{version}-#{platform})" }
    <<~LOCK
      GEM
        remote: https://rubygems.org/
        specs:
          rb_sys (0.9.130)
      #{entries.join("\n")}

      DEPENDENCIES
        trellis-pg (~> #{version})
    LOCK
  end

  def write(path, content)
    path = File.join(@root, path)
    FileUtils.mkdir_p(File.dirname(path))
    File.write(path, content)
  end

  def commit(message)
    git "add", "--all"
    git "commit", "--quiet", "--message", message
  end

  def git(*)
    out, status = Open3.capture2e("git", *, chdir: @root)
    assert status.success?, "git #{[*].join(' ')} failed: #{out}"
    out
  end
end
