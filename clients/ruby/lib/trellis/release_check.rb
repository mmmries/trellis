# frozen_string_literal: true

require "open3"
require "pathname"

module Trellis
  # A static check for CI: does this release both change the trellis-pg
  # gem's version and add a migration that uses Trellis?
  #
  # Such a release fails where migrations run before the old processes stop
  # (a release phase, a pre-deploy job): while a worker of the old version is
  # live, a define, ALTER, PAUSE, RESUME or DROP from the new version refuses
  # with Trellis::VersionHandoverError (docs/embedding.md, "Rolling
  # deploys"). The remedy is to ship the version change by itself and the
  # migrations in the next release, and this check says so on the pull
  # request instead of in the deploy.
  #
  #   result = Trellis::ReleaseCheck.call(root: Rails.root, base: "origin/main")
  #   abort result.message unless result.ok?
  #
  # `rails trellis:release_check BASE=origin/main` runs it in a Rails app.
  #
  # It reads git and files only: no database, no Trellis handle, and none of
  # the gem's extension. It compares the working tree under `root` with its
  # merge base against `base`, so a branch that is behind `base` is judged by
  # what the branch itself changes, and uncommitted work counts.
  #
  # It sees migrations only. A state-changing call from a deploy script or a
  # console isn't in any file it reads; the engine's refusal covers those.
  module ReleaseCheck
    GEM_NAME = "trellis-pg"

    # The check couldn't run: no base ref, not a git checkout, or a base
    # this clone has no merge base with (a shallow clone, usually).
    class Error < StandardError; end

    # What the check found. `base` is the merge-base commit it compared with,
    # the versions are trellis-pg's in the lockfile there and in the working
    # tree (nil where the lockfile doesn't list the gem), and `migrations` are
    # the migrations added since `base` that mention Trellis, relative to
    # `root`.
    Result = Data.define(:base, :old_version, :new_version, :migrations) do
      # Any difference counts, a patch release and a downgrade too: Trellis
      # versions match exactly, so every change of release is a handover. A
      # release that adds the gem has no old worker to hand over from.
      def version_changed?
        !old_version.nil? && !new_version.nil? && old_version != new_version
      end

      def ok?
        !(version_changed? && migrations.any?)
      end

      def message
        ok? ? ok_message : failure_message
      end

      private

      def ok_message
        version = if version_changed?
                    "#{GEM_NAME} changes from #{old_version} to #{new_version}"
                  else
                    "#{GEM_NAME} #{new_version || 'is not in the lockfile'}, unchanged"
                  end
        "trellis:release_check: ok (#{version}; #{migrations.size} Trellis migration(s) added since #{base[0, 12]})"
      end

      def failure_message
        <<~MESSAGE.chomp
          trellis:release_check: this release changes #{GEM_NAME} from #{old_version} to #{new_version} and adds Trellis migrations:
          #{migrations.map { |path| "  #{path}" }.join("\n")}
          While a worker of #{old_version} is still running, #{new_version} may not define, alter, pause, resume
          or drop a transform (Trellis::VersionHandoverError), so a deploy that migrates before the old
          worker stops fails at these migrations. Ship the #{GEM_NAME} change by itself, then these
          migrations in the next release.
        MESSAGE
      end
    end

    class << self
      # Runs the check for the app at `root` against the git ref `base`: the
      # branch a pull request targets, or the last deployed ref for a release
      # of several merged pull requests. `migrations_paths` and `lockfile` are
      # relative to `root` (absolute paths under it work too).
      #
      # Returns a Result; raises Error when the check can't run.
      def call(root:, base:, migrations_paths: ["db/migrate"], lockfile: "Gemfile.lock")
        root = Pathname(root).expand_path
        base = base.to_s.strip
        if base.empty?
          raise Error, "set BASE to the git ref this release is compared with: the branch a pull " \
                       "request targets (BASE=origin/main), or the last deployed ref"
        end

        # Never an option to git, whatever the environment variable held.
        raise Error, "#{base.inspect} is not a git ref" if base.start_with?("-")

        merge_base = merge_base(root, base)
        lockfile = relative(root, lockfile)
        Result.new(
          base: merge_base,
          old_version: locked_version(git(root, "show", "#{merge_base}:./#{lockfile}", allow_failure: true)),
          new_version: locked_version(read(root.join(lockfile))),
          migrations: added_trellis_migrations(root, merge_base, migrations_paths.map { |path| relative(root, path) })
        )
      end

      private

      def merge_base(root, base)
        git(root, "rev-parse", "--is-inside-work-tree")
        unless git(root, "rev-parse", "--verify", "--quiet", "#{base}^{commit}", allow_failure: true)
          raise Error, "#{base} is not a commit this clone knows: fetch it first"
        end

        git(root, "merge-base", base, "HEAD", allow_failure: true)&.strip or
          raise Error, "this checkout has no merge base with #{base}: fetch more history " \
                       "(a shallow clone has none; with actions/checkout, set fetch-depth: 0)"
      end

      # trellis-pg's version in a lockfile's text: its `specs:` entry, without
      # the platform a platform gem's entry carries (`0.2.0-x86_64-linux`).
      # nil for no text or no entry.
      def locked_version(text)
        text&.[](/^ {4}#{Regexp.escape(GEM_NAME)} \(([^)-]+)/, 1)
      end

      # The migrations under `paths` that the working tree has and
      # `merge_base` hasn't (committed, staged or untracked), and that
      # mention Trellis: the Trellis::Migration helpers, or a direct
      # Trellis.apply. An edited old migration doesn't run again, so only
      # added (and renamed) files count.
      def added_trellis_migrations(root, merge_base, paths)
        added = git(root, "diff", "--name-only", "-z", "--diff-filter=AR", "--relative", merge_base, "--", *paths)
        untracked = git(root, "ls-files", "-z", "--others", "--exclude-standard", "--", *paths)
        (added.split("\0") + untracked.split("\0"))
          .uniq
          .select { |path| path.end_with?(".rb") && read(root.join(path))&.match?(/\bTrellis\b/) }
          .sort
      end

      def read(path)
        path.file? ? path.read : nil
      end

      def relative(root, path)
        path = Pathname(path)
        (path.absolute? ? path.relative_path_from(root) : path).to_s
      end

      # git's stdout, run in `root`. A failing command raises Error, or
      # returns nil with allow_failure.
      def git(root, *args, allow_failure: false)
        out, err, status = Open3.capture3("git", *args, chdir: root.to_s)
        return out if status.success?
        return nil if allow_failure

        raise Error, "git #{args.first} failed in #{root}: #{err.strip}"
      rescue Errno::ENOENT => e
        raise Error, "can't run git in #{root}: #{e.message}"
      end
    end
  end
end
