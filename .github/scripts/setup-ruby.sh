#!/usr/bin/env bash
# Installs the Ruby version a .tool-versions file pins, from ruby/ruby-builder's
# prebuilt builds (the same builds ruby/setup-ruby installs), checked against
# the sha256 digest GitHub records for the release asset.
#
# Vendored as a script rather than using ruby/setup-ruby because the org's
# Actions policy allows only a curated set of third-party actions (see
# .github/actions/rust-toolchain/action.yml), and whether setup-ruby is on it
# couldn't be confirmed. The same reasoning as setup-beam.sh.
#
# usage: setup-ruby.sh <.tool-versions>
#
# The builds aren't relocatable: they're configured with the prefix
# $RUNNER_TOOL_CACHE/Ruby/<version>/x64 (their scripts' shebangs and the
# interpreter's library path point there), so that's where this puts them,
# as setup-ruby does. Puts the bin directory on $GITHUB_PATH when run in
# Actions, and prints it either way. The OS build flavor defaults to this
# machine's (`ubuntu-24.04` on ubuntu-latest); SETUP_RUBY_OS overrides it.
# GITHUB_TOKEN, if set, authenticates the one GitHub API call.
set -euo pipefail

tool_versions="${1:?usage: setup-ruby.sh <.tool-versions>}"

version="$(awk '$1 == "ruby" { print $2 }' "$tool_versions")"
if [ -z "$version" ]; then
  echo "setup-ruby: $tool_versions must pin ruby" >&2
  exit 1
fi

if [ -z "${SETUP_RUBY_OS:-}" ]; then
  # shellcheck disable=SC1091
  . /etc/os-release
  SETUP_RUBY_OS="${ID}-${VERSION_ID}"
fi
if [ "$(uname -m)" != x86_64 ]; then
  echo "setup-ruby: only x86_64 is supported (CI's runners), not $(uname -m)" >&2
  exit 1
fi

tool_cache="${RUNNER_TOOL_CACHE:-/opt/hostedtoolcache}"
prefix="$tool_cache/Ruby/$version/x64"
tag="ruby-$version"
asset="ruby-$version-$SETUP_RUBY_OS-x64.tar.gz"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

auth=()
if [ -n "${GITHUB_TOKEN:-}" ]; then
  auth=(-H "Authorization: Bearer $GITHUB_TOKEN")
fi
expected="$(
  curl -sSfL "${auth[@]}" -H "Accept: application/vnd.github+json" \
    "https://api.github.com/repos/ruby/ruby-builder/releases/tags/$tag" |
    jq -r --arg name "$asset" '.assets[] | select(.name == $name) | .digest'
)"
case "$expected" in
  sha256:*) expected="${expected#sha256:}" ;;
  *)
    echo "setup-ruby: no sha256 digest for $asset in ruby-builder's $tag release" >&2
    exit 1
    ;;
esac

curl -sSfL -o "$work/ruby.tar.gz" \
  "https://github.com/ruby/ruby-builder/releases/download/$tag/$asset"
echo "$expected  $work/ruby.tar.gz" | sha256sum --check --quiet

# The tarball holds a single `x64/` directory, the prefix's last component.
mkdir -p "$(dirname "$prefix")"
rm -rf "$prefix"
tar -xzf "$work/ruby.tar.gz" -C "$(dirname "$prefix")"
"$prefix/bin/ruby" --version >&2

echo "$prefix/bin"
if [ -n "${GITHUB_PATH:-}" ]; then
  echo "$prefix/bin" >>"$GITHUB_PATH"
fi
