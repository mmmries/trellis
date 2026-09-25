#!/usr/bin/env bash
# Throwaway Postgres cluster for the #558 experiments. No Trellis code.
#
#   cluster.sh init <dir>               initdb (port from EXP_PORT, default 54321)
#   cluster.sh start <dir>              start (unix socket in <dir>/sock)
#   cluster.sh stop <dir>               pg_ctl stop -m fast
#   cluster.sh psql <dir> [args...]     psql against it (db postgres)
#   cluster.sh jump-epoch <dir> <epoch> <hexxid>
#       stop, pg_resetwal so the next xid is <hexxid> in epoch <epoch>, restart.
#       Jumps are staged in steps of < 2^31 with a VACUUM FREEZE of every
#       database between them so the wraparound stop limit never trips.
#   cluster.sh destroy <dir>
set -euo pipefail
cmd=$1; dir=$2; shift 2
port=${EXP_PORT:-54321}
sock="$dir/sock"
data="$dir/data"

pg() { psql -h "$sock" -p "$port" -U postgres -X -q -v ON_ERROR_STOP=1 "$@"; }

wait_ready() { for _ in $(seq 1 100); do pg_isready -h "$sock" -p "$port" -U postgres >/dev/null 2>&1 && return 0; sleep 0.1; done; echo "server did not come up" >&2; return 1; }

start() {
  pg_ctl -D "$data" -l "$dir/postgres.log" -o "-h '' -k $sock -p $port -c wal_level=logical -c max_replication_slots=8 -c max_wal_senders=8 -c fsync=off -c synchronous_commit=off -c full_page_writes=off -c shared_buffers=2GB -c max_connections=200 -c autovacuum=off -c log_min_messages=warning" -w -t 60 start >/dev/null
  wait_ready
}
stop() { pg_ctl -D "$data" -m fast -w -t 60 stop >/dev/null; }

freeze_all() {
  pg -d postgres -c "alter database template0 allow_connections true"
  for db in postgres template1 template0; do pg -d "$db" -c "vacuum freeze"; done
  pg -d postgres -c "alter database template0 allow_connections false"
  pg -d postgres -c "checkpoint"
}

case "$cmd" in
  init)
    rm -rf "$dir"; mkdir -p "$sock"
    initdb -D "$data" -U postgres --auth=trust --no-sync >/dev/null
    ;;
  start) start ;;
  stop) stop ;;
  psql) pg -d postgres "$@" ;;
  destroy) stop 2>/dev/null || true; rm -rf "$dir" ;;
  jump-epoch)
    epoch=$1; target=$((16#$2))
    # current next xid
    cur=$(pg -d postgres -Atc "select pg_snapshot_xmax(pg_current_snapshot())::text::numeric % 4294967296")
    cur=${cur%.*}
    stop
    # staged jumps: each step < 2^31 - 3M from the previous (frozen) position
    step=$((2147483648 - 100000000))
    pos=$cur
    while [ $((target - pos)) -gt $step ]; do
      # page-aligned (32768 xids per clog page) so TrimCLOG needs no existing page
      pos=$(( (pos + step) / 32768 * 32768 ))
      echo "  resetwal -x $pos (staging)"
      pg_resetwal -D "$data" -x "$pos" >/dev/null
      start; freeze_all; stop
    done
    echo "  resetwal -e $epoch -x $target"
    pg_resetwal -D "$data" -e "$epoch" -x "$target" >/dev/null
    start; freeze_all
    pg -d postgres -Atc "select 'next xid8 = ' || pg_snapshot_xmax(pg_current_snapshot())::text"
    ;;
  *) echo "unknown command $cmd" >&2; exit 64 ;;
esac
