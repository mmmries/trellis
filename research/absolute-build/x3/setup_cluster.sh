#!/usr/bin/env bash
# X3 scratch cluster (E5's settings): Postgres 18.6, checksums, fsync on, shared_buffers 128MB,
# max_wal_size 1GB, work_mem 4MB, auto_explain + pg_stat_statements. usage: setup_cluster.sh
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); ROOT=$(cd "$HERE/../../.." && pwd)
export PATH=$HOME/.asdf/installs/postgres/18.6/bin:$PATH
S=${PGDATA_DIR:-$ROOT/target/research-pg-x3}; PORT=${PORT:-54873}
[ -e "$S" ] && { pg_ctl -D "$S" stop -m fast >/dev/null 2>&1 || true; rm -rf "$S"; }
initdb -D "$S" -U postgres -A trust -k >/dev/null
cat >> "$S/postgresql.conf" <<CONF
port = $PORT
listen_addresses = '127.0.0.1'
unix_socket_directories = '$S'
fsync = on
shared_buffers = 128MB
max_wal_size = 1GB
work_mem = 4MB
max_connections = 200
shared_preload_libraries = 'pg_stat_statements,auto_explain'
pg_stat_statements.track = all
pg_stat_statements.track_utility = on
pg_stat_statements.max = 10000
auto_explain.log_min_duration = '200ms'
auto_explain.log_nested_statements = on
auto_explain.log_analyze = off
CONF
pg_ctl -D "$S" -l "$S/server.log" -w start >/dev/null
SU="psql -X -q -At -h $S -p $PORT -U postgres"
$SU -c "create role app login" postgres
$SU -c "create extension pg_stat_statements" postgres
echo "cluster up at $S port $PORT"
