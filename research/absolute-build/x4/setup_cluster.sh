#!/usr/bin/env bash
# X4 scratch cluster: X3's settings (Postgres 18.6, checksums, fsync on, shared_buffers 128MB,
# max_wal_size 1GB, work_mem 4MB, pg_stat_statements), its own dir and port.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); ROOT=$(cd "$HERE/../../.." && pwd)
export PATH=$HOME/.asdf/installs/postgres/18.6/bin:$PATH
S=$ROOT/target/research-pg-x4; PORT=54874
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
shared_preload_libraries = 'pg_stat_statements'
pg_stat_statements.track = all
pg_stat_statements.max = 10000
CONF
pg_ctl -D "$S" -l "$S/server.log" -w start >/dev/null
SU="psql -X -q -At -h $S -p $PORT -U postgres"
$SU -c "create role app login" postgres
$SU -c "create extension pg_stat_statements" postgres
echo "cluster up at $S port $PORT"
