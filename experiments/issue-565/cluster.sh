#!/usr/bin/env bash
# Scratch PG cluster for #565 experiments. usage: cluster.sh start|stop <name> [port] [extra -c args...]
set -uo pipefail
cmd=$1; name=$2; port=${3:-5565}; shift 3 2>/dev/null || shift $#
D=/tmp/tc-565/$name
case $cmd in
start)
  rm -rf "$D"; mkdir -p "$D"
  initdb -D "$D/data" -U postgres -A trust >/dev/null 2>&1 || { echo initdb failed; exit 1; }
  postgres -D "$D/data" -h '' -k "$D" -p $port -c wal_level=${WAL_LEVEL:-logical} -c max_replication_slots=50 -c max_wal_senders=50 \
    -c dynamic_shared_memory_type=mmap -c logging_collector=off -c max_connections=200 "$@" > "$D/pg.log" 2>&1 &
  for i in $(seq 1 100); do pg_isready -h $D -p $port -q && break; sleep 0.1; done
  echo "-h $D -p $port -U postgres";;
stop)
  pg_ctl -D "$D/data" stop -m immediate -s; rm -rf "$D";;
esac
