#!/usr/bin/env bash
# Samples the benchmark process's memory, and the engine's ring/definition state, every
# INTERVAL seconds until the benchmark exits. Usage: memsample.sh <out.tsv> [disk-dir] [interval]
# MEMSAMPLE_DB=0 skips the database columns (their count(*) scans perturb a measured run).
# Waits for a `benchmark` process to appear first. Columns:
#   time  elapsed_s  rss_mb  hwm_mb  threads  def_status  segments(state:rows...)  live_regs  pg_rss_mb
out=$1; DISK=${2:-/home/mike/exp558/tmpdisk}; IV=${3:-5}
until pid=$(pgrep -x benchmark | head -1) && [ -n "$pid" ]; do sleep 1; done
t0=$(date +%s)
echo -e "time\telapsed_s\trss_mb\thwm_mb\tthreads\tdef_status\tsegments\tregistry\tpg_rss_mb" > "$out"
while [ -d /proc/$pid ]; do
  rss=$(awk '/^VmRSS/{print int($2/1024)}' /proc/$pid/status 2>/dev/null)
  hwm=$(awk '/^VmHWM/{print int($2/1024)}' /proc/$pid/status 2>/dev/null)
  thr=$(awk '/^Threads/{print $2}' /proc/$pid/status 2>/dev/null)
  sock=$(ls -d "$DISK"/trellis-testkit-*/sock 2>/dev/null | head -1)
  st=-; seg=-; reg=-
  if [ -n "$sock" ] && [ "${MEMSAMPLE_DB:-1}" = 1 ]; then
    port=$(ls "$sock" | sed -n 's/^\.s\.PGSQL\.\([0-9]*\)$/\1/p' | head -1)
    db=$(psql -U postgres -h "$sock" -p "$port" -d postgres -XAtc \
      "select datname from pg_database where datname not in ('postgres','template0','template1') order by oid desc limit 1" 2>/dev/null)
    if [ -n "$db" ]; then
      q() { timeout 20 psql -U postgres -h "$sock" -p "$port" -d "$db" -XAtc "$1" 2>/dev/null | tr '\n' ' '; }
      st=$(q "select status from transform_definitions limit 1")
      seg=$(q "select seg_seq||'@'||ring_slot||':'||state||':'||coalesce((xpath('/row/c/text()', query_to_xml('select count(*) as c from trellis.seg_'||ring_slot, false, true, '')))[1]::text,'?') from trellis.segments order by seg_seq")
      reg=$(q "select count(*) from trellis.segments")
    fi
  fi
  pgrss=$(ps -C postgres -o rss= 2>/dev/null | awk '{s+=$1} END{print int(s/1024)}')
  echo -e "$(date +%T)\t$(( $(date +%s) - t0 ))\t$rss\t$hwm\t$thr\t$st\t$seg\t$reg\t$pgrss" >> "$out"
  sleep "$IV"
done
echo "# benchmark exited $(date +%T)" >> "$out"
