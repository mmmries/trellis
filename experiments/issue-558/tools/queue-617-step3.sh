#!/usr/bin/env bash
# #617 step 3: 100M on disk, control then ledger build, back to back (lane j), with a
# free-space guard. The box's snapper takes hourly btrfs snapshots of / (including /home), and
# each one pins the running cluster's data and WAL as the run rewrites them, so disk use grows
# well past the cluster's own size. The guard samples `df /` every 10 s into <tag>.dfpeak; if
# free space falls below FLOOR it stops the cluster cleanly (`pg_ctl -m fast`), kills the
# benchmark, and aborts the queue instead of letting Postgres hit ENOSPC.
# Memory cap (mandatory since 2026-09-27): every run goes in its own transient systemd scope with
# MemoryMax=$MEMCAP and no swap, so an OOM kills only that scope (benchmark + its cluster), never
# claude-rc.service. Three earlier "container restarts" were the global OOM killer taking
# `benchmark` and the session with it (RESULTS.md, experiment 5, "Step 3 blocked: drain memory").
# A cap kill shows as "exit 137" in the .out; the queue then stops rather than start the next run.
# Launch: setsid nohup ./queue-617-step3.sh > /home/mike/exp558/queue-617-step3.out 2>&1 < /dev/null &
T=/home/mike/code/trellis-lane-j/experiments/issue-558/tools
L=/home/mike/code/trellis-lane-j/experiments/issue-558/logs/exp5
DISK=/home/mike/exp558/tmpdisk
FLOOR=$((30 * 1000 * 1000 * 1000))
ABORT=/tmp/617-step3.abort
MEMCAP=${MEMCAP:-16G}
# RUNS: which of the two runs to make ("control ledger" by default; "ledger" alone after the
# control's OOM, per the user's decision on #617). RING_SAMPLE=1 adds a 30 s sampler with the
# ring's segment sizes (count(*) per segment) and the definition's status, in <tag>.ring.tsv.
RUNS=${RUNS:-control ledger}
RING_SAMPLE=${RING_SAMPLE:-0}
ARGS="--rows 100000000 --groups 1000000 --build-timeout-secs 36000 --grace-secs 1800"
rm -f "$ABORT"

clean() {
  for d in "$DISK"/trellis-testkit-*; do
    [ -d "$d" ] || continue
    echo "=== $(date +%T) cleaning leftover $d"
    [ -f "$d/data/postmaster.pid" ] && pg_ctl -D "$d/data" -m fast -w -t 300 stop 2>/dev/null
    rm -rf "$d"
  done
  df -h / | tail -1
}

quiet() {
  echo "=== $(date +%T) box check: cargo=$(pgrep -x cargo | wc -l) rustc=$(pgrep -x rustc | wc -l)" \
    "postgres=$(pgrep -x postgres | wc -l) benchmark=$(pgrep -x benchmark | wc -l)" \
    "load=$(cut -d' ' -f1-3 /proc/loadavg)"
}

guard() {
  local tag=$1 trace=$L/$1.dfpeak
  local base peak used avail minavail
  read -r base avail < <(df -B1 --output=used,avail / | tail -1)
  peak=$base; minavail=$avail
  echo "# $(date '+%F %T') baseline used=$((base / 1000000000)) GB avail=$((avail / 1000000000)) GB; floor=$((FLOOR / 1000000000)) GB" > "$trace"
  while :; do
    read -r used avail < <(df -B1 --output=used,avail / | tail -1)
    [ "$used" -gt "$peak" ] && peak=$used
    [ "$avail" -lt "$minavail" ] && minavail=$avail
    echo "$(date +%T) +$(((used - base) / 1000000000)) GB avail=$((avail / 1000000000)) GB" >> "$trace"
    echo "peak over baseline $(((peak - base) / 1000000000)) GB, min avail $((minavail / 1000000000)) GB" > "$trace.summary"
    if [ "$avail" -lt "$FLOOR" ]; then
      echo "$(date +%T) ABORT: avail below floor, stopping cluster" | tee -a "$trace" > "$ABORT"
      for d in "$DISK"/trellis-testkit-*/data; do
        [ -f "$d/postmaster.pid" ] && pg_ctl -D "$d" -m fast -w -t 300 stop >> "$trace" 2>&1
      done
      sleep 30
      pkill -x benchmark
      return
    fi
    sleep 10
  done
}

run() {
  local tag=$1; shift
  clean; quiet
  guard "$tag" & local g=$!
  mkdir -p "$L/mem"; MEMSAMPLE_DB=0 "$T/memsample.sh" "$L/mem/$tag.tsv" "$DISK" 5 > /dev/null 2>&1 & local m=$!
  local r=
  if [ "$RING_SAMPLE" = 1 ]; then
    "$T/memsample.sh" "$L/mem/$tag.ring.tsv" "$DISK" 30 > /dev/null 2>&1 & r=$!
  fi
  systemd-run --user --scope -p MemoryMax="$MEMCAP" -p MemorySwapMax=0 --unit="exp617-$tag-$$" "$@"
  local rc=$?
  kill "$g" 2>/dev/null; wait "$g" 2>/dev/null
  kill "$m" $r 2>/dev/null; wait "$m" $r 2>/dev/null
  echo "=== $(date +%T) $tag peak benchmark RSS: $(awk -F'\t' 'NR>1 && $4+0>p {p=$4} END {print p+0}' "$L/mem/$tag.tsv") MB"
  echo "=== $(date +%T) $tag disk: $(cat "$L/$tag.dfpeak.summary")"
  if [ ! -s "$L/$tag.jsonl" ]; then
    echo "$(date +%T) $tag produced no result (rc=$rc; memory cap $MEMCAP kill? see journalctl -k)" > "$ABORT"
  fi
  clean
}

cd "$T" || exit 1
case " $RUNS " in *" control "*)
  run control-100m-disk env -u TRELLIS_EXP558_LEDGER -u TRELLIS_EXP558_BUILD ./run-exp5.sh j control-100m-disk $ARGS
  if [ -e "$ABORT" ]; then echo "=== $(date +%T) aborted after control: $(cat "$ABORT")"; echo "=== $(date +%T) queue done"; exit 1; fi
esac
case " $RUNS " in *" ledger "*)
  run ledger-100m-disk env TRELLIS_EXP558_LEDGER=contrib TRELLIS_EXP558_BUILD=ledger ./run-exp5.sh j ledger-100m-disk $ARGS
  [ -e "$ABORT" ] && echo "=== $(date +%T) aborted during ledger: $(cat "$ABORT")"
esac
echo "=== $(date +%T) queue done"
