#!/usr/bin/env bash
# #565 phase-2 validations queued 2026-09-26. Every python step takes /tmp/trellis-bench.lock itself.
# Storage tiers: tmpfs (/tmp) for CPU-only and correctness; NVMe (btrfs nodatacow, ~/tc-565/disk);
# slow (xfs on dm-delay, 1 ms per write, /mnt/tc565slow) as a stand-in for a hosted volume.
cd /home/mike/tc-565 || exit 1
PY=./venv/bin/python
R=/home/mike/tc-565/results
L=/home/mike/tc-565/logs; mkdir -p "$L"
export TC565_PORT=5581
REC="--ptr mirror --enc format --pin"          # the recommended trigger: mirror pointer, format('%s'), pinned GUCs
mark() { echo "=== $(date +%T) $1 (cargo procs: $(pgrep -x cargo | wc -l))"; }

# A0. Bisect the GROUP BY drift the unlocked smoke runs found under trigger capture: one script at a time.
for s in upsert range_update regroup other_only delete move twice savepoint merge; do
  mark "A0 bisect trigger read_committed $s"
  TC565_BASE=/tmp/tc-565/va $PY v_e2e.py correctness --capture trigger --scripts $s --iso read_committed --secs 60 --clients 16 \
    --timeout 300 --out $R/v_bisect.jsonl >> $L/va0.out 2>&1
done

# A. Correctness through the real seal/drain, trigger vs today's slot, at each isolation level.
for iso in read_committed repeatable_read serializable; do
  for cap in trigger slot; do
    mark "A correctness $cap $iso"
    TC565_BASE=/tmp/tc-565/va $PY v_e2e.py correctness --capture $cap --iso $iso --secs 120 --clients 16 --timeout 900 \
      --out $R/v_correctness.jsonl >> $L/va.out 2>&1
  done
done
mark "A correctness trigger read_committed skip_noop"
TC565_BASE=/tmp/tc-565/va $PY v_e2e.py correctness --capture trigger --iso read_committed --shape skip_noop --secs 120 --clients 16 \
  --timeout 900 --out $R/v_correctness.jsonl >> $L/va.out 2>&1

# B. Cost of the phase-2 trigger shapes (tmpfs, CPU only): NEW-only is free to measure later on the ledger;
#    here: skip_noop (item 4) and reread (the nested-write fix candidate).
for wl in insert update update_other; do
  mark "B shapes $wl none"
  TC565_BASE=/tmp/tc-565/vb $PY e1.py --variants none --rpc 1000 --clients 1,16 --workload $wl $REC --out $R/v_shapes.jsonl >> $L/vb.out 2>&1
  case $wl in
    insert) shapes=", reread" ;;
    update) shapes=", skip_noop, reread, reread;skip_noop" ;;
    update_other) shapes=", skip_noop, reread;skip_noop" ;;
  esac
  IFS=',' read -ra SH <<< "$shapes"
  for sh in "${SH[@]}"; do
    sh=$(echo "$sh" | xargs | tr ';' ',')
    mark "B shapes $wl stmt [$sh]"
    TC565_BASE=/tmp/tc-565/vb $PY e1.py --variants stmt --rpc 1000 --clients 1,16 --workload $wl --shape "$sh" $REC \
      --out $R/v_shapes.jsonl >> $L/vb.out 2>&1
  done
done

# C. Drain backpressure on the NVMe: capture at a paced rate vs the real drain.
DISK="/home/mike/tc-565/disk/run"
CK="checkpoint_timeout=30s;max_wal_size=4GB"
mark "C backpressure 1-1, 4 drain threads"
TC565_BASE=$DISK $PY v_e2e.py backpressure --rates 25000,50000,100000,200000 --rpc 100 --clients 16 --secs 120 \
  --drain-threads 4 --timeout 1800 --pg "$CK" --out $R/v_backpressure.jsonl >> $L/vc.out 2>&1
mark "C backpressure 1-1, 8 drain threads"
TC565_BASE=$DISK $PY v_e2e.py backpressure --rates 100000,200000 --rpc 100 --clients 16 --secs 120 \
  --drain-threads 8 --timeout 1800 --pg "$CK" --out $R/v_backpressure.jsonl >> $L/vc.out 2>&1
mark "C backpressure 1-1 + GROUP BY, 4 drain threads"
TC565_BASE=$DISK $PY v_e2e.py backpressure --aggregate --rates 50000,100000 --rpc 100 --clients 16 --secs 120 \
  --drain-threads 4 --timeout 1800 --pg "$CK" --out $R/v_backpressure.jsonl >> $L/vc.out 2>&1

# D. E1 on the NVMe with the recommended trigger and a 30 s checkpoint (so full-page writes land).
for v in none stmt idx_btree slot; do
  mark "D e1 disk $v rpc 1"
  TC565_BASE=$DISK $PY e1.py --variants $v --rpc 1 --clients 1,16 --duration 50000 $REC --pg "$CK" --out $R/v_e1_disk.jsonl >> $L/vd.out 2>&1
  mark "D e1 disk $v rpc 1000"
  TC565_BASE=$DISK $PY e1.py --variants $v --rpc 1000 --clients 1,16 $REC --pg "$CK" --out $R/v_e1_disk.jsonl >> $L/vd.out 2>&1
done

# E. E2 on the NVMe: where the ceiling moves from CPU to WAL bandwidth; plus the device's own numbers.
mark "E device: pg_test_fsync + sustained direct write"
( exec 9>/tmp/trellis-bench.lock; flock 9
  pg_test_fsync -f /home/mike/tc-565/disk/fsync.test -s 3 > $L/ve-device.out 2>&1
  dd if=/dev/zero of=/home/mike/tc-565/disk/dd.test bs=1M count=8192 oflag=direct conv=fsync >> $L/ve-device.out 2>&1
  rm -f /home/mike/tc-565/disk/fsync.test /home/mike/tc-565/disk/dd.test
  pg_test_fsync -f /mnt/tc565slow/fsync.test -s 3 > $L/ve-device-slow.out 2>&1; rm -f /mnt/tc565slow/fsync.test )
for v in none stmt; do
  mark "E e2 disk $v"
  TC565_BASE=$DISK $PY e1.py --variants $v --rpc 1000 --clients 1,2,4,8,16,32 $REC --pg "$CK" --out $R/v_e2_disk.jsonl >> $L/ve.out 2>&1
done

# F. Trellis down for 10 minutes on the NVMe: retained WAL (slot) vs ring growth (triggers).
mark "F down trigger,slot 20k rows/s x 600 s"
TC565_BASE=$DISK $PY v_e2e.py down --capture trigger,slot --rates 20000 --rpc 100 --clients 4 --secs 600 --pg "$CK" \
  --out $R/v_down.jsonl >> $L/vf.out 2>&1

# G. The slow volume (1 ms per write): E1's headline cells where commit latency dominates.
SLOW=/mnt/tc565slow/run
for v in none stmt slot; do
  mark "G e1 slow $v rpc 1"
  TC565_BASE=$SLOW $PY e1.py --variants $v --rpc 1 --clients 1,16 --duration 20000 $REC --pg "$CK" --out $R/v_e1_slow.jsonl >> $L/vg.out 2>&1
  mark "G e1 slow $v rpc 1000"
  TC565_BASE=$SLOW $PY e1.py --variants $v --rpc 1000 --clients 1,16 --duration 1000000 $REC --pg "$CK" --out $R/v_e1_slow.jsonl >> $L/vg.out 2>&1
done
mark "all done"
