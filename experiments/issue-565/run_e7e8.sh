#!/usr/bin/env bash
# #565 E7 (join/drop locking) and E8 (idle cost), after E9, each holding the bench lock.
cd ~/tc-565
while ! grep -q "E9 DONE" results/e3e9.out 2>/dev/null; do sleep 10; done
flock -x /tmp/trellis-bench.lock ./venv/bin/python e7_join_lock.py publication trigger trigger_lt publication_drop drop_trigger drop_trigger_lt
echo "E7 DONE $(date)"
flock -x /tmp/trellis-bench.lock ./venv/bin/python e8_idle.py staging no_staging staging no_staging
echo "E8 DONE $(date)"
