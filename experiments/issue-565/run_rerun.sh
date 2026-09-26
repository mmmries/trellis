#!/usr/bin/env bash
# Re-runs lost to the harness's ProcessLookupError race, plus E7 after its DDL-split fix.
cd ~/tc-565
O=~/tc-565/results/e3.jsonl
./venv/bin/python e1.py --variants none --rpc 1000 --clients 1,16 --enc coltext --out $O
./venv/bin/python e1.py --variants stmt --rpc 1,1000 --clients 1,16 --enc coltext --out $O
./venv/bin/python e1.py --variants stmt --rpc 1000 --clients 1,16 --enc hstore --pin --out $O
./venv/bin/python e1.py --variants stmt --rpc 10 --clients 1 --workload toast_update --enc coltext_narrow --out $O
echo "RERUN E3 DONE $(date)"
rm -f results/e7.jsonl.tmp; grep -v '"mode": "publication"' results/e7.jsonl > /dev/null
flock -x /tmp/trellis-bench.lock ./venv/bin/python e7_join_lock.py trigger trigger_lt publication_drop drop_trigger drop_trigger_lt
echo "RERUN E7 DONE $(date)"
