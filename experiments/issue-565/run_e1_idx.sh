#!/usr/bin/env bash
# Runs after run_e1.sh: the index comparison points, plus a same-session control/stmt re-run.
cd ~/tc-565
while ! grep -q "E1 DONE" results/e1.out 2>/dev/null; do sleep 10; done
./venv/bin/python e1.py --variants none,idx_btree,idx_regex,stmt --rpc 1,100,1000 --clients 1,4,16 --out ~/tc-565/results/e1-idx.jsonl
./venv/bin/python e1.py --variants idx_btree,idx_regex --copy 10000000 --out ~/tc-565/results/e1-copy.jsonl
echo "IDX DONE $(date)"
