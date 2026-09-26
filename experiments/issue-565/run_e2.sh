#!/usr/bin/env bash
# #565 E2: capture ceiling vs writer count, with wait-event sampling.
cd ~/tc-565
O=~/tc-565/results/e2.jsonl
./venv/bin/python e1.py --variants none,stmt --rpc 1,1000 --clients 1,2,4,8,16,32 --out $O
./venv/bin/python e1.py --variants stmt --rpc 1,1000 --clients 1,2,4,8,16,32 --ptr seq --seq-cache 64 --out $O
echo "E2 DONE $(date)"
