#!/usr/bin/env bash
# #565 E1 battery: the insert matrix, then a 10M-row COPY per variant. Waits for the bench lock per run.
cd ~/tc-565
O=~/tc-565/results/e1.jsonl
./venv/bin/python e1.py --variants none,slot,row,stmt --rpc 1,100,1000 --clients 1,4,16 --out $O
./venv/bin/python e1.py --variants none,stmt,row,slot --copy 10000000 --out ~/tc-565/results/e1-copy.jsonl
echo "E1 DONE $(date)"
