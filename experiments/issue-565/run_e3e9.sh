#!/usr/bin/env bash
# #565 E3 encoding cost + TOAST, and E9 wide rows. Runs after E2.
cd ~/tc-565
while ! grep -q "E2 DONE" results/e2.out 2>/dev/null; do sleep 10; done
O=~/tc-565/results/e3.jsonl
./venv/bin/python e1.py --variants none,stmt --rpc 1,1000 --clients 1,16 --enc coltext --out $O
./venv/bin/python e1.py --variants stmt --rpc 1,1000 --clients 1,16 --enc hstore --out $O
./venv/bin/python e1.py --variants stmt --rpc 1,1000 --clients 1,16 --enc hstore --pin --out $O
for v in none slot; do ./venv/bin/python e1.py --variants $v --rpc 1,10 --clients 1 --workload toast_update --out $O; done
./venv/bin/python e1.py --variants stmt --rpc 1,10 --clients 1 --workload toast_update --enc hstore --pin --out $O
./venv/bin/python e1.py --variants stmt --rpc 1,10 --clients 1 --workload toast_update --enc coltext_narrow --out $O
echo "E3 DONE $(date)"
W=~/tc-565/results/e9.jsonl
./venv/bin/python e1.py --variants none,slot --rpc 1,100,1000 --clients 1,16 --wide --out $W
./venv/bin/python e1.py --variants stmt --rpc 1,100,1000 --clients 1,16 --wide --enc hstore --pin --out $W
echo "E9 DONE $(date)"
