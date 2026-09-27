#!/usr/bin/env python3
"""Render #558 benchmark jsonl files as markdown rows. usage: tables.py rel|exp3 <label=file>..."""
import json, sys
def rows(f):
    out=[]
    for line in open(f):
        line=line.strip()
        if not line.startswith('{'): continue
        try: out.append(json.loads(line))
        except Exception: pass
    return out
kind=sys.argv[1]
files=[a.split('=',1) for a in sys.argv[2:]]
if kind=='rel':
    print('| label | children/parent | parent upd/s | converged s (tail) | oracle | WAL MB | deadlocks | rollbacks |')
    print('|---|---|---|---|---|---|---|---|')
    for label,f in files:
        for d in rows(f):
            print(f"| {label} | {d.get('children_per_parent')} | {d.get('parent_rate')} | {d.get('converged_secs')} ({d.get('tail_secs')}) | {'ok' if d.get('oracle_ok') else 'WRONG '+str(d.get('oracle_mismatched_groups'))} | {round((d.get('wal_bytes') or 0)/1e6)} | {d.get('deadlocks')} | {d.get('xact_rollbacks')} |")
else:
    print('| label | scenario | ratio / groups | workers | folded rows/s | in-window rows/s | WAL/row | oracle |')
    print('|---|---|---|---|---|---|---|---|')
    for label,f in files:
        for d in rows(f):
            print(f"| {label} | {d.get('scenario')} | {d.get('fold_in_ratio')} / {d.get('groups')} | {d.get('application_threads')} | {d.get('folded_rows_per_sec')} | {d.get('in_window_folded_rows_per_sec')} | {d.get('wal_bytes_per_row')} | {d.get('oracle_ok')} drained={d.get('drained')} |")
