#!/usr/bin/env python3
"""Rate-limited load on the soak schema that reconnects on every error.

Each thread runs single-statement autocommit transactions drawn from MIX, paced so
the threads together offer TPS. Any error (a restart, a terminated backend, a
refused connection) closes the connection; the next statement reconnects. Every 30 s
it prints the rate and the errors seen, and at the end one JSON line:
{"ok": ..., "err": ..., "rows": ...}, where rows is the sum of affected rows.
"""

import argparse
import json
import random
import threading
import time

import psycopg2

# name -> (default weight, SQL); %(…)s parameters come from params().
OPS = {
    "insert_order": (
        35,
        "insert into orders(account_id, amount, status) values (%(aid)s, %(amt)s / 100.0, %(status)s)",
    ),
    "update_order": (25, "update orders set amount = amount + 1, status = 'paid' where id = %(oid)s"),
    "delete_order": (8, "delete from orders where id = %(oid)s"),
    "update_balance": (
        22,
        "update accounts set balance = balance + %(amt)s / 1000.0, updated_at = now() where id = %(aid)s",
    ),
    "move_account": (4, "update accounts set region = %(reg)s, active = %(flag)s where id = %(aid)s"),
    "upsert_account": (
        3,
        "insert into accounts(id, region, name, balance)"
        " values (%(aid)s, %(reg)s, 'acct ' || %(aid)s, %(amt)s / 100.0)"
        " on conflict (id) do update set name = excluded.name || 'x'",
    ),
    "delete_account": (3, "delete from accounts where id = %(aid)s"),
}


def parse_mix(text):
    """'insert_order=50,delete_order=0' -> weights, the rest at their defaults."""
    weights = {name: w for name, (w, _) in OPS.items()}
    for part in filter(None, (text or "").split(",")):
        name, _, value = part.partition("=")
        if name not in OPS or not value.isdigit():
            raise ValueError(f"bad mix entry {part!r}; ops are {', '.join(OPS)}")
        weights[name] = int(value)
    if not any(weights.values()):
        raise ValueError("the mix has no positive weight")
    return weights


def params(accounts, orders):
    # Ids range 20% past the seed so some writes miss and some inserts are new keys.
    return {
        "aid": random.randint(1, int(accounts * 1.2)),
        "oid": random.randint(1, int(orders * 1.2)),
        "amt": random.randint(1, 50000),
        "reg": random.randint(1, 50),
        "flag": random.random() < 0.5,
        "status": random.choice(("paid", "open")),
    }


class Counters:
    def __init__(self):
        self.lock = threading.Lock()
        self.ok = self.err = self.rows = 0
        self.errors = {}

    def error(self, exc):
        first = (str(exc).strip().splitlines() or [""])[0][:80]
        key = f"{getattr(exc, 'pgcode', None) or type(exc).__name__} {first}"
        with self.lock:
            self.err += 1
            self.errors[key] = self.errors.get(key, 0) + 1


def worker(args, weights, stop_at, counters):
    names, cum = list(weights), list(weights.values())
    period = args.threads / args.tps
    conn, due = None, time.time()
    while time.time() < stop_at:
        try:
            if conn is None:
                conn = psycopg2.connect(args.dsn, connect_timeout=5)
                conn.autocommit = True
            name = random.choices(names, weights=cum)[0]
            with conn.cursor() as cur:
                cur.execute(OPS[name][1], params(args.accounts, args.orders))
                affected = max(cur.rowcount, 0)
            with counters.lock:
                counters.ok += 1
                counters.rows += affected
        except Exception as exc:  # any error: drop the connection and reconnect
            counters.error(exc)
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
            conn = None
            time.sleep(0.2)
        due += period
        delay = due - time.time()
        if delay > 0:
            time.sleep(delay)
        elif delay < -5:  # far behind (an outage): don't burst to catch up
            due = time.time()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dsn", required=True)
    ap.add_argument("--threads", type=int, default=12)
    ap.add_argument("--tps", type=float, default=1000)
    ap.add_argument("--secs", type=int, default=120)
    ap.add_argument("--accounts", type=int, required=True, help="seeded account count")
    ap.add_argument("--orders", type=int, required=True, help="seeded order count")
    ap.add_argument(
        "--mix", default="", help="op=weight,…; ops: " + ", ".join(f"{n} ({w})" for n, (w, _) in OPS.items())
    )
    args = ap.parse_args()
    weights = parse_mix(args.mix)
    counters, stop_at = Counters(), time.time() + args.secs
    threads = [
        threading.Thread(target=worker, args=(args, weights, stop_at, counters), daemon=True)
        for _ in range(args.threads)
    ]
    for t in threads:
        t.start()
    last_ok = last_err = 0
    last_at = time.time()
    while time.time() < stop_at:
        time.sleep(min(30, max(0.1, stop_at - time.time())))
        with counters.lock:
            now = time.time()
            top = dict(sorted(counters.errors.items(), key=lambda kv: -kv[1])[:4])
            rate = round((counters.ok - last_ok) / max(now - last_at, 0.001))
            print(time.strftime("%T"), "ok/s", rate, "err", counters.err - last_err, top, flush=True)
            last_ok, last_err, last_at = counters.ok, counters.err, now
            counters.errors.clear()
    for t in threads:
        t.join()
    print(json.dumps({"ok": counters.ok, "err": counters.err, "rows": counters.rows}), flush=True)


if __name__ == "__main__":
    main()
