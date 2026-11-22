#!/usr/bin/env python3
"""Rate-limited load on the soak schema that reconnects on every error.

Each thread runs single-statement autocommit transactions drawn from MIX, paced so
the threads together offer the rate of the current SCHEDULE segment. A schedule is
`PROFILE:SECS[:RATE],...`; the profiles are idle (no writes), trickle (default one a
minute), steady (default TPS) and spike (default 10x TPS). RATE is `N` per second,
`N/m` per minute or `xN` times TPS. Any error (a restart, a terminated backend, a
refused connection) closes the connection; the next statement reconnects. Every 30 s
it prints the rate and the errors seen, and at the end one JSON line:
{"ok": ..., "err": ..., "rows": ..., "segments": [[profile, ok, err], ...]}, where
rows is the sum of affected rows.
"""

import argparse
import json
import math
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


PROFILES = {"idle": "0", "trickle": "1/m", "steady": "x1", "spike": "x10"}  # default rates


def parse_rate(text, base_tps):
    """'250' -> 250 per second, '2/m' -> 2 per minute, 'x10' -> 10 * base_tps."""
    try:
        if text.startswith("x"):
            v = float(text[1:]) * base_tps
        elif text.endswith("/m"):
            v = float(text[:-2]) / 60
        else:
            v = float(text)
    except ValueError:
        raise ValueError(f"bad rate {text!r}; use N (per second), N/m (per minute) or xN (times --tps)") from None
    if not 0 <= v < math.inf:
        raise ValueError(f"rate {text!r} is negative or not finite")
    return v


def parse_schedule(text, base_tps):
    """'steady:60,idle:3600,spike:60:x100' -> [(profile, secs, tps), ...]."""
    segments = []
    for part in filter(None, (p.strip() for p in (text or "").split(","))):
        profile, *rest = part.split(":")
        if profile not in PROFILES or not 1 <= len(rest) <= 2:
            raise ValueError(f"bad schedule segment {part!r}; use PROFILE:SECS[:RATE], profiles {', '.join(PROFILES)}")
        try:
            secs = float(rest[0])
        except ValueError:
            raise ValueError(f"bad duration in {part!r}") from None
        if not 0 < secs < math.inf:
            raise ValueError(f"segment {part!r} has no duration")
        if profile == "idle" and len(rest) == 2:
            raise ValueError(f"idle takes no rate: {part!r}")
        segments.append((profile, secs, parse_rate(rest[1] if len(rest) == 2 else PROFILES[profile], base_tps)))
    if not segments:
        raise ValueError("the schedule is empty")
    return segments


class Pacer:
    """Hands out statement start times so all threads together follow the schedule.

    A slot never jumps over a segment boundary, so a slow segment (a trickle) doesn't
    delay the first statement of the next one, and an idle segment hands out none. A
    backlog never carries into a later segment or past the end, so a spike the threads
    can't keep up with doesn't keep writing into the idle after it."""

    def __init__(self, segments, start):
        self.lock = threading.Lock()
        self.bounds = []  # (start, end, index, tps)
        t = start
        for i, (_, secs, tps) in enumerate(segments):
            self.bounds.append((t, t + secs, i, tps))
            t += secs
        self.end = t
        self.next = start

    def segment_at(self, t):
        for s, e, i, tps in self.bounds:
            if s <= t < e:
                return s, e, i, tps
        return None

    def take(self, now):
        """(start time, segment index) of the next statement, or None after the schedule."""
        with self.lock:
            if now >= self.end:
                return None
            t = self.next
            if now - t > 5:  # far behind (an outage, or more rate than the threads give): don't burst
                t = now
            current = self.segment_at(now)
            if current and t < current[0]:  # behind into a later segment: drop the backlog
                t = now
            while True:
                seg = self.segment_at(t)
                if seg is None:
                    return None
                s, e, i, tps = seg
                if tps > 0:
                    break
                t = e
            self.next = min(t + 1 / tps, e)
            return t, i


class Counters:
    def __init__(self, nsegments=1):
        self.lock = threading.Lock()
        self.ok = self.err = self.rows = 0
        self.errors = {}
        self.seg_ok = [0] * nsegments
        self.seg_err = [0] * nsegments

    def error(self, exc, seg=0):
        first = (str(exc).strip().splitlines() or [""])[0][:80]
        key = f"{getattr(exc, 'pgcode', None) or type(exc).__name__} {first}"
        with self.lock:
            self.err += 1
            self.seg_err[seg] += 1
            self.errors[key] = self.errors.get(key, 0) + 1


IDLE_PING_SECS = 2.0


def revive(conn):
    """A connection that sat unused may have been broken by a Postgres restart meanwhile.
    Probe it with a statement that writes nothing, and reconnect if it fails, so the next write
    doesn't report the harness's own dead connection as an error. A write is never retried: one
    that fails still counts, whatever the cause."""
    try:
        with conn.cursor() as cur:
            cur.execute("select 1")
        return conn
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        return None


def worker(args, weights, pacer, counters):
    names, cum = list(weights), list(weights.values())
    conn = None
    last_used = 0.0
    while True:
        slot = pacer.take(time.time())
        if slot is None:
            break
        due, seg = slot
        delay = due - time.time()
        if delay > 0:
            time.sleep(delay)
        if conn is not None and time.time() - last_used > IDLE_PING_SECS:
            conn = revive(conn)
        try:
            if conn is None:
                conn = psycopg2.connect(args.dsn, connect_timeout=5)
                conn.autocommit = True
            name = random.choices(names, weights=cum)[0]
            with conn.cursor() as cur:
                cur.execute(OPS[name][1], params(args.accounts, args.orders))
                affected = max(cur.rowcount, 0)
            last_used = time.time()
            with counters.lock:
                counters.ok += 1
                counters.seg_ok[seg] += 1
                counters.rows += affected
        except Exception as exc:  # any error: drop the connection and reconnect
            counters.error(exc, seg)
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
            conn = None
            time.sleep(0.2)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dsn", required=True)
    ap.add_argument("--threads", type=int, default=12)
    ap.add_argument("--tps", type=float, default=1000)
    ap.add_argument("--secs", type=int, default=120, help="shorthand for --schedule steady:SECS")
    ap.add_argument("--schedule", help="PROFILE:SECS[:RATE],... (see above); overrides --secs")
    ap.add_argument("--start-at", type=float, help="epoch seconds the schedule starts at (default now)")
    ap.add_argument("--accounts", type=int, required=True, help="seeded account count")
    ap.add_argument("--orders", type=int, required=True, help="seeded order count")
    ap.add_argument(
        "--mix", default="", help="op=weight,…; ops: " + ", ".join(f"{n} ({w})" for n, (w, _) in OPS.items())
    )
    args = ap.parse_args()
    weights = parse_mix(args.mix)
    segments = parse_schedule(args.schedule or f"steady:{args.secs}", args.tps)
    pacer = Pacer(segments, args.start_at or time.time())
    counters = Counters(len(segments))
    threads = [
        threading.Thread(target=worker, args=(args, weights, pacer, counters), daemon=True) for _ in range(args.threads)
    ]
    for t in threads:
        t.start()
    last_ok = last_err = 0
    last_at = time.time()
    while time.time() < pacer.end:
        time.sleep(min(30, max(0.1, pacer.end - time.time())))
        with counters.lock:
            now = time.time()
            seg = pacer.segment_at(now)
            profile = segments[seg[2]][0] if seg else "end"
            top = dict(sorted(counters.errors.items(), key=lambda kv: -kv[1])[:4])
            rate = round((counters.ok - last_ok) / max(now - last_at, 0.001))
            print(time.strftime("%T"), profile, "ok/s", rate, "err", counters.err - last_err, top, flush=True)
            last_ok, last_err, last_at = counters.ok, counters.err, now
            counters.errors.clear()
    for t in threads:
        t.join()
    per_segment = [[p, ok, err] for (p, _, _), ok, err in zip(segments, counters.seg_ok, counters.seg_err, strict=True)]
    print(
        json.dumps({"ok": counters.ok, "err": counters.err, "rows": counters.rows, "segments": per_segment}),
        flush=True,
    )


if __name__ == "__main__":
    main()
