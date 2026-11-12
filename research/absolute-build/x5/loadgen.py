#!/usr/bin/env python3
"""X5 writer load for #1917: paced small transactions against X3's `posts`.

usage: loadgen.py DSN N RATE WORKERS OUTDIR [SEED]
  N       rows generated (ids 1..N, grp_hi 0..N-1, author_id 1..N/10)
  RATE    target row changes per second, all workers together
Runs until SIGTERM/SIGINT. Each worker writes OUTDIR/w<i>.tsv:
  t_start(epoch s)  dur_ms  rows_changed  err(0/1/2: ok/deadlock-or-serialization/other)
Mix per operation (each transaction = K operations, ~1 row each):
  30% insert into an existing group     10% insert creating a new group (new grp_hi, new author)
  30% update likes/score                15% move a row to another group (grp_hi and author_id)
  13% delete a random row (s2_agg_hi: empties its group)
   2% delete every row of one author (s2_agg: empties a group, ~10 rows)
Keys: 80% uniform over the whole key space, 20% from a hot subset (ids 1..1000, authors 1..100).
"""
import os, random, signal, sys, time
import multiprocessing as mp
import psycopg

K = 4  # operations per transaction
HOT_IDS, HOT_AUTH = 1000, 100


def worker(i, dsn, n, tx_rate, outdir, seed):
    rnd = random.Random(seed * 1000 + i)
    stop = {"v": False}
    signal.signal(signal.SIGTERM, lambda *_: stop.__setitem__("v", True))
    signal.signal(signal.SIGINT, lambda *_: stop.__setitem__("v", True))
    conn = psycopg.connect(dsn, application_name=f"x5-writer-{i}")
    cur = conn.cursor()
    nauth = max(1, n // 10)
    # ids of new rows: disjoint per worker, above every generated id
    next_id = 100_000_000 + i * 10_000_000
    new_grp = 1_000_000_000 + i * 10_000_000  # new grp_hi values
    new_auth = 1_000_000_000 + i * 10_000_000  # new author ids
    mine = []  # ids this worker inserted (deletable)
    out = open(os.path.join(outdir, f"w{i}.tsv"), "w", buffering=1 << 16)
    period = 1.0 / tx_rate
    nxt = time.monotonic() + rnd.random() * period

    def rid():
        if rnd.random() < 0.2:
            return rnd.randint(1, HOT_IDS)
        if mine and rnd.random() < 0.1:
            return rnd.choice(mine)
        return rnd.randint(1, n)

    def rauth():
        return rnd.randint(1, HOT_AUTH) if rnd.random() < 0.2 else rnd.randint(1, nauth)

    def rgrp():
        return rnd.randint(0, 999) if rnd.random() < 0.2 else rnd.randint(0, n - 1)

    while not stop["v"]:
        now = time.monotonic()
        if now < nxt:
            time.sleep(nxt - now)
            continue
        nxt += period
        if nxt < now - 1.0:  # fell more than a second behind: don't burst
            nxt = now
        ops = []
        for _ in range(K):
            p = rnd.random()
            if p < 0.30:
                next_id += 1
                ops.append(("insert into posts values (%s,%s,1,1,%s,%s,%s,'x5')",
                            (next_id, rauth(), rgrp(), rnd.randint(0, 1000), rnd.randint(0, 999999) / 100), next_id))
            elif p < 0.40:
                next_id += 1; new_grp += 1; new_auth += 1
                ops.append(("insert into posts values (%s,%s,1,1,%s,%s,%s,'x5')",
                            (next_id, new_auth, new_grp, rnd.randint(0, 1000), rnd.randint(0, 999999) / 100), next_id))
            elif p < 0.70:
                ops.append(("update posts set likes = likes + 1, score = score + 0.5 where id = %s", (rid(),), None))
            elif p < 0.85:
                ops.append(("update posts set grp_hi = %s, author_id = %s where id = %s", (rgrp(), rauth(), rid()), None))
            elif p < 0.98:
                ops.append(("delete from posts where id = %s", (rid(),), None))
            else:
                ops.append(("delete from posts where author_id = %s", (rauth(),), None))
        t0 = time.time(); m0 = time.monotonic()
        rows = 0; err = 0
        try:
            for sql, args, ins in ops:
                cur.execute(sql, args)
                rows += max(cur.rowcount, 0)
            conn.commit()
            for _, _, ins in ops:
                if ins is not None:
                    mine.append(ins)
            if len(mine) > 5000:
                del mine[:2500]
        except (psycopg.errors.DeadlockDetected, psycopg.errors.SerializationFailure):
            conn.rollback(); err = 1; rows = 0
        except Exception as e:  # keep going, record it
            try:
                conn.rollback()
            except Exception:
                conn = psycopg.connect(dsn, application_name=f"x5-writer-{i}"); cur = conn.cursor()
            err = 2; rows = 0
            sys.stderr.write(f"w{i}: {type(e).__name__}: {e}\n")
        out.write(f"{t0:.4f}\t{(time.monotonic() - m0) * 1000:.3f}\t{rows}\t{err}\n")
    out.close()
    conn.close()


def main():
    dsn, n, rate, workers, outdir = sys.argv[1], int(sys.argv[2]), float(sys.argv[3]), int(sys.argv[4]), sys.argv[5]
    seed = int(sys.argv[6]) if len(sys.argv) > 6 else 1
    os.makedirs(outdir, exist_ok=True)
    tx_rate = rate / K / workers
    ps = [mp.Process(target=worker, args=(i, dsn, n, tx_rate, outdir, seed)) for i in range(workers)]
    for p in ps:
        p.start()

    def fwd(*_):
        for p in ps:
            if p.is_alive():
                os.kill(p.pid, signal.SIGTERM)
    signal.signal(signal.SIGTERM, fwd)
    signal.signal(signal.SIGINT, fwd)
    for p in ps:
        p.join()


if __name__ == "__main__":
    main()
