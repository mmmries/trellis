"""X1 soak (#1917): variant x seed x appliers, short runs with builds throughout.

One run: a fresh database; the source is seeded without capture (a fresh
build over a pre-existing source); writers, appliers (pages), one builder and
one GC run concurrently for `duration` seconds. The builder loops: the first
build is fresh on an empty hidden table; later ones are, at random, in place,
in place after corrupting h (some rows perturbed, some deleted), or fresh
(h emptied first). Then writers stop, the builder finishes with a successful
build, appliers drain the ring to empty and h is compared with the source.

Usage:
  python3 soak.py one VARIANT SEED APPLIERS [DURATION]
  python3 soak.py matrix OUT.jsonl [--parallel N] [--duration S] [--plan NAME]
"""

import json
import random
import statistics
import sys
import threading
import time
import traceback

import model as M


class Run:
    def __init__(self, variant, seed, appliers, duration=20.0, writers=4, groups=6, db=None, jitter_ms=40,
                 checkpoint_every=2.0):
        self.checkpoint_every = checkpoint_every
        self.variant = variant
        # X1b: per-process module state (one run per worker process at a time)
        M.TARGET = M.has_target(variant)
        M.RETAIN = True
        self.target = M.TARGET
        self.seed = seed
        self.nappliers = appliers
        self.duration = duration
        self.nwriters = writers
        self.groups = groups
        self.db = db or f"x1_soak_{variant.replace('-', '_').lower()}_{seed}_{appliers}"
        self.jitter_ms = jitter_ms
        self.stop_writers = threading.Event()
        self.stop_builder = threading.Event()
        self.stop_appliers = threading.Event()
        self.stop_gc = threading.Event()
        self.pause_writers = threading.Event()
        self.pause_builder = threading.Event()
        self.writers_idle = threading.Semaphore(0)
        self.builder_idle = threading.Event()
        self.checkpoints = []
        self.lock = threading.Lock()
        self.next_id = 1
        self.m = dict(
            rows_written=0, writer_commits=0, writer_rollbacks=0, writer_deadlocks=0,
            page_deadlocks=0, page_commits=0, build_deadlocks=0, gc_deleted=0, gc_skipped=0,
            builds={"fresh": 0, "inplace": 0, "corrupt": 0, "resume": 0}, errors=[],
            xleaf=0, uncaptured_rows=0,
        )
        self.lock_waits = []
        self.stragglers = []  # (mode, stragglers, enumerated)
        self.t_start = None

    # ------------------------------------------------------------------
    def new_id(self):
        with self.lock:
            i = self.next_id
            self.next_id += 1
            return i

    def pick_g(self, rng):
        if rng.random() < 0.1:
            return None
        return min(int(rng.expovariate(0.35)), self.groups - 1)

    def seed_source(self, rng):
        c = M.connect(self.db)
        c.execute("set session_replication_role = replica")  # no capture: pre-existing source
        for _ in range(8):
            c.execute("insert into src values (%s, %s, %s)", (self.new_id(), self.pick_g(rng), rng.randint(-5, 20)))
        c.commit()
        c.close()

    def writer(self, k):
        rng = random.Random(self.seed * 1000 + k)
        c = M.connect(self.db)
        try:
            while not self.stop_writers.is_set():
                if self.pause_writers.is_set():
                    self.writers_idle.release()
                    while self.pause_writers.is_set() and not self.stop_writers.is_set():
                        time.sleep(0.005)
                    continue
                rows = 0
                try:
                    cur = c.cursor()
                    for _ in range(rng.randint(1, 4)):
                        op = rng.random()
                        maxid = self.next_id
                        r = rng.randint(1, max(1, maxid))
                        pick = "(select id from src where id >= %s order by id limit 1)"
                        cur.execute("select count(*) from src")
                        size = cur.fetchone()[0]
                        p_ins = 0.45 if size < 8 else 0.2
                        if op < p_ins:
                            cur.execute("insert into src values (%s, %s, %s)", (self.new_id(), self.pick_g(rng), rng.randint(-5, 20)))
                        elif op < p_ins + 0.15:
                            cur.execute(f"update src set v = %s where id = {pick}", (rng.randint(-5, 20), r))
                        elif op < p_ins + 0.45:
                            cur.execute(f"update src set g = %s where id = {pick}", (self.pick_g(rng), r))
                        else:
                            cur.execute(f"delete from src where id = {pick}", (r,))
                        rows += cur.rowcount
                        if rng.random() < 0.15:
                            time.sleep(rng.uniform(0.01, 0.15))  # hold the transaction open
                    if rng.random() < 0.1:
                        c.rollback()
                        with self.lock:
                            self.m["writer_rollbacks"] += 1
                    else:
                        c.commit()
                        with self.lock:
                            self.m["writer_commits"] += 1
                            self.m["rows_written"] += rows
                except Exception as e:  # noqa
                    c.rollback()
                    if M.sqlstate(e) == "40P01":
                        with self.lock:
                            self.m["writer_deadlocks"] += 1
                    else:
                        self.err("writer", e)
                        return
                time.sleep(rng.uniform(0, 0.01))
        finally:
            c.close()

    def applier(self, k):
        rng = random.Random(self.seed * 2000 + k)
        c = M.connect(self.db)
        try:
            while not self.stop_appliers.is_set():
                p = M.Page(c, gate=M.page_gate(self.variant))
                try:
                    n = p.run(batch=rng.randint(1, 20), order=rng.choice(["asc", "desc", "random"]))
                    if n:
                        with self.lock:
                            self.m["page_commits"] += 1
                            self.lock_waits.append(p.lock_wait)
                        if not self.stop_writers.is_set():
                            time.sleep(rng.uniform(0, 0.03))  # let a backlog build up
                    else:
                        time.sleep(0.002)
                except Exception as e:  # noqa
                    c.rollback()
                    if M.sqlstate(e) == "40P01":
                        with self.lock:
                            self.m["page_deadlocks"] += 1
                            self.dl_detail("page", e)
                    else:
                        self.err("applier", e)
                        return
        finally:
            c.close()

    def builder(self):
        rng = random.Random(self.seed * 3000)
        c = M.connect(self.db)
        c.execute("select set_config('x1.sleep_ms', %s, false)", (str(self.jitter_ms),))
        c.commit()
        dirty = True  # h empty, source seeded: needs a build
        first = True
        try:
            while True:
                if self.stop_builder.is_set() and not dirty:
                    return
                if self.pause_builder.is_set() and not dirty:
                    self.builder_idle.set()
                    while self.pause_builder.is_set() and not self.stop_builder.is_set():
                        time.sleep(0.005)
                    self.builder_idle.clear()
                    continue
                if first:
                    mode = "fresh"
                elif self.stop_builder.is_set() or self.pause_builder.is_set():
                    mode = "inplace"
                else:
                    x = rng.random()
                    if self.target:
                        mode = "inplace" if x < 0.4 else ("resume" if x < 0.75 else ("corrupt" if x < 0.85 else "fresh"))
                    else:
                        mode = "inplace" if x < 0.5 else ("corrupt" if x < 0.75 else "fresh")
                try:
                    if mode == "fresh" and not first:
                        # key-ordered, like every other writer of h (an unordered
                        # delete deadlocks with pages: a harness artifact)
                        c.execute("delete from h where ctid = any(array(select ctid from h order by g for update))")
                        if self.target:
                            c.execute("delete from tgt")  # a true fresh build: no target either
                        c.commit()
                        dirty = True
                    elif mode == "resume":
                        # changes the ring never saw (a capture disruption), then
                        # the hidden table emptied with the visible target kept
                        cur = c.cursor()
                        cur.execute("set local session_replication_role = replica")
                        k = 0
                        for _ in range(rng.randint(1, 3)):
                            r = rng.randint(1, max(1, self.next_id))
                            pick = "(select id from src where id >= %s order by id limit 1)"
                            if rng.random() < 0.5:
                                cur.execute(f"delete from src where id = {pick}", (r,))
                            else:
                                cur.execute(f"update src set g = %s where id = {pick}", (self.pick_g(rng), r))
                            k += cur.rowcount
                        c.commit()
                        with self.lock:
                            self.m["uncaptured_rows"] += k
                        c.execute("delete from h where ctid = any(array(select ctid from h order by g for update))")
                        c.commit()
                        dirty = True
                    elif mode == "corrupt":
                        cur = c.cursor()
                        cur.execute("select g from h where random() < 0.3 order by g for update")
                        ks = [r[0] for r in cur.fetchall()]
                        up = [k for k in ks if rng.random() < 0.5]
                        dl = [k for k in ks if k not in up]
                        cur.execute("update h set n = n + 1, s = s + 3 where g = any(%s) or (g is null and %s)",
                                    ([k for k in up if k is not None], None in up))
                        cur.execute("delete from h where g = any(%s) or (g is null and %s)",
                                    ([k for k in dl if k is not None], None in dl))
                        c.commit()
                        dirty = True
                    info = {}
                    M.build(c, self.variant, info)
                    dirty = False
                    first = False
                    with self.lock:
                        self.m["builds"][mode] += 1
                        self.stragglers.append((mode, info.get("stragglers", 0), info.get("enumerated", 0)))
                        self.m["xleaf"] += info.get("xleaf", 0)
                except Exception as e:  # noqa
                    c.rollback()
                    if M.sqlstate(e) == "40P01":
                        with self.lock:
                            self.m["build_deadlocks"] += 1
                            self.dl_detail("build", e)
                    else:
                        self.err("builder", e)
                        return
                time.sleep(rng.uniform(0, 0.3))
        finally:
            c.close()

    def gc(self):
        c = M.connect(self.db)
        g = M.GC(c, skip_under_build=M.gc_skips(self.variant))
        try:
            while not self.stop_gc.is_set():
                try:
                    g.pass_()
                except Exception as e:  # noqa
                    c.rollback()
                    if M.sqlstate(e) == "40P01":
                        with self.lock:
                            self.m.setdefault("gc_deadlocks", 0)
                            self.m["gc_deadlocks"] += 1
                    else:
                        self.err("gc", e)
                        return
                time.sleep(0.005)
        finally:
            self.m["gc_deleted"] = g.deleted
            self.m["gc_skipped"] = g.skipped
            c.close()

    def dl_detail(self, who, e):
        d = self.m.setdefault("deadlock_details", [])
        if len(d) < 4:
            d.append(f"{who}: {getattr(e.diag, 'message_detail', '')}"[:600])

    def checkpoint(self):
        """Quiesce (writers idle, builder idle after a clean build, ring
        drained) and compare; then resume."""
        self.pause_writers.set()
        for _ in range(self.nwriters):
            self.writers_idle.acquire(timeout=60)
        self.pause_builder.set()
        self.builder_idle.wait(120)
        hc = M.connect(self.db, autocommit=True)
        deadline = time.monotonic() + 60
        while hc.execute("select count(*) from ring where not drained").fetchone()[0] > 0 and time.monotonic() < deadline:
            time.sleep(0.01)
        div = M.divergence(hc)
        hc.close()
        self.checkpoints.append([list(map(str, r)) for r in div[:4]])
        self.pause_builder.clear()
        self.pause_writers.clear()

    def err(self, who, e):
        with self.lock:
            self.m["errors"].append(f"{who}: {e!r}"[:300])

    # ------------------------------------------------------------------
    def run(self):
        rng = random.Random(self.seed)
        M.create_db(self.db)
        t0 = time.monotonic()
        self.t_start = t0
        try:
            self.seed_source(rng)
            ws = [threading.Thread(target=self.writer, args=(k,)) for k in range(self.nwriters)]
            aps = [threading.Thread(target=self.applier, args=(k,)) for k in range(self.nappliers)]
            b = threading.Thread(target=self.builder)
            g = threading.Thread(target=self.gc)
            for t in ws + aps + [b, g]:
                t.start()
            end = time.monotonic() + self.duration
            while time.monotonic() < end:
                time.sleep(min(self.checkpoint_every, max(0.0, end - time.monotonic())))
                if time.monotonic() < end - 0.5:
                    self.checkpoint()
            self.stop_writers.set()
            for t in ws:
                t.join()
            self.stop_builder.set()
            b.join(120)
            if b.is_alive():
                self.err("harness", "builder did not stop")
            # drain the ring to empty
            hc = M.connect(self.db, autocommit=True)
            deadline = time.monotonic() + 120
            while hc.execute("select count(*) from ring where not drained").fetchone()[0] > 0 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.stop_appliers.set()
            for t in aps:
                t.join()
            self.stop_gc.set()
            g.join()
            left = hc.execute("select count(*) from ring where not drained").fetchone()[0]
            if left:
                self.err("harness", f"ring not drained: {left}")
            hc.close()
            c = M.connect(self.db)
            div = M.divergence(c)
            c.close()
        finally:
            M.drop_db(self.db)
        lw = sorted(self.lock_waits)
        A = [s for s in self.stragglers]
        res = dict(
            variant=self.variant, seed=self.seed, appliers=self.nappliers, duration=self.duration,
            elapsed=round(time.monotonic() - t0, 1),
            divergent=bool(div) or any(self.checkpoints), divergent_final=bool(div),
            checkpoints=len(self.checkpoints), checkpoints_divergent=sum(1 for c in self.checkpoints if c),
            first_checkpoint_divergence=next((c for c in self.checkpoints if c), None),
            divergence=[list(map(str, r)) for r in div[:6]], ndiv=len(div),
            **{k: v for k, v in self.m.items()},
            builds_total=sum(self.m["builds"].values()),
            lock_wait_ms=dict(
                n=len(lw),
                p50=round(1000 * lw[len(lw) // 2], 2) if lw else None,
                p99=round(1000 * lw[int(len(lw) * 0.99)], 2) if lw else None,
                max=round(1000 * lw[-1], 2) if lw else None,
                mean=round(1000 * statistics.fmean(lw), 3) if lw else None,
            ),
            stragglers=dict(
                fresh=_strag([s for s in A if s[0] == "fresh"]),
                inplace=_strag([s for s in A if s[0] == "inplace"]),
                corrupt=_strag([s for s in A if s[0] == "corrupt"]),
                resume=_strag([s for s in A if s[0] == "resume"]),
            ),
            page_wait_s=round(sum(lw), 3),
        )
        return res


def _strag(xs):
    if not xs:
        return None
    return dict(builds=len(xs), stragglers=sum(s for _, s, _ in xs), enumerated=sum(e for _, _, e in xs))


def one(variant, seed, appliers, duration, jitter_ms=40):
    try:
        r = Run(variant, int(seed), int(appliers), float(duration), jitter_ms=int(jitter_ms),
                db=f"x1_s_{variant.replace('-', '_').replace('+', 'p').lower()}_{seed}_{appliers}_{jitter_ms}").run()
        r["jitter_ms"] = jitter_ms
        return r
    except Exception as e:  # noqa
        return dict(variant=variant, seed=seed, appliers=appliers, harness_error=traceback.format_exc()[-2000:])


PLANS = {
    # (variant, seeds, jitter_ms); each seed runs with 2, 4 and 8 appliers
    "full": [("A", 50, 40), ("B", 50, 40), ("T0", 15, 40), ("A-gc", 10, 40), ("A-2pass", 10, 40),
             ("A-noenum", 10, 40), ("N", 10, 40), ("T0-early", 10, 40), ("B-early", 10, 40),
             ("A", 5, 0), ("B", 5, 0), ("T0", 5, 0)],
    "smoke": [(v, 1, 40) for v in M.VARIANTS],
    "probe": [("A-gc", 3, 40), ("A-noenum", 3, 40)],
    # X1b
    "x1b": [("A-preRHS", 20, 40), ("L-A-preRHS", 25, 40), ("L-A", 15, 40), ("L-A-preRHS-span", 10, 40),
            ("L-B", 15, 40), ("L-Bdef", 10, 40), ("A-preRHS-late", 5, 40), ("A-s2gb", 5, 40),
            ("A-s2probe", 5, 40), ("A-s2each", 5, 40), ("A-x+T", 10, 40), ("L-A-preRHS+T", 10, 40),
            ("L-B+T", 5, 40), ("A", 5, 40),
            ("L-A-preRHS", 5, 0), ("L-B", 5, 0), ("L-Bdef", 5, 0), ("L-A", 5, 0),
            ("A-preRH", 10, 40), ("L-A-preRH", 10, 40), ("A-preRHS-nogc", 10, 40), ("L-A-nogc", 10, 40),
            ("A-notarget+T", 10, 40), ("L-N", 10, 40)],
    "x1bsmoke": [(v, 1, 40) for v in ("A-preRHS", "L-A-preRHS", "L-A-preRHS-span", "L-B", "L-Bdef", "A-x+T",
                                      "A-notarget+T", "L-N", "A-s2each")],
}


def matrix(out, parallel=8, duration=20.0, plan="full", seed_base=0):
    from concurrent.futures import ProcessPoolExecutor, as_completed

    M.create_template()
    jobs = []
    for variant, nseeds, jitter in PLANS[plan]:
        for s in range(nseeds):
            for ap in (2, 4, 8):
                jobs.append((variant, seed_base + s + 1, ap, duration, jitter))
    random.Random(1917).shuffle(jobs)
    total_rows = 0
    with open(out, "a") as f, ProcessPoolExecutor(parallel) as ex:
        futs = [ex.submit(one, *j) for j in jobs]
        for i, fu in enumerate(as_completed(futs), 1):
            r = fu.result()
            total_rows += r.get("rows_written", 0) + 8
            f.write(json.dumps(r) + "\n")
            f.flush()
            print(f"[{i}/{len(jobs)}] {r.get('variant')} seed={r.get('seed')} ap={r.get('appliers')} "
                  f"div={r.get('divergent')} builds={r.get('builds_total')} pdl={r.get('page_deadlocks')} "
                  f"bdl={r.get('build_deadlocks')} err={len(r.get('errors', [])) or r.get('harness_error', '')[:80]}",
                  flush=True)
    print(f"TOTAL_ROWS {total_rows}")
    return total_rows


if __name__ == "__main__":
    a = sys.argv[1:]
    if a[0] == "one":
        M.create_template()
        dur = a[4] if len(a) > 4 else 20
        print(json.dumps(one(a[1], a[2], a[3], dur), indent=1))
    else:
        out = a[1]
        kw = {}
        it = iter(a[2:])
        for x in it:
            if x == "--parallel":
                kw["parallel"] = int(next(it))
            elif x == "--duration":
                kw["duration"] = float(next(it))
            elif x == "--plan":
                kw["plan"] = next(it)
            elif x == "--seed-base":
                kw["seed_base"] = int(next(it))
        matrix(out, **kw)
