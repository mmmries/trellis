"""Hand-driven deterministic scenarios for X1 (#1917).

Each scenario is a function(variant) run on a fresh database. The build is
stalled at named points with advisory-lock hooks held by the harness; pages
are run step by step on their own connections. Each scenario returns an
outcome ('exact', 'wrong', with 'deadlock' noted) and asserts its own
intermediate expectations where they are variant-independent.

Usage: python3 scenarios.py [scenario ...] [--variants A,B,...]
"""

import sys
import threading
import time
import traceback

import model as M
from psycopg import errors

GROUPS_KEY = lambda g: (g is None, g if g is not None else 0)


class Bg:
    def __init__(self, fn, name=""):
        self.name = name
        self.result = None
        self.exc = None
        self.t = threading.Thread(target=self._run, args=(fn,), daemon=True)
        self.t.start()

    def _run(self, fn):
        try:
            self.result = fn()
        except Exception as e:  # noqa
            self.exc = e

    def done(self):
        return not self.t.is_alive()

    def join(self, timeout=30):
        self.t.join(timeout)
        if self.t.is_alive():
            raise RuntimeError(f"{self.name} did not finish")
        return self


class Env:
    def __init__(self, variant, db="x1_scn"):
        self.variant = variant
        self.db = db
        M.create_db(db)
        self.hc = M.connect(db, autocommit=True)  # harness: holds hook locks, observes
        self.wc = M.connect(db)  # writer
        self.conns = []
        self.next_id = 1
        self.deadlocks = []
        self.notes = []

    def conn(self):
        c = M.connect(self.db)
        self.conns.append(c)
        return c

    def close(self):
        for c in self.conns + [self.wc, self.hc]:
            try:
                c.close()
            except Exception:
                pass
        M.drop_db(self.db)

    # writers -------------------------------------------------------------
    def w(self, *stmts, conn=None, commit=True):
        c = conn or self.wc
        cur = c.cursor()
        for s in stmts:
            if isinstance(s, tuple):
                cur.execute(*s)
            else:
                cur.execute(s)
        if commit:
            c.commit()

    def ins(self, g, v=1, conn=None, commit=True):
        i = self.next_id
        self.next_id += 1
        self.w(("insert into src(id, g, v) values (%s, %s, %s)", (i, g, v)), conn=conn, commit=commit)
        return i

    def move(self, i, g, conn=None, commit=True):
        self.w(("update src set g = %s where id = %s", (g, i)), conn=conn, commit=commit)

    def delete(self, i, conn=None, commit=True):
        self.w(("delete from src where id = %s", (i,)), conn=conn, commit=commit)

    def ring_positions(self):
        return [r[0] for r in self.hc.execute("select pos from ring order by pos").fetchall()]

    # pages ----------------------------------------------------------------
    def page(self, positions=None, hooks=None):
        c = self.conn()
        if hooks:
            c.execute("select set_config('x1.hooks', %s, false)", (hooks,))
            c.commit()
        return M.Page(c, gate=M.page_gate(self.variant)), c

    def run_page(self, positions=None, hooks=None):
        p, c = self.page(hooks=hooks)
        if p.claim(positions=positions) == 0:
            c.rollback()
            return None
        p.stage()
        p.lock()
        p.apply()
        p.finish()
        return p

    def drain_all(self):
        c = self.conn()
        while True:
            p = M.Page(c, gate=M.page_gate(self.variant))
            if p.run(batch=1000) == 0:
                break

    def reset_h(self):
        self.w("delete from h")

    # builds ---------------------------------------------------------------
    def build_bg(self, hooks="", rollback=False):
        c = self.conn()
        c.execute("select set_config('x1.hooks', %s, false)", (hooks,))
        c.commit()
        self.bpid = c.info.backend_pid
        info = {}

        def go():
            try:
                if rollback:
                    info["rollback"] = True
                    _build_then_rollback(c, self.variant, info)
                else:
                    M.build(c, self.variant, info)
                return info
            except Exception:
                c.rollback()
                raise

        b = Bg(go, "build")
        b.pid = c.info.backend_pid
        b.info = info
        return b

    def build(self):
        c = self.conn()
        info = {}
        M.build(c, self.variant, info)
        return info

    def gc(self, **kw):
        return M.GC(self.conn(), skip_under_build=M.gc_skips(self.variant), **kw)

    # hooks / waiting ------------------------------------------------------
    def hold(self, key):
        self.hc.execute("select pg_advisory_lock(hashtext(%s))", (key,))

    def release(self, key):
        self.hc.execute("select pg_advisory_unlock(hashtext(%s))", (key,))

    def waiting(self, pid):
        r = self.hc.execute(
            "select wait_event_type, wait_event from pg_stat_activity where pid = %s", (pid,)
        ).fetchone()
        return r if r and r[0] == "Lock" else None

    def wait_blocked(self, pid, timeout=5.0, bg=None):
        """Wait until pid waits on a lock (or bg finished). Returns the wait event or 'done'."""
        t = time.monotonic()
        while time.monotonic() - t < timeout:
            w = self.waiting(pid)
            if w:
                return w[1]
            if bg is not None and bg.done():
                return "done"
            time.sleep(0.01)
        return "timeout"

    def outcome(self, extra=None):
        self.drain_all()
        d = M.divergence(self.hc)
        return {
            "outcome": "exact" if not d else "wrong",
            "divergence": d,
            "deadlocks": self.deadlocks,
            "notes": self.notes,
            **(extra or {}),
        }

    def collect(self, *bgs):
        for b in bgs:
            b.join()
            if b.exc is not None:
                if M.sqlstate(b.exc) == "40P01":
                    self.deadlocks.append(b.name)
                else:
                    raise b.exc


def _build_then_rollback(conn, variant, info):
    # Run the build but roll back instead of committing (after the precommit hook).
    orig = conn.commit

    def fake_commit():
        conn.commit = orig
        conn.rollback()

    conn.commit = fake_commit
    try:
        M.build(conn, variant, info)
    finally:
        conn.commit = orig


def page_bg(env, positions=None, hooks=None, pause_before_commit=None):
    p, c = env.page(hooks=hooks)

    def go():
        try:
            if p.claim(positions=positions) == 0:
                c.rollback()
                return 0
            p.stage()
            p.lock()
            p.apply()
            if pause_before_commit is not None:
                pause_before_commit.wait(30)
            p.finish()
            return len(p.rows)
        except Exception:
            c.rollback()
            raise

    b = Bg(go, "page")
    b.pid = c.info.backend_pid
    b.page = p
    return b


# --------------------------------------------------------------------------
# Scenarios
# --------------------------------------------------------------------------


def s1_absent_group_post_s1(variant, g=2):
    """A page applies a change S1 does not see to a group absent from h,
    between the build's read and its insert."""
    e = Env(variant)
    try:
        e.ins(g, 10)
        e.drain_all()
        e.reset_h()  # fresh build: hidden table empty
        e.hold("read:N")
        b = e.build_bg("read:N")
        assert e.wait_blocked(b.pid, bg=b) == "advisory", "build should stall at read"
        e.ins(g, 5)  # committed after S1
        p = page_bg(e)
        st = e.wait_blocked(p.pid, bg=p, timeout=1.5)
        e.notes.append(f"page {st} while build stalled")
        e.release("read:N")
        e.collect(b, p)
        return e.outcome({"stragglers": b.info.get("stragglers")})
    finally:
        e.close()


def s1n_absent_null_group(variant):
    return s1_absent_group_post_s1(variant, g=None)


def s2_blocked_page_refuses_seen(variant, g=3):
    """A page blocked on the build's row applies after the commit: a change
    S1 saw is refused, a later one applies once."""
    e = Env(variant)
    try:
        e.ins(g, 1)
        e.drain_all()
        e.reset_h()
        e.ins(g, 2)  # c1: before S1, undrained
        e.hold("read:N")
        e.hold("precommit:N")
        b = e.build_bg("read:N,precommit:N")
        assert e.wait_blocked(b.pid, bg=b) == "advisory"
        e.ins(g, 4)  # c3: after S1
        e.release("read:N")
        assert e.wait_blocked(b.pid, bg=b) == "advisory", "build should stall at precommit"
        p = page_bg(e)
        st = e.wait_blocked(p.pid, bg=p, timeout=1.5)
        e.notes.append(f"page {st} before build commit")
        e.release("precommit:N")
        e.collect(b, p)
        row = e.hc.execute("select n, s from h where g is not distinct from %s", (g,)).fetchone()
        e.notes.append(f"h({g}) after page = {row}")
        return e.outcome({"page_blocked": st not in ("done", "timeout"), "h_after_page": row})
    finally:
        e.close()


def s3_gc_counterexample(variant, g=4):
    """DESIGN.md's counterexample: c_a before S1, c_b after; both drained and
    the zero row collected before the build's insert."""
    e = Env(variant)
    try:
        i = e.ins(g, 7)  # c_a, before S1
        e.hold("read:N")
        b = e.build_bg("read:N")
        assert e.wait_blocked(b.pid, bg=b) == "advisory"
        e.delete(i)  # c_b, after S1
        p = page_bg(e)
        st = e.wait_blocked(p.pid, bg=p, timeout=1.5)
        e.notes.append(f"page {st}")
        gc = e.gc()
        d = gc.pass_()
        e.notes.append(f"gc deleted {d}, skipped {gc.skipped}")
        e.release("read:N")
        e.collect(b, p)
        return e.outcome({"gc_deleted": d})
    finally:
        e.close()


def s4a_vacated_drained(variant, g=6, g2=7):
    """A group whose only row moved out before S1, the move drained before S1
    (fresh build: h was reset, so the page left g at -1)."""
    e = Env(variant)
    try:
        i = e.ins(g, 3)
        e.drain_all()
        e.reset_h()
        e.move(i, g2)
        e.drain_all()
        e.notes.append(f"h before build = {M.h_state(e.hc)}")
        e.build()
        return e.outcome()
    finally:
        e.close()


def s4b_vacated_undrained(variant, g=6, g2=7):
    """As s4a, but the move is undrained at S1."""
    e = Env(variant)
    try:
        i = e.ins(g, 3)
        e.drain_all()
        e.reset_h()
        e.move(i, g2)
        info = e.build()
        e.notes.append(f"h after build = {M.h_state(e.hc)}")
        return e.outcome({"stragglers": info.get("stragglers")})
    finally:
        e.close()


def s4bn_vacated_null(variant):
    return s4b_vacated_undrained(variant, g=None, g2=1)


def s4c_vacated_drain_in_flight(variant, g=12):
    """A page that drained the vacating change commits after S1 but before
    the build reaches g (ring row visible at S1, h row not)."""
    e = Env(variant)
    try:
        i = e.ins(g, 3)
        e.drain_all()
        e.reset_h()
        e.delete(i)
        gate = threading.Event()
        p = page_bg(e, pause_before_commit=gate)
        time.sleep(0.3)
        e.hold("read:N")
        b = e.build_bg("read:N")
        st = e.wait_blocked(b.pid, bg=b)
        e.notes.append(f"build {st}")
        gate.set()
        p.join()
        e.release("read:N")
        e.collect(b, p)
        return e.outcome()
    finally:
        e.close()


def s5a_key_ordered_wait(variant):
    """A page's uncommitted insert of a group makes the build's insert wait;
    both key-ordered: no deadlock."""
    e = Env(variant)
    try:
        for g in (7, 8, 9):
            e.ins(g, 1)  # undrained; h empty: consistent
        before = set(e.ring_positions())
        a = e.ins(7, 2, commit=False)
        e.ins(9, 3)  # one transaction touching 7 and 9
        cp = [x for x in e.ring_positions() if x not in before]
        e.hold("plock:9")
        p = page_bg(e, positions=cp, hooks="plock:9")
        assert e.wait_blocked(p.pid, bg=p) == "advisory", "page should stall between 7 and 9"
        b = e.build_bg("")
        st = e.wait_blocked(b.pid, bg=b, timeout=1.5)
        e.notes.append(f"build {st} while page holds 7")
        e.release("plock:9")
        e.collect(b, p)
        return e.outcome({"stragglers": b.info.get("stragglers")})
    finally:
        e.close()


def s5b_two_pass_deadlock(variant):
    """Group 7 exists, 9 absent. The build has handled 7 and stalls before 9
    (A-2pass: after pass 1 inserted 9). A page locks 7 then wants 9."""
    e = Env(variant)
    try:
        e.ins(7, 1)
        e.drain_all()  # h(7) exists, committed
        e.ins(9, 1)  # undrained, h has no 9
        before = set(e.ring_positions())
        e.ins(7, 2, commit=False)
        e.ins(9, 3)
        cp = [x for x in e.ring_positions() if x not in before]
        stall = {
            "A-2pass": "pass2:N",
            "T0": "lock:9",
            "T0-early": "lock:9",
        }.get(variant, "ins:9")
        e.hold(stall)
        b = e.build_bg(stall)
        e.notes.append(f"build {e.wait_blocked(b.pid, bg=b)} at {stall}")
        p = page_bg(e, positions=cp)
        e.notes.append(f"page {e.wait_blocked(p.pid, bg=p, timeout=1.5)}")
        e.release(stall)
        b.t.join(10)
        p.t.join(10)
        e.collect(b, p)
        if b.exc is not None:
            e.build()  # retry the aborted build
        return e.outcome()
    finally:
        e.close()


def s7_build_rollback(variant, g=10):
    """The build rolls back after its insert; a blocked page then proceeds."""
    e = Env(variant)
    try:
        e.ins(g, 5)  # undrained, h empty: consistent
        e.hold("precommit:N")
        b = e.build_bg("precommit:N", rollback=True)
        assert e.wait_blocked(b.pid, bg=b) == "advisory"
        p = page_bg(e)
        e.notes.append(f"page {e.wait_blocked(p.pid, bg=p, timeout=1.5)}")
        e.release("precommit:N")
        e.collect(b, p)
        mid = M.divergence(e.hc)
        e.notes.append(f"after rollback+page: {'exact' if not mid else mid}")
        e.ins(g, 6)
        e.build()
        return e.outcome({"after_rollback": "exact" if not mid else "wrong"})
    finally:
        e.close()


def s8_rebuild_in_place(variant, corrupt=False):
    """Every row exists (populated h); c1 before S1, c2 after S1 in the same
    groups, a page drains both between the build's read and its write."""
    e = Env(variant)
    try:
        ids = [e.ins(g, (g or 0) + 1) for g in (1, 2, 3, None)]
        e.drain_all()
        e.build()  # populate bases
        e.drain_all()
        if corrupt:
            e.w("update h set n = n + 1 where g = 1", "delete from h where g = 2")
        e.ins(1, 9)  # c1 before S1
        e.hold("read:N")
        b = e.build_bg("read:N")
        assert e.wait_blocked(b.pid, bg=b) == "advisory"
        e.ins(1, 11)  # c2 after S1
        e.move(ids[1], 3)
        e.move(ids[3], 1)
        p = page_bg(e)
        e.notes.append(f"page {e.wait_blocked(p.pid, bg=p, timeout=1.5)}")
        e.release("read:N")
        e.collect(b, p)
        return e.outcome({"enumerated": b.info.get("enumerated"), "stragglers": b.info.get("stragglers")})
    finally:
        e.close()


def s8c_rebuild_corrupted(variant):
    return s8_rebuild_in_place(variant, corrupt=True)


def s9_page_claimed_before_s1(variant, g=11):
    """A page claimed a change S1 sees but reaches g after the build inserted
    it: the change must be refused."""
    e = Env(variant)
    try:
        e.ins(g, 1)
        e.drain_all()
        e.reset_h()
        e.ins(g, 2)  # c1, before S1, undrained
        e.hold("plock:" + str(g))
        p = page_bg(e, hooks="plock:" + str(g))
        e.notes.append(f"page {e.wait_blocked(p.pid, bg=p)} before {g}")
        e.hold("precommit:N")
        b = e.build_bg("precommit:N")
        e.notes.append(f"build {e.wait_blocked(b.pid, bg=b)}")
        e.release("plock:" + str(g))
        e.notes.append(f"page {e.wait_blocked(p.pid, bg=p, timeout=1.5)}")
        e.release("precommit:N")
        e.collect(b, p)
        return e.outcome()
    finally:
        e.close()


def s11_gc_prefix_soundness(variant, g=13, naive=False):
    """A zero row whose stamp equals the position of a change its basis saw
    that an uncommitted writer held when the GC computed its prefix."""
    e = Env(variant)
    try:
        i = e.ins(g, 1)
        e.drain_all()
        wconn = e.conn()
        gc = e.gc(naive=naive)
        e.delete(i, conn=wconn, commit=False)  # W: ring pos p, uncommitted
        if naive:
            # Naive prefix read in its own statement while W is in progress:
            # the ring looks empty, so prefix = last assigned = p.
            gc.pass_()
        else:
            gc.pass_()
            gc.pass_()
        e.notes.append(f"prefix while W open = {gc.prefix}")
        wconn.commit()
        e.build()  # g zero, basis sees W, stamp = p
        e.notes.append(f"h = {M.h_state(e.hc)}")
        if naive:
            # the delete statement of the same (logical) pass, with the stale prefix
            cur = gc.conn.cursor()
            cur.execute(
                "delete from h where ctid = any(array(select ctid from h where n = 0 and s = 0 "
                "and (basis is null or stamp <= %s) for update skip locked))",
                (gc.prefix,),
            )
            e.notes.append(f"naive gc deleted {cur.rowcount}")
            gc.conn.commit()
        else:
            gc.pass_()
            gc.pass_()
            e.notes.append(f"sound gc prefix {gc.prefix}, deleted {gc.deleted}")
        return e.outcome()
    finally:
        e.close()


def s11n_gc_prefix_naive(variant):
    return s11_gc_prefix_soundness(variant, naive=True)


def s14_commit_order_differs(variant, g=15):
    """W1 takes the lower ring position but commits after S1; W2 commits
    before S1. A page drains both around the build's insert."""
    e = Env(variant)
    try:
        w1 = e.conn()
        e.ins(g, 1, conn=w1, commit=False)  # lower pos, open
        e.ins(g, 2)  # W2 commits before S1
        e.hold("read:N")
        b = e.build_bg("read:N")
        assert e.wait_blocked(b.pid, bg=b) == "advisory"
        w1.commit()  # after S1
        p = page_bg(e)
        e.notes.append(f"page {e.wait_blocked(p.pid, bg=p, timeout=1.5)}")
        e.release("read:N")
        e.collect(b, p)
        return e.outcome({"stragglers": b.info.get("stragglers")})
    finally:
        e.close()


def s15_gc_mid_txn_when_build_starts(variant, g=16):
    """The counterexample with a GC transaction that took the skip lock
    (shared) before the build: A's build must wait for it before S1."""
    e = Env(variant)
    try:
        i = e.ins(g, 7)  # c_a, undrained; h empty: consistent
        gconn = e.conn()
        cur = gconn.cursor()
        if M.gc_skips(variant):
            cur.execute("select pg_try_advisory_xact_lock_shared(%s)", (M.GCLOCK_KEY,))
            assert cur.fetchone()[0]
        e.hold("read:N")
        b = e.build_bg("read:N")
        st = e.wait_blocked(b.pid, bg=b)
        key = e.hc.execute(
            "select objid from pg_locks where pid = %s and not granted and locktype = 'advisory'", (b.pid,)
        ).fetchone()
        waits_gc = key is not None and key[0] == M.GCLOCK_KEY
        e.notes.append(f"build {st} {'on GC lock' if waits_gc else 'at read (S1 taken)'}")
        e.delete(i)  # c_b
        p = page_bg(e)
        e.notes.append(f"page {e.wait_blocked(p.pid, bg=p, timeout=1.5)}")
        cur.execute("delete from h where n = 0 and s = 0 and basis is null")
        e.notes.append(f"gc txn deleted {cur.rowcount}")
        gconn.commit()
        e.release("read:N")
        e.collect(b, p)
        return e.outcome({"build_waited_for_gc": waits_gc})
    finally:
        e.close()


def s16_page_blocked_on_straggler(variant, g=17):
    """g exists (a straggler under A). c2 commits after S1, c3 after the
    straggler lock; a page holding both waits on g and must refuse both
    (S2 sees them) under A; under T0/B the basis is S and both apply."""
    e = Env(variant)
    try:
        e.ins(g, 1)
        e.drain_all()  # h(g) exists, NULL basis
        e.hold("read:N")
        stall2 = "strag:N" if variant in ("A", "A-gc", "A-noenum", "A-2pass") else "precommit:N"
        e.hold(stall2)
        b = e.build_bg("read:N," + stall2)
        assert e.wait_blocked(b.pid, bg=b) == "advisory"
        e.ins(g, 2)  # c2 after S1
        e.release("read:N")
        e.notes.append(f"build {e.wait_blocked(b.pid, bg=b)} at {stall2}")
        e.ins(g, 3)  # c3
        p = page_bg(e)
        e.notes.append(f"page {e.wait_blocked(p.pid, bg=p, timeout=1.5)}")
        e.release(stall2)
        e.collect(b, p)
        return e.outcome({"stragglers": b.info.get("stragglers")})
    finally:
        e.close()


def s18_hidden_truncate_vs_s1(variant):
    """Mechanism check for A (and every variant): a TRUNCATE of the hidden
    table (the engine's truncate page) can't land between the build's
    snapshot and its commit. (a) truncate holds its lock first: the build's
    statement waits on the relation lock and its snapshot is taken after the
    truncate commits. (b) build stalled after S1: the truncate waits."""
    e = Env(variant)
    try:
        e.ins(1, 1)
        e.ins(2, 1)
        e.drain_all()
        tc = e.conn()
        tc.execute("truncate h")
        txid = tc.execute("select pg_current_xact_id()::text").fetchone()[0]
        b = e.build_bg("")
        st = e.wait_blocked(b.pid, bg=b)
        e.notes.append(f"(a) build {st} while truncate open")
        tc.commit()
        e.collect(b)
        vis = e.hc.execute(
            "select bool_and(pg_visible_in_snapshot(%s::xid8, basis)) from h where basis is not null", (txid,)
        ).fetchone()[0]
        e.notes.append(f"(a) truncate visible in every written basis: {vis}")
        e.hold("read:N")
        b = e.build_bg("read:N")
        assert e.wait_blocked(b.pid, bg=b) == "advisory"
        t = Bg(lambda: (tc.execute("truncate h"), tc.commit()), "truncate")
        st2 = e.wait_blocked(tc.info.backend_pid, bg=t, timeout=1.5)
        e.notes.append(f"(b) truncate {st2} while build stalled after S1")
        e.release("read:N")
        e.collect(b, t)
        e.build()
        r = e.outcome()
        r["snapshot_after_lock"] = vis
        r["truncate_waited"] = st2
        if not vis or st2 != "relation":
            r["outcome"] = "wrong"
        return r
    finally:
        e.close()


SCENARIOS = [
    s1_absent_group_post_s1,
    s1n_absent_null_group,
    s2_blocked_page_refuses_seen,
    s3_gc_counterexample,
    s4a_vacated_drained,
    s4b_vacated_undrained,
    s4bn_vacated_null,
    s4c_vacated_drain_in_flight,
    s5a_key_ordered_wait,
    s5b_two_pass_deadlock,
    s7_build_rollback,
    s8_rebuild_in_place,
    s8c_rebuild_corrupted,
    s9_page_claimed_before_s1,
    s11_gc_prefix_soundness,
    s11n_gc_prefix_naive,
    s14_commit_order_differs,
    s15_gc_mid_txn_when_build_starts,
    s16_page_blocked_on_straggler,
    s18_hidden_truncate_vs_s1,
]


def main(argv):
    variants = M.VARIANTS
    names = []
    for a in argv:
        if a.startswith("--variants="):
            variants = a.split("=", 1)[1].split(",")
        elif not a.startswith("-"):
            names.append(a)
    M.create_template()
    scns = [s for s in SCENARIOS if not names or s.__name__ in names or any(s.__name__.startswith(n) for n in names)]
    table = {}
    for s in scns:
        for v in variants:
            try:
                r = s(v)
                tag = r["outcome"] + ("+deadlock" if r["deadlocks"] else "")
            except Exception as ex:  # noqa
                tag = "ERROR"
                r = {"error": repr(ex), "tb": traceback.format_exc()}
            table[(s.__name__, v)] = tag
            print(f"{s.__name__:34s} {v:9s} {tag:16s} {r if tag != 'exact' or '-v' in argv else ''}", flush=True)
    print()
    print("| scenario | " + " | ".join(variants) + " |")
    print("|---|" + "---|" * len(variants))
    for s in scns:
        print(f"| {s.__name__} | " + " | ".join(table[(s.__name__, v)] for v in variants) + " |")


if __name__ == "__main__":
    main(sys.argv[1:])
