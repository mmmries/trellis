"""X1b hand-driven scenarios (#1917): pre-enumeration, partition leaves,
per-leaf B, the resume shape (visible target) and step 2's read forms.

Reuses scenarios.py's harness. Every scenario runs on a fresh database with
M.RETAIN on (a page marks its ring rows drained; the GC retires them in order
up to its sound prefix, as the engine's segments) unless it says otherwise.

Usage: python3 scenarios_b.py [group|scenario ...] [--variants=A,B] [-v]
Groups: pre, leaf, resume, step2, hole.
"""

import sys
import threading
import time
import traceback

import model as M
import scenarios as S
from scenarios import Bg, page_bg


def is_leaf(v):
    return M.XV.get(M.base(v), {}).get("leaves", False)


def has_pre(v):
    return M.XV.get(M.base(v), {}).get("pre") is not None


def kind(v):
    b = M.base(v)
    if b in M.XV:
        return M.XV[b]["kind"]
    return {"T0": "T0", "T0-early": "T0", "B": "B", "N": "B", "B-early": "B"}.get(b, "A")


def H(v, tag, g=None, leaf=None):
    """Hook key for a build point of the transaction that owns group g."""
    if is_leaf(v):
        return f"{tag}:{leaf if leaf is not None else M.route(g)}"
    return f"{tag}:N"


class Env(S.Env):
    def __init__(self, variant, retain=True, db="x1b_scn"):
        M.TARGET = M.has_target(variant)
        M.RETAIN = retain
        super().__init__(variant, db=db)

    def uncaptured(self, *stmts):
        """Source writes the ring never sees (a capture disruption while paused)."""
        c = self.conn()
        c.execute("set session_replication_role = replica")
        for s in stmts:
            c.execute(*s) if isinstance(s, tuple) else c.execute(s)
        c.commit()

    def resume_reset(self):
        """Resume shape: the hidden table emptied, the visible target kept."""
        self.w("delete from h")

    def tgt(self):
        return self.hc.execute("select g, n, s from tgt order by g").fetchall()


def between(e, v, g, action, also_hold=()):
    """Run `action` between the build's pre-enumeration E and its S1 (for a
    variant without E: before the transaction that owns g starts; for a
    non-leaf one without E: before the build)."""
    if has_pre(v):
        key = H(v, "pre", g)
    elif is_leaf(v):
        key = H(v, "leaf", g)
    else:
        key = None
    if key is None:
        action()
        b = e.build_bg(",".join(also_hold))
        return b, None
    e.hold(key)
    b = e.build_bg(",".join((key,) + tuple(also_hold)))
    st = e.wait_blocked(b.pid, bg=b)
    assert st == "advisory", f"build should stall at {key}, got {st}"
    action()
    return b, key


# --------------------------------------------------------------------------
# Question 1: pre-enumeration
# --------------------------------------------------------------------------


def p1_moveout_undrained(v, g=2, g2=7):
    """A group whose last row moves out between E and S1, the move undrained
    at S1 (DESIGN hypothesis for A-preRH: wrong)."""
    e = Env(v)
    try:
        i = e.ins(g, 3)
        e.drain_all()
        e.reset_h()  # fresh build: h empty, source has g
        b, key = between(e, v, g, lambda: e.move(i, g2))
        if key:
            e.release(key)
        e.collect(b)
        e.notes.append(f"h after build = {M.h_state(e.hc)}")
        return e.outcome({"stragglers": b.info.get("stragglers")})
    finally:
        e.close()


def p1n_moveout_undrained_null(v):
    return p1_moveout_undrained(v, g=None, g2=1)


def p2_moveout_drained_before_s1(v, g=2, g2=7):
    """As p1, the move drained by a page that commits before S1 (h row
    visible at S1, not at E)."""
    e = Env(v)
    try:
        i = e.ins(g, 3)
        e.drain_all()
        e.reset_h()

        def act():
            e.move(i, g2)
            e.drain_all()

        b, key = between(e, v, g, act)
        if key:
            e.release(key)
        e.collect(b)
        return e.outcome({"stragglers": b.info.get("stragglers")})
    finally:
        e.close()


def p3_moveout_drain_in_flight(v, g=2, g2=7):
    """The move happens between E and S1; the page that drains it applies
    before S1 but commits after S1, before the insert reaches g."""
    e = Env(v)
    try:
        i = e.ins(g, 3)
        e.drain_all()
        e.reset_h()
        gate = threading.Event()
        rk = H(v, "read", g)
        e.hold(rk)
        box = {}

        def act():
            e.move(i, g2)
            box["p"] = page_bg(e, pause_before_commit=gate)
            time.sleep(0.3)  # the page has applied, not committed

        b, key = between(e, v, g, act, also_hold=(rk,))
        if key:
            e.release(key)
        st = e.wait_blocked(b.pid, bg=b)
        e.notes.append(f"build {st} at {rk}")
        gate.set()
        box["p"].join()
        e.release(rk)
        e.collect(b, box["p"])
        return e.outcome({"stragglers": b.info.get("stragglers")})
    finally:
        e.close()


def p4_group_created_between(v):
    """Between E and S1: a new group is created and drained (h row after E,
    source has it at S1), another created and emptied, both drained (zero
    row after E, not in the source at S1), a third created undrained."""
    e = Env(v)
    try:
        e.ins(1, 1)
        e.drain_all()
        e.reset_h()

        def act():
            e.ins(5, 4)
            j = e.ins(8, 2)
            e.delete(j)
            e.drain_all()
            e.ins(11, 6)  # undrained at S1

        b, key = between(e, v, 5, act)
        if key:
            e.release(key)
        e.collect(b)
        return e.outcome({"stragglers": b.info.get("stragglers")})
    finally:
        e.close()


def p5_emptied_between(v, g=2, drained=False):
    """Every row of g deleted between E and S1 (undrained; or drained)."""
    e = Env(v)
    try:
        a = e.ins(g, 3)
        c = e.ins(g, 4)
        e.drain_all()
        e.reset_h()

        def act():
            e.delete(a)
            e.delete(c)
            if drained:
                e.drain_all()

        b, key = between(e, v, g, act)
        if key:
            e.release(key)
        e.collect(b)
        return e.outcome()
    finally:
        e.close()


def p5d_emptied_between_drained(v):
    return p5_emptied_between(v, drained=True)


def p6_gc_between_e_and_s1(v, g=4):
    """Steady h. Between E and S1 g's last row is deleted and drained (zero
    row) and a GC pass runs (A-preRHS-late: before the skip lock, so it
    collects); after S1 a new row lands in g and is drained before the insert
    reaches g."""
    e = Env(v)
    try:
        i = e.ins(g, 7)
        e.drain_all()
        e.build()  # steady state: bases set
        e.drain_all()
        rk = H(v, "read", g)
        e.hold(rk)
        box = {}

        def act():
            e.delete(i)
            e.drain_all()
            gc = e.gc()
            gc.pass_(); gc.pass_(); gc.pass_()
            box["gc"] = (gc.deleted, gc.skipped)

        b, key = between(e, v, g, act, also_hold=(rk,))
        if key:
            e.release(key)
        e.notes.append(f"gc deleted/skipped {box.get('gc')}; build {e.wait_blocked(b.pid, bg=b)}")
        e.ins(g, 5)  # after S1
        p = page_bg(e)
        e.notes.append(f"page {e.wait_blocked(p.pid, bg=p, timeout=1.5)}")
        e.release(rk)
        e.collect(b, p)
        return e.outcome({"gc": box.get("gc")})
    finally:
        e.close()


def p7_s3_after_s1(v, g=4):
    """X1's s3 counterexample in the pre form: c_a before E, c_b after S1,
    both drained and the zero row offered to the GC before the insert."""
    e = Env(v)
    try:
        i = e.ins(g, 7)  # c_a
        rk = H(v, "read", g)
        e.hold(rk)
        b = e.build_bg(rk)
        assert e.wait_blocked(b.pid, bg=b) == "advisory"
        e.delete(i)  # c_b after S1
        p = page_bg(e)
        e.notes.append(f"page {e.wait_blocked(p.pid, bg=p, timeout=1.5)}")
        gc = e.gc()
        for _ in range(3):
            gc.pass_()
        e.notes.append(f"gc deleted {gc.deleted}, skipped {gc.skipped}")
        e.release(rk)
        e.collect(b, p)
        return e.outcome({"gc_deleted": gc.deleted})
    finally:
        e.close()


def p8_move_out_and_back(v, g=2, g2=7):
    """Row moves g -> g2 between E and S1 (undrained), back g2 -> g after S1
    (undrained); both drained after the build."""
    e = Env(v)
    try:
        i = e.ins(g, 3)
        e.drain_all()
        e.reset_h()
        rk = H(v, "read", g)
        e.hold(rk)
        b, key = between(e, v, g, lambda: e.move(i, g2), also_hold=(rk,))
        if key:
            e.release(key)
        e.wait_blocked(b.pid, bg=b)
        e.move(i, g)  # after S1
        e.release(rk)
        e.collect(b)
        return e.outcome()
    finally:
        e.close()


def p9_build_rollback_with_pre(v, g=2):
    """The build (one transaction / one leaf transaction) rolls back after
    step 1; a page blocked on its insert proceeds; a later build is exact."""
    e = Env(v)
    try:
        e.ins(g, 3)
        e.drain_all()
        e.reset_h()
        e.ins(g, 4)  # undrained
        pk = H(v, "precommit", g)
        e.hold(pk)
        c = e.conn()
        c.execute("select set_config('x1.hooks', %s, false)", (pk,))
        c.commit()

        # Stall the transaction that owns g at its precommit, then cancel it
        # (it rolls back); earlier leaf transactions stay committed.
        b = Bg(lambda: M.build(c, v, {}), "build")
        b.pid = c.info.backend_pid
        assert e.wait_blocked(b.pid, bg=b) == "advisory"
        p = page_bg(e)
        e.notes.append(f"page {e.wait_blocked(p.pid, bg=p, timeout=1.5)}")
        e.hc.execute("select pg_cancel_backend(%s)", (b.pid,))
        b.t.join(10)
        e.notes.append(f"build ended by cancel: {type(b.exc).__name__ if b.exc else None}")
        try:
            c.rollback()
        except Exception:
            pass
        e.release(pk)
        p.join()
        if p.exc:
            raise p.exc
        e.build()
        return e.outcome()
    finally:
        e.close()


PRE = [p1_moveout_undrained, p1n_moveout_undrained_null, p2_moveout_drained_before_s1, p3_moveout_drain_in_flight,
       p4_group_created_between, p5_emptied_between, p5d_emptied_between_drained, p6_gc_between_e_and_s1,
       p7_s3_after_s1, p8_move_out_and_back, p9_build_rollback_with_pre]
PRE_VARIANTS = ["A-x", "A-preRH", "A-preRHS", "A-preRHS-late", "A-preRHS-nogc", "L-A", "L-A-preRHS", "L-A-preRH",
                "L-A-preRHS-span", "L-A-nogc", "L-B", "L-Bdef", "L-N"]


# --------------------------------------------------------------------------
# Question 2/3: partition leaves
# --------------------------------------------------------------------------


def _seed(e, groups, v=1):
    """Source rows that predate capture (a fresh build over an existing source)."""
    ids = {}
    c = e.conn()
    c.execute("set session_replication_role = replica")
    for g in groups:
        i = e.next_id
        e.next_id += 1
        c.execute("insert into src(id, g, v) values (%s, %s, %s)", (i, g, v + (g or 0)))
        ids[g] = i
    c.commit()
    return ids


def l1_pages_between_leaves(v):
    """Fresh leaf build; between leaf 1 and leaf 2, writes and drains hit
    built leaves (0, 1) and unbuilt ones (2, remainder)."""
    e = Env(v)
    try:
        ids = _seed(e, [0, 1, 2, 3, 4, 5, None])
        k = "leaf:2"
        e.hold(k)
        b = e.build_bg(k)
        assert e.wait_blocked(b.pid, bg=b) == "advisory"
        e.ins(3, 9)
        e.move(ids[0], 2)  # leaf 0 -> leaf 2 (unbuilt)
        e.move(ids[5], None)  # leaf 2 -> remainder
        e.delete(ids[4])
        e.ins(8, 2)  # new group in leaf 2
        e.drain_all()
        e.ins(2, 1)  # undrained at leaf 2's S1
        e.release(k)
        e.collect(b)
        return e.outcome({"stragglers": b.info.get("stragglers"), "xleaf": b.info.get("xleaf")})
    finally:
        e.close()


def l2_cross_leaf_move_and_back(v, drain_mid=True):
    """A row moves from a group of a built leaf (3 -> leaf 0) to a group of an
    unbuilt one (4 -> leaf 1) while leaf 1 waits, then back while leaf 2
    waits; drained in between (or not)."""
    e = Env(v)
    try:
        ids = _seed(e, [3, 4])
        e.hold("leaf:1")
        e.hold("leaf:2")
        b = e.build_bg("leaf:1,leaf:2")
        assert e.wait_blocked(b.pid, bg=b) == "advisory"
        e.move(ids[3], 4)
        if drain_mid:
            e.drain_all()
        e.release("leaf:1")
        assert e.wait_blocked(b.pid, bg=b) == "advisory"
        e.move(ids[3], 3)
        if drain_mid:
            e.drain_all()
        e.release("leaf:2")
        e.collect(b)
        return e.outcome({"stragglers": b.info.get("stragglers"), "xleaf": b.info.get("xleaf")})
    finally:
        e.close()


def l2u_cross_leaf_move_undrained(v):
    return l2_cross_leaf_move_and_back(v, drain_mid=False)


def l2r_reverse_move_during_leaf(v):
    """A row of an unbuilt leaf's group (4, leaf 1) moves into a built
    leaf's group (3, leaf 0) after leaf 1's S1 and back after its insert."""
    e = Env(v)
    try:
        ids = _seed(e, [3, 4])
        rk = "read:1"
        pk = "precommit:1"
        e.hold(rk)
        e.hold(pk)
        b = e.build_bg(f"{rk},{pk}")
        assert e.wait_blocked(b.pid, bg=b) == "advisory"
        e.move(ids[4], 3)  # after leaf 1's S1
        p = page_bg(e)
        e.notes.append(f"page {e.wait_blocked(p.pid, bg=p, timeout=1.5)}")
        e.release(rk)
        e.notes.append(f"build {e.wait_blocked(b.pid, bg=b)}")
        e.move(ids[4], 4)
        e.release(pk)
        e.collect(b, p)
        return e.outcome({"stragglers": b.info.get("stragglers")})
    finally:
        e.close()


def l3a_s3_inside_leaf(v):
    """X1's s3 inside leaf 1's transaction (GC after its S1)."""
    e = Env(v)
    try:
        i = e.ins(4, 7)  # c_a before leaf 1's S1
        rk = "read:1"
        e.hold(rk)
        b = e.build_bg(rk)
        assert e.wait_blocked(b.pid, bg=b) == "advisory"
        e.delete(i)  # c_b after S1
        p = page_bg(e)
        e.notes.append(f"page {e.wait_blocked(p.pid, bg=p, timeout=1.5)}")
        gc = e.gc()
        for _ in range(3):
            gc.pass_()
        e.notes.append(f"gc deleted {gc.deleted}, skipped {gc.skipped}")
        e.release(rk)
        e.collect(b, p)
        return e.outcome({"gc_deleted": gc.deleted})
    finally:
        e.close()


def l3b_gc_between_leaves(v):
    """GC passes between leaf 0's commit and leaf 1's start: zero rows of the
    built leaf (3) and of the unbuilt one (4, whose history straddles nothing
    yet) are collectable; one group of leaf 1 keeps a pre-capture row."""
    e = Env(v)
    try:
        ids = _seed(e, [4, 7])
        i3 = e.ins(3, 2)
        e.hold("leaf:1")
        b = e.build_bg("leaf:1")
        assert e.wait_blocked(b.pid, bg=b) == "advisory"
        e.delete(i3)  # leaf 0 group back to zero (built: basis set)
        j = e.ins(4, 5)
        e.delete(j)  # 4: +1 -1 drained -> zero row on the unbuilt leaf
        k = e.ins(10, 1)
        e.delete(k)  # 10 (leaf 1): zero row, no source rows
        e.drain_all()
        e.move(ids[7], 10)  # undrained at leaf 1's S1
        gc = e.gc()
        for _ in range(4):
            gc.pass_()
        e.notes.append(f"gc deleted {gc.deleted}, skipped {gc.skipped}")
        e.release("leaf:1")
        e.collect(b)
        return e.outcome({"gc_deleted": gc.deleted})
    finally:
        e.close()


def l3c_s3_across_leaf_boundary(v):
    """c_a before the build starts, c_b between leaf 0's commit and leaf 1's
    start (both on leaf 1's group 4), drained, zero row offered to the GC
    before leaf 1 runs: leaf 1's S1 sees both."""
    e = Env(v)
    try:
        i = e.ins(4, 7)
        e.drain_all()
        e.reset_h()  # fresh: h empty; src has 4
        e.hold("leaf:1")
        b = e.build_bg("leaf:1")
        assert e.wait_blocked(b.pid, bg=b) == "advisory"
        e.delete(i)
        e.drain_all()  # h(4) = -1 (fresh), not zero
        j = e.ins(4, 7)
        e.drain_all()  # h(4) = 0: zero row, NULL basis
        e.delete(j)  # undrained
        gc = e.gc()
        for _ in range(4):
            gc.pass_()
        e.notes.append(f"gc deleted {gc.deleted}, skipped {gc.skipped}")
        e.release("leaf:1")
        e.collect(b)
        return e.outcome({"gc_deleted": gc.deleted})
    finally:
        e.close()


def l4_remainder_null_absent(v):
    """s1n in the remainder transaction: a page applies a change S1 does not
    see to the absent NULL group between the remainder's read and insert."""
    e = Env(v)
    try:
        e.ins(None, 10)
        e.drain_all()
        e.reset_h()
        rk = "read:3"
        e.hold(rk)
        b = e.build_bg(rk)
        assert e.wait_blocked(b.pid, bg=b) == "advisory"
        e.ins(None, 5)
        p = page_bg(e)
        e.notes.append(f"page {e.wait_blocked(p.pid, bg=p, timeout=1.5)}")
        e.release(rk)
        e.collect(b, p)
        return e.outcome({"stragglers": b.info.get("stragglers")})
    finally:
        e.close()


def l5_page_spans_two_leaves(v):
    """One writer transaction touches groups 3 (leaf 0) and 4 (leaf 1). The
    build stalls inside leaf 1 after its read. A page of that change: under
    per-leaf B it takes gate 0 then waits on gate 1; under definition B it
    waits on the gate; under A it locks 3 and inserts/locks 4 freely (4 not
    yet inserted). A second page touches only leaf 0's group 6."""
    e = Env(v)
    try:
        _seed(e, [3, 4, 6])
        e.ins(3, 1, commit=False)
        e.ins(4, 2)
        before = set(e.ring_positions())
        e.ins(6, 3)
        p2pos = [x for x in e.ring_positions() if x not in before]
        rk = "read:1"
        e.hold(rk)
        b = e.build_bg(rk)
        assert e.wait_blocked(b.pid, bg=b) == "advisory"
        p = page_bg(e, positions=[x for x in before])
        st1 = e.wait_blocked(p.pid, bg=p, timeout=1.5)
        p2 = page_bg(e, positions=p2pos)
        st2 = e.wait_blocked(p2.pid, bg=p2, timeout=1.5)
        e.notes.append(f"page(3,4) {st1}; page(6) {st2}")
        e.release(rk)
        e.collect(b, p, p2)
        return e.outcome({"page_3_4": st1, "page_6": st2})
    finally:
        e.close()


def l6_page_holds_built_leaf_group(v):
    """A page has inserted group 3 (leaf 0, uncommitted) and stalls before
    group 4 (leaf 1); the build's leaf 0 waits on 3; the page then takes 4
    and commits; leaf 0 proceeds. Key order across leaves: no deadlock."""
    e = Env(v)
    try:
        e.ins(3, 1, commit=False)
        e.ins(4, 2)
        cp = e.ring_positions()
        e.hold("plock:4")
        p = page_bg(e, positions=cp, hooks="plock:4")
        assert e.wait_blocked(p.pid, bg=p) == "advisory"
        b = e.build_bg("")
        e.notes.append(f"build {e.wait_blocked(b.pid, bg=b, timeout=1.5)}")
        e.release("plock:4")
        e.collect(b, p)
        return e.outcome({"stragglers": b.info.get("stragglers")})
    finally:
        e.close()


LEAF = [l1_pages_between_leaves, l2_cross_leaf_move_and_back, l2u_cross_leaf_move_undrained,
        l2r_reverse_move_during_leaf, l3a_s3_inside_leaf, l3b_gc_between_leaves, l3c_s3_across_leaf_boundary,
        l4_remainder_null_absent, l5_page_spans_two_leaves, l6_page_holds_built_leaf_group]
LEAF_VARIANTS = ["L-A", "L-A-preRHS", "L-A-preRHS-span", "L-A-preRH", "L-A-nogc", "L-A-preRHS-s2each", "L-B",
                 "L-Bdef", "L-N"]


# --------------------------------------------------------------------------
# Question 4: resume shape (visible target kept)
# --------------------------------------------------------------------------


def r1_resume_lost_changes(v):
    """While paused, changes the ring never saw: group 6 loses its last row,
    a row moves 2 -> 3. Resume: h emptied, tgt kept, then the build."""
    e = Env(v)
    try:
        a = e.ins(2, 1)
        e.ins(2, 5)
        b6 = e.ins(6, 4)
        e.drain_all()
        e.build()
        e.drain_all()
        e.notes.append(f"tgt before = {e.tgt()}")
        e.uncaptured(("delete from src where id = %s", (b6,)), ("update src set g = 3 where id = %s", (a,)))
        e.resume_reset()
        e.build()
        e.notes.append(f"tgt after build = {e.tgt()}")
        return e.outcome({"tgt_after": e.tgt()})
    finally:
        e.close()


def r2_resume_pages_running(v):
    """Resume with pages running: after h is emptied, a page applies a change
    of group 2 (fresh h row, tgt(2) rewritten from the partial) before the
    build's insert reaches 2; group 9 lost its rows uncaptured (tgt only)."""
    e = Env(v)
    try:
        e.ins(2, 1)
        n9 = e.ins(9, 3)
        e.drain_all()
        e.build()
        e.drain_all()
        e.uncaptured(("delete from src where id = %s", (n9,)))
        e.resume_reset()
        e.ins(2, 8)
        e.drain_all()  # h(2) = 1 (partial), tgt(2) = (1, 8)
        e.notes.append(f"tgt mid = {e.tgt()}")
        rk = H(v, "read", 2)
        e.hold(rk)
        b = e.build_bg(rk)
        assert e.wait_blocked(b.pid, bg=b) == "advisory"
        e.ins(2, 4)  # after S1
        p = page_bg(e)
        e.notes.append(f"page {e.wait_blocked(p.pid, bg=p, timeout=1.5)}")
        e.release(rk)
        e.collect(b, p)
        return e.outcome({"stragglers": b.info.get("stragglers")})
    finally:
        e.close()


def r3_resume_drained_sibling(v):
    """Resume after an out-of-order drain on one row (insert into 2
    undrained, move 2 -> 7 drained): needs the drained, unretired ring row
    (RETAIN) or a new-image branch."""
    e = Env(v)
    try:
        e.ins(7, 1)
        e.drain_all()
        e.build()
        e.drain_all()
        before = set(e.ring_positions())
        i = e.ins(2, 3)
        e.move(i, 7)
        pos = [p for p in e.ring_positions() if p not in before]
        e.run_page(positions=[pos[1]])
        e.resume_reset()
        e.build()
        return e.outcome()
    finally:
        e.close()


RESUME = [r1_resume_lost_changes, r2_resume_pages_running, r3_resume_drained_sibling]
RESUME_VARIANTS = ["A-x+T", "A-notarget+T", "A-preRHS+T", "A-preRH+T", "L-A+T", "L-A-preRHS+T", "L-B+T", "B-x+T"]


# --------------------------------------------------------------------------
# Question 5: step 2's read
# --------------------------------------------------------------------------


def q1_many_stragglers(v, ngroups=200, touched=190):
    """Fresh build over 200 pre-capture groups; after S1 (every transaction's
    S1) pages touch 190 of them, so 95 % become stragglers; some groups also
    have a change committed before S1 and drained only after it."""
    e = Env(v)
    try:
        _seed(e, list(range(ngroups)) + [None])
        for g in range(0, ngroups, 7):
            e.ins(g, 100)  # before S1, undrained
        keys = [H(v, "read", leaf=lf) for lf in range(M.LEAVES + 1)] if is_leaf(v) else [H(v, "read")]
        keys = sorted(set(keys))
        for k in keys:
            e.hold(k)
        b = e.build_bg(",".join(keys))
        st = e.wait_blocked(b.pid, bg=b)
        assert st == "advisory", st
        # leaf builds: release leaf by leaf, touching that leaf's groups after its S1
        order = keys if is_leaf(v) else keys
        for k in order:
            lf = None if k.endswith(":N") else int(k.split(":")[1])
            gs = [g for g in range(touched) if lf is None or M.route(g) == lf]
            c = e.conn()
            cur = c.cursor()
            for g in gs:
                cur.execute("insert into src(id, g, v) values (%s, %s, %s)", (e.next_id, g, 1))
                e.next_id += 1
            c.commit()
            e.drain_all()
            e.release(k)
            if k != order[-1]:
                st = e.wait_blocked(b.pid, bg=b)
        e.collect(b)
        info = b.info
        return e.outcome({"stragglers": info.get("stragglers"), "enumerated": info.get("enumerated")})
    finally:
        e.close()


STEP2 = [q1_many_stragglers]
STEP2_VARIANTS = ["A", "A-x", "A-s2gb", "A-s2probe", "A-s2each", "A-preRHS", "L-A-preRHS", "L-A-preRHS-s2each"]


# --------------------------------------------------------------------------
# The reset-with-drained-sibling hole (found while building X1b)
# --------------------------------------------------------------------------


def n1_reset_drained_sibling(v, retain=False):
    """h emptied (fresh rebuild / resume) after an out-of-order drain on one
    row: c1 inserts into 2 (undrained), c2 moves it 2 -> 7 (drained). With
    X1's ring (a page deletes what it drained) nothing names group 2."""
    e = Env(v, retain=retain)
    try:
        before = set(e.ring_positions())
        i = e.ins(2, 3)
        e.move(i, 7)
        pos = [p for p in e.ring_positions() if p not in before]
        e.run_page(positions=[pos[1]])
        e.reset_h()
        e.build()
        return e.outcome()
    finally:
        e.close()


def n1r_reset_drained_sibling_retain(v):
    return n1_reset_drained_sibling(v, retain=True)


HOLE = [n1_reset_drained_sibling, n1r_reset_drained_sibling_retain]
HOLE_VARIANTS = ["T0", "A", "B", "A-newimg", "A-preRHS", "L-A-preRHS", "L-B"]

GROUPS = {"pre": (PRE, PRE_VARIANTS), "leaf": (LEAF, LEAF_VARIANTS), "resume": (RESUME, RESUME_VARIANTS),
          "step2": (STEP2, STEP2_VARIANTS), "hole": (HOLE, HOLE_VARIANTS)}


def main(argv):
    verbose = "-v" in argv
    override = None
    names = []
    for a in argv:
        if a.startswith("--variants="):
            override = a.split("=", 1)[1].split(",")
        elif not a.startswith("-"):
            names.append(a)
    M.create_template()
    plan = []
    for gname, (scns, vs) in GROUPS.items():
        for s in scns:
            if names and gname not in names and not any(s.__name__.startswith(n) for n in names):
                continue
            plan.append((gname, s, override or vs))
    tables = {}
    for gname, s, vs in plan:
        for v in vs:
            t0 = time.monotonic()
            try:
                r = s(v)
                tag = r["outcome"] + ("+deadlock" if r["deadlocks"] else "")
            except Exception as ex:  # noqa
                tag = "ERROR"
                r = {"error": repr(ex), "tb": traceback.format_exc()}
            tables.setdefault(gname, {})[(s.__name__, v)] = tag
            extra = r if (tag != "exact" or verbose) else {k: r[k] for k in r if k in ("stragglers", "notes")}
            print(f"{s.__name__:34s} {v:18s} {tag:16s} {time.monotonic() - t0:5.1f}s {extra}", flush=True)
    for gname, t in tables.items():
        scns = list(dict.fromkeys(k[0] for k in t))
        vs = list(dict.fromkeys(k[1] for k in t))
        print(f"\n### {gname}\n")
        print("| scenario | " + " | ".join(vs) + " |")
        print("|---|" + "---|" * len(vs))
        for s in scns:
            print(f"| {s} | " + " | ".join(t.get((s, v), "") for v in vs) + " |")


if __name__ == "__main__":
    main(sys.argv[1:])
