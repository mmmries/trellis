"""X1 correctness model of the group basis (#1917).

A standalone model on real Postgres: source `src`, trigger capture into `ring`
(with the writer's txid), a hidden table `h` keyed by group with a basis and a
stamp, pages (appliers), the zero-group GC and the build variants of
DESIGN.md's X1 table (plus B-early, a negative control for B).

Nothing here is Trellis code; it mirrors the rules named in DESIGN.md.
"""

import os
import random
import threading
import time

import psycopg
from psycopg import errors

PGHOST = os.environ.get("X1_PGHOST", "/home/mike/code/trellis-lane-r/target/research-pg-x1b")
PGPORT = int(os.environ.get("X1_PGPORT", "54761"))

GATE_KEY = 1917001  # B's per-definition build gate
GCLOCK_KEY = 1917002  # A's "GC skips a definition under a build"

VARIANTS = ["T0", "A", "A-gc", "A-2pass", "A-noenum", "B", "N", "T0-early", "B-early"]

# X1b: when True, pages and builds maintain the visible target `tgt` (resume shape).
TARGET = False
# X1b: when True, a page marks its ring rows drained instead of deleting them, and
# the GC retires drained rows only up to its sound prefix (in-order retirement, as
# the engine's segments). The enumeration's ring branch then also reads drained,
# unretired rows. When False: X1's behaviour (a page deletes what it drained).
RETAIN = False
LEAVES = 3  # leaf index = g % 3; remainder (NULL group) = 3
GATE_LEAF_BASE = 1917100  # per-leaf B gates: GATE_LEAF_BASE + leaf

SCHEMA = r"""
create table src(id bigint primary key, g int, v int not null);
create sequence ring_pos_seq;
create table ring(
  pos bigint primary key default nextval('ring_pos_seq'),
  txid xid8 not null default pg_current_xact_id(),
  old_g int, old_v int, has_old bool not null,
  new_g int, new_v int, has_new bool not null,
  drained bool not null default false);  -- X1b RETAIN: drained rows kept until retired
create function cap() returns trigger language plpgsql as $$
begin
  if tg_op = 'INSERT' then
    insert into ring(has_old, new_g, new_v, has_new) values (false, new.g, new.v, true);
  elsif tg_op = 'UPDATE' then
    insert into ring(old_g, old_v, has_old, new_g, new_v, has_new) values (old.g, old.v, true, new.g, new.v, true);
  else
    insert into ring(old_g, old_v, has_old, has_new) values (old.g, old.v, true, false);
  end if;
  return null;
end $$;
create trigger cap after insert or update or delete on src for each row execute function cap();
create table h(g int, n bigint not null default 0, s numeric not null default 0,
  basis pg_snapshot, stamp bigint, unique nulls not distinct (g));
-- X1b: the visible target (resume keeps it); maintained only when TARGET is on.
create table tgt(g int, n bigint not null, s numeric not null, unique nulls not distinct (g));
-- X1b: partition routing of a group to a leaf (g % 3), NULL to the remainder (3).
create function x1_route(g int) returns int language sql immutable as
  $f$ select case when g is null then 3 else ((g % 3) + 3) % 3 end $f$;

-- Stall hook. The harness lists 'tag:g' keys in x1.hooks and holds a session
-- advisory lock on hashtext(key); the hook waits for it. x1.sleep_ms adds a
-- random sleep at the 'read' point (soak jitter).
create function x1_hook(tag text, g int) returns int language plpgsql volatile as $$
declare
  k text := tag || ':' || coalesce(g::text, 'N');
  ms int := coalesce(nullif(current_setting('x1.sleep_ms', true), ''), '0')::int;
begin
  if ms > 0 and tag in ('read', 'pass2', 'strag', 'pre', 'leaf') then
    perform pg_sleep(random() * ms / 1000.0);
  end if;
  if position(',' || k || ',' in ',' || coalesce(current_setting('x1.hooks', true), '') || ',') > 0 then
    perform pg_advisory_lock(hashtext(k));
    perform pg_advisory_unlock(hashtext(k));
  end if;
  return 0;
end $$;
"""

# The enumeration (R8): source groups, groups named by old images of
# undrained ring changes, and hidden rows; all read in the statement's snapshot.
ENUM_FULL = "select g from src union select old_g from ring where has_old union select g from h"
ENUM_SRC_ONLY = "select distinct g from src"

SNAP_CTE = """snap as materialized (
  select pg_current_snapshot() as s,
         (select case when is_called then last_value else 0 end from ring_pos_seq) as st)"""

AGG_OVER = """(select e.g, coalesce(a.n, 0) as n, coalesce(a.s, 0) as s
   from ({enum}) e
   left join (select g, count(*) as n, sum(v) as s from src group by g) a
     on a.g is not distinct from e.g)"""


def connect(db, autocommit=False):
    return psycopg.connect(host=PGHOST, port=PGPORT, user="postgres", dbname=db, autocommit=autocommit)


def admin():
    return connect("postgres", autocommit=True)


def create_template(name="x1_template"):
    with admin() as c:
        c.execute(f"drop database if exists {name}")
        c.execute(f"create database {name}")
    with connect(name, autocommit=True) as c:
        c.execute(SCHEMA)


def create_db(name, template="x1_template"):
    with admin() as c:
        c.execute(f"drop database if exists {name} with (force)")
        c.execute(f"create database {name} template {template}")


def drop_db(name):
    with admin() as c:
        c.execute(f"drop database if exists {name} with (force)")


def sqlstate(e):
    return getattr(e, "sqlstate", None)


# --------------------------------------------------------------------------
# Pages
# --------------------------------------------------------------------------


class Page:
    """One page = one transaction: claim, stage, (gate), lock, apply, delete, commit."""

    def __init__(self, conn, gate=False):
        self.conn = conn
        self.gate = gate
        self.rows = []
        self.lock_wait = 0.0

    def claim(self, positions=None, batch=10, order="asc"):
        cur = self.conn.cursor()
        if positions is not None:
            cur.execute(
                "select pos, txid::text, old_g, old_v, has_old, new_g, new_v, has_new from ring "
                "where pos = any(%s) and not drained order by pos for update skip locked",
                (list(positions),),
            )
        else:
            ob = {"asc": "pos", "desc": "pos desc", "random": "random()"}[order]
            cur.execute(
                "select pos, txid::text, old_g, old_v, has_old, new_g, new_v, has_new from ring "
                f"where not drained order by {ob} limit %s for update skip locked",
                (batch,),
            )
        self.rows = cur.fetchall()
        return len(self.rows)

    def stage(self):
        sums = {}
        for pos, txid, og, ov, ho, ng, nv, hn in self.rows:
            if ho:
                k = (og, txid)
                dn, ds = sums.get(k, (0, 0))
                sums[k] = (dn - 1, ds - ov)
            if hn:
                k = (ng, txid)
                dn, ds = sums.get(k, (0, 0))
                sums[k] = (dn + 1, ds + nv)
        self.sides = sums
        self.groups = sorted({g for g, _ in sums}, key=lambda g: (g is None, g if g is not None else 0))
        return self.groups

    def lock(self):
        t = time.monotonic()
        cur = self.conn.cursor()
        if self.gate == "leaf":
            # X1b per-leaf B: a shared gate for every leaf the page's groups
            # route to, in leaf order, before the lock statement.
            for lf in sorted({route(g) for g in self.groups}):
                cur.execute("select pg_advisory_xact_lock_shared(%s)", (GATE_LEAF_BASE + lf,))
        elif self.gate:
            cur.execute("select pg_advisory_xact_lock_shared(%s)", (GATE_KEY,))
        # The page's lock statement: one key-ordered pass, inserting a zero
        # row with a NULL basis for an absent group, locking an existing one.
        cur.execute(
            "insert into h(g) select g from (select g from unnest(%s::int[]) g order by g offset 0) q "
            "where x1_hook('plock', g) = 0 "
            "on conflict (g) do update set n = excluded.n where false",
            (self.groups,),
        )
        self.lock_wait = time.monotonic() - t

    def apply(self):
        keys = list(self.sides.items())
        gs = [k[0] for k, _ in keys]
        ts = [k[1] for k, _ in keys]
        dns = [v[0] for _, v in keys]
        dss = [v[1] for _, v in keys]
        cur = self.conn.cursor()
        # The apply statement: add each (group, txid) sum whose txid the
        # group's basis does not see (a NULL basis sees nothing).
        cur.execute(
            """
            update h set n = h.n + x.dn, s = h.s + x.ds
            from (select d.g, sum(d.dn) as dn, sum(d.ds) as ds
                  from unnest(%s::int[], %s::text[], %s::bigint[], %s::numeric[]) d(g, t, dn, ds)
                  join h hh on hh.g is not distinct from d.g
                  where hh.basis is null or not pg_visible_in_snapshot(d.t::xid8, hh.basis)
                  group by d.g) x
            where h.g is not distinct from x.g
            """,
            (gs, ts, dns, dss),
        )
        if TARGET:
            sync_tgt(cur, self.groups)

    def finish(self):
        cur = self.conn.cursor()
        if RETAIN:
            cur.execute("update ring set drained = true where pos = any(%s)", ([r[0] for r in self.rows],))
        else:
            cur.execute("delete from ring where pos = any(%s)", ([r[0] for r in self.rows],))
        self.conn.commit()

    def run(self, **claim_kw):
        """Whole page; returns number of ring rows drained (0 = nothing claimed)."""
        n = self.claim(**claim_kw)
        if n == 0:
            self.conn.rollback()
            return 0
        self.stage()
        self.lock()
        self.apply()
        self.finish()
        return n


def route(g):
    return LEAVES if g is None else g % LEAVES


def gsort(keys):
    return sorted(keys, key=lambda g: (g is None, g if g is not None else 0))


def sync_tgt(cur, groups):
    """X1b: rewrite the visible target rows of `groups` from h (visible only
    while n > 0). Callers hold the h rows of these groups locked."""
    groups = list(groups)
    if not groups:
        return
    nn = [g for g in groups if g is not None]
    hn = None in groups
    cur.execute("delete from tgt where g = any(%s) or (g is null and %s)", (nn, hn))
    cur.execute(
        "insert into tgt(g, n, s) select g, n, s from h where (g = any(%s) or (g is null and %s)) and n > 0",
        (nn, hn),
    )


# --------------------------------------------------------------------------
# Zero-group GC
# --------------------------------------------------------------------------


class GC:
    """The zero-group GC with a sound drained prefix.

    Prefix soundness: a candidate is (L, X): L the last assigned ring
    position, then X = an xid this GC transaction assigns itself in a later
    statement. Every position <= L was assigned by a transaction whose txid
    was assigned before its nextval, hence before X: its txid is < X. Once a
    later statement's snapshot has xmin > X, every such transaction has
    finished; that statement sees every committed ring row with pos <= L
    that no committed page has deleted. So prefix = least(L, min visible
    pos - 1) is a true and monotone fact: every pos <= prefix is drained or
    aborted. (pg_snapshot_xmax is NOT a bound on assigned xids: it is
    latestCompletedXid + 1, so an in-progress writer's xid can be >= it.)
    """

    def __init__(self, conn, skip_under_build=False, k=50, naive=False):
        self.conn = conn
        self.skip = skip_under_build
        self.k = k
        self.pending = None
        self.prefix = 0
        self.naive = naive
        self.deleted = 0
        self.skipped = 0

    def pass_(self):
        cur = self.conn.cursor()
        if self.skip:
            cur.execute("select pg_try_advisory_xact_lock_shared(%s)", (GCLOCK_KEY,))
            if not cur.fetchone()[0]:
                self.conn.rollback()
                self.skipped += 1
                return 0
        if self.naive:
            cur.execute(
                "select coalesce((select min(pos) - 1 from ring where not drained),"
                " (select case when is_called then last_value else 0 end from ring_pos_seq))"
            )
            self.prefix = cur.fetchone()[0]
        else:
            if self.pending is not None:
                L, xmax = self.pending
                cur.execute(
                    "select case when pg_snapshot_xmin(pg_current_snapshot()) > %s::xid8 "
                    "then least(%s, coalesce((select min(pos) from ring where not drained), %s + 1) - 1) end",
                    (xmax, L, L),
                )
                p = cur.fetchone()[0]
                if p is not None:
                    self.prefix = max(self.prefix, p)
                    self.pending = None
            if self.pending is None:
                cur.execute("select case when is_called then last_value else 0 end from ring_pos_seq")
                L = cur.fetchone()[0]
                cur.execute("select pg_current_xact_id()::text")
                self.pending = (L, cur.fetchone()[0])
        cur.execute(
            "delete from h where ctid = any(array(select ctid from h where n = 0 and s = 0 "
            "and (basis is null or stamp <= %s) order by g limit %s for update skip locked))",
            (self.prefix, self.k),
        )
        d = cur.rowcount
        if RETAIN:
            # retire drained ring rows in order: only up to the sound prefix
            cur.execute("delete from ring where drained and pos <= %s", (self.prefix,))
        self.conn.commit()
        self.deleted += d
        return d


# --------------------------------------------------------------------------
# Builds
# --------------------------------------------------------------------------


def hook(cur, tag, g=None):
    cur.execute("select x1_hook(%s, %s)", (tag, g))


def build(conn, variant, info=None):
    """Run one build of `variant` in one transaction on `conn`; commits.

    Returns a dict with stragglers / enumerated counts. Raises on errors
    (the caller rolls back)."""
    info = {} if info is None else info
    variant = base(variant)
    if variant in XV:
        return build_x(conn, variant, info)
    cur = conn.cursor()
    if variant in ("T0", "T0-early"):
        return _build_t0(cur, conn, variant, info)
    if variant in ("A", "A-gc", "A-2pass", "A-noenum"):
        return _build_a(cur, conn, variant, info)
    if variant in ("B", "N", "B-early"):
        return _build_upsert(cur, conn, variant, info)
    raise ValueError(variant)


LOCK_KEYS_SQL = (
    "insert into h(g) select g from (select g from unnest(%s::int[]) g order by g offset 0) q "
    "where x1_hook('lock', g) = 0 on conflict (g) do update set n = excluded.n where false"
)


def _build_t0(cur, conn, variant, info):
    # Enumerate (its own statement), then lock, then absolute write.
    cur.execute(f"select array_agg(g) from ({ENUM_FULL}) e")
    keys = cur.fetchone()[0] or []
    keys = sorted(keys, key=lambda g: (g is None, g if g is not None else 0))
    info["enumerated"] = len(keys)
    if variant == "T0":
        cur.execute(LOCK_KEYS_SQL, (keys,))
        cur.execute(
            f"""
            with {SNAP_CTE},
            stall as materialized (select x1_hook('read', null) as z),
            a as {AGG_OVER.format(enum="select unnest(%s::int[]) as g")}
            update h set n = a.n, s = a.s, basis = snap.s, stamp = snap.st
            from a, snap, stall
            where h.g is not distinct from a.g
            """,
            (keys,),
        )
    else:  # T0-early: the snapshot and values are taken before the lock (R1 broken)
        cur.execute(
            f"""
            with {SNAP_CTE},
            a as {AGG_OVER.format(enum="select unnest(%s::int[]) as g")}
            select (select s::text from snap), (select st from snap), array_agg(a.g), array_agg(a.n), array_agg(a.s) from a
            """,
            (keys,),
        )
        S, st, gs, ns, ss = cur.fetchone()
        hook(cur, "read")
        cur.execute(LOCK_KEYS_SQL, (keys,))
        cur.execute(
            """
            update h set n = a.n, s = a.s, basis = %s::pg_snapshot, stamp = %s
            from unnest(%s::int[], %s::bigint[], %s::numeric[]) a(g, n, s)
            where h.g is not distinct from a.g
            """,
            (S, st, gs or [], ns or [], ss or []),
        )
    hook(cur, "precommit")
    conn.commit()
    info["stragglers"] = 0
    return info


def _build_a(cur, conn, variant, info):
    if variant != "A-gc":
        # GC skips this definition under a build: exclusive against the GC's
        # shared try-lock, taken in a statement before S1.
        cur.execute("select pg_advisory_xact_lock(%s)", (GCLOCK_KEY,))
    enum = ENUM_SRC_ONLY if variant == "A-noenum" else ENUM_FULL
    if variant == "A-2pass":
        conflict = "on conflict (g) do nothing"
    else:
        conflict = "on conflict (g) do update set n = excluded.n where false"
    # Step 1: one statement takes S1 and the stamp, enumerates, aggregates
    # and inserts every absent group once with its final values, locking the
    # existing ones (A) / skipping them (A-2pass), in key order.
    cur.execute(
        f"""
        with {SNAP_CTE},
        e as materialized ({enum}),
        a as {AGG_OVER.format(enum="select g from e")},
        stall as materialized (select x1_hook('read', null) as z),
        ins as (
          insert into h(g, n, s, basis, stamp)
          select q.g, q.n, q.s, (select s from snap), (select st from snap)
          from (select a.g, a.n, a.s from a, stall order by a.g offset 0) q
          where x1_hook('ins', q.g) = 0
          {conflict}
          returning g)
        select (select array_agg(g) from e), (select array_agg(g) from ins)
        """
    )
    enumerated, inserted = cur.fetchone()
    enumerated = enumerated or []
    inserted = inserted or []
    ins_set = set(inserted)
    strag = [g for g in enumerated if g not in ins_set]
    strag.sort(key=lambda g: (g is None, g if g is not None else 0))
    info["enumerated"] = len(enumerated)
    info["stragglers"] = len(strag)
    if variant == "A-2pass" and strag:
        hook(cur, "pass2")
        cur.execute(
            "select g from h where g = any(%s) or (g is null and %s) order by g for update",
            ([g for g in strag if g is not None], None in strag),
        )
    if strag:
        hook(cur, "strag")
        # Step 2: S2 after the lock; today's absolute write over the stragglers.
        cur.execute(
            f"""
            with {SNAP_CTE},
            a as {AGG_OVER.format(enum="select unnest(%s::int[]) as g")}
            update h set n = a.n, s = a.s, basis = snap.s, stamp = snap.st
            from a, snap
            where h.g is not distinct from a.g
            """,
            (strag,),
        )
    hook(cur, "precommit")
    conn.commit()
    return info


def _build_upsert(cur, conn, variant, info):
    if variant == "B":
        cur.execute("select pg_advisory_xact_lock(%s)", (GATE_KEY,))
    gate = (
        "gate as materialized (select pg_advisory_xact_lock(%s) as z from stall)," % GATE_KEY
        if variant == "B-early"
        else "gate as materialized (select 0 as z from stall),"
    )
    cur.execute(
        f"""
        with {SNAP_CTE},
        e as materialized ({ENUM_FULL}),
        a as {AGG_OVER.format(enum="select g from e")},
        stall as materialized (select x1_hook('read', null) as z),
        {gate}
        ins as (
          insert into h(g, n, s, basis, stamp)
          select q.g, q.n, q.s, (select s from snap), (select st from snap)
          from (select a.g, a.n, a.s from a, gate order by a.g offset 0) q
          where x1_hook('ins', q.g) = 0
          on conflict (g) do update set n = excluded.n, s = excluded.s, basis = excluded.basis, stamp = excluded.stamp
          returning g)
        select count(*) from ins
        """
    )
    info["enumerated"] = cur.fetchone()[0]
    info["stragglers"] = 0
    hook(cur, "precommit")
    conn.commit()
    return info


def gc_skips(variant):
    variant = base(variant)
    if variant in XV:
        o = XV[variant]
        return o["kind"] == "A" and o.get("gc", "early") != "none"
    return variant in ("A", "A-2pass", "A-noenum")


def page_gate(variant):
    variant = base(variant)
    if variant in XV:
        g = XV[variant].get("gate", "none")
        return "leaf" if g == "leaf" else (g == "def")
    return variant in ("B", "B-early")


# --------------------------------------------------------------------------
# Check
# --------------------------------------------------------------------------


def divergence(conn):
    """Multiset difference between h's non-zero rows and the source's groups."""
    cur = conn.cursor()
    cur.execute(
        """
        with t as (select g, count(*)::bigint as n, sum(v)::numeric as s from src group by g),
             m as (select g, n, s from h where n <> 0 or s <> 0)
        (select 'missing_or_wrong_in_h' as side, g, n, s from (select * from t except all select * from m) x)
        union all
        (select 'extra_or_wrong_in_h', g, n, s from (select * from m except all select * from t) y)
        order by 2, 1
        """
    )
    rows = cur.fetchall()
    if TARGET:
        cur.execute(
            """
            with t as (select g, count(*)::bigint as n, sum(v)::numeric as s from src group by g)
            (select 'tgt_missing_or_wrong' as side, g, n, s from (select * from t except all select g, n, s from tgt) x)
            union all
            (select 'tgt_extra_or_wrong', g, n, s from (select g, n, s from tgt except all select * from t) y)
            order by 2, 1
            """
        )
        rows += cur.fetchall()
    conn.rollback()
    return rows


def h_state(conn):
    cur = conn.cursor()
    cur.execute("select g, n, s, basis is not null, stamp from h order by g")
    r = cur.fetchall()
    conn.rollback()
    return r


# --------------------------------------------------------------------------
# X1b: pre-enumeration, partition leaves, per-leaf B, resume (target),
# step 2's read forms. One generic builder; the original variants above are
# untouched.
# --------------------------------------------------------------------------

XV = {
    # non-leaf
    "A-x": dict(kind="A"),  # A re-implemented by the generic builder (sanity)
    "A-notarget": dict(kind="A", tbranch=False),  # resume control
    "A-newimg": dict(kind="A", newimg=True),  # A + ring new-image branch
    "A-preRH": dict(kind="A", pre="RH"),
    "A-preRHS": dict(kind="A", pre="RHS"),
    "A-preRHS-late": dict(kind="A", pre="RHS", gc="late"),  # GC lock after E, before S1
    "A-preRHS-nogc": dict(kind="A", pre="RHS", gc="none"),
    "A-s2gb": dict(kind="A", s2="gb"),
    "A-s2probe": dict(kind="A", s2="probe"),
    "A-s2each": dict(kind="A", s2="each"),
    "B-x": dict(kind="B", gate="def"),
    # partition leaves: K+1 transactions
    "L-A": dict(kind="A", leaves=True),
    "L-A-nogc": dict(kind="A", leaves=True, gc="none"),
    "L-A-preRHS": dict(kind="A", leaves=True, pre="RHS"),
    "L-A-preRHS-span": dict(kind="A", leaves=True, pre="RHS", gc="span"),
    "L-A-preRH": dict(kind="A", leaves=True, pre="RH"),
    "L-A-preRHS-s2each": dict(kind="A", leaves=True, pre="RHS", s2="each"),
    "L-B": dict(kind="B", leaves=True, gate="leaf"),
    "L-Bdef": dict(kind="B", leaves=True, gate="def"),
    "L-N": dict(kind="B", leaves=True, gate="none"),
}


def base(variant):
    return variant.split("+", 1)[0]


def has_target(variant):
    return variant.endswith("+T")


def _f(col, leaf):
    return "true" if leaf is None else f"x1_route({col}) = {int(leaf)}"


def enum_sql(o, leaf, src=True):
    parts = [
        f"select g from h where {_f('g', leaf)}",
        f"select old_g from ring where has_old and {_f('old_g', leaf)}",
    ]
    if o.get("newimg"):
        parts.append(f"select new_g from ring where has_new and {_f('new_g', leaf)}")
    if TARGET and o.get("tbranch", True):
        parts.append(f"select g from tgt where {_f('g', leaf)}")
    if src:
        parts.append(f"select g from src where {_f('g', leaf)}")
    return " union ".join(parts)


def _lk(leaf):
    return None if leaf is None else int(leaf)


def build_x(conn, variant, info):
    o = XV[variant]
    info.setdefault("enumerated", 0)
    info.setdefault("stragglers", 0)
    info.setdefault("xleaf", 0)
    info.setdefault("txns", 0)
    if not o.get("leaves"):
        _txn(conn, o, None, info)
        return info
    cur = conn.cursor()
    span = o.get("gc") == "span"
    if span:
        # GC skip held across the whole multi-transaction build (session lock).
        cur.execute("select pg_advisory_lock(%s)", (GCLOCK_KEY,))
        conn.commit()
    try:
        for leaf in range(LEAVES + 1):
            cur.execute("select x1_hook('leaf', %s)", (leaf,))
            conn.commit()
            _txn(conn, o, leaf, info)
    finally:
        if span:
            try:
                conn.rollback()
                cur.execute("select pg_advisory_unlock(%s)", (GCLOCK_KEY,))
                conn.commit()
            except Exception:
                pass
    return info


def _txn(conn, o, leaf, info):
    cur = conn.cursor()
    lk = _lk(leaf)
    if o["kind"] == "B":
        return _txn_b(conn, cur, o, leaf, info)
    gc = o.get("gc", "early")
    if gc == "early":
        cur.execute("select pg_advisory_xact_lock(%s)", (GCLOCK_KEY,))
    pre = o.get("pre")
    if pre:
        # Statement E: the pre-enumeration, its own snapshot.
        cur.execute(f"select array_agg(g) from ({enum_sql(o, leaf, src=(pre == 'RHS'))}) e")
        prekeys = cur.fetchone()[0] or []
        cur.execute("select x1_hook('pre', %s)", (lk,))
        if gc == "late":
            cur.execute("select pg_advisory_xact_lock(%s)", (GCLOCK_KEY,))
        e_sql = f"select g from unnest(%(pre)s::int[]) g union select g from src where {_f('g', leaf)}"
        params = {"pre": prekeys, "lk": lk}
    else:
        if gc == "late":
            cur.execute("select pg_advisory_xact_lock(%s)", (GCLOCK_KEY,))
        e_sql = enum_sql(o, leaf)
        params = {"lk": lk}
    # Step 1 at S1: insert absent groups with final values, lock existing ones.
    cur.execute(
        f"""
        with {SNAP_CTE},
        e as materialized ({e_sql}),
        a as (select e.g, coalesce(x.n, 0) as n, coalesce(x.s, 0) as s
              from e left join (select g, count(*) as n, sum(v) as s from src where {_f('g', leaf)} group by g) x
              on x.g is not distinct from e.g),
        stall as materialized (select x1_hook('read', %(lk)s) as z),
        ins as (
          insert into h(g, n, s, basis, stamp)
          select q.g, q.n, q.s, (select s from snap), (select st from snap)
          from (select a.g, a.n, a.s from a, stall order by a.g offset 0) q
          where x1_hook('ins', q.g) = 0
          on conflict (g) do update set n = excluded.n where false
          returning g)
        select (select array_agg(g) from e), (select array_agg(g) from ins)
        """,
        params,
    )
    enumerated, inserted = cur.fetchone()
    enumerated = enumerated or []
    ins_set = set(inserted or [])
    strag = gsort([g for g in enumerated if g not in ins_set])
    info["enumerated"] += len(enumerated)
    info["stragglers"] += len(strag)
    info["txns"] += 1
    if leaf is not None:
        info["xleaf"] += sum(1 for g in enumerated if route(g) != leaf)
    if strag:
        cur.execute("select x1_hook('strag', %s)", (lk,))
        _step2(cur, o.get("s2", "agg"), strag)
    if TARGET:
        sync_tgt(cur, gsort(enumerated))
    cur.execute("select x1_hook('precommit', %s)", (lk,))
    conn.commit()


def _step2(cur, form, strag):
    nn = [g for g in strag if g is not None]
    hn = None in strag
    upd = "update h set n = a.n, s = a.s, basis = snap.s, stamp = snap.st from a, snap where h.g is not distinct from a.g"
    if form == "agg":  # X1's form: full GROUP BY joined to the stragglers
        cur.execute(f"with {SNAP_CTE}, a as {AGG_OVER.format(enum='select unnest(%s::int[]) as g')} {upd}", (strag,))
    elif form == "gb":  # one GROUP BY filtered to the stragglers
        cur.execute(
            f"""with {SNAP_CTE},
            a as (select k.g, coalesce(x.n, 0) as n, coalesce(x.s, 0) as s
                  from unnest(%s::int[]) k(g)
                  left join (select g, count(*) as n, sum(v) as s from src
                             where g = any(%s) or (g is null and %s) group by g) x
                  on x.g is not distinct from k.g)
            {upd}""",
            (strag, nn, hn),
        )
    elif form == "probe":  # per-straggler probes, one statement (one snapshot S2)
        cur.execute(
            f"""with {SNAP_CTE},
            a as (select k.g, p.n, p.s from unnest(%s::int[]) k(g)
                  cross join lateral (select count(*) as n, coalesce(sum(v), 0) as s
                                      from src where src.g is not distinct from k.g) p)
            {upd}""",
            (strag,),
        )
    elif form == "each":  # one statement per straggler: each its own snapshot
        for g in strag:
            cur.execute(
                f"""with {SNAP_CTE},
                a as (select %s::int as g, count(*) as n, coalesce(sum(v), 0) as s
                      from src where src.g is not distinct from %s::int)
                {upd}""",
                (g, g),
            )
    else:
        raise ValueError(form)


def _txn_b(conn, cur, o, leaf, info):
    lk = _lk(leaf)
    gate = o.get("gate", "none")
    if gate == "def":
        cur.execute("select pg_advisory_xact_lock(%s)", (GATE_KEY,))
    elif gate == "leaf":
        cur.execute("select pg_advisory_xact_lock(%s)", (GATE_LEAF_BASE + (LEAVES if leaf is None else leaf),))
    cur.execute(
        f"""
        with {SNAP_CTE},
        e as materialized ({enum_sql(o, leaf)}),
        a as (select e.g, coalesce(x.n, 0) as n, coalesce(x.s, 0) as s
              from e left join (select g, count(*) as n, sum(v) as s from src where {_f('g', leaf)} group by g) x
              on x.g is not distinct from e.g),
        stall as materialized (select x1_hook('read', %(lk)s) as z),
        ins as (
          insert into h(g, n, s, basis, stamp)
          select q.g, q.n, q.s, (select s from snap), (select st from snap)
          from (select a.g, a.n, a.s from a, stall order by a.g offset 0) q
          where x1_hook('ins', q.g) = 0
          on conflict (g) do update set n = excluded.n, s = excluded.s, basis = excluded.basis, stamp = excluded.stamp
          returning g)
        select array_agg(g) from ins
        """,
        {"lk": lk},
    )
    enumerated = cur.fetchone()[0] or []
    info["enumerated"] += len(enumerated)
    info["txns"] += 1
    if leaf is not None:
        info["xleaf"] += sum(1 for g in enumerated if route(g) != leaf)
    if TARGET:
        sync_tgt(cur, gsort(enumerated))
    cur.execute("select x1_hook('precommit', %s)", (lk,))
    conn.commit()
