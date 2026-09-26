"""#565 phase-2 correctness probes (no bench lock: tiny scratch cluster, no timing).

A1  nested same-key write: an application AFTER ROW trigger updates the row its statement just
    wrote. The nested statement's capture trigger fires *before* the outer statement's, so the
    ring holds the newer image first. Folded by (lsn, change_id), does the target match source?
    Compared: the generated statement trigger, a row trigger (name-ordered against the app's), and a
    're-read' statement trigger whose images come from the live row at fire time.
A3  partitioned and inherited sources: which trigger forms are accepted, and which writes fire them.
A4  statement trigger + column list + transition tables (the 'WHEN/UPDATE OF' skip in #565 item 4).
A5  the mirror read as a role without privilege on the sequence (PG 17 returns NULL).
"""
import os, shutil, signal, subprocess, sys, time
import psycopg
sys.path.insert(0, os.path.dirname(__file__))
import capture_sql

BASE, PORT = "/tmp/tc-565/probes", 5575


def start():
    shutil.rmtree(BASE, ignore_errors=True); os.makedirs(BASE)
    subprocess.run(["initdb", "-D", f"{BASE}/data", "-U", "postgres", "-A", "trust"], check=True, capture_output=True)
    pg = subprocess.Popen(["postgres", "-D", f"{BASE}/data", "-h", "", "-k", BASE, "-p", str(PORT),
                           "-c", "logging_collector=off", "-c", "dynamic_shared_memory_type=mmap"],
                          stdout=open(f"{BASE}/pg.log", "w"), stderr=subprocess.STDOUT)
    for _ in range(100):
        if subprocess.run(["pg_isready", "-h", BASE, "-p", str(PORT), "-q"]).returncode == 0:
            break
        time.sleep(0.1)
    return pg


def dsn(user="postgres"):
    return f"host={BASE} port={PORT} user={user} dbname=postgres"


def ring_schema(db):
    db.execute("drop schema if exists trellis cascade; create schema trellis")
    db.execute("create table trellis.segment_pointer (id bool primary key default true, ring_slot smallint not null)")
    db.execute("insert into trellis.segment_pointer values (true, 0)")
    db.execute("create sequence trellis.staging_change_id_seq")
    for i in range(4):
        db.execute(f"create table trellis.seg_{i} (src_table text, key text, op text, lsn pg_lsn, old_image jsonb, "
                   f"new_image jsonb, origin_lsn pg_lsn, src_changed timestamptz, "
                   f"change_id bigint default nextval('trellis.staging_change_id_seq'))")


def fold(db):
    """Last image per key by (lsn, change_id): what the drain would apply."""
    rows = db.execute("select key, op, new_image from trellis.seg_0 order by lsn, change_id").fetchall()
    state = {}
    for k, op, img in rows:
        if op == "delete":
            state.pop(k, None)
        else:
            state[k] = img
    return state


def source(db):
    return {str(r[0]): {"id": str(r[0]), "val": None if r[1] is None else str(r[1])}
            for r in db.execute("select id, val from public.gen")}


def reread_stmt_trigger():
    """Statement trigger whose images are the live row at fire time (keys from the transition table)."""
    out = []
    for ev, refs, keys in [("insert", "NEW TABLE AS t", "t.id"), ("update", "OLD TABLE AS o NEW TABLE AS t",
                            None), ("delete", "OLD TABLE AS t", "t.id")]:
        src = ("(select id from o union select id from t)" if ev == "update" else "(select id from t)")
        out.append(f"""
CREATE OR REPLACE FUNCTION trellis.cap_rr_{ev}() RETURNS trigger LANGUAGE plpgsql AS $f$
DECLARE l pg_lsn := pg_current_wal_insert_lsn();
BEGIN
  INSERT INTO trellis.seg_0 (src_table, key, op, lsn, new_image, origin_lsn)
  SELECT 'public.gen', k.id::text, CASE WHEN cur.id IS NULL THEN 'delete' ELSE 'upsert' END, l,
         CASE WHEN cur.id IS NOT NULL THEN jsonb_build_object('id', cur.id::text, 'val', cur.val::text) END, l
  FROM {src} k(id) LEFT JOIN public.gen cur ON cur.id = k.id;
  RETURN NULL;
END $f$;
CREATE TRIGGER trellis_capture_{ev} AFTER {ev.upper()} ON public.gen REFERENCING {refs}
  FOR EACH STATEMENT EXECUTE FUNCTION trellis.cap_rr_{ev}();""")
    return "\n".join(out)


def a1(db):
    print("\n## A1 nested same-key write (app AFTER ROW trigger re-updates the row)")
    for capture in ["stmt", "row", "row_app_sorts_after", "reread", "gen_reread", "gen_reread_skip"]:
        for app in ["insert_then_bump", "update_then_bump", "insert_then_delete"]:
            db.execute("drop table if exists public.gen cascade")
            ring_schema(db)
            db.execute("create table public.gen (id bigint primary key, val numeric)")
            capture_sql.ENC.update(mode="coltext", pin=False)
            if capture == "stmt":
                db.execute(capture_sql.stmt_trigger("gen", ["id", "val"], ["id"], definer=False))
            elif capture == "reread":
                db.execute(reread_stmt_trigger())
            elif capture.startswith("gen_reread"):
                capture_sql.SHAPE.update(reread=True, skip_noop=capture.endswith("skip"))
                db.execute(capture_sql.stmt_trigger("gen", ["id", "val"], ["id"], definer=False))
                capture_sql.SHAPE.update(reread=False, skip_noop=False)
            else:
                db.execute(capture_sql.row_trigger("gen", ["id", "val"], ["id"], definer=False))
            tname = "zz_app" if capture == "row_app_sorts_after" else "app_bump"
            if app == "insert_then_bump":
                body, ev = "UPDATE public.gen SET val = val + 100 WHERE id = NEW.id;", "INSERT"
            elif app == "update_then_bump":
                body, ev = "IF NEW.val < 100 THEN UPDATE public.gen SET val = val + 100 WHERE id = NEW.id; END IF;", "UPDATE"
            else:
                body, ev = "DELETE FROM public.gen WHERE id = NEW.id AND NEW.val < 0;", "INSERT"
            db.execute(f"create or replace function public.app_fn() returns trigger language plpgsql as $$ begin {body} return null; end $$")
            db.execute(f"create trigger {tname} after {ev} on public.gen for each row execute function public.app_fn()")
            if app == "update_then_bump":
                db.execute("alter table public.gen disable trigger " + tname)
                db.execute("insert into public.gen values (1, 1), (2, 2)")
                db.execute("alter table public.gen enable trigger " + tname)
                db.execute("update public.gen set val = val + 1")
            elif app == "insert_then_delete":
                db.execute("insert into public.gen values (1, -1), (2, 2)")
            else:
                db.execute("insert into public.gen values (1, 1), (2, 2)")
            src, got = source(db), fold(db)
            ring = db.execute("select op, key, new_image->>'val' from trellis.seg_0 order by lsn, change_id").fetchall()
            verdict = "MATCH" if src == got else "DIVERGE"
            print(f"  {capture:22s} {app:20s} {verdict:8s} source={ {k: v['val'] for k, v in src.items()} } "
                  f"folded={ {k: (v or {}).get('val') for k, v in got.items()} } ring={ring}")


def a3(db):
    print("\n## A3 partitioned / inherited sources")
    db.execute("drop table if exists public.p, public.inh_parent cascade")
    db.execute("create table public.p (id bigint primary key, val numeric) partition by range (id)")
    db.execute("create table public.p1 partition of public.p for values from (0) to (1000)")
    db.execute("create table public.log (who text, n bigint)")
    db.execute("""create or replace function public.logit() returns trigger language plpgsql as $$
        begin insert into public.log select TG_TABLE_NAME || ':' || TG_LEVEL, count(*) from n; return null; end $$""")
    def attempt(label, sql):
        try:
            db.execute(sql); print(f"  [ok]    {label}")
        except Exception as e:
            print(f"  [error] {label}: {str(e).splitlines()[0]}")
    attempt("stmt trigger + transition table on partitioned parent",
            "create trigger t_parent after insert on public.p referencing new table as n for each statement execute function public.logit()")
    attempt("stmt trigger + transition table on a leaf",
            "create trigger t_leaf after insert on public.p1 referencing new table as n for each statement execute function public.logit()")
    attempt("row trigger + transition table on a leaf",
            "create trigger t_leaf_row after insert on public.p1 referencing new table as n for each row execute function public.logit()")
    db.execute("insert into public.p values (1, 1)")
    db.execute("insert into public.p1 values (2, 2)")
    db.execute("create table public.p2 partition of public.p for values from (1000) to (2000)")
    db.execute("insert into public.p2 values (1001, 1)")
    db.execute("create table public.p3 (id bigint primary key, val numeric)")
    db.execute("alter table public.p attach partition public.p3 for values from (2000) to (3000)")
    db.execute("insert into public.p3 values (2001, 1)")
    db.execute("insert into public.p values (2002, 1)")
    print("  fired:", db.execute("select who, n from public.log").fetchall())
    print("  (statements: insert via parent id 1; direct into p1 id 2; direct into new p2 id 1001; "
          "direct into attached p3 id 2001; via parent into p3 id 2002)")
    db.execute("create table public.inh_parent (id bigint primary key, val numeric)")
    db.execute("create table public.inh_child () inherits (public.inh_parent)")
    attempt("row trigger + transition table on an inheritance child",
            "create trigger t_ic after insert on public.inh_child referencing new table as n for each row execute function public.logit()")
    attempt("stmt trigger + transition table on an inheritance child",
            "create trigger t_ics after insert on public.inh_child referencing new table as n for each statement execute function public.logit()")


def a4(db):
    print("\n## A4 skipping updates that touch no read column")
    db.execute("drop table if exists public.w cascade")
    db.execute("create table public.w (id bigint primary key, val numeric, other text)")
    db.execute("create or replace function public.noop() returns trigger language plpgsql as $$ begin return null; end $$")
    for label, sql in [
        ("UPDATE OF val + transition tables (statement)",
         "create trigger a after update of val on public.w referencing old table as o new table as n for each statement execute function public.noop()"),
        ("WHEN (old.val is distinct from new.val) on a statement trigger",
         "create trigger b after update on public.w for each statement when (old.val is distinct from new.val) execute function public.noop()"),
        ("UPDATE OF val, statement, no transition tables",
         "create trigger c after update of val on public.w for each statement execute function public.noop()"),
        ("WHEN on a row trigger with transition tables",
         "create trigger d after update on public.w referencing new table as n for each row when (old.val is distinct from new.val) execute function public.noop()"),
    ]:
        try:
            db.execute(sql); print(f"  [ok]    {label}")
        except Exception as e:
            print(f"  [error] {label}: {str(e).splitlines()[0]}")


def a5(db):
    print("\n## A5 mirror read without privilege on the sequence")
    db.execute("drop role if exists nopriv; create role nopriv login")
    db.execute("create schema if not exists trellis")
    db.execute("drop sequence if exists trellis.ring_slot_mirror; create sequence trellis.ring_slot_mirror minvalue 0")
    db.execute("select setval('trellis.ring_slot_mirror', 2, true)")
    db.execute("grant usage on schema trellis to nopriv")
    u = psycopg.connect(dsn("nopriv"), autocommit=True)
    def lv(label):
        try:
            print(f"  {label}: last_value =", u.execute("select pg_sequence_last_value('trellis.ring_slot_mirror'::regclass)").fetchone()[0])
        except Exception as e:
            print(f"  {label}: {type(e).__name__}: {str(e).splitlines()[0]}")
    lv("no grant on the sequence")
    db.execute("grant usage on sequence trellis.ring_slot_mirror to nopriv"); lv("USAGE only")
    db.execute("revoke usage on sequence trellis.ring_slot_mirror from nopriv")
    db.execute("grant select on sequence trellis.ring_slot_mirror to nopriv"); lv("SELECT only")
    db.execute("revoke select on sequence trellis.ring_slot_mirror from nopriv")
    db.execute("grant update on sequence trellis.ring_slot_mirror to nopriv"); lv("UPDATE only")
    db.execute("revoke update on sequence trellis.ring_slot_mirror from nopriv")
    try:
        db.execute("""do $$ declare slot smallint := NULL;
                     begin case slot when 0 then null; when 1 then null; when 2 then null; when 3 then null; end case; end $$""")
        print("  CASE over NULL slot: no error (row silently dropped!)")
    except Exception as e:
        print(f"  CASE over NULL slot: {type(e).__name__}: {' | '.join(str(e).splitlines())}")
    db.execute("select setval('trellis.ring_slot_mirror', 2, false)")
    print("  pg_sequence_last_value when is_called = false (as owner):",
          db.execute("select pg_sequence_last_value('trellis.ring_slot_mirror'::regclass)").fetchone())
    u.close()


if __name__ == "__main__":
    pg = start()
    db = psycopg.connect(dsn(), autocommit=True)
    try:
        for step in (sys.argv[1:] or ["a1", "a3", "a4", "a5"]):
            globals()[step](db)
    finally:
        db.close(); pg.send_signal(signal.SIGINT); pg.wait(); shutil.rmtree(BASE, ignore_errors=True)
