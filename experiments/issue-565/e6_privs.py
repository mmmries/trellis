"""#565 E6: privileges and failure coupling, on a real Trellis schema.

Roles: `app` owns public.gen and writes it; `trellis_capture` owns the SECURITY DEFINER capture
functions (pinned search_path) and has only what capture needs on the trellis schema. `app` has no
grant on the trellis schema at all. Then the ring is broken on purpose and we record exactly what
the application's INSERT returns.
"""
import os, shutil, signal, subprocess, sys, time
import psycopg
sys.path.insert(0, os.path.dirname(__file__))
import capture_sql

TRELLIS = "/home/mike/code/trellis-565/target/release/trellis"
BASE, PORT = "/tmp/tc-565/e6", 5568


def main():
    shutil.rmtree(BASE, ignore_errors=True); os.makedirs(BASE)
    subprocess.run(["initdb", "-D", f"{BASE}/data", "-U", "postgres", "-A", "trust"], check=True, capture_output=True)
    pg = subprocess.Popen(["postgres", "-D", f"{BASE}/data", "-h", "", "-k", BASE, "-p", str(PORT),
                           "-c", "wal_level=logical", "-c", "logging_collector=off", "-c", "dynamic_shared_memory_type=mmap"],
                          stdout=open(f"{BASE}/pg.log", "w"), stderr=subprocess.STDOUT)
    time.sleep(1.5)
    dsn = lambda u: f"host={BASE} port={PORT} user={u} dbname=postgres"
    su = psycopg.connect(dsn("postgres"), autocommit=True)
    try:
        su.execute("create role app login; create role trellis_capture login")
        su.execute("grant create on schema public to app")
        su.execute("set role app; create table public.gen (id bigint primary key, val numeric); reset role")
        subprocess.run([TRELLIS, "-d", f"postgresql://postgres@/postgres?host={BASE}&port={PORT}", "apply",
                        "TRANSFORM gen_out FROM gen SELECT val AS val"], check=True, capture_output=True)
        # hstore in its own schema, not public: a SECURITY DEFINER function must not resolve anything
        # through a schema the application can create objects in.
        su.execute("create schema trellis_ext; create extension hstore schema trellis_ext")
        su.execute("grant usage on schema trellis, trellis_ext to trellis_capture")
        su.execute("grant insert on " + ", ".join(f"trellis.seg_{i}" for i in range(4)) + " to trellis_capture")
        su.execute("grant select on trellis.segment_pointer to trellis_capture")
        su.execute("grant usage on sequence trellis.staging_change_id_seq to trellis_capture")
        su.execute("grant create on schema trellis to trellis_capture")   # to own its functions there
        su.execute("set role app; grant trigger on public.gen to trellis_capture; reset role")
        capture_sql.ENC.update(mode="hstore", pin=True)
        ddl = capture_sql.stmt_trigger("gen", ["id", "val"], ["id"]).replace("public.hstore", "trellis_ext.hstore")
        tc = psycopg.connect(dsn("trellis_capture"), autocommit=True)
        try:
            tc.execute(ddl)
            print("trellis_capture (TRIGGER privilege, not owner) created the capture triggers: OK")
        except Exception as e:
            print(f"trellis_capture could not create triggers: {e}")
            su.execute(ddl.replace("CREATE OR REPLACE FUNCTION", "CREATE OR REPLACE FUNCTION"))
        app = psycopg.connect(dsn("app"), autocommit=True)

        def attempt(label, sql, conn=app):
            try:
                conn.execute(sql)
                print(f"[ok]    {label}")
            except Exception as e:
                msg = str(e).strip().replace("\n", " | ")
                print(f"[error] {label}: {type(e).__name__}: {msg}")

        def ring_rows():
            return su.execute("select " + " + ".join(f"(select count(*) from trellis.seg_{i})" for i in range(4))).fetchone()[0]

        attempt("app inserts with no grant on the trellis schema", "insert into public.gen values (1, 1)")
        print(f"        ring rows now: {ring_rows()}")
        attempt("app forges a ring row directly", "insert into trellis.seg_0 (src_table, key, op) values ('public.gen','9','insert')")
        attempt("app reads the ring", "select count(*) from trellis.seg_0")
        attempt("app calls the capture function directly", "select trellis.capture_stmt_insert_gen()")
        attempt("app (owner) disables the capture trigger", "alter table public.gen disable trigger trellis_capture_insert")
        attempt("app inserts with capture disabled", "insert into public.gen values (2, 2)")
        print(f"        ring rows now: {ring_rows()}  (the insert above was silently not captured)")
        attempt("app re-enables it", "alter table public.gen enable trigger trellis_capture_insert")
        attempt("app sets session_replication_role = replica", "set session_replication_role = replica")
        attempt("app drops the capture trigger", "drop trigger trellis_capture_update on public.gen")

        # failure coupling: break the ring and see what the application's write returns
        su.execute("revoke insert on " + ", ".join(f"trellis.seg_{i}" for i in range(4)) + " from trellis_capture")
        attempt("ring insert privilege revoked", "insert into public.gen values (3, 3)")
        su.execute("grant insert on " + ", ".join(f"trellis.seg_{i}" for i in range(4)) + " to trellis_capture")
        slot = su.execute("select ring_slot from trellis.segment_pointer").fetchone()[0]
        su.execute(f"alter table trellis.seg_{slot} rename to seg_{slot}_gone")
        attempt("active ring table missing", "insert into public.gen values (4, 4)")
        su.execute(f"alter table trellis.seg_{slot}_gone rename to seg_{slot}")
        su.execute("update trellis.segment_pointer set ring_slot = 7")
        attempt("pointer names a slot outside the CASE", "insert into public.gen values (5, 5)")
        su.execute(f"update trellis.segment_pointer set ring_slot = {slot}")
        blocker = psycopg.connect(dsn("postgres"))
        blocker.execute(f"lock table trellis.seg_{slot} in access exclusive mode")   # e.g. a reclaim TRUNCATE
        app.execute("set lock_timeout = '2s'")
        t0 = time.time()
        attempt("active ring table held ACCESS EXCLUSIVE by another session", "insert into public.gen values (6, 6)")
        print(f"        waited {time.time() - t0:.1f}s")
        blocker.rollback(); blocker.close()
        app.execute("reset lock_timeout")
        attempt("healthy again", "insert into public.gen values (7, 7)")
    finally:
        su.close()
        pg.send_signal(signal.SIGINT); pg.wait(); shutil.rmtree(BASE, ignore_errors=True)


if __name__ == "__main__":
    main()
