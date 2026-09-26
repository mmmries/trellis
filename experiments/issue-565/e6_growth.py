"""#565 E6 addendum: on-disk growth per captured row with nothing draining (Trellis down)."""
import os, shutil, signal, subprocess, sys, time
import psycopg
sys.path.insert(0, os.path.dirname(__file__)); import capture_sql
BASE, PORT = "/tmp/tc-565/e6g", 5571
shutil.rmtree(BASE, ignore_errors=True); os.makedirs(BASE)
subprocess.run(["initdb", "-D", f"{BASE}/data", "-U", "postgres", "-A", "trust"], check=True, capture_output=True)
pg = subprocess.Popen(["postgres", "-D", f"{BASE}/data", "-h", "", "-k", BASE, "-p", str(PORT), "-c", "wal_level=logical",
                       "-c", "logging_collector=off", "-c", "dynamic_shared_memory_type=mmap"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(1.5)
db = psycopg.connect(f"host={BASE} port={PORT} user=postgres dbname=postgres", autocommit=True)
try:
    db.execute("create table public.gen (id bigint primary key, val numeric)")
    subprocess.run(["/home/mike/code/trellis-565/target/release/trellis", "-d", f"postgresql://postgres@/postgres?host={BASE}&port={PORT}",
                    "apply", "TRANSFORM gen_out FROM gen SELECT val AS val"], check=True, capture_output=True)
    capture_sql.ENC.update(mode="format", pin=True)
    db.execute(capture_sql.stmt_trigger("gen", ["id", "val"], ["id"]))
    for i in range(10):
        db.execute(f"insert into public.gen select g, g from generate_series({i*100000+1}, {(i+1)*100000}) g")
    ring = db.execute("select " + " + ".join(f"pg_total_relation_size('trellis.seg_{i}')" for i in range(4))).fetchone()[0]
    src = db.execute("select pg_total_relation_size('public.gen')").fetchone()[0]
    print(f"1M rows: source table {src/1e6:.1f} MB ({src/1e6:.0f} B/row), ring {ring/1e6:.1f} MB ({ring/1e6:.0f} B/row)")
finally:
    db.close(); pg.send_signal(signal.SIGINT); pg.wait(); shutil.rmtree(BASE, ignore_errors=True)
