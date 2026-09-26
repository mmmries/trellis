"""#565 E3: image parity. Ground truth is today's path: a real `trellis run --drain-threads 0`
decodes the source table's changes through pgoutput and writes `tuple_to_json` images into the ring.
On the same writes, a capture trigger writes four candidate encodings into a side table:

  coltext  jsonb_build_object('c', NEW.c::text, ...)      generated per table (breaks on DDL)
  tojsonb  to_jsonb(NEW)                                   column-agnostic, typed JSON
  eachtxt  jsonb_object_agg(k, v) from jsonb_each_text(to_jsonb(NEW))   column-agnostic, all text
  hstore   hstore_to_json(hstore(NEW))                     column-agnostic, type output functions

Each is diffed per column against intake's image for the same (key, op).
"""
import json, os, shutil, subprocess, sys, time
import psycopg

sys.path.insert(0, os.path.dirname(__file__))
TRELLIS = "/home/mike/code/trellis-565/target/release/trellis"
BASE, PORT = "/tmp/tc-565/e3", 5567

COLS = [  # (name, type, [values...]) one row per value index; NULLs added as a separate row
    ("c_int2", "smallint", ["-32768", "7"]),
    ("c_int4", "integer", ["2147483647", "0"]),
    ("c_int8", "bigint", ["-9223372036854775808", "42"]),
    ("c_oid", "oid", ["4294967295", "1"]),
    ("c_num", "numeric", ["1.50", "'NaN'", "'Infinity'", "-0.000", "123456789012345678901234567890.123"]),
    ("c_real", "real", ["1.1", "'NaN'", "'-Infinity'", "'-0'", "3.4028235e38"]),
    ("c_dbl", "double precision", ["0.1", "'Infinity'", "'-0'", "1e-300", "12345678.901234567"]),
    ("c_bool", "boolean", ["true", "false"]),
    ("c_uuid", "uuid", ["'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'"]),
    ("c_text", "text", ["'plain'", "E'quote\" back\\\\ nl\\n tab\\t ctl\\x01 uni ☃ 𝄞'", "''"]),
    ("c_vchar", "varchar(20)", ["'v'"]),
    ("c_char", "char(5)", ["'ab'"]),
    ("c_citext", "citext", ["'MiXeD'"]),
    ("c_bytea", "bytea", ["'\\xdeadbeef'", "''"]),
    ("c_date", "date", ["'2024-02-29'", "'infinity'", "'0044-03-15 BC'"]),
    ("c_time", "time", ["'23:59:59.999999'"]),
    ("c_timetz", "timetz", ["'12:00:00+05:30'"]),
    ("c_ts", "timestamp", ["'2024-01-01 00:00:00'", "'infinity'", "'2024-06-01 12:34:56.789'"]),
    ("c_tstz", "timestamptz", ["'2024-01-01 00:00:00+00'", "'-infinity'", "'2024-06-01 12:34:56.789+02'"]),
    ("c_intv", "interval", ["'1 day 02:03:04'", "'-1 mon'", "'1 year 2 months'"]),
    ("c_jsonb", "jsonb", ["'{\"b\": 1.50, \"a\": [1, \"x\", null], \"n\": {\"t\": true}}'", "'\"str\"'", "'null'"]),
    ("c_json", "json", ["'{\"b\":1.50,  \"a\":[1]}'"]),
    ("c_inet", "inet", ["'10.0.0.1'", "'10.0.0.0/8'", "'::1/128'"]),
    ("c_cidr", "cidr", ["'10.0.0.0/8'"]),
    ("c_mac", "macaddr", ["'08:00:2b:01:02:03'"]),
    ("c_mac8", "macaddr8", ["'08:00:2b:01:02:03:04:05'"]),
    ("c_enum", "mood", ["'happy'"]),
    ("c_bit", "bit(4)", ["B'1010'"]),
    ("c_vbit", "bit varying", ["B'101'"]),
    ("c_iarr", "integer[]", ["'{1,2,NULL}'", "'{}'"]),
    ("c_tarr", "text[]", ["'{\"a b\",\"c,d\",\"q\\\"\"}'"]),
    ("c_range", "int4range", ["'[1,10)'", "'empty'"]),
    ("c_comp", "pair", ["ROW(1, 'x y')::pair"]),
    ("c_point", "point", ["'(1.5,2)'"]),
    ("c_money", "money", ["12.34"]),
    ("c_xml", "xml", ["'<a b=\"1\">t</a>'"]),
    ("c_tsv", "tsvector", ["'a fat cat'"]),
    ("c_tsq", "tsquery", ["'fat & cat'"]),
]
NROWS = max(len(v) for _, _, v in COLS)
MODE = sys.argv[1] if len(sys.argv) > 1 else "default"   # default | hostile | pinned
# What intake/the pool pin today (pool.rs DETERMINISTIC_TEXT_OUTPUT_GUCS), and an application
# session that set every one of them differently.
PINNED = "SET datestyle = 'ISO, YMD' SET bytea_output = 'hex' SET extra_float_digits = 1 " \
         "SET intervalstyle = 'postgres' SET timezone = 'UTC'"
HOSTILE = "set datestyle to 'SQL, DMY'; set bytea_output to 'escape'; set extra_float_digits to 0; " \
          "set intervalstyle to 'sql_standard'; set timezone to 'Asia/Kolkata'"


def main():
    shutil.rmtree(BASE, ignore_errors=True); os.makedirs(BASE)
    subprocess.run(["initdb", "-D", f"{BASE}/data", "-U", "postgres", "-A", "trust"], check=True, capture_output=True)
    pg = subprocess.Popen(["postgres", "-D", f"{BASE}/data", "-h", "", "-k", BASE, "-p", str(PORT),
                           "-c", "wal_level=logical", "-c", "logging_collector=off",
                           "-c", "dynamic_shared_memory_type=mmap"],
                          stdout=open(f"{BASE}/pg.log", "w"), stderr=subprocess.STDOUT)
    time.sleep(1.5)
    url = f"postgresql://postgres@/postgres?host={BASE}&port={PORT}"
    db = psycopg.connect(f"host={BASE} port={PORT} user=postgres dbname=postgres", autocommit=True)
    trellis = None
    try:
        db.execute("create extension citext; create extension hstore")
        db.execute("create type mood as enum ('sad', 'happy'); create type pair as (a int, b text)")
        db.execute("create table public.src (id int primary key, " + ", ".join(f"{n} {t}" for n, t, _ in COLS) + ")")
        subprocess.run([TRELLIS, "-d", url, "apply", "TRANSFORM src_out FROM src SELECT c_int4 AS c_int4"],
                       check=True, capture_output=True)
        db.execute("alter table public.src replica identity full")
        db.execute("create table public.enc (enc text, key text, op text, old_image jsonb, new_image jsonb)")
        names = [n for n, _, _ in COLS]
        coltext = lambda r: "jsonb_build_object('id', " + r + ".id::text, " + ", ".join(f"'{n}', {r}.{n}::text" for n in names) + ")"
        fmt = lambda r: "jsonb_build_object('id', format('%s', " + r + ".id), " + ", ".join(
            f"'{n}', CASE WHEN {r}.{n} IS NULL THEN NULL ELSE format('%s', {r}.{n}) END" for n in names) + ")"
        encs = {
            "format": (fmt("NEW"), fmt("OLD")),
            "coltext": (coltext("NEW"), coltext("OLD")),
            "tojsonb": ("to_jsonb(NEW)", "to_jsonb(OLD)"),
            "eachtxt": ("(select jsonb_object_agg(key, value) from jsonb_each_text(to_jsonb(NEW)))",
                        "(select jsonb_object_agg(key, value) from jsonb_each_text(to_jsonb(OLD)))"),
            "hstore": ("hstore_to_json(hstore(NEW))::jsonb", "hstore_to_json(hstore(OLD))::jsonb"),
        }
        body = "\n".join(
            f"insert into public.enc values ('{e}', coalesce(NEW.id, OLD.id)::text, lower(TG_OP), "
            f"case when TG_OP <> 'INSERT' then {o} end, case when TG_OP <> 'DELETE' then {n} end);"
            for e, (n, o) in encs.items())
        db.execute(f"create function public.cap() returns trigger language plpgsql {PINNED if MODE == 'pinned' else ''} as $f$ begin {body} return null; end $f$")
        db.execute("create trigger cap after insert or update or delete on public.src for each row execute function public.cap()")

        trellis = subprocess.Popen([TRELLIS, "-d", url, "run", "--staging", "--drain-threads", "0"],
                                   stdout=open(f"{BASE}/trellis.log", "w"), stderr=subprocess.STDOUT)
        for _ in range(300):
            ok = db.execute("select exists(select 1 from pg_publication_tables where tablename='src') "
                            "and not exists(select 1 from trellis.pending_backfill) "
                            "and exists(select 1 from pg_replication_slots where active)").fetchone()[0]
            if ok: break
            time.sleep(0.2)
        time.sleep(1)
        if MODE in ("hostile", "pinned"):
            db.execute(HOSTILE)
        print(f"mode={MODE}")
        # rows: one per value index (missing values NULL), plus an all-NULL row
        for i in range(NROWS):
            vals = [(v[i] if i < len(v) else "NULL") for _, _, v in COLS]
            db.execute(f"insert into public.src values ({i + 1}, " + ", ".join(vals) + ")")
        db.execute(f"insert into public.src (id) values ({NROWS + 1})")
        db.execute("update public.src set c_vchar = coalesce(c_vchar, '') || 'u'")   # old + new images
        db.execute(f"delete from public.src where id = {NROWS + 1}")
        lsn = db.execute("select pg_current_wal_lsn()").fetchone()[0]
        for _ in range(300):
            if db.execute("select max(confirmed_lsn) >= %s::pg_lsn from trellis.replication_progress", (lsn,)).fetchone()[0]:
                break
            time.sleep(0.2)
        ring = " union all ".join(f"select key, op, old_image, new_image from trellis.seg_{s} where src_table = 'public.src'" for s in range(4))
        truth = {(k, op): (o, n) for k, op, o, n in db.execute(ring).fetchall()}
        print(f"intake ring rows: {len(truth)}")
        diffs = {e: {} for e in encs}
        for e, k, op, o, n in db.execute("select enc, key, op, old_image, new_image from public.enc").fetchall():
            to, tn = truth[(k, op)]
            for side, got, want in (("old", o, to), ("new", n, tn)):
                if want is None and got is None: continue
                for col in sorted(set(want or {}) | set(got or {})):
                    w, g = (want or {}).get(col, "<absent>"), (got or {}).get(col, "<absent>")
                    if w != g:
                        diffs[e].setdefault(col, set()).add((json.dumps(w, ensure_ascii=False), json.dumps(g, ensure_ascii=False)))
        for e, d in diffs.items():
            print(f"\n== {e}: {len(d)} column(s) differ from tuple_to_json")
            for col, pairs in sorted(d.items()):
                for w, g in sorted(pairs)[:4]:
                    print(f"   {col:9} intake={w[:60]:62} trigger={g[:60]}")
    except Exception:
        import traceback; traceback.print_exc()
        print(open(f"{BASE}/trellis.log").read()[-3000:])
    finally:
        if trellis: trellis.terminate(); trellis.wait()
        db.close()
        import signal; pg.send_signal(signal.SIGINT); pg.wait(); shutil.rmtree(BASE, ignore_errors=True)


if __name__ == "__main__":
    main()
