"""#565 E4: hand-driven interleavings, two connections, no engine.

A scratch ring (change_id from a sequence, row_txid = pg_current_xact_id()) is fed by an
AFTER ROW trigger. The oracle for commit order is a test_decoding slot on the same cluster:
the order commits appear in the logical stream is exactly the order today's intake stages them.
Each case asserts, per key, that the ring's change_id order equals the stream's commit order.
"""
import sys, threading, time, psycopg

DSN = "host=/tmp/tc-565/e4 port=5565 user=postgres dbname=postgres"
CACHE = int(sys.argv[1]) if len(sys.argv) > 1 else 1
ORDER_BY = sys.argv[2] if len(sys.argv) > 2 else "change_id"   # e.g. "lsn, change_id"

def conn():
    return psycopg.connect(DSN, autocommit=True)

admin = conn()

def setup():
    admin.execute("drop table if exists src, parent, child, ring cascade")
    admin.execute("drop sequence if exists ring_change_id")
    admin.execute(f"create sequence ring_change_id cache {CACHE}")
    admin.execute("""create table ring (change_id bigint not null default nextval('ring_change_id'),
        row_txid xid8 not null default pg_current_xact_id(), lsn pg_lsn not null default pg_current_wal_insert_lsn(), src_table text, key text, op text,
        old_image jsonb, new_image jsonb)""")
    admin.execute("""create or replace function capture() returns trigger language plpgsql as $$
    begin
      if TG_OP = 'TRUNCATE' then
        insert into ring(src_table, key, op) values (TG_TABLE_NAME, null, 'T');
        return null;
      end if;
      insert into ring(src_table, key, op, old_image, new_image) values (TG_TABLE_NAME,
        case when TG_OP = 'DELETE' then OLD.id::text else NEW.id::text end, left(TG_OP,1),
        case when TG_OP <> 'INSERT' then to_jsonb(OLD) end, case when TG_OP <> 'DELETE' then to_jsonb(NEW) end);
      return null;
    end $$""")
    for t, extra in [("src", ""), ("parent", ""), ("child", ", parent_id bigint")]:
        admin.execute(f"create table {t} (id bigint primary key, val int{extra})")
        admin.execute(f"create trigger cap after insert or update or delete on {t} for each row execute function capture()")
        admin.execute(f"create trigger cap_t after truncate on {t} for each statement execute function capture()")
    if admin.execute("select 1 from pg_replication_slots where slot_name='oracle'").fetchone():
        admin.execute("select pg_drop_replication_slot('oracle')")
    admin.execute("select pg_create_logical_replication_slot('oracle', 'test_decoding')")

def stream_commit_order():
    """xid -> commit position, from the logical stream (today's intake order)."""
    rows = admin.execute("select xid::text, data from pg_logical_slot_get_changes('oracle', null, null)").fetchall()
    order, pos = {}, 0
    for xid, data in rows:
        if data.startswith("COMMIT"):
            order[int(xid)] = pos; pos += 1
    return order

def check(case, expect_same=True):
    order = stream_commit_order()
    ORDER = globals().get("ORDER_BY", "change_id")
    ring = admin.execute(f"select change_id, row_txid::text::bigint, src_table, coalesce(key,'*'), op from ring order by {ORDER}").fetchall()
    admin.execute("truncate ring")
    # epoch-qualified xid8 -> 32-bit xid for the stream's xid
    ring = [(cid, txid & 0xffffffff, t, k, op) for cid, txid, t, k, op in ring]
    per_key = {}
    for cid, xid, t, k, op in ring:
        per_key.setdefault((t, k), []).append((cid, order.get(xid), op, xid))
    ok = True
    for key, rows in per_key.items():
        by_commit = sorted(rows, key=lambda r: (r[1], r[0]))
        if [r[0] for r in by_commit] != [r[0] for r in rows]:
            ok = False
    # cross-key: does global change_id order equal global commit order?
    glob = [(cid, order.get(xid)) for cid, xid, *_ in ring]
    cross = [c for _, c in glob] == sorted(c for _, c in glob)
    verdict = "PASS" if ok else "FAIL"
    print(f"[{verdict}] {case}: per-key change_id order == commit order: {ok}; "
          f"global change_id order == commit order: {cross}")
    for cid, xid, t, k, op in ring:
        print(f"      change_id={cid:<4} xid={xid} commit_pos={order.get(xid)} {t}.{k} {op}")
    return ok

def in_thread(fn):
    th = threading.Thread(target=fn); th.start(); return th

def wait_blocked(c, n=1):
    """Wait until n backends are waiting on a lock."""
    for _ in range(200):
        r = admin.execute("select count(*) from pg_stat_activity where wait_event_type='Lock'").fetchone()[0]
        if r >= n: return
        time.sleep(0.01)
    raise RuntimeError("never blocked")

results = {}
setup()

# (a) same key: T2 takes its xid first (lower xid), T1 locks the row first; T2 blocks on the
# row lock and commits second. Also a READ COMMITTED EvalPlanQual re-check on T2's UPDATE.
admin.execute("insert into src values (1, 0)"); stream_commit_order(); admin.execute("truncate ring")
a, b = conn(), conn()
b.execute("begin"); b.execute("select pg_current_xact_id()")     # T2 gets the lower xid
a.execute("begin"); a.execute("update src set val = val + 1 where id = 1")
th = in_thread(lambda: (b.execute("update src set val = val + 10 where id = 1"), b.execute("commit")))
wait_blocked(admin); a.execute("commit"); th.join()
results["a"] = check("(a) same key, lower-xid writer blocks on row lock, commits second")

# (a') same key, three writers queued on one row, released in order.
cs = [conn() for _ in range(3)]
cs[0].execute("begin"); cs[0].execute("update src set val = 100 where id = 1")
ths = []
for i, c in enumerate(cs[1:], 1):
    ths.append(in_thread(lambda c=c, i=i: (c.execute("begin"), c.execute(f"update src set val = {100+i} where id = 1"), c.execute("commit"))))
    wait_blocked(admin, i)
cs[0].execute("commit"); [t.join() for t in ths]
results["a2"] = check("(a') same key, three writers queued on one row lock")

# (b) delete then re-insert of one key by two writers: T2's INSERT waits on the unique index.
a.execute("begin"); a.execute("delete from src where id = 1")
th = in_thread(lambda: (b.execute("begin"), b.execute("insert into src values (1, 7)"), b.execute("commit")))
wait_blocked(admin); a.execute("commit"); th.join()
results["b"] = check("(b) delete then re-insert of one key by two writers")

# (b') primary-key move: T1 moves 1 -> 2, T2 inserts key 1 (waits on T1's old index entry).
a.execute("begin"); a.execute("update src set id = 2 where id = 1")
th = in_thread(lambda: (b.execute("begin"), b.execute("insert into src values (1, 8)"), b.execute("commit")))
wait_blocked(admin); a.execute("commit"); th.join()
results["b2"] = check("(b') primary-key move 1->2 racing an insert of key 1")

# (c) from-side insert referencing a to-side row inserted by a concurrent transaction, no FK.
a.execute("begin"); a.execute("insert into parent values (10, 0)")
b.execute("begin"); b.execute("insert into child values (20, 0, 10)"); b.execute("commit")
a.execute("commit")
results["c"] = check("(c) no FK: child commits before its parent (cross-key only)")

# (c') same with a foreign key: the child's RI check.
admin.execute("truncate child, parent"); stream_commit_order(); admin.execute("truncate ring")
admin.execute("alter table child add foreign key (parent_id) references parent(id)")
a.execute("begin"); a.execute("insert into parent values (11, 0)")
err = None
def child_fk():
    global err
    try:
        b.execute("begin"); b.execute("insert into child values (21, 0, 11)"); b.execute("commit")
    except Exception as e:
        err = e; b.execute("rollback")
th = in_thread(child_fk); time.sleep(0.3)
blocked = admin.execute("select count(*) from pg_stat_activity where wait_event_type='Lock'").fetchone()[0]
a.execute("commit"); th.join()
print(f"      (c') child FK insert while parent uncommitted: blocked={blocked>0} error={err!r}")
results["c2"] = check("(c') with FK")

# (d) TRUNCATE racing an in-flight writer on the same table, both directions.
a.execute("begin"); a.execute("insert into src values (30, 0)")
th = in_thread(lambda: (b.execute("begin"), b.execute("truncate src"), b.execute("commit")))
wait_blocked(admin); a.execute("commit"); th.join()
results["d"] = check("(d) in-flight insert, then TRUNCATE (waits for the writer)")
a.execute("begin"); a.execute("truncate src")
th = in_thread(lambda: (b.execute("begin"), b.execute("insert into src values (31, 0)"), b.execute("commit")))
wait_blocked(admin); a.execute("commit"); th.join()
results["d2"] = check("(d') in-flight TRUNCATE, then an insert (waits for the truncate)")

# (f) one transaction touching the same key several times, interleaved with another writer's
# rows on other keys: (lsn, change_id) must keep intra-transaction order under any CACHE.
a.execute("begin"); a.execute("insert into src values (50, 0)")
b.execute("insert into src values (51, 0)")
a.execute("update src set val = 1 where id = 50"); a.execute("update src set val = 2 where id = 50"); a.execute("commit")
results["f"] = check("(f) one transaction updates a key twice, another writer interleaves")

# (e) sequence cache: a session holding a cached change_id range writes after another session.
admin.execute("insert into src values (40, 0)"); stream_commit_order(); admin.execute("truncate ring")
a.execute("insert into src values (41, 0)")     # a's backend caches a range if CACHE > 1
b.execute("update src set val = 1 where id = 40")  # b's backend caches the next range
a.execute("update src set val = 2 where id = 40")  # a writes key 40 after b committed
results["e"] = check(f"(e) sequence CACHE {CACHE}: key 40 updated by b then a (autocommit)")

print("ALL PER-KEY PASS" if all(results.values()) else "PER-KEY FAILURES: " + ",".join(k for k, v in results.items() if not v))
