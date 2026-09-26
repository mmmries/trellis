"""#565 E4 addendum: which pointer value does a capture trigger read under each isolation level?
The fence (docs/staging-and-claiming/03) assumes a writer's pointer read happens after its xid is
assigned AND reflects the latest committed flip. A trigger runs in the application's transaction."""
import psycopg
DSN = "host=/tmp/tc-565/e4 port=5565 user=postgres dbname=postgres"
adm = psycopg.connect(DSN, autocommit=True)
adm.execute("drop table if exists src, ptr, ring cascade; drop sequence if exists ptr_seq")
adm.execute("create table ptr (id bool primary key default true, slot int not null); insert into ptr values (true, 0)")
adm.execute("create sequence ptr_seq minvalue 0 start 0")
adm.execute("create table ring (slot_mvcc int, slot_seq bigint, row_txid xid8 default pg_current_xact_id(), iso text)")
adm.execute("create table src (id int primary key)")
adm.execute("""create function cap() returns trigger language plpgsql as $$ begin
  insert into ring(slot_mvcc, slot_seq, iso) values ((select slot from ptr), pg_sequence_last_value('ptr_seq'),
    current_setting('transaction_isolation'));
  return null; end $$""")
adm.execute("create trigger cap after insert on src for each row execute function cap()")
adm.execute("select setval('ptr_seq', 0, true)")
for iso in ["read committed", "repeatable read", "serializable"]:
    t = psycopg.connect(DSN, autocommit=True)
    t.execute(f"begin isolation level {iso}")
    t.execute("select count(*) from src")                 # the app's first statement takes the snapshot
    for _ in range(2):                                    # two seals flip the pointer meanwhile
        adm.execute("update ptr set slot = (slot + 1) % 4")
        adm.execute("select setval('ptr_seq', (select slot from ptr), true)")
    t.execute("insert into src values ((select coalesce(max(id),0)+1 from src))")  # xid assigned here
    t.execute("commit")
    cur = adm.execute("select slot from ptr").fetchone()[0]
    r = adm.execute("select slot_mvcc, slot_seq from ring order by row_txid desc limit 1").fetchone()
    print(f"{iso:16} active slot now={cur}  trigger read via table={r[0]}  via pg_sequence_last_value={r[1]}"
          f"  -> table read {'STALE' if r[0] != cur else 'fresh'}")
