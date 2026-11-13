# Postgres lock-queue check for per-leaf B gates with two concurrent leaf builders.
import threading, time, psycopg
H="/home/mike/code/trellis-lane-r/target/research-pg-x1b"
def c(): return psycopg.connect(host=H, port=54761, user="postgres", dbname="postgres")
def run(order_p3):
    P2,P3,B0,B1=c(),c(),c(),c()
    res={}
    P2.execute("select pg_advisory_xact_lock_shared(100)")         # P2 holds sh0
    first = order_p3[0]
    P3.execute(f"select pg_advisory_xact_lock_shared({first})")    # P3 holds its first gate
    def t(name, conn, sql):
        t0=time.monotonic()
        try: conn.execute(sql); res[name]="ok %.0fms"%(1000*(time.monotonic()-t0))
        except Exception as e: res[name]=type(e).__name__
        conn.rollback()
    ths=[threading.Thread(target=t,args=("B0",B0,"select pg_advisory_xact_lock(100)")),
         threading.Thread(target=t,args=("B1",B1,"select pg_advisory_xact_lock(101)"))]
    for x in ths: x.start()
    time.sleep(0.3)
    ths+= [threading.Thread(target=t,args=("P2",P2,"select pg_advisory_xact_lock_shared(101)")),
           threading.Thread(target=t,args=("P3",P3,f"select pg_advisory_xact_lock_shared({order_p3[1]})"))]
    for x in ths[2:]: x.start()
    for x in ths: x.join(5)
    for k in ("P2","P3"):
        pass
    (P2.commit() if False else None)
    return {k:res.get(k,"hung") for k in ("P2","P3","B0","B1")}
print("P3 opposite order (1 then 0):", run((101,100)))
print("P3 leaf order     (0 then 1):", run((100,101)))
