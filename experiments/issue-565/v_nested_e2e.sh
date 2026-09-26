#!/usr/bin/env bash
# #565 V1: nested same-key writes through the real drain (pseudo-spike), 1-1 and aggregate targets.
cd ~/tc-565; ./cluster.sh stop nest >/dev/null 2>&1; C=$(./cluster.sh start nest 5577); P="psql $C -q -v ON_ERROR_STOP=1"
T=/home/mike/code/trellis-565/target/release/trellis; U="postgresql://postgres@/postgres?host=/tmp/tc-565/nest&port=5577"
$P -c "create table public.gen (id bigint primary key, grp int, val numeric)" -c "alter table public.gen replica identity full"
$T -d "$U" apply "TRANSFORM gen_out FROM gen SELECT val AS val" >/dev/null
$T -d "$U" apply "TRANSFORM gen_sum FROM gen GROUP BY grp SELECT sum(val) AS total" >/dev/null
$P -c "update trellis.transform_definitions set status='live'"
./venv/bin/python -c "
import capture_sql as c; c.PTR['mode']='mirror'; c.ENC.update(mode='format', pin=True)
print(c.stmt_trigger('gen',['id','grp','val'],['id']))" | $P
$P -c "create function public.app_fn() returns trigger language plpgsql as \$\$ begin update public.gen set val = val + 100, grp = grp + 1 where id = NEW.id; return null; end \$\$" \
   -c "create trigger app_bump after insert on public.gen for each row execute function public.app_fn()" -c "alter table public.gen ${NESTED:-enable} trigger app_bump" \
   -c "insert into gen select g, g % 3, g from generate_series(1,100) g"
(cd /tmp/tc-565/nest; TRELLIS_SPIKE_TRIGGER_CAPTURE=1 timeout 20 $T -d "$U" run --staging --drain-threads 2 > run.log 2>&1 &); sleep 12
psql $C -c "select (select count(*) from (select id, val from gen except select id, val from gen_out) d) one_to_one_rows_differing" \
  -c "select s.grp, s.total src_total, t.total tgt_total from (select grp, sum(val) total from gen group by grp) s full join gen_sum t using (grp) order by 1"
[ -z "$KEEP" ] && ./cluster.sh stop nest
