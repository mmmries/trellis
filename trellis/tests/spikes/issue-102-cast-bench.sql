\set ON_ERROR_STOP on
\timing off
create or replace function bench(sql text, n int) returns numeric language plpgsql as $$
declare t0 timestamptz; ms numeric[] := '{}'; i int;
begin
  for i in 1..(n+1) loop
    t0 := clock_timestamp();
    execute sql;
    if i > 1 then ms := ms || extract(epoch from clock_timestamp()-t0)*1000; end if;
  end loop;
  return (select percentile_cont(0.5) within group (order by x) from unnest(ms) x);
end $$;
-- 100 random join keys, as text (the engine's convention)
create temp table jk as select array_agg(id::text) k from (select id from posts order by random() limit 100) s;
select 'to-side fetch, ::text (as rendered today)' lbl,
  bench(format('select count(*) from (select id::text jk, to_jsonb(t.*) doc from posts t where id::text = any(%L::text[])) m', (select k from jk)), 5) ms
union all select 'to-side fetch, native-typed',
  bench(format('select count(*) from (select id::text jk, to_jsonb(t.*) doc from posts t where id = any(%L::text[]::bigint[])) m', (select k from jk)), 5)
union all select 'from-side enumeration, ::text (as rendered today)',
  bench(format('select count(*) from (select post::text, tag from post_tags where post::text = any(%L::text[])) m', (select k from jk)), 5)
union all select 'from-side enumeration, native-typed',
  bench(format('select count(*) from (select post::text, tag from post_tags where post = any(%L::text[]::bigint[])) m', (select k from jk)), 5);
