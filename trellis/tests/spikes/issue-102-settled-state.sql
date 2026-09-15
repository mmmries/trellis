-- Issue #102 follow-up: how little settled state makes a relationship-keyed
-- aggregate's delta sound? Five candidate designs, one differential fuzzer.
--
--   D0  issue #102 as written: no settled state; the forward delta resolves
--       the join against LIVE posts, the reverse enumerates LIVE post_tags.
--   D1  per-PARENT settled projection (posts.id -> author), advanced
--       transactionally with the reverse deltas it justifies.
--   D2  D1 + reconcile the live enumeration against the staging ring.
--   D3  per-FROM-SIDE-ROW settled attribution ((post,tag) -> author).
--       No live enumeration anywhere.
--   D4  D2 + a watermark barrier: the reverse enumeration may only run once
--       intake has staged everything committed up to the snapshot it reads
--       (real engine: enumerate under REPEATABLE READ, capture
--       pg_current_wal_insert_lsn() at snapshot start, and require
--       replication_progress.confirmed_lsn >= it before applying).
--
-- The model deliberately reproduces INTAKE LAG: a COMMIT is visible to any
-- live read immediately, but only becomes a *staged* change once intake
-- catches up. That window is what D1 and D2 cannot close.
--
-- Run order: this file, then the authoritative definitions at the bottom
-- (they supersede the earlier copies), then `select * from fuzz(...)`.

-- ===================================================================
-- Q3: how little settled state is enough for a sound delta?
--
-- Three candidate designs, same scenarios, diffed against the oracle.
--   D1  per-PARENT settled projection only (posts id -> author), advanced
--       transactionally when the reverse work applies. Forward deltas
--       resolve the join against the projection, never live. Reverse
--       enumerates the from-side LIVE.
--   D2  D1 + reconcile the live enumeration against the ring: exclude
--       from-side rows with a pending staged insert, add back rows with a
--       pending staged delete (reconstruct A_settled = A_live (-) dA_pending).
--   D3  per-FROM-SIDE-ROW settled attribution (post,tag -> settled author).
--       No live enumeration at all.
--
-- Crucially this models INTAKE LAG: a user COMMIT is visible to any live
-- read immediately, but only becomes a *staged* change when intake catches
-- up. D2's reconciliation can only see staged changes.
-- ===================================================================
drop schema if exists q3 cascade; create schema q3; set search_path to q3, public;
create unlogged table dbuf (tag text, author int, dc bigint, dw numeric);

create table src_posts (id bigint primary key, author int, word_count int);
create table src_post_tags (post bigint, tag text, primary key (post, tag));
create table tgt (tag text, author int, post_count bigint, total_words numeric, primary key (tag, author));

-- committed-but-maybe-not-yet-staged changes
create table wal (seq bigserial primary key, staged bool default false, applied bool default false,
                  kind text, post bigint, tag text,
                  old_author int, new_author int, old_wc int, new_wc int);
-- reverse work queued by a drained parent change, due one batch later
create table rev (due int, post bigint, old_author int, new_author int, old_wc int, new_wc int);
create table state (batch int); insert into state values (0);

-- D1/D2 settled parent projection; D3 settled per-row attribution
create table proj (id bigint primary key, author int, word_count int);
create table attr (post bigint, tag text, author int, word_count int, primary key (post, tag));

create function op_pt_ins(p bigint, t text) returns void language plpgsql as $$
begin insert into src_post_tags values (p,t); insert into wal(kind,post,tag) values ('pt_ins',p,t); end $$;
create function op_pt_del(p bigint, t text) returns void language plpgsql as $$
begin delete from src_post_tags where post=p and tag=t; insert into wal(kind,post,tag) values ('pt_del',p,t); end $$;
create function op_post_upd(p bigint, a int, w int) returns void language plpgsql as $$
declare oa int; ow int;
begin select author,word_count into oa,ow from src_posts where id=p;
      update src_posts set author=a, word_count=w where id=p;
      insert into wal(kind,post,old_author,new_author,old_wc,new_wc) values ('post_upd',p,oa,a,ow,w); end $$;
-- intake catches up: everything committed so far becomes staged
create function intake() returns void language plpgsql as $$
begin update wal set staged=true where not staged; end $$;

create function emit(t text, a int, dc bigint, dw numeric) returns void language plpgsql as $$
begin
  insert into tgt values (t,a,dc,dw)
  on conflict (tag,author) do update set post_count=tgt.post_count+excluded.post_count,
       total_words=coalesce(tgt.total_words,0)+coalesce(excluded.total_words,0);
end $$;

-- ---------------- D1 / D2 ----------------
create function apply_d(reconcile bool) returns void language plpgsql as $$
declare bn int;
begin
  update state set batch=batch+1 returning batch into bn;
  truncate dbuf;

  -- FORWARD: group resolved against the settled parent projection (never live)
  insert into dbuf
  select w.tag, pr.author,
         case when w.kind='pt_ins' then 1 else -1 end,
         case when w.kind='pt_ins' then pr.word_count else -pr.word_count end
  from wal w left join proj pr on pr.id = w.post
  where w.staged and not w.applied and w.kind in ('pt_ins','pt_del');

  -- a drained parent change queues reverse work for the NEXT batch
  insert into rev select bn+1, post, old_author, new_author, old_wc, new_wc
  from wal where staged and not applied and kind='post_upd';
  update wal set applied=true where staged and not applied;

  -- REVERSE, due now: enumerate the from-side.
  insert into dbuf
  select e.tag, a.author, a.sign, a.sign*a.wc
  from rev r
  cross join lateral (
    select pt.tag from src_post_tags pt where pt.post = r.post
      and (not reconcile or not exists (            -- drop rows whose own
        select 1 from wal w where w.staged and not w.applied            -- insert is still pending
          and w.kind='pt_ins' and w.post=pt.post and w.tag=pt.tag))
    union all
    select w.tag from wal w where reconcile and w.staged and not w.applied  -- add back rows whose
      and w.kind='pt_del' and w.post = r.post                               -- delete is still pending
  ) e
  cross join lateral (values (r.old_author,-1,r.old_wc),(r.new_author,1,r.new_wc)) a(author,sign,wc)
  where r.due = bn;

  -- the projection advances with the deltas it justifies
  update proj p set author=r.new_author, word_count=r.new_wc from rev r where r.due=bn and p.id=r.post;
  delete from rev where due=bn;

  insert into tgt (tag,author,post_count,total_words)
  select tag,author,sum(dc),sum(dw) from dbuf group by 1,2
  on conflict (tag,author) do update set post_count=tgt.post_count+excluded.post_count,
      total_words=coalesce(tgt.total_words,0)+coalesce(excluded.total_words,0);
  delete from tgt where post_count=0 and coalesce(total_words,0)=0;
  truncate dbuf;
end $$;

-- ---------------- D3: per-row settled attribution ----------------
create function apply_d3() returns void language plpgsql as $$
declare bn int;
begin
  update state set batch=batch+1 returning batch into bn;
  truncate dbuf;

  -- FORWARD insert: attribute against the settled parent projection, and
  -- RECORD the attribution.
  insert into dbuf select w.tag, pr.author, 1, pr.word_count
  from wal w left join proj pr on pr.id=w.post
  where w.staged and not w.applied and w.kind='pt_ins';
  insert into attr select w.post, w.tag, pr.author, pr.word_count
  from wal w left join proj pr on pr.id=w.post
  where w.staged and not w.applied and w.kind='pt_ins'
  on conflict (post,tag) do update set author=excluded.author, word_count=excluded.word_count;

  -- FORWARD delete: subtract from the group the row was RECORDED in.
  insert into dbuf select a.tag, a.author, -1, -a.word_count
  from wal w join attr a on a.post=w.post and a.tag=w.tag
  where w.staged and not w.applied and w.kind='pt_del';
  delete from attr using wal w where w.staged and not w.applied and w.kind='pt_del'
    and attr.post=w.post and attr.tag=w.tag;

  insert into rev select bn+1, post, old_author, new_author, old_wc, new_wc
  from wal where staged and not applied and kind='post_upd';
  update wal set applied=true where staged and not applied;

  -- REVERSE: drive off the RECORDED attribution, not a live read.
  insert into dbuf
  select a.tag, v.author, v.sign, v.sign*v.wc
  from rev r join attr a on a.post = r.post
  cross join lateral (values (a.author,-1,a.word_count),(r.new_author,1,r.new_wc)) v(author,sign,wc)
  where r.due = bn;
  update attr a set author=r.new_author, word_count=r.new_wc from rev r where r.due=bn and a.post=r.post;
  update proj p set author=r.new_author, word_count=r.new_wc from rev r where r.due=bn and p.id=r.post;
  delete from rev where due=bn;

  insert into tgt (tag,author,post_count,total_words)
  select tag,author,sum(dc),sum(dw) from dbuf group by 1,2
  on conflict (tag,author) do update set post_count=tgt.post_count+excluded.post_count,
      total_words=coalesce(tgt.total_words,0)+coalesce(excluded.total_words,0);
  delete from tgt where post_count=0 and coalesce(total_words,0)=0;
  truncate dbuf;
end $$;

create view oracle as
  select pt.tag, p.author, count(*)::bigint post_count, sum(p.word_count)::numeric total_words
  from src_post_tags pt left join src_posts p on p.id=pt.post group by 1,2;

create function diff() returns table(tag text, author int, tc bigint, tw numeric, oc bigint, ow numeric)
language sql as $$
  select coalesce(t.tag,o.tag), coalesce(t.author,o.author), t.post_count, t.total_words, o.post_count, o.total_words
  from tgt t full join oracle o on o.tag=t.tag and o.author is not distinct from t.author
  where t.post_count is distinct from o.post_count or t.total_words is distinct from o.total_words order by 1,2; $$;

create function reset_world() returns void language plpgsql as $$
begin
  delete from wal; delete from rev; truncate tgt; delete from src_post_tags; delete from src_posts;
  truncate proj; truncate attr; update state set batch=0;
  insert into src_posts values (1,100,10),(2,200,20);
  insert into src_post_tags values (1,'x'),(1,'y'),(2,'x');
  insert into proj select * from src_posts;
  insert into attr select pt.post,pt.tag,p.author,p.word_count from src_post_tags pt join src_posts p on p.id=pt.post;
  insert into tgt select * from oracle;
end $$;

create function verdict(label text) returns text language plpgsql as $$
declare n int; begin
  select count(*) into n from diff();
  if n=0 then return format('PASS  %s', label);
  else return format('FAIL  %s -> %s', label,
    (select string_agg(format('(%s,%s) tgt=%s/%s oracle=%s/%s',tag,author,tc,tw,oc,ow),'; ') from diff())); end if; end $$;

set search_path to q3, public; \pset footer off
create or replace function A(w text) returns void language plpgsql as $$
begin if w='D1' then perform apply_d(false); elsif w='D2' then perform apply_d(true); else perform apply_d3(); end if; end $$;
-- drain everything, letting intake run first each round
create or replace function settle(w text) returns void language plpgsql as $$
begin for i in 1..8 loop perform intake(); perform A(w); end loop; end $$;

create or replace function scen(w text, s int) returns text language plpgsql as $$
declare lbl text;
begin
  perform reset_world();
  if s=1 then lbl:='T1 author change alone';
    perform op_post_upd(1,999,10); perform intake(); perform settle(w);

  elsif s=2 then lbl:='T2 from-side INSERT drains in a batch BETWEEN the parent change and its reverse';
    perform op_post_upd(1,999,10); perform intake(); perform A(w);   -- batch1: parent; rev due 2
    update rev set due = due + 1;                                     -- reverse slips one batch (Spike A2 schedule)
    perform op_pt_ins(1,'z'); perform intake(); perform A(w);         -- batch2: the insert, alone
    perform settle(w);                                                -- batch3: the reverse

  elsif s=3 then lbl:='T3 from-side INSERT is LIVE but UNSTAGED when the reverse fires (intake lag)';
    perform op_post_upd(1,999,10); perform intake(); perform A(w);   -- batch1: parent; rev due 2
    perform op_pt_ins(1,'z');                                         -- committed, NOT staged
    perform A(w);                                                     -- batch2: the reverse. live read sees z
    perform settle(w);                                                -- z stages and drains afterwards

  elsif s=4 then lbl:='T4 from-side DELETE is LIVE-absent but UNSTAGED when the reverse fires';
    perform op_post_upd(1,999,10); perform intake(); perform A(w);
    perform op_pt_del(1,'y');
    perform A(w);
    perform settle(w);

  elsif s=5 then lbl:='T5 from-side INSERT staged but its batch drains AFTER the reverse';
    perform op_post_upd(1,999,10); perform intake(); perform A(w);
    perform op_pt_ins(1,'z'); perform intake();                       -- staged, undrained
    update wal set staged=true where kind='pt_ins' and tag='z';
    perform A(w);                                                     -- batch2: reverse + the insert together
    perform settle(w);

  elsif s=6 then lbl:='T6 from-side DELETE staged, drains together with the reverse';
    perform op_post_upd(1,999,10); perform intake(); perform A(w);
    perform op_pt_del(1,'y'); perform intake();
    perform A(w); perform settle(w);
  end if;
  return verdict(format('[%s] %s', w, lbl));
end $$;
select scen(w,s) from (values ('D1'),('D2'),('D3')) a(w), generate_series(1,6) s order by s,w;

set search_path to q3, public; \pset footer off
-- Randomized differential test: random op streams x random intake/apply
-- interleavings, each run diffed against a from-scratch GROUP BY.
create or replace function fuzz(w text, runs int, ops int, seed0 int)
returns table(design text, runs_total int, runs_failed int, worst text) language plpgsql as $$
declare r int; i int; a int; p bigint; t text; roll float; bad int := 0; ex text := null; n int;
begin
  perform setseed(0.5);
  for r in 1..runs loop
    perform setseed(((seed0 + r) % 1000)::float / 1000.0);
    perform reset_world();
    delete from deferrals;
    for i in 1..ops loop
      roll := random();
      p := 1 + (random()*1)::int;                   -- posts 1..2
      t := (array['x','y','z','w'])[1+(random()*3)::int];
      if roll < 0.30 then perform op_pt_ins(p,t);
      elsif roll < 0.50 then perform op_pt_del(p,t);
      elsif roll < 0.75 then
        a := (array[100,200,999,555])[1+(random()*3)::int];
        perform op_post_upd(p, a, (array[10,20,77])[1+(random()*2)::int]);
      elsif roll < 0.88 then perform intake();       -- intake catches up
      else perform A(w);                             -- a batch drains
      end if;
    end loop;
    -- settle
    for i in 1..20 loop perform intake(); perform A(w); end loop;
    select count(*) into n from diff();
    if n > 0 then
      bad := bad + 1;
      if ex is null then ex := format('seed=%s %s', seed0+r,
        (select string_agg(format('(%s,%s) tgt=%s/%s oracle=%s/%s',tag,author,tc,tw,oc,ow),'; ') from diff())); end if;
    end if;
  end loop;
  return query select w, runs, bad, coalesce(ex,'-');
end $$;
select * from fuzz('D1', 400, 40, 1000)
union all select * from fuzz('D2', 400, 40, 1000)
union all select * from fuzz('D3', 400, 40, 1000)
union all select * from fuzz('D4', 400, 40, 1000);

-- ==============================================================
-- Authoritative current definitions (supersede the copies above).
-- ==============================================================
set search_path to q3, public;
SET
create or replace view pending_fold as  SELECT post,
    tag,
    COALESCE(min(
        CASE
            WHEN kind = 'pt_del'::text THEN seq
            ELSE NULL::bigint
        END) = min(seq), false) AS existed_before,
    (array_agg(kind ORDER BY seq DESC))[1] = 'pt_ins'::text AS exists_after
   FROM wal
  WHERE staged AND NOT applied AND (kind = ANY (ARRAY['pt_ins'::text, 'pt_del'::text]))
  GROUP BY post, tag;
SET
create or replace view pending_post_fold as  SELECT post,
    (array_agg(old_author ORDER BY seq))[1] AS old_author,
    (array_agg(old_wc ORDER BY seq))[1] AS old_wc,
    (array_agg(new_author ORDER BY seq DESC))[1] AS new_author,
    (array_agg(new_wc ORDER BY seq DESC))[1] AS new_wc
   FROM wal
  WHERE staged AND NOT applied AND kind = 'post_upd'::text
  GROUP BY post;
SET
CREATE OR REPLACE FUNCTION q3.apply_common(w text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
declare bn int; blocked bool; reconcile bool := (w in ('D2','D4'));
        src text := case when w='D0' then 'live' else 'proj' end;
begin
  update state set batch=batch+1 returning batch into bn;
  truncate dbuf;
  if w = 'D3' then
    insert into dbuf select n.tag, pr.author, 1, pr.word_count
      from pending_net n left join proj pr on pr.id=n.post where n.net=1;
    insert into dbuf select a.tag, a.author, -1, -a.word_count
      from pending_net n join attr a on a.post=n.post and a.tag=n.tag where n.net=-1;
    delete from attr using pending_net n where n.net=-1 and attr.post=n.post and attr.tag=n.tag;
    insert into attr select n.post, n.tag, pr.author, pr.word_count
      from pending_net n left join proj pr on pr.id=n.post where n.net=1
      on conflict (post,tag) do update set author=excluded.author, word_count=excluded.word_count;
  elsif w = 'D0' then
    insert into dbuf select n.tag, sp.author, n.net, n.net*sp.word_count
      from pending_net n left join src_posts sp on sp.id=n.post where n.net<>0;
  else
    insert into dbuf select n.tag, pr.author, n.net, n.net*pr.word_count
      from pending_net n left join proj pr on pr.id=n.post where n.net<>0;
  end if;

  insert into rev select bn+1, post, old_author, new_author, old_wc, new_wc from pending_post_fold
    on conflict (post) do update set new_author=excluded.new_author, new_wc=excluded.new_wc,
      due = least(rev.due, excluded.due);
  update wal set applied=true where staged and not applied;

  blocked := (w='D4') and exists(select 1 from wal where not staged);
  if blocked then
    update rev set due = bn+1 where due <= bn;
  elsif w = 'D3' then
    insert into dbuf select a.tag, v.author, v.sign, v.sign*v.wc
      from rev r join attr a on a.post=r.post
      cross join lateral (values (a.author,-1,a.word_count),(r.new_author,1,r.new_wc)) v(author,sign,wc)
      where r.due<=bn;
    update attr a set author=r.new_author, word_count=r.new_wc from rev r where r.due<=bn and a.post=r.post;
  else
    insert into dbuf select e.tag, v.author, v.sign, v.sign*v.wc
      from rev r
      cross join lateral (
        select pt.tag from src_post_tags pt where pt.post=r.post
          and (not reconcile or not exists (select 1 from pending_net n where n.net=1 and n.post=pt.post and n.tag=pt.tag))
        union all
        select n.tag from pending_net n where reconcile and n.net=-1 and n.post=r.post
      ) e
      cross join lateral (values (r.old_author,-1,r.old_wc),(r.new_author,1,r.new_wc)) v(author,sign,wc)
      where r.due<=bn;
  end if;
  if not blocked then
    update proj p set author=r.new_author, word_count=r.new_wc from rev r where r.due<=bn and p.id=r.post;
    delete from rev where due<=bn;
  end if;
  insert into tgt (tag,author,post_count,total_words)
    select tag,author,sum(dc),sum(dw) from dbuf group by 1,2
    on conflict (tag,author) do update set post_count=tgt.post_count+excluded.post_count,
      total_words=coalesce(tgt.total_words,0)+coalesce(excluded.total_words,0);
  delete from tgt where post_count=0 and coalesce(total_words,0)=0;
end $function$
;
