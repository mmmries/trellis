-- =====================================================================
-- Issue #102 / #94 — settled-state designs for a to-one delta, v2.
--
-- Rewrite of trellis/tests/spikes/issue-102-settled-state.sql. That file
-- was not self-contained (it referenced views/tables it never created and
-- its dispatcher routed D4 to D3's body), so none of its published numbers
-- are reproducible. This file runs end to end from `psql -f` on an empty
-- database and prints every number it claims.
--
-- What it models that the previous one did not:
--   * a real staging ring: segments, buckets, per-unit drain, and
--     OUT-OF-ORDER drain across segments and buckets;
--   * an "applied" frontier that is per (segment, bucket), not global;
--   * intake lag as a confirmed-LSN watermark, so committed-but-unstaged
--     changes are invisible to any reconcile;
--   * parent INSERT, parent DELETE, from-side FK re-point, and NULL groups;
--   * per-parent reverse ORDERING (two parent changes draining out of order).
--
-- Designs under test:
--   D0  the issue as written: no settled state, live join both directions.
--   D1  per-parent settled projection only.
--   D3  per-from-side-row settled attribution (== the enrichment table).
--   D5  D1 + LSN barrier + optimistic per-parent generation check
--       + per-parent reverse ordering guard.   <- the candidate
-- =====================================================================
drop schema if exists m cascade;
create schema m;
set search_path to m, public;

create sequence ptid_seq;

create table src_posts     (id bigint primary key, author int, word_count int);
create table src_post_tags (ptid bigint primary key, post bigint, tag text);

-- one row per committed user change, in commit (LSN) order
create table chg (
  lsn     bigserial primary key,
  tbl     text,            -- 'pt' | 'post'
  key     bigint,
  kind    text,            -- 'ins' | 'del' | 'upd'
  o       jsonb,           -- pre-image
  n       jsonb,           -- post-image
  plsn    bigint,          -- previous chg.lsn for this key (parent chain)
  seg     bigint,          -- null until intake stages it
  bucket  int,
  applied bool not null default false
);

create table segs    (seg bigint primary key, sealed bool not null default false);
create table drained (seg bigint, bucket int, primary key (seg, bucket));

-- reverse work: parent-keyed, staged into the ACTIVE segment (hop_gen+1)
create table rev (
  id       bigserial primary key,
  seg      bigint, bucket int,
  post     bigint,
  oa int, ow int, na int, nw int,
  prev_lsn bigint, lsn bigint,
  applied  bool not null default false,
  tries    int  not null default 0
);
-- D5's captured enumeration: taken once, re-validated on every retry
create table rev_hold (id bigint primary key, x bigint, gen bigint);
create table rev_hold_rows (id bigint, ptid bigint, tag text);

create table proj (id bigint primary key, author int, word_count int,
                   lsn bigint not null default 0, gen bigint not null default 0);
create table attr (ptid bigint primary key, post bigint, tag text, author int, word_count int);

create table tgt (tag text, author int, pc bigint, tw numeric);
create unique index tgt_pk on tgt (tag, author) nulls not distinct;

create table eng   (confirmed bigint, active_seg bigint, nb int, design text);
create table stats (name text primary key, v bigint);

create function bump(nm text, d bigint default 1) returns void language sql as $$
  insert into stats values (nm, d) on conflict (name) do update set v = stats.v + d; $$;
create function bkt(k bigint) returns int language sql stable as $$
  select (abs(k) % (select nb from eng))::int $$;
set search_path to m, public;

-- ---------------- user ops (each commits and appends one chg) ----------
create function op_pt_ins(p bigint, t text) returns void language plpgsql as $$
declare i bigint := nextval('ptid_seq');
begin
  insert into src_post_tags values (i, p, t);
  insert into chg(tbl,key,kind,o,n,plsn) values
    ('pt', i, 'ins', null, jsonb_build_object('post',p,'tag',t), 0);
  perform bump('op_pt_ins');
end $$;

create function op_pt_del(i bigint) returns void language plpgsql as $$
declare r src_post_tags; pl bigint;
begin
  select * into r from src_post_tags where ptid=i; if not found then return; end if;
  delete from src_post_tags where ptid=i;
  select coalesce(max(lsn),0) into pl from chg where tbl='pt' and key=i;
  insert into chg(tbl,key,kind,o,n,plsn) values
    ('pt', i, 'del', jsonb_build_object('post',r.post,'tag',r.tag), null, pl);
  perform bump('op_pt_del');
end $$;

create function op_pt_repoint(i bigint, np bigint) returns void language plpgsql as $$
declare r src_post_tags; pl bigint;
begin
  select * into r from src_post_tags where ptid=i; if not found then return; end if;
  if r.post is not distinct from np then return; end if;
  update src_post_tags set post=np where ptid=i;
  select coalesce(max(lsn),0) into pl from chg where tbl='pt' and key=i;
  insert into chg(tbl,key,kind,o,n,plsn) values
    ('pt', i, 'upd', jsonb_build_object('post',r.post,'tag',r.tag),
                     jsonb_build_object('post',np,'tag',r.tag), pl);
  perform bump('op_pt_repoint');
end $$;

create function op_post_ins(i bigint, a int, w int) returns void language plpgsql as $$
declare pl bigint;
begin
  if exists(select 1 from src_posts where id=i) then return; end if;
  insert into src_posts values (i,a,w);
  select coalesce(max(lsn),0) into pl from chg where tbl='post' and key=i;
  insert into chg(tbl,key,kind,o,n,plsn) values
    ('post', i, 'ins', null, jsonb_build_object('author',a,'wc',w), pl);
  perform bump('op_post_ins');
end $$;

create function op_post_upd(i bigint, a int, w int) returns void language plpgsql as $$
declare r src_posts; pl bigint;
begin
  select * into r from src_posts where id=i; if not found then return; end if;
  if r.author is not distinct from a and r.word_count is not distinct from w then return; end if;
  update src_posts set author=a, word_count=w where id=i;
  select coalesce(max(lsn),0) into pl from chg where tbl='post' and key=i;
  insert into chg(tbl,key,kind,o,n,plsn) values
    ('post', i, 'upd', jsonb_build_object('author',r.author,'wc',r.word_count),
                       jsonb_build_object('author',a,'wc',w), pl);
  perform bump('op_post_upd');
end $$;

create function op_post_del(i bigint) returns void language plpgsql as $$
declare r src_posts; pl bigint;
begin
  select * into r from src_posts where id=i; if not found then return; end if;
  delete from src_posts where id=i;
  select coalesce(max(lsn),0) into pl from chg where tbl='post' and key=i;
  insert into chg(tbl,key,kind,o,n,plsn) values
    ('post', i, 'del', jsonb_build_object('author',r.author,'wc',r.word_count), null, pl);
  perform bump('op_post_del');
end $$;

-- ---------------- engine: intake / seal ----------------
create function intake(k int) returns void language plpgsql as $$
begin
  update eng set confirmed = confirmed + k;
  update chg set seg = (select active_seg from eng), bucket = bkt(key)
   where seg is null and lsn <= (select confirmed from eng);
  perform bump('intake');
end $$;

create function seal() returns void language plpgsql as $$
declare a bigint;
begin
  select active_seg into a from eng;
  update segs set sealed=true where seg=a;
  insert into segs(seg) values (a+1);
  update eng set active_seg=a+1;
  perform bump('seal');
end $$;

-- ---------------- the aggregate target ----------------
create function emit(t text, a int, dc bigint, dw numeric) returns void language plpgsql as $$
begin
  insert into tgt values (t,a,dc,dw)
  on conflict (tag,author) do update
     set pc = tgt.pc + excluded.pc, tw = coalesce(tgt.tw,0) + coalesce(excluded.tw,0);
end $$;

-- group resolution: D0 reads the parent LIVE, every other design reads the
-- settled projection.
create function grp(p bigint, out a int, out w int) language plpgsql as $$
begin
  if (select design from eng) = 'D0' then
    select author, word_count into a,w from src_posts where id=p;
  else
    select author, word_count into a,w from proj where id=p;
  end if;
end $$;
set search_path to m, public;

create function touch_gen(p bigint) returns void language plpgsql as $$
begin
  if p is null then return; end if;
  insert into proj(id,author,word_count,lsn,gen) values (p,null,null,0,1)
  on conflict (id) do update set gen = proj.gen + 1;
end $$;

create function tidy() returns void language plpgsql as $$
begin delete from tgt where pc=0 and coalesce(tw,0)=0; end $$;

-- ---------------- forward drain: one (segment, bucket) unit ------------
create function drain_fwd(s bigint, b int) returns void language plpgsql as $$
declare f record; d text := (select design from eng); bef jsonb; aft jsonb;
        ga int; gw int; a2 int; w2 int; ar record; act bigint;
begin
  if not exists (select 1 from segs where seg=s and sealed) then return; end if;
  if exists (select 1 from drained where seg=s and bucket=b) then return; end if;

  -- from-side changes, folded per key within the unit
  for f in
    select key,
      (array_agg(kind order by lsn))[1]      as k0,
      (array_agg(o    order by lsn))[1]      as o0,
      (array_agg(kind order by lsn desc))[1] as k1,
      (array_agg(n    order by lsn desc))[1] as n1
    from chg where tbl='pt' and seg=s and bucket=b and not applied group by key
  loop
    bef := case when f.k0='ins' then null else f.o0 end;
    aft := case when f.k1='del' then null else f.n1 end;
    if bef is null and aft is null then continue; end if;

    if bef is not null then
      if d in ('D3','D3g') then
        select author, word_count into ga, gw from attr where ptid=f.key;
        perform emit(bef->>'tag', ga, -1, -gw);
        delete from attr where ptid=f.key;
      else
        select a,w into ga,gw from grp((bef->>'post')::bigint);
        perform emit(bef->>'tag', ga, -1, -gw);
      end if;
      perform touch_gen((bef->>'post')::bigint);
    end if;

    if aft is not null then
      select a,w into a2,w2 from grp((aft->>'post')::bigint);
      perform emit(aft->>'tag', a2, 1, w2);
      if d in ('D3','D3g') then
        insert into attr values (f.key,(aft->>'post')::bigint, aft->>'tag', a2, w2)
        on conflict (ptid) do update set post=excluded.post, tag=excluded.tag,
                                         author=excluded.author, word_count=excluded.word_count;
      end if;
      perform touch_gen((aft->>'post')::bigint);
    end if;
  end loop;

  -- parent changes, folded per key -> one reverse record, staged at hop_gen+1
  select active_seg into act from eng;
  for f in
    select key,
      (array_agg(kind order by lsn))[1]      as k0,
      (array_agg(o    order by lsn))[1]      as o0,
      (array_agg(plsn order by lsn))[1]      as p0,
      (array_agg(kind order by lsn desc))[1] as k1,
      (array_agg(n    order by lsn desc))[1] as n1,
      max(lsn)                               as mx
    from chg where tbl='post' and seg=s and bucket=b and not applied group by key
  loop
    insert into proj(id,author,word_count,lsn,gen) values (f.key,null,null,0,0)
      on conflict (id) do nothing;
    insert into rev(seg,bucket,post,oa,ow,na,nw,prev_lsn,lsn) values (
      act, bkt(f.key), f.key,
      case when f.k0='ins' then null else (f.o0->>'author')::int end,
      case when f.k0='ins' then null else (f.o0->>'wc')::int end,
      case when f.k1='del' then null else (f.n1->>'author')::int end,
      case when f.k1='del' then null else (f.n1->>'wc')::int end,
      f.p0, f.mx);
    perform bump('rev_staged');
  end loop;

  update chg set applied=true where seg=s and bucket=b and not applied;
  insert into drained values (s,b);
  perform tidy();
  perform bump('drain_fwd');
end $$;
set search_path to m, public;

create function drain_rev(rid bigint) returns void language plpgsql as $$
declare r rev; d text := (select design from eng); e record; h rev_hold; g bigint; pl bigint;
begin
  select * into r from rev where id=rid; if not found or r.applied then return; end if;
  if not exists (select 1 from segs where seg=r.seg and sealed) then return; end if;

  if d in ('D0','D1') then
    for e in select ptid, tag from src_post_tags where post=r.post loop
      perform emit(e.tag, r.oa, -1, -r.ow);
      perform emit(e.tag, r.na,  1,  r.nw);
    end loop;
    update proj set author=r.na, word_count=r.nw, lsn=r.lsn, gen=gen+1 where id=r.post;

  elsif d in ('D3','D3g') then
    if d='D3g' then
      select lsn into pl from proj where id=r.post;
      if r.prev_lsn is distinct from pl then
        update rev set tries=tries+1 where id=rid; perform bump('block_order'); return;
      end if;
    end if;
    for e in select ptid, tag, author, word_count from attr where post=r.post loop
      perform emit(e.tag, e.author, -1, -e.word_count);
      perform emit(e.tag, r.na,      1,  r.nw);
    end loop;
    update attr set author=r.na, word_count=r.nw where post=r.post;
    update proj set author=r.na, word_count=r.nw, lsn=r.lsn, gen=gen+1 where id=r.post;

  else -- D5
    if not exists (select 1 from rev_hold where id=rid) then
      insert into rev_hold(id,x,gen)
        select rid, (select coalesce(max(lsn),0) from chg), (select gen from proj where id=r.post);
      insert into rev_hold_rows(id,ptid,tag)
        select rid, ptid, tag from src_post_tags where post=r.post;
      perform bump('d5_capture');
    end if;
    select * into h from rev_hold where id=rid;

    -- (a) the LSN barrier: everything visible in the captured snapshot must
    --     be staged, or the reconcile below cannot see it at all.
    if d <> 'D5-a' and (select confirmed from eng) < h.x then
      update rev set tries=tries+1 where id=rid; perform bump('d5_block_barrier'); return;
    end if;
    -- (b) optimistic: no forward apply may have touched this parent since capture
    select gen into g from proj where id=r.post;
    if d <> 'D5-b' and g is distinct from h.gen then
      delete from rev_hold_rows where id=rid; delete from rev_hold where id=rid;
      update rev set tries=tries+1 where id=rid; perform bump('d5_block_gen'); return;
    end if;
    -- (c) no from-side change for this parent committed at or before the
    --     snapshot may still be unapplied
    if d <> 'D5-c' and exists (select 1 from chg
               where tbl='pt' and not applied and seg is not null and lsn <= h.x
                 and (coalesce((o->>'post')::bigint,-1)=r.post
                   or coalesce((n->>'post')::bigint,-1)=r.post)) then
      delete from rev_hold_rows where id=rid; delete from rev_hold where id=rid;
      update rev set tries=tries+1 where id=rid; perform bump('d5_block_inflight'); return;
    end if;
    -- (d) parent-reverse ordering: this record must be the next link
    select lsn into pl from proj where id=r.post;
    if d <> 'D5-d' and r.prev_lsn is distinct from pl then
      delete from rev_hold_rows where id=rid; delete from rev_hold where id=rid;
      update rev set tries=tries+1 where id=rid; perform bump('d5_block_order'); return;
    end if;

    for e in select ptid, tag from rev_hold_rows where id=rid loop
      perform emit(e.tag, r.oa, -1, -r.ow);
      perform emit(e.tag, r.na,  1,  r.nw);
    end loop;
    update proj set author=r.na, word_count=r.nw, lsn=r.lsn, gen=gen+1 where id=r.post;
    delete from rev_hold_rows where id=rid; delete from rev_hold where id=rid;
  end if;

  update rev set applied=true where id=rid;
  perform tidy();
  perform bump('drain_rev');
end $$;

-- ---------------- oracle / diff ----------------
create view oracle as
  select pt.tag, p.author, count(*)::bigint pc, coalesce(sum(p.word_count),0)::numeric tw
  from src_post_tags pt left join src_posts p on p.id=pt.post group by 1,2;

create function diff() returns table(tag text, author int, tc bigint, tw numeric, oc bigint, ow numeric)
language sql as $$
  select coalesce(t.tag,o.tag), coalesce(t.author,o.author), t.pc, t.tw, o.pc, o.tw
  from tgt t full join oracle o
    on o.tag = t.tag and coalesce(o.author,-2147483648) = coalesce(t.author,-2147483648)
  where t.pc is distinct from o.pc or coalesce(t.tw,0) is distinct from coalesce(o.tw,0)
  order by 1,2; $$;

-- ---------------- world reset ----------------
create function reset_world(d text) returns void language plpgsql as $$
begin
  delete from chg; delete from rev; delete from rev_hold; delete from rev_hold_rows;
  delete from drained; delete from segs; delete from proj; delete from attr;
  delete from tgt; delete from src_post_tags; delete from src_posts;
  alter sequence chg_lsn_seq restart with 1;
  alter sequence rev_id_seq restart with 1;
  alter sequence ptid_seq  restart with 1;
  delete from eng; insert into eng values (0, 1, 4, d);
  insert into segs(seg) values (1);
  insert into src_posts values (1,100,10),(2,200,20),(3,300,30);
  insert into src_post_tags select nextval('ptid_seq'), p, t
    from (values (1,'x'),(1,'y'),(2,'x'),(3,'z')) v(p,t);
  insert into proj select id, author, word_count, 0, 0 from src_posts;
  insert into attr select pt.ptid, pt.post, pt.tag, p.author, p.word_count
    from src_post_tags pt left join src_posts p on p.id=pt.post;
  insert into tgt select * from oracle;
end $$;
set search_path to m, public;

create function pending_units() returns bigint language sql stable as $$
  select (select count(*) from segs s cross join generate_series(0,(select nb from eng)-1) b
          where s.sealed and not exists (select 1 from drained d where d.seg=s.seg and d.bucket=b))
       + (select count(*) from rev r join segs s on s.seg=r.seg and s.sealed where not r.applied); $$;

create function drain_one_random() returns void language plpgsql as $$
declare u record; lo bigint;
begin
  select * into u from (
    select 'f' as kind, s.seg, b as bucket, null::bigint as rid
      from segs s cross join generate_series(0,(select nb from eng)-1) b
     where s.sealed and not exists (select 1 from drained d where d.seg=s.seg and d.bucket=b)
    union all
    select 'r', r.seg, r.bucket, r.id from rev r join segs s on s.seg=r.seg
     where s.sealed and not r.applied
  ) q order by random() limit 1;
  if not found then return; end if;
  select min(s.seg) into lo from segs s cross join generate_series(0,(select nb from eng)-1) b
    where s.sealed and not exists (select 1 from drained d where d.seg=s.seg and d.bucket=b);
  if lo is not null and u.seg > lo then perform bump('out_of_order'); end if;
  if u.kind='f' then perform drain_fwd(u.seg,u.bucket); else perform drain_rev(u.rid); end if;
end $$;

create function settle() returns bool language plpgsql as $$
declare i int; u record; n bigint;
begin
  for i in 1..80 loop
    perform intake(1000000);
    perform seal();
    for u in select * from (
               select 'f' k, s.seg, b bucket, null::bigint rid
                 from segs s cross join generate_series(0,(select nb from eng)-1) b
                where s.sealed and not exists (select 1 from drained d where d.seg=s.seg and d.bucket=b)
               union all
               select 'r', r.seg, r.bucket, r.id from rev r join segs s on s.seg=r.seg
                where s.sealed and not r.applied) q
             order by random()
    loop
      if u.k='f' then perform drain_fwd(u.seg,u.bucket); else perform drain_rev(u.rid); end if;
    end loop;
    select pending_units() into n;
    if n = 0 and not exists (select 1 from chg where not applied)
             and not exists (select 1 from rev where not applied) then return true; end if;
  end loop;
  return false;
end $$;

create function fuzz(d text, runs int, nops int, seed0 int)
returns table(design text, runs_total int, corrupted int, unsettled int, worst text)
language plpgsql as $$
declare r int; i int; roll float; p bigint; t text; pt bigint; bad int:=0; uns int:=0;
        ex text := null; n int; ok bool;
begin
  for r in 1..runs loop
    perform setseed(((seed0 + r) % 100000)::float / 100000.0);
    perform reset_world(d);
    for i in 1..nops loop
      roll := random();
      p := 1 + (random()*3)::int;                                  -- posts 1..4 (4 starts absent)
      t := (array['x','y','z','w'])[1+(random()*3)::int];
      select ptid into pt from src_post_tags order by random() limit 1;
      if    roll < 0.16 then perform op_pt_ins(p,t);
      elsif roll < 0.25 then perform op_pt_del(pt);
      elsif roll < 0.33 then perform op_pt_repoint(pt,p);
      elsif roll < 0.45 then perform op_post_upd(p,(array[100,200,300,999,555])[1+(random()*4)::int],
                                                   (array[10,20,30,77])[1+(random()*3)::int]);
      elsif roll < 0.50 then perform op_post_ins(p,(array[100,999])[1+(random()*1)::int],
                                                   (array[10,77])[1+(random()*1)::int]);
      elsif roll < 0.55 then perform op_post_del(p);
      elsif roll < 0.66 then perform intake(1+(random()*4)::int);
      elsif roll < 0.74 then perform seal();
      else perform drain_one_random();
      end if;
    end loop;
    ok := settle();
    if not ok then uns := uns + 1; end if;
    select count(*) into n from diff();
    if n > 0 then
      bad := bad + 1;
      if ex is null then ex := format('seed=%s: %s', seed0+r,
        (select string_agg(format('(%s,%s) tgt=%s/%s oracle=%s/%s',
          coalesce(tag,'-'),coalesce(author::text,'NULL'),tc,tw,oc,ow),'; ') from diff())); end if;
    end if;
  end loop;
  return query select d, runs, bad, uns, coalesce(ex,'-');
end $$;
