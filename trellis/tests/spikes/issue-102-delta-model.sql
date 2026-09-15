-- ===================================================================
-- Spike B: a faithful scratch model of issue #102's proposed
-- "image-aware reverse delta" maintenance for
--   TRANSFORM author_tag_totals FROM post_tags GROUP BY tag, post.author
--   SELECT count(*) AS post_count, sum(post.word_count) AS total_words
--
-- Models trellis's actual execution model:
--   * user transactions commit into src_* immediately;
--   * each commit appends its CDC images to a pending queue;
--   * apply_batch() drains the queue LATER, and every join resolution
--     (forward group lookup, reverse from-side enumeration) reads the
--     source tables LIVE at apply time -- which is what
--     apply::build_relationship_context and
--     apply::from_side_keys_for_join do today;
--   * a reverse (parent-driven) change is staged at hop_gen+1, i.e. it
--     is processed in the NEXT batch, per apply.rs's reverse_recomputes.
-- ===================================================================
drop schema if exists b cascade; create schema b; set search_path to b, public;

create table src_posts (id bigint primary key, author int, word_count int);
create table src_post_tags (post bigint, tag text, primary key (post, tag));

create table tgt (tag text, author int, post_count bigint, total_words numeric, primary key (tag, author));

-- pending CDC queue: one row per committed change, with images.
create table q (
  seq bigserial primary key,
  batch int,                   -- filled when applied
  kind text,                   -- 'pt_ins' | 'pt_del' | 'post_upd'
  post bigint, tag text,
  old_author int, new_author int, old_wc int, new_wc int
);
create table state (batch int);
insert into state values (0);

-- ---------- user-facing ops (commit + enqueue images) ----------
create function op_pt_ins(p bigint, t text) returns void language plpgsql as $$
begin
  insert into src_post_tags values (p, t);
  insert into q(kind, post, tag) values ('pt_ins', p, t);
end $$;

create function op_pt_del(p bigint, t text) returns void language plpgsql as $$
begin
  delete from src_post_tags where post = p and tag = t;
  insert into q(kind, post, tag) values ('pt_del', p, t);
end $$;

create function op_post_upd(p bigint, a int, w int) returns void language plpgsql as $$
declare oa int; ow int;
begin
  select author, word_count into oa, ow from src_posts where id = p;
  update src_posts set author = a, word_count = w where id = p;
  insert into q(kind, post, old_author, new_author, old_wc, new_wc)
  values ('post_upd', p, oa, a, ow, w);
end $$;

-- ---------- the proposed delta maintenance ----------
-- One batch: drain every unapplied q row. Forward (post_tags) changes
-- resolve their group by reading src_posts LIVE. Parent (posts) changes
-- emit the paired subtract-old / add-new over the from-side rows
-- enumerated LIVE -- exactly the design in issue #102's M3.
create function apply_batch() returns void language plpgsql as $$
declare bn int;
begin
  update state set batch = batch + 1 returning batch into bn;

  create temp table d (tag text, author int, dc bigint, dw numeric) on commit drop;

  -- forward: post_tags insert/delete, group resolved by a live join
  insert into d
  select x.tag, p.author, case when x.kind='pt_ins' then 1 else -1 end,
         case when x.kind='pt_ins' then p.word_count else -p.word_count end
  from q x left join src_posts p on p.id = x.post
  where x.batch is null and x.kind in ('pt_ins','pt_del');

  -- reverse: posts update. subtract old image / add new image over the
  -- from-side rows enumerated live at apply time.
  insert into d
  select pt.tag, a.author, a.sign, a.sign * a.wc
  from q x
  join src_post_tags pt on pt.post = x.post
  cross join lateral (values (x.old_author, -1, x.old_wc), (x.new_author, 1, x.new_wc)) a(author, sign, wc)
  where x.batch is null and x.kind = 'post_upd';

  update q set batch = bn where batch is null;

  insert into tgt (tag, author, post_count, total_words)
  select tag, author, sum(dc), sum(dw) from d group by 1,2
  on conflict (tag, author) do update
    set post_count = tgt.post_count + excluded.post_count,
        total_words = coalesce(tgt.total_words,0) + coalesce(excluded.total_words,0);
  delete from tgt where post_count = 0 and total_words = 0;
  drop table d;
end $$;

-- ---------- oracle ----------
create view oracle as
  select pt.tag, p.author, count(*)::bigint post_count, sum(p.word_count)::numeric total_words
  from src_post_tags pt left join src_posts p on p.id = pt.post
  group by 1,2;

create function diff() returns table(tag text, author int, tgt_c bigint, tgt_w numeric, or_c bigint, or_w numeric)
language sql as $$
  select coalesce(t.tag,o.tag), coalesce(t.author,o.author), t.post_count, t.total_words, o.post_count, o.total_words
  from tgt t full join oracle o on o.tag=t.tag and o.author is not distinct from t.author
  where t.post_count is distinct from o.post_count or t.total_words is distinct from o.total_words
  order by 1,2;
$$;
set search_path to b, public;
\pset footer off
create or replace function reset_world() returns void language plpgsql as $$
begin
  delete from q; truncate tgt; delete from src_post_tags; delete from src_posts; update state set batch=0;
  insert into src_posts values (1, 100, 10), (2, 200, 20);
  insert into src_post_tags values (1,'x'), (1,'y'), (2,'x');
  insert into tgt select * from oracle;  -- start converged (post-backfill)
end $$;

create or replace function verdict(label text) returns text language plpgsql as $$
declare n int;
begin
  select count(*) into n from diff();
  if n = 0 then return format('PASS  %s', label);
  else return format('FAIL  %s  (%s group(s) wrong: %s)', label, n,
    (select string_agg(format('(%s,%s) tgt=%s/%s oracle=%s/%s', tag,author,tgt_c,tgt_w,or_c,or_w), '; ') from diff()));
  end if;
end $$;

-- ---- S1: plain post_tags insert
select reset_world(); select op_pt_ins(2,'y'); select apply_batch(); select apply_batch();
select verdict('S1 insert a post_tags row');

-- ---- S2: plain post_tags delete
select reset_world(); select op_pt_del(1,'y'); select apply_batch(); select apply_batch();
select verdict('S2 delete a post_tags row');

-- ---- S3: parent author change only
select reset_world(); select op_post_upd(1, 999, 10); select apply_batch(); select apply_batch();
select verdict('S3 posts.author 100->999');

-- ---- S4: author + word_count changed in ONE update
select reset_world(); select op_post_upd(1, 999, 77); select apply_batch(); select apply_batch();
select verdict('S4 posts.author AND word_count in one UPDATE');

-- ---- S5: word_count only (value-side delta, issue #102 M5)
select reset_world(); select op_post_upd(1, 100, 77); select apply_batch(); select apply_batch();
select verdict('S5 posts.word_count only');

-- ---- S6: THE INTERLEAVING. author change commits, then a post_tags
--          insert commits, then BOTH are applied. The insert's forward
--          delta resolves the join live (sees the NEW author); the
--          reverse delta then enumerates that same row live and moves it
--          again, from a group it was never in.
select reset_world();
select op_post_upd(1, 999, 10);
select op_pt_ins(1, 'z');
select apply_batch();   -- forward: pt_ins(1,z) joins live -> author 999
select apply_batch();   -- reverse for posts.1 staged at hop_gen+1
select verdict('S6 author change then post_tags INSERT on the same post');

-- ---- S7: same, but a delete
select reset_world();
select op_post_upd(1, 999, 10);
select op_pt_del(1, 'y');
select apply_batch();
select apply_batch();
select verdict('S7 author change then post_tags DELETE on the same post');

-- ---- S8: the "safe" ordering (insert applied BEFORE the author commits)
select reset_world();
select op_pt_ins(1,'z'); select apply_batch();
select op_post_upd(1, 999, 10); select apply_batch(); select apply_batch();
select verdict('S8 insert fully applied before the author change commits');

-- ---- S9: two author changes on the same post in one batch window
select reset_world();
select op_post_upd(1, 999, 10);
select op_post_upd(1, 555, 10);
select apply_batch(); select apply_batch();
select verdict('S9 two author changes on one post in one batch');
set search_path to b, public;
\pset footer off
-- staged reverse deltas: a posts change is resolved into from-side work
-- one batch LATER (apply.rs stages reverse_recomputes at hop_gen+1).
create table if not exists staged_rev (
  due int, post bigint, old_author int, new_author int, old_wc int, new_wc int);

-- Variant with the textbook IVM correction: the forward (post_tags) delta
-- resolves its group against posts as of BEFORE the parent changes that are
-- in flight -- i.e. d(A)|><|B_old  +  A_new|><|d(B), rather than
-- d(A)|><|B_new + A_new|><|d(B), which double-counts the d(A)|><|d(B) cross
-- term. `horizon` controls how far back the forward path can see:
--   'same_batch' -> only parent changes drained in THIS batch (the most a
--                   batch-local implementation could ever know)
create or replace function apply_batch_x() returns void language plpgsql as $$
declare bn int;
begin
  update state set batch = batch + 1 returning batch into bn;
  create temp table d (tag text, author int, dc bigint, dw numeric) on commit drop;

  -- FORWARD, corrected: if this batch also carries a parent change for the
  -- row's post, use that change's OLD image instead of the live row.
  insert into d
  select x.tag,
         coalesce(pu.old_author, p.author),
         case when x.kind='pt_ins' then 1 else -1 end,
         case when x.kind='pt_ins' then coalesce(pu.old_wc, p.word_count)
              else -coalesce(pu.old_wc, p.word_count) end
  from q x
  left join src_posts p on p.id = x.post
  left join lateral (select old_author, old_wc from q u
                     where u.batch is null and u.kind='post_upd' and u.post=x.post
                     order by u.seq limit 1) pu on true
  where x.batch is null and x.kind in ('pt_ins','pt_del');

  -- parent changes seen this batch get STAGED for the next one (hop_gen+1)
  insert into staged_rev select bn+1, post, old_author, new_author, old_wc, new_wc
  from q where batch is null and kind='post_upd';
  update q set batch = bn where batch is null;

  -- REVERSE, due this batch: enumerate the from-side LIVE (A_new)
  insert into d
  select pt.tag, a.author, a.sign, a.sign*a.wc
  from staged_rev r join src_post_tags pt on pt.post = r.post
  cross join lateral (values (r.old_author,-1,r.old_wc),(r.new_author,1,r.new_wc)) a(author,sign,wc)
  where r.due = bn;
  delete from staged_rev where due = bn;

  insert into tgt (tag, author, post_count, total_words)
  select tag, author, sum(dc), sum(dw) from d group by 1,2
  on conflict (tag,author) do update
    set post_count = tgt.post_count + excluded.post_count,
        total_words = coalesce(tgt.total_words,0) + coalesce(excluded.total_words,0);
  delete from tgt where post_count = 0 and total_words = 0;
  drop table d;
end $$;

create or replace function reset_world() returns void language plpgsql as $$
begin
  delete from q; delete from staged_rev; truncate tgt;
  delete from src_post_tags; delete from src_posts; update state set batch=0;
  insert into src_posts values (1,100,10),(2,200,20);
  insert into src_post_tags values (1,'x'),(1,'y'),(2,'x');
  insert into tgt select * from oracle;
end $$;

-- S10: author change + post_tags insert land in the SAME batch
select reset_world();
select op_post_upd(1,999,10); select op_pt_ins(1,'z');
select apply_batch_x(); select apply_batch_x(); select apply_batch_x();
select verdict('S10 [x-term fix] both changes in ONE batch');

-- S11: the post_tags insert is drained in a batch that does NOT carry the
--      parent change (it was drained one segment earlier) but BEFORE the
--      parent's staged reverse comes due. Nothing batch-local can see it.
select reset_world();
select op_post_upd(1,999,10);
select apply_batch_x();          -- batch 1: parent drained, reverse staged for batch 2
delete from staged_rev; insert into staged_rev values (3,1,100,999,10,10); -- reverse deferred to batch 3
select op_pt_ins(1,'z');
select apply_batch_x();          -- batch 2: only the insert -> resolves live (author 999)
select apply_batch_x();          -- batch 3: the reverse fires
select verdict('S11 [x-term fix] insert drained between the parent change and its reverse');
