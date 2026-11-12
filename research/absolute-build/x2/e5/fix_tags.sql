truncate post_tags;
create temp table t1 as select id, (random()*15)::int t from posts;
insert into post_tags select id, t from t1 order by id;
insert into post_tags select id, (t + 1 + (random()*14)::int) % 16 from t1 where random() < 0.9 order by id;
vacuum (analyze) post_tags;
select count(*)::float/(select count(*) from posts) tags_per_post, (select count(*) from (select distinct p.author_id, t.tag from post_tags t join posts p on p.id=t.post_id) x) groups from post_tags;