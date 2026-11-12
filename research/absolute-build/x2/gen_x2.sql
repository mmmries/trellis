-- X2: E5 gen.sql without post_tags (posts rows identical: tags are generated after posts). usage: psql -v N=1000000 -f gen.sql <db>   (db must be owned by role app)
\set ON_ERROR_STOP on
select setseed(0.42);
create table regions    (id int primary key, name text not null);
create table categories (id int primary key, name text not null);
create table authors    (id int primary key, name text not null, tier int not null);
create table posts (
  id int primary key,
  author_id int not null,
  region_id int not null,
  category_id int not null,
  grp_hi int not null,          -- permutation of 0..N-1: ~N groups
  likes int not null,
  score numeric(10,2) not null,
  title text not null
);
insert into regions select g, 'region-'||g from generate_series(1,50) g;
insert into categories select g, 'category-'||g from generate_series(1,200) g;
insert into authors select g, 'author-'||g, g%5 from generate_series(1,:N/10) g;
insert into posts
select g, 1+(random()*(:N/10-1))::int, 1+(random()*49)::int, 1+(random()*199)::int,
       ((g::bigint*7919) % :N)::int, (random()*1000)::int, round((random()*10000)::numeric,2),
       'post title '||md5(g::text)
from generate_series(1,:N) g;
create index posts_author_idx on posts (author_id);
create index posts_grp_hi_idx on posts (grp_hi);
vacuum (analyze) regions, categories, authors, posts;
select count(*) posts, count(distinct author_id) authors_used from posts;
