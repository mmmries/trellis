insert into plain_ins select author_id, count(*) as n, sum(score) as total_score, sum(likes) as total_likes from posts group by author_id;
