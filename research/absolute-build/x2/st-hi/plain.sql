insert into plain_ins select grp_hi, count(*) as n, sum(score) as total_score, sum(likes) as total_likes from posts group by grp_hi;
