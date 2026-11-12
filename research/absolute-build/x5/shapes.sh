# Copied from X3 (E5 shapes). X5 uses s2_agg and s2_agg_hi only.
# Shape table: name | target table | trellis TRANSFORM | plain SELECT (column order = target column order)
# sourced by run.sh. Plain SELECT column order must match the target's column order for EXCEPT ALL.
declare -A TARGET TRANSFORM PLAIN
SHAPES="s1_plain s2_agg s2_agg_hi s3_aggjoin s4_join1 s5_star"

TARGET[s1_plain]=post_copy
TRANSFORM[s1_plain]="TRANSFORM post_copy FROM posts SELECT author_id AS author_id, likes AS likes, score AS score, title AS title"
PLAIN[s1_plain]="select id, author_id, likes, score, title from posts"

TARGET[s2_agg]=author_totals
TRANSFORM[s2_agg]="TRANSFORM author_totals FROM posts GROUP BY author_id SELECT author_id AS author_id, COUNT(*) AS n, SUM(score) AS total_score, SUM(likes) AS total_likes"
PLAIN[s2_agg]="select author_id, count(*) as n, sum(score) as total_score, sum(likes) as total_likes from posts group by author_id"

TARGET[s2_agg_hi]=hi_totals
TRANSFORM[s2_agg_hi]="TRANSFORM hi_totals FROM posts GROUP BY grp_hi SELECT grp_hi AS grp_hi, COUNT(*) AS n, SUM(score) AS total_score, SUM(likes) AS total_likes"
PLAIN[s2_agg_hi]="select grp_hi, count(*) as n, sum(score) as total_score, sum(likes) as total_likes from posts group by grp_hi"

TARGET[s3_aggjoin]=author_tag_totals
TRANSFORM[s3_aggjoin]="TRANSFORM author_tag_totals FROM posts JOIN post_tags ON posts.id = post_tags.post_id GROUP BY posts.author_id, post_tags.tag SELECT posts.author_id AS author_id, post_tags.tag AS tag, COUNT(*) AS n, SUM(posts.score) AS total_score"
PLAIN[s3_aggjoin]="select p.author_id, t.tag, count(*) as n, sum(p.score) as total_score from posts p join post_tags t on p.id = t.post_id group by p.author_id, t.tag"

TARGET[s4_join1]=post_view
TRANSFORM[s4_join1]="TRANSFORM post_view FROM posts LEFT JOIN authors ON posts.author_id = authors.id SELECT authors.name AS author_name, authors.tier AS tier, posts.score AS score, posts.title AS title"
PLAIN[s4_join1]="select p.id, a.name as author_name, a.tier, p.score, p.title from posts p left join authors a on p.author_id = a.id"

TARGET[s5_star]=post_star
TRANSFORM[s5_star]="TRANSFORM post_star FROM posts LEFT JOIN authors ON posts.author_id = authors.id LEFT JOIN regions ON posts.region_id = regions.id JOIN categories ON posts.category_id = categories.id SELECT authors.name AS author_name, regions.name AS region, categories.name AS category, posts.score AS score"
PLAIN[s5_star]="select p.id, a.name as author_name, r.name as region, c.name as category, p.score from posts p left join authors a on p.author_id = a.id left join regions r on p.region_id = r.id join categories c on p.category_id = c.id"
