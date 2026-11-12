-- X4 partitioned source: N rows, N groups (grp_hi = id), L leaves ranged on grp_hi.
-- psql -v N=1000000 -v L=32
create table posts (id bigint not null, grp_hi bigint not null, grp_lo int not null,
  score int not null, likes int not null, primary key (grp_hi, id)) partition by range (grp_hi);
select format('create table posts_p%s partition of posts for values from (%s) to (%s)',
              i, i * (:N / :L), case when i = :L - 1 then :N else (i + 1) * (:N / :L) end)
from generate_series(0, :L - 1) i \gexec
