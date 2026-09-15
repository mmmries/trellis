#!/bin/bash
P="psql -h /home/mike/spike102/sock -U mike -d spike -t -A"
run() { # $1=sql -> returns exec ms
  $P -c "explain (analyze, timing off) $1" 2>&1 | grep 'Execution Time' | grep -oE '[0-9.]+'
}
echo "variant,N,groups,force_recompute_ms,delta_ms"
for V in sparse dense; do
  if [ $V = sparse ]; then AC=author; NA=4096; TGT=author_tag_totals; else AC=author_dense; NA=64; TGT=dense_totals; fi
  for N in 10 100 1000; do
    CHG="select p.id, p.$AC as old_author, mod(abs(hashint8(p.id*13)), $NA) as new_author, p.word_count from posts p where p.id in (select 1+mod(abs(hashint8(g*13+7)),1000000) from generate_series(1,$N) g)"
    KEY="with chg as ($CHG) select distinct pt.tag, a.author from chg join post_tags pt on pt.post=chg.id cross join lateral (values (chg.old_author),(chg.new_author)) a(author)"
    G=$($P -c "select count(*) from ($KEY) x;")
    FR=$(run "with k as ($KEY) select s.tag, r0.$AC, count(*), sum(r0.word_count) from k join post_tags s on s.tag=k.tag left join posts r0 on r0.id=s.post where r0.$AC=k.author group by s.tag, r0.$AC;")
    DL=$(run "with chg as ($CHG), d as (select pt.tag, a.author, a.sign::bigint dc, (a.sign*chg.word_count)::numeric dw from chg join post_tags pt on pt.post=chg.id cross join lateral (values (chg.old_author,-1),(chg.new_author,1)) a(author,sign)), agg as (select tag,author,sum(dc) dc,sum(dw) dw from d group by 1,2) select t.tag, t.author, t.post_count+agg.dc, t.total_words+agg.dw from agg join $TGT t on t.tag=agg.tag and t.author=agg.author;")
    echo "$V,$N,$G,$FR,$DL"
  done
done
