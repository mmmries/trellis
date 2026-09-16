import subprocess, statistics, re, sys
DSN=["psql","-h","/tmp/spike102/sock","-p","5599","-d","fixt","-v","ON_ERROR_STOP=1","-q","-t","-A"]
def run(sql):
    p=subprocess.run(DSN+["-c",sql],capture_output=True,text=True)
    if p.returncode: print(p.stderr); sys.exit(1)
    return p.stdout
def bench(label, stmts, reps=5):
    ms=[]
    for _ in range(reps):
        body="begin;\n"+"\n".join(f"explain (analyze, timing off) {s};" for s in stmts)+"\nrollback;"
        out=run(body)
        tot=sum(float(m) for m in re.findall(r"Execution Time: ([\d.]+) ms", out))
        ms.append(tot)
    print(f"{label:58s} {statistics.median(ms):9.1f} ms")
    return statistics.median(ms)

KEYSET = """(select distinct pt.tag, a.author from chg1k c
             join post_tags pt on pt.post = c.post
             cross join lateral (values (c.old_author),(c.new_author)) a(author))"""

print("\n--- 1,000 posts.author+word_count UPDATEs (the parent/reverse path) ---")
bench("force_every_group: recompute every touched group", [
  f"""create temp table ks as select * from {KEYSET}""",
  """delete from tgt t using ks where t.tag=ks.tag and t.author=ks.author""",
  """insert into tgt select pt.tag, p.author, count(*), sum(p.word_count)
       from post_tags pt left join posts p on p.id=pt.post
       join ks on ks.tag=pt.tag and ks.author=p.author
      group by 1,2"""])
bench("delta (D5 core): move each affected child row", [
  """insert into tgt (tag,author,post_count,total_words)
     select pt.tag, v.author, sum(v.sign), sum(v.sign*v.wc)
       from chg1k c join post_tags pt on pt.post=c.post
       cross join lateral (values (c.old_author,-1,c.old_wc),(c.new_author,1,c.new_wc)) v(author,sign,wc)
      group by 1,2
     on conflict (tag,author) do update set post_count=tgt.post_count+excluded.post_count,
        total_words=tgt.total_words+excluded.total_words"""])
bench("  + advance the per-parent projection (D5 state)", [
  """update proj p set author=c.new_author, word_count=c.new_wc from chg1k c where p.id=c.post"""])
bench("2-transform workaround: hop1 (facts) + hop2 (aggregate)", [
  """create temp table fd as
     select f.post, f.tag, f.author old_author, f.word_count old_wc, c.new_author, c.new_wc
       from facts f join chg1k c on c.post=f.post""",
  """update facts f set author=fd.new_author, word_count=fd.new_wc from fd where f.post=fd.post and f.tag=fd.tag""",
  """insert into tgt_wa (tag,author,post_count,total_words)
     select tag, v.author, sum(v.sign), sum(v.sign*v.wc) from fd
     cross join lateral (values (fd.old_author,-1,fd.old_wc),(fd.new_author,1,fd.new_wc)) v(author,sign,wc)
     group by 1,2
     on conflict (tag,author) do update set post_count=tgt_wa.post_count+excluded.post_count,
        total_words=tgt_wa.total_words+excluded.total_words"""])

print("\n--- 1,000 post_tags INSERTs (the forward path) ---")
bench("force_every_group: recompute every touched group", [
  """create temp table ks2 as select distinct i.tag, p.author from ins1k i join posts p on p.id=i.post""",
  """delete from tgt t using ks2 where t.tag=ks2.tag and t.author=ks2.author""",
  """insert into tgt select pt.tag, p.author, count(*), sum(p.word_count)
       from post_tags pt left join posts p on p.id=pt.post
       join ks2 on ks2.tag=pt.tag and ks2.author=p.author
      group by 1,2"""])
bench("delta via the settled projection (D5)", [
  """insert into tgt (tag,author,post_count,total_words)
     select i.tag, pr.author, count(*), sum(pr.word_count)
       from ins1k i join proj pr on pr.id=i.post group by 1,2
     on conflict (tag,author) do update set post_count=tgt.post_count+excluded.post_count,
        total_words=tgt.total_words+excluded.total_words"""])
bench("2-transform workaround: hop1 (facts) + hop2 (aggregate)", [
  """insert into facts select i.post, i.tag, p.author, p.word_count from ins1k i join posts p on p.id=i.post""",
  """insert into tgt_wa (tag,author,post_count,total_words)
     select i.tag, p.author, count(*), sum(p.word_count) from ins1k i join posts p on p.id=i.post group by 1,2
     on conflict (tag,author) do update set post_count=tgt_wa.post_count+excluded.post_count,
        total_words=tgt_wa.total_words+excluded.total_words"""])
