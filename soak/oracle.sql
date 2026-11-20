-- Two-way EXCEPT ALL of each target in defs.txt against its query (NULL-safe; a duplicate counts).
-- One 'target|diff_rows' line each.
select 'acct_view' t, count(*) from ((select id, name uname, balance*2 dbl, region from accounts where active except all select id, uname, dbl, region from public.acct_view) union all (select id, uname, dbl, region from public.acct_view except all select id, name, balance*2, region from accounts where active)) x
union all
select 'region_totals', count(*) from ((select region, count(*) n, sum(balance) total, avg(balance) from accounts group by region except all select region,n,total,avg_bal from public.region_totals) union all (select region,n,total,avg_bal from public.region_totals except all select region, count(*), sum(balance), avg(balance) from accounts group by region)) x
union all
select 'acct_order_totals', count(*) from ((select account_id, count(*) n, sum(amount), min(amount), max(amount) from orders group by 1 except all select account_id,n,total,mn,mx from public.acct_order_totals) union all (select account_id,n,total,mn,mx from public.acct_order_totals except all select account_id, count(*) n, sum(amount), min(amount), max(amount) from orders group by 1)) x
union all
select 'region_order_totals', count(*) from ((select a.region, sum(o.amount), count(*) from orders o join accounts a on o.account_id=a.id group by 1 except all select region,total,n from public.region_order_totals) union all (select region,total,n from public.region_order_totals except all select a.region, sum(o.amount), count(*) from orders o join accounts a on o.account_id=a.id group by 1)) x
union all
select 'order_view', count(*) from ((select o.id, a.name, a.region, o.amount, o.status from orders o left join accounts a on o.account_id=a.id except all select id, acct_name, region, amount, status from public.order_view) union all (select id, acct_name, region, amount, status from public.order_view except all select o.id, a.name, a.region, o.amount, o.status from orders o left join accounts a on o.account_id=a.id)) x
union all
select 'big_accts', count(*) from ((select account_id, total, n from public.acct_order_totals where total > 1700 except all select account_id, total, n from public.big_accts) union all (select account_id, total, n from public.big_accts except all select account_id, total, n from public.acct_order_totals where total > 1700)) x;
