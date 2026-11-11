-- render region
a."region"

-- render_signed region
__g."region"

-- render n
count(*)

-- render_signed n
sum(__trellis_w)

-- render q
sum(b."qty")

-- render_signed q
(case when sum(__trellis_w * (__x2 is not null)::int) = 0 then null else (coalesce(sum(__x2) filter (where __trellis_w > 0), 0) - coalesce(sum(__x2) filter (where __trellis_w < 0), 0)) end)

-- render av
avg(a."total")

-- render_signed av
(case when sum(__trellis_w * (__x1 is not null)::int) = 0 then null else ((case when coalesce(sum(__trellis_w) filter (where __x1 = 'NaN'), 0) > 0 or (coalesce(sum(__trellis_w) filter (where __x1 = 'Infinity'), 0) > 0 and coalesce(sum(__trellis_w) filter (where __x1 = '-Infinity'), 0) > 0) then 'NaN'::numeric when coalesce(sum(__trellis_w) filter (where __x1 = 'Infinity'), 0) > 0 then 'Infinity'::numeric when coalesce(sum(__trellis_w) filter (where __x1 = '-Infinity'), 0) > 0 then '-Infinity'::numeric else round(coalesce(sum(__trellis_w * __x1) filter (where scale(__x1) is not null), 0), (select coalesce(max(__z.__s), 0) from ( select __u.__s from ( select array_agg(scale(__x1)) as __as, array_agg(__trellis_w) as __aw) as __a cross join lateral unnest(__a.__as, __a.__aw) as __u(__s, __w) where __u.__s is not null group by __u.__s having sum(__u.__w) > 0) as __z)) end))::numeric / (sum(__trellis_w * (__x1 is not null)::int))::numeric end)

-- render c1
(count(b."qty") + 1::integer)

-- render_signed c1
(sum(__trellis_w * (__x2 is not null)::int) + 1::integer)

-- render cs
(case when ((sum(b."qty") > 0::integer) AND (not (sum(b."qty") is null))) then (-(sum(b."qty"))) else null end)

-- render_signed cs
(case when (((case when sum(__trellis_w * (__x2 is not null)::int) = 0 then null else (coalesce(sum(__x2) filter (where __trellis_w > 0), 0) - coalesce(sum(__x2) filter (where __trellis_w < 0), 0)) end) > 0::integer) AND (not ((case when sum(__trellis_w * (__x2 is not null)::int) = 0 then null else (coalesce(sum(__x2) filter (where __trellis_w > 0), 0) - coalesce(sum(__x2) filter (where __trellis_w < 0), 0)) end) is null))) then (-((case when sum(__trellis_w * (__x2 is not null)::int) = 0 then null else (coalesce(sum(__x2) filter (where __trellis_w > 0), 0) - coalesce(sum(__x2) filter (where __trellis_w < 0), 0)) end))) else null end)

-- render r
round(avg(a."total"), (2)::integer)

-- render_signed r
round((case when sum(__trellis_w * (__x1 is not null)::int) = 0 then null else ((case when coalesce(sum(__trellis_w) filter (where __x1 = 'NaN'), 0) > 0 or (coalesce(sum(__trellis_w) filter (where __x1 = 'Infinity'), 0) > 0 and coalesce(sum(__trellis_w) filter (where __x1 = '-Infinity'), 0) > 0) then 'NaN'::numeric when coalesce(sum(__trellis_w) filter (where __x1 = 'Infinity'), 0) > 0 then 'Infinity'::numeric when coalesce(sum(__trellis_w) filter (where __x1 = '-Infinity'), 0) > 0 then '-Infinity'::numeric else round(coalesce(sum(__trellis_w * __x1) filter (where scale(__x1) is not null), 0), (select coalesce(max(__z.__s), 0) from ( select __u.__s from ( select array_agg(scale(__x1)) as __as, array_agg(__trellis_w) as __aw) as __a cross join lateral unnest(__a.__as, __a.__aw) as __u(__s, __w) where __u.__s is not null group by __u.__s having sum(__u.__w) > 0) as __z)) end))::numeric / (sum(__trellis_w * (__x1 is not null)::int))::numeric end), (2)::integer)

-- render ca
cast(sum(b."qty") as numeric)

-- render_signed ca
cast((case when sum(__trellis_w * (__x2 is not null)::int) = 0 then null else (coalesce(sum(__x2) filter (where __trellis_w > 0), 0) - coalesce(sum(__x2) filter (where __trellis_w < 0), 0)) end) as numeric)

-- render y
(extract(year from '2024-01-01'::date) + count(*))

-- render_signed y
(extract(year from '2024-01-01'::date) + sum(__trellis_w))

-- render co
coalesce(sum(a."total"), 0::integer)

-- render_signed co
coalesce((case when sum(__trellis_w * (__x1 is not null)::int) = 0 then null else (case when coalesce(sum(__trellis_w) filter (where __x1 = 'NaN'), 0) > 0 or (coalesce(sum(__trellis_w) filter (where __x1 = 'Infinity'), 0) > 0 and coalesce(sum(__trellis_w) filter (where __x1 = '-Infinity'), 0) > 0) then 'NaN'::numeric when coalesce(sum(__trellis_w) filter (where __x1 = 'Infinity'), 0) > 0 then 'Infinity'::numeric when coalesce(sum(__trellis_w) filter (where __x1 = '-Infinity'), 0) > 0 then '-Infinity'::numeric else round(coalesce(sum(__trellis_w * __x1) filter (where scale(__x1) is not null), 0), (select coalesce(max(__z.__s), 0) from ( select __u.__s from ( select array_agg(scale(__x1)) as __as, array_agg(__trellis_w) as __aw) as __a cross join lateral unnest(__a.__as, __a.__aw) as __u(__s, __w) where __u.__s is not null group by __u.__s having sum(__u.__w) > 0) as __z)) end) end), 0::integer)

-- render rx
strpos(a."region", 'x'::text)

-- render_signed rx
strpos(__g."region", 'x'::text)

-- render lo
min(b."qty")

-- render_signed lo
error: a field reading SourceColumn { schema: None, table: "items", column: "qty" } outside an aggregate, which a JOIN definition can't hold

-- render bare column
reads the unqualified column 'qty', which a JOIN definition can't hold

