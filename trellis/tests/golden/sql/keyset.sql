-- 1: live rows
select coalesce(replace(t."id"::text, chr(1), chr(1) || chr(1)), chr(1)) as k, jsonb_build_object('id', t."id"::text, 'total', t."total"::text) as doc from "public"."src" t join unnest($1::text[]::integer[]) as u(c0) on true where t."id" = u.c0 and t."id" = any($1::text[]::integer[]) union all select coalesce(replace(t."id"::text, chr(1), chr(1) || chr(1)), chr(1)) as k, jsonb_build_object('id', t."id"::text, 'total', t."total"::text) as doc from "public"."src" t where t."id" is null

-- 1: new images
select k.key, case when t.ctid is null then null else (jsonb_build_object('id', t."id"::text, 'total', t."total"::text))::text end from unnest($1::text[], $2::text[]::integer[]) as k(key, c0) left join "public"."tgt" t on t."id" = k.c0 and t."id" = any($2::text[]::integer[]) union all select k.key, case when t.ctid is null then null else (jsonb_build_object('id', t."id"::text, 'total', t."total"::text))::text end from unnest($3::text[]) as k(key) left join "public"."tgt" t on t."id" is null

-- 1 v: sweep source
join unnest($2::text[]::integer[]) as u(__trellis_p0) on s."id" = u.__trellis_p0

-- 1 v: sweep target
exists (select 1 from unnest($2::text[]::integer[]) as u(__trellis_p0) where t."id" = u.__trellis_p0)

-- 1 v: lock
select cols from tgt as t where t."id" = any($1::text[]::integer[]) order by t."id" for update

-- 1 v: delete
delete from tgt as t where t."id" = any($1::text[]::integer[]) returning ret as pk

-- 1 N: sweep source
where s."id" is null

-- 1 N: sweep target
t."id" is null

-- 1 N: lock
select cols from tgt as t where t."id" is null for update of t

-- 1 N: delete
delete from tgt as t where t."id" is null returning ret as pk

-- 2: live rows
select (array_to_string(array[replace(replace(coalesce(replace(t."warehouse"::text, chr(1), chr(1) || chr(1)), chr(1)), chr(30), chr(30) || chr(30)), chr(31), chr(30) || chr(31)), replace(replace(coalesce(replace(t."sku"::text, chr(1), chr(1) || chr(1)), chr(1)), chr(30), chr(30) || chr(30)), chr(31), chr(30) || chr(31))], chr(31)) collate "C") as k, jsonb_build_object('id', t."id"::text, 'total', t."total"::text) as doc from "public"."src" t join unnest($1::text[]::integer[], $2::text[]::text[]) as u(c0, c1) on true where t."warehouse" = u.c0 and t."sku" = u.c1 union all select (array_to_string(array[replace(replace(coalesce(replace(t."warehouse"::text, chr(1), chr(1) || chr(1)), chr(1)), chr(30), chr(30) || chr(30)), chr(31), chr(30) || chr(31)), replace(replace(coalesce(replace(t."sku"::text, chr(1), chr(1) || chr(1)), chr(1)), chr(30), chr(30) || chr(30)), chr(31), chr(30) || chr(31))], chr(31)) collate "C") as k, jsonb_build_object('id', t."id"::text, 'total', t."total"::text) as doc from "public"."src" t join unnest($3::text[]::integer[]) as u(c0) on true where t."warehouse" = u.c0 and t."sku" is null union all select (array_to_string(array[replace(replace(coalesce(replace(t."warehouse"::text, chr(1), chr(1) || chr(1)), chr(1)), chr(30), chr(30) || chr(30)), chr(31), chr(30) || chr(31)), replace(replace(coalesce(replace(t."sku"::text, chr(1), chr(1) || chr(1)), chr(1)), chr(30), chr(30) || chr(30)), chr(31), chr(30) || chr(31))], chr(31)) collate "C") as k, jsonb_build_object('id', t."id"::text, 'total', t."total"::text) as doc from "public"."src" t join unnest($4::text[]::text[]) as u(c1) on true where t."warehouse" is null and t."sku" = u.c1 union all select (array_to_string(array[replace(replace(coalesce(replace(t."warehouse"::text, chr(1), chr(1) || chr(1)), chr(1)), chr(30), chr(30) || chr(30)), chr(31), chr(30) || chr(31)), replace(replace(coalesce(replace(t."sku"::text, chr(1), chr(1) || chr(1)), chr(1)), chr(30), chr(30) || chr(30)), chr(31), chr(30) || chr(31))], chr(31)) collate "C") as k, jsonb_build_object('id', t."id"::text, 'total', t."total"::text) as doc from "public"."src" t where t."warehouse" is null and t."sku" is null

-- 2: new images
select k.key, case when t.ctid is null then null else (jsonb_build_object('id', t."id"::text, 'total', t."total"::text))::text end from unnest($1::text[], $2::text[]::integer[], $3::text[]::text[]) as k(key, c0, c1) left join "public"."tgt" t on t."warehouse" = k.c0 and t."sku" = k.c1 union all select k.key, case when t.ctid is null then null else (jsonb_build_object('id', t."id"::text, 'total', t."total"::text))::text end from unnest($4::text[], $5::text[]::integer[]) as k(key, c0) left join "public"."tgt" t on t."warehouse" = k.c0 and t."sku" is null union all select k.key, case when t.ctid is null then null else (jsonb_build_object('id', t."id"::text, 'total', t."total"::text))::text end from unnest($6::text[], $7::text[]::text[]) as k(key, c1) left join "public"."tgt" t on t."warehouse" is null and t."sku" = k.c1 union all select k.key, case when t.ctid is null then null else (jsonb_build_object('id', t."id"::text, 'total', t."total"::text))::text end from unnest($8::text[]) as k(key) left join "public"."tgt" t on t."warehouse" is null and t."sku" is null

-- 2 vv: sweep source
join unnest($2::text[]::integer[], $3::text[]::text[]) as u(__trellis_p0, __trellis_p1) on s."warehouse" = u.__trellis_p0 and s."sku" = u.__trellis_p1

-- 2 vv: sweep target
exists (select 1 from unnest($2::text[]::integer[], $3::text[]::text[]) as u(__trellis_p0, __trellis_p1) where t."warehouse" = u.__trellis_p0 and t."sku" = u.__trellis_p1)

-- 2 vv: lock
select cols from tgt as t join unnest($1::text[]::integer[], $2::text[]::text[]) as k(c0, c1) on (t."warehouse" = k.c0 and t."sku" = k.c1) order by t."warehouse", t."sku" for update of t

-- 2 vv: delete
delete from tgt as t where exists (select 1 from unnest($1::text[]::integer[], $2::text[]::text[]) as k(c0, c1) where t."warehouse" = k.c0 and t."sku" = k.c1) returning ret as pk

-- 2 Nv: sweep source
join unnest($2::text[]::text[]) as u(__trellis_p0) on s."warehouse" is null and s."sku" = u.__trellis_p0

-- 2 Nv: sweep target
exists (select 1 from unnest($2::text[]::text[]) as u(__trellis_p0) where t."warehouse" is null and t."sku" = u.__trellis_p0)

-- 2 Nv: lock
select cols from tgt as t join unnest($1::text[]::text[]) as k(c1) on (t."warehouse" is null and t."sku" = k.c1) order by t."warehouse", t."sku" for update of t

-- 2 Nv: delete
delete from tgt as t using unnest($1::text[]::text[]) as k(c1) where t."warehouse" is null and t."sku" = k.c1 returning ret as pk

-- 2 vN: sweep source
join unnest($2::text[]::integer[]) as u(__trellis_p0) on s."warehouse" = u.__trellis_p0 and s."sku" is null

-- 2 vN: sweep target
exists (select 1 from unnest($2::text[]::integer[]) as u(__trellis_p0) where t."warehouse" = u.__trellis_p0 and t."sku" is null)

-- 2 vN: lock
select cols from tgt as t join unnest($1::text[]::integer[]) as k(c0) on (t."warehouse" = k.c0 and t."sku" is null) order by t."warehouse", t."sku" for update of t

-- 2 vN: delete
delete from tgt as t using unnest($1::text[]::integer[]) as k(c0) where t."warehouse" = k.c0 and t."sku" is null returning ret as pk

-- 2 NN: sweep source
where s."warehouse" is null and s."sku" is null

-- 2 NN: sweep target
t."warehouse" is null and t."sku" is null

-- 2 NN: lock
select cols from tgt as t where t."warehouse" is null and t."sku" is null for update of t

-- 2 NN: delete
delete from tgt as t where t."warehouse" is null and t."sku" is null returning ret as pk

-- 3: live rows
select (array_to_string(array[replace(replace(coalesce(replace(t."a"::text, chr(1), chr(1) || chr(1)), chr(1)), chr(30), chr(30) || chr(30)), chr(31), chr(30) || chr(31)), replace(replace(coalesce(replace(t."b"::text, chr(1), chr(1) || chr(1)), chr(1)), chr(30), chr(30) || chr(30)), chr(31), chr(30) || chr(31)), replace(replace(coalesce(replace(t."c"::text, chr(1), chr(1) || chr(1)), chr(1)), chr(30), chr(30) || chr(30)), chr(31), chr(30) || chr(31))], chr(31)) collate "C") as k, jsonb_build_object('id', t."id"::text, 'total', t."total"::text) as doc from "public"."src" t join unnest($1::text[]::integer[], $2::text[]::text[], $3::text[]::bigint[]) as u(c0, c1, c2) on true where t."a" = u.c0 and t."b" = u.c1 and t."c" = u.c2 union all select (array_to_string(array[replace(replace(coalesce(replace(t."a"::text, chr(1), chr(1) || chr(1)), chr(1)), chr(30), chr(30) || chr(30)), chr(31), chr(30) || chr(31)), replace(replace(coalesce(replace(t."b"::text, chr(1), chr(1) || chr(1)), chr(1)), chr(30), chr(30) || chr(30)), chr(31), chr(30) || chr(31)), replace(replace(coalesce(replace(t."c"::text, chr(1), chr(1) || chr(1)), chr(1)), chr(30), chr(30) || chr(30)), chr(31), chr(30) || chr(31))], chr(31)) collate "C") as k, jsonb_build_object('id', t."id"::text, 'total', t."total"::text) as doc from "public"."src" t join unnest($4::text[]::integer[]) as u(c0) on true where t."a" = u.c0 and t."b" is null and t."c" is null union all select (array_to_string(array[replace(replace(coalesce(replace(t."a"::text, chr(1), chr(1) || chr(1)), chr(1)), chr(30), chr(30) || chr(30)), chr(31), chr(30) || chr(31)), replace(replace(coalesce(replace(t."b"::text, chr(1), chr(1) || chr(1)), chr(1)), chr(30), chr(30) || chr(30)), chr(31), chr(30) || chr(31)), replace(replace(coalesce(replace(t."c"::text, chr(1), chr(1) || chr(1)), chr(1)), chr(30), chr(30) || chr(30)), chr(31), chr(30) || chr(31))], chr(31)) collate "C") as k, jsonb_build_object('id', t."id"::text, 'total', t."total"::text) as doc from "public"."src" t join unnest($5::text[]::text[], $6::text[]::bigint[]) as u(c1, c2) on true where t."a" is null and t."b" = u.c1 and t."c" = u.c2

-- 3: new images
select k.key, case when t.ctid is null then null else (jsonb_build_object('id', t."id"::text, 'total', t."total"::text))::text end from unnest($1::text[], $2::text[]::integer[], $3::text[]::text[], $4::text[]::bigint[]) as k(key, c0, c1, c2) left join "public"."tgt" t on t."a" = k.c0 and t."b" = k.c1 and t."c" = k.c2 union all select k.key, case when t.ctid is null then null else (jsonb_build_object('id', t."id"::text, 'total', t."total"::text))::text end from unnest($5::text[], $6::text[]::integer[]) as k(key, c0) left join "public"."tgt" t on t."a" = k.c0 and t."b" is null and t."c" is null union all select k.key, case when t.ctid is null then null else (jsonb_build_object('id', t."id"::text, 'total', t."total"::text))::text end from unnest($7::text[], $8::text[]::text[], $9::text[]::bigint[]) as k(key, c1, c2) left join "public"."tgt" t on t."a" is null and t."b" = k.c1 and t."c" = k.c2

-- 3 vvv: sweep source
join unnest($2::text[]::integer[], $3::text[]::text[], $4::text[]::bigint[]) as u(__trellis_p0, __trellis_p1, __trellis_p2) on s."a" = u.__trellis_p0 and s."b" = u.__trellis_p1 and s."c" = u.__trellis_p2

-- 3 vvv: sweep target
exists (select 1 from unnest($2::text[]::integer[], $3::text[]::text[], $4::text[]::bigint[]) as u(__trellis_p0, __trellis_p1, __trellis_p2) where t."a" = u.__trellis_p0 and t."b" = u.__trellis_p1 and t."c" = u.__trellis_p2)

-- 3 vvv: lock
select cols from tgt as t join unnest($1::text[]::integer[], $2::text[]::text[], $3::text[]::bigint[]) as k(c0, c1, c2) on (t."a" = k.c0 and t."b" = k.c1 and t."c" = k.c2) order by t."a", t."b", t."c" for update of t

-- 3 vvv: delete
delete from tgt as t where exists (select 1 from unnest($1::text[]::integer[], $2::text[]::text[], $3::text[]::bigint[]) as k(c0, c1, c2) where t."a" = k.c0 and t."b" = k.c1 and t."c" = k.c2) returning ret as pk

-- 3 Nvv: sweep source
join unnest($2::text[]::text[], $3::text[]::bigint[]) as u(__trellis_p0, __trellis_p1) on s."a" is null and s."b" = u.__trellis_p0 and s."c" = u.__trellis_p1

-- 3 Nvv: sweep target
exists (select 1 from unnest($2::text[]::text[], $3::text[]::bigint[]) as u(__trellis_p0, __trellis_p1) where t."a" is null and t."b" = u.__trellis_p0 and t."c" = u.__trellis_p1)

-- 3 Nvv: lock
select cols from tgt as t join unnest($1::text[]::text[], $2::text[]::bigint[]) as k(c1, c2) on (t."a" is null and t."b" = k.c1 and t."c" = k.c2) order by t."a", t."b", t."c" for update of t

-- 3 Nvv: delete
delete from tgt as t using unnest($1::text[]::text[], $2::text[]::bigint[]) as k(c1, c2) where t."a" is null and t."b" = k.c1 and t."c" = k.c2 returning ret as pk

-- 3 vNN: sweep source
join unnest($2::text[]::integer[]) as u(__trellis_p0) on s."a" = u.__trellis_p0 and s."b" is null and s."c" is null

-- 3 vNN: sweep target
exists (select 1 from unnest($2::text[]::integer[]) as u(__trellis_p0) where t."a" = u.__trellis_p0 and t."b" is null and t."c" is null)

-- 3 vNN: lock
select cols from tgt as t join unnest($1::text[]::integer[]) as k(c0) on (t."a" = k.c0 and t."b" is null and t."c" is null) order by t."a", t."b", t."c" for update of t

-- 3 vNN: delete
delete from tgt as t using unnest($1::text[]::integer[]) as k(c0) where t."a" = k.c0 and t."b" is null and t."c" is null returning ret as pk

