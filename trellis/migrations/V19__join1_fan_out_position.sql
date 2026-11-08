-- A 1-1 JOIN's pending fan-outs are per joined position (ADR-0021): a
-- change of the table joined at `position` fans out to the `R` entries on its
-- key there, so a marker names the position with the key. Every marker an
-- earlier binary wrote is the two-table JOIN's, position 1.
--
-- Declares no catalog floor: no release before 0.2.0 has `join1_fan_out`
-- (V10), so none reads or writes a marker, and the column's default is the
-- position every marker before it named.
alter table join1_fan_out
    add column if not exists position smallint not null default 1;

do $$
begin
    if not exists (
        select 1 from pg_catalog.pg_constraint c
        join pg_catalog.pg_attribute a
          on a.attrelid = c.conrelid and a.attname = 'position'
        where c.conrelid = 'join1_fan_out'::regclass
          and c.contype = 'p'
          and a.attnum = any (c.conkey)
    ) then
        alter table join1_fan_out drop constraint if exists join1_fan_out_pkey;
        alter table join1_fan_out add primary key (transform_id, position, join_key);
    end if;
end
$$;
