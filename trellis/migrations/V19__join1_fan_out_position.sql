-- A 1-1 JOIN's pending fan-outs are per joined position (ADR-0021): a
-- change of the table joined at `position` fans out to the `R` entries on its
-- key there, so a marker names the position with the key. Every marker an
-- earlier binary wrote is the two-table JOIN's, position 1.
alter table join1_fan_out
    add column position smallint not null default 1;

alter table join1_fan_out drop constraint join1_fan_out_pkey;

alter table join1_fan_out add primary key (transform_id, position, join_key);
