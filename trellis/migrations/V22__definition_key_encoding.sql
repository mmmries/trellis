-- Each table's key encoding, recorded with its key (#2147, #2217). A key
-- column named here is encoded in every key text Trellis renders or reads
-- for the table: a `NULL` is `chr(1)` and a real `chr(1)` is doubled
-- (`defs::ddl::encode_key_part`). Any other key column renders raw. Every
-- encoder and decoder reads this, never the live catalog, so a `SET NOT
-- NULL`, `DROP NOT NULL` or `VALIDATE CONSTRAINT` never changes how a staged
-- or stored key decodes. A record written from here on encodes every key
-- column when the key's index is `UNIQUE ... NULLS NOT DISTINCT`, and none
-- otherwise (`defs::recorded_keys`).
--
-- An existing record keeps the encoding the release before this one used:
-- each key column that can hold `NULL` now. Stored keys aren't rewritten.
--
-- Runs again harmlessly (a test re-runs the migrations after V15): only a
-- record with no encoding yet is filled.
--
-- Raises the catalog floor to 0.4.0: an older release reads key encodings
-- from the live catalog, so it would decode the keys this one stores under
-- another encoding.
alter table definition_keys add column if not exists encoded_columns text[];

update definition_keys k
   set encoded_columns = array(
       select a.attname::text
         from pg_catalog.pg_attribute a
        where a.attrelid = pg_catalog.to_regclass(
                  pg_catalog.format('%I.%I', split_part(k.table_name, '.', 1),
                                    split_part(k.table_name, '.', 2)))
          and a.attname::text = any(k.key_columns)
          and a.attnum > 0 and not a.attisdropped
          and not (a.attnotnull and not exists (
                   select 1 from pg_catalog.pg_constraint nn
                    where nn.conrelid = a.attrelid and nn.contype = 'n'
                      and not nn.convalidated and a.attnum = any(nn.conkey)))
        order by a.attnum)
 where k.encoded_columns is null;

alter table definition_keys alter column encoded_columns set default '{}';
alter table definition_keys alter column encoded_columns set not null;

update catalog_floor set version = '0.4.0'
 where string_to_array(version, '.')::int[] < '{0,4,0}';
