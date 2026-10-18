-- #703 R1 and R3b: what each column a captured table's capture images looked
-- like when Trellis last looked, one row per (table, node, column). A node is
-- the table itself, or each leaf of a partitioned table (a partitioned root
-- has no storage of its own).
--
-- An install, widen or narrow records the columns its spec images, in its own
-- transaction. The capture pass compares the live `pg_attribute` row and file
-- node with each row here, acts on what it finds, and writes what it read
-- back in the same transaction (`capture::baseline`):
--
-- - `attnum`, `type_oid`/`type_name` and `typmod`: the column's identity, for
--   a column dropped and re-added, or retyped (R1);
-- - `attr_xmin`, `attr_hash` and `filenode`: the `xmin` of the column's
--   `pg_attribute` row, a hash of the rest of that row with its default
--   expression, and the node's `pg_relation_filenode`, for a same-type
--   `ALTER COLUMN ... TYPE ... USING` that rewrote values (R3b).
create table capture_baselines (
    table_name text not null,
    node oid not null,
    column_name text not null,
    filenode oid,
    attnum smallint not null,
    type_oid oid not null,
    type_name text not null,
    typmod integer not null,
    attr_xmin text not null,
    attr_hash text not null,
    primary key (table_name, node, column_name)
);
