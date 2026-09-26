-- Issue #558 (experiment 4 prototype): the source transaction's id, widened
-- to xid8 at append time, on every ring row. NULL for rows intake did not
-- stage (recomputes, reverse records, truncates).
alter table seg_0 add column if not exists src_xid xid8;
alter table seg_1 add column if not exists src_xid xid8;
alter table seg_2 add column if not exists src_xid xid8;
alter table seg_3 add column if not exists src_xid xid8;
