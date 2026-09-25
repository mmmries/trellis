-- Experiment 2: the two operations of design note #558, implemented literally on scratch tables.
-- Target: sum(amt), count(*) of src rows grouped by the name of the parent they reference.

drop table if exists parent, src, ledger, groups cascade;
create table parent(id int primary key, name text not null);
create table src(id int primary key, parent_id int not null, amt bigint not null);

-- I3: per source row, the group the target counts it in, its contribution, and its basis.
create table ledger(
  from_key    int primary key,
  present     bool not null default false,   -- false: not derived yet, or a tombstone (I4)
  parent_id   int,                           -- join key the target used
  group_key   text,                          -- group the row is counted in (null: none)
  contrib     bigint not null default 0,
  basis       pg_snapshot,                   -- snapshot of the last Re-derive read (I2)
  applied_lsn pg_lsn                         -- commit position of the last Apply ("lsn" mode only)
);
create index ledger_parent on ledger(parent_id);
create index ledger_group on ledger(group_key);

-- Groups are sums, maintained only by increments; a count of zero is a tombstone.
create table groups(group_key text primary key, total bigint not null, member_count bigint not null);

-- Sorted on-conflict increments: the ONLY way a group row is ever written.
create or replace function bump_groups(gk text[], dt bigint[], dc bigint[]) returns void language sql as $$
  insert into groups(group_key, total, member_count)
  select g, sum(t), sum(c) from unnest(gk, dt, dc) as v(g, t, c) where g is not null group by g order by g
  on conflict (group_key) do update set total = groups.total + excluded.total, member_count = groups.member_count + excluded.member_count;
$$;

create or replace function hold(h bigint) returns void language plpgsql as $$
begin
  if h is not null then perform pg_advisory_lock(h); perform pg_advisory_unlock(h); end if;
end $$;

-- Re-derive over a batch of keys under I5: every ledger entry in key order first (creating absent
-- ones), then ONE statement that takes the snapshot and reads every source row, then one sorted
-- group increment. hold_after_read: advisory lock id the driver holds to freeze the step between
-- the read and the write. skip_visible: a to-side commit id; a child whose basis already sees it
-- is left alone (the scenario-7 optimization).
create or replace function rederive_batch(keys int[], hold_after_read bigint default null, skip_visible xid8 default null) returns text language plpgsql as $$
declare
  ks int[]; olds ledger[]; ex bool[]; par int[]; amt bigint[]; nm text[]; snap pg_snapshot;
  gk text[] := '{}'; dt bigint[] := '{}'; dc bigint[] := '{}'; i int; o ledger; n int := 0; skipped int := 0;
begin
  select coalesce(array_agg(distinct k order by k), '{}') into ks from unnest(keys) k;
  if ks = '{}' then return 'rederived 0, skipped 0'; end if;
  insert into ledger(from_key) select unnest(ks) on conflict do nothing;
  select array_agg(l order by l.from_key) into olds
    from (select * from ledger where from_key = any(ks) order by from_key for update) l;
  -- read after lock: the snapshot and every source read in one statement
  select pg_current_snapshot(),
         array_agg(s.id is not null order by k), array_agg(s.parent_id order by k), array_agg(s.amt order by k), array_agg(p.name order by k)
    into snap, ex, par, amt, nm
    from unnest(ks) k left join src s on s.id = k left join parent p on p.id = s.parent_id;
  perform hold(hold_after_read);
  for i in 1..array_length(ks, 1) loop
    o := olds[i];
    if skip_visible is not null and o.basis is not null and pg_visible_in_snapshot(skip_visible, o.basis) then
      skipped := skipped + 1; continue;
    end if;
    n := n + 1;
    if o.present then gk := gk || o.group_key; dt := dt || -o.contrib; dc := dc || -1::bigint; end if;
    if ex[i] and nm[i] is not null then gk := gk || nm[i]; dt := dt || amt[i]; dc := dc || 1::bigint; end if;
    update ledger set present = ex[i], parent_id = par[i], group_key = case when ex[i] then nm[i] end,
                      contrib = case when ex[i] then amt[i] else 0 end, basis = snap
      where from_key = ks[i];
  end loop;
  perform bump_groups(gk, dt, dc);
  return format('rederived %s, skipped %s', n, skipped);
end $$;

create or replace function rederive(r int, hold_after_read bigint default null) returns text language plpgsql as $$
begin
  perform rederive_batch(array[r], hold_after_read);
  return 'rederived';
end $$;

-- Apply(r, C, image): lock the entry; skip if C is visible in the basis (I2); in "lsn" mode also skip if
-- C's commit position is at or below the last applied one; else diff from the carried image, reading the
-- to-side live under I1. mode: literal | literal-snap (Apply also stamps basis := now) | lsn.
create or replace function apply(r int, c xid8, l pg_lsn, kind text, new_parent int, new_amt bigint,
                                 mode text, hold_after_read bigint default null, dep_lock bool default false) returns text language plpgsql as $$
declare
  old ledger; g text; new_present bool; snap pg_snapshot;
begin
  insert into ledger(from_key) values (r) on conflict do nothing;
  select * into old from ledger where from_key = r for update;
  if old.basis is not null and pg_visible_in_snapshot(c, old.basis) then return 'skip:visible'; end if;
  if mode = 'lsn' and old.applied_lsn is not null and l <= old.applied_lsn then return 'skip:lsn'; end if;
  if dep_lock and kind <> 'delete' then
    perform pg_advisory_xact_lock_shared(hashtext('dep'), new_parent);
  end if;
  new_present := kind <> 'delete';
  if new_present then
    select pg_current_snapshot(), p.name into snap, g from (select 1) one left join parent p on p.id = new_parent;
  else
    select pg_current_snapshot() into snap; new_amt := 0;
  end if;
  perform hold(hold_after_read);
  perform bump_groups(array[case when old.present then old.group_key end, case when new_present then g end],
                      array[-old.contrib, new_amt], array[-1, 1]);
  update ledger set present = new_present, parent_id = new_parent, group_key = case when new_present then g end,
                    contrib = new_amt,
                    basis = case when mode = 'literal-snap' then snap else basis end,
                    applied_lsn = greatest(applied_lsn, l)
    where from_key = r;
  return 'applied';
end $$;

-- A to-side change for parent p: find the children and re-derive them as one I5 batch.
-- enumeration: ledger | ledger+source | ledger+deplock
create or replace function reverse(p int, enumeration text, c xid8 default null) returns text language plpgsql as $$
declare
  keys int[];
begin
  if enumeration = 'ledger+deplock' then perform pg_advisory_xact_lock(hashtext('dep'), p); end if;
  select coalesce(array_agg(distinct from_key), '{}') into keys from (
    select from_key from ledger where parent_id = p
    union all
    select id from src where enumeration in ('ledger+source') and parent_id = p) u;
  return rederive_batch(keys, null, c);
end $$;

-- Batch Apply under I5: every ledger entry in key order, then one sorted group increment.
create or replace function apply_batch(keys int[], parents int[], amts bigint[], mode text, sorted bool) returns text language plpgsql as $$
declare
  i int; olds ledger[]; gk text[] := '{}'; dt bigint[] := '{}'; dc bigint[] := '{}'; g text; o ledger;
begin
  if sorted then
    insert into ledger(from_key) select unnest(keys) on conflict do nothing;
    select array_agg(x order by x.from_key) into olds from (
      select l.* from ledger l where from_key = any(keys) order by from_key for update) x;
  end if;
  for i in 1..array_length(keys, 1) loop
    if sorted then
      o := olds[i];
    else
      insert into ledger(from_key) values (keys[i]) on conflict do nothing;
      select * into o from ledger where from_key = keys[i] for update;
    end if;
    select name into g from parent where id = parents[i];
    if o.present then gk := gk || o.group_key; dt := dt || -o.contrib; dc := dc || -1::bigint; end if;
    update ledger set present = true, parent_id = parents[i], group_key = g, contrib = amts[i] where from_key = keys[i];
    if sorted then
      gk := gk || g; dt := dt || amts[i]; dc := dc || 1::bigint;
    else
      perform bump_groups(array[g], array[amts[i]], array[1::bigint]);
    end if;
  end loop;
  if sorted then perform bump_groups(gk, dt, dc); end if;
  return 'ok';
end $$;
