-- Source schema and seed for the soak harness. Run as the Trellis role with
-- psql -v accounts=N -v orders=N; regions are fixed at 50.
create table regions (id int primary key, name text not null);
create table accounts (id bigint primary key, region int not null, name text not null, balance numeric(14,2) not null, active boolean not null default true, updated_at timestamptz not null default now());
create table orders (id bigserial primary key, account_id bigint not null, amount numeric(12,2) not null, status text not null, created_at timestamptz not null default now());
create index on orders(account_id);
create index on orders(account_id, amount);
create index on accounts(region);
insert into regions select g, 'region '||g from generate_series(1,50) g;
insert into accounts select g, 1+(g%50), 'acct '||g, (g%1000)::numeric/7, true from generate_series(1,:accounts) g;
insert into orders(account_id, amount, status) select 1+(g%:accounts), (g%997)::numeric/3, case when g%3=0 then 'paid' else 'open' end from generate_series(1,:orders) g;
analyze;
