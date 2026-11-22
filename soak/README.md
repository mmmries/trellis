# Chaos and soak harness

Runs Trellis workers on a scratch Postgres under load, injects faults, and then checks every
target against a recompute. It runs by hand on a dev box, not in CI. It needs Postgres binaries
on `PATH`, `psycopg2`, `systemd-run --user`, and passwordless `sudo -n` for the blackhole,
tablespace and volume actions.

```sh
soak/chaos.py setup                                    # once: build, seed, define, snapshot
soak/chaos.py run --scenario smoke --lane f --issue 2188
soak/chaos.py run --step sleep:60 --step 'pg-restart:immediate+kill9:w2' --load-secs 300
soak/chaos.py run --schedule steady:600,idle:3600,spike:60:x100 --tps 50 --step until:2:1800 --step pg-restart
soak/chaos.py actions                                  # every action and scenario
soak/chaos.py stop                                     # after an interrupted run
```

## What a run does

- **Cluster.** `setup` puts a Postgres cluster in `target/soak/` (`--dir`, `$TRELLIS_SOAK_DIR`).
  It seeds `schema.sql` (100k accounts, 1M orders by default), applies the six transforms in
  `defs.txt`, waits for them to go live and snapshots the data directory. The role `app` owns
  the database and the sources and runs the load. Trellis runs as `trellis_runner`, a member of
  `app` and not a superuser, as [the recommendations](../docs/recommendations.md) set it up.
  The harness itself uses the cluster's superuser.
- **Restore.** Every `run` stops the load and the workers by pid file, then stops Postgres,
  and only then restores the snapshot. A pid is signalled only if its command line names the
  cluster's port.
- **Teardown.** A run that ends, fails, or gets Ctrl-C, SIGTERM or SIGHUP stops everything,
  removes the blackhole's nft table and unmounts the tablespace. So does `stop`, after a
  `kill -9`.
- **Workers.** Each worker runs under `systemd-run --scope -p MemoryMax=6G` and serves
  `/metrics` on port 59501, 59502 and so on. `w1` connects over TCP so `blackhole` reaches it.
  A worker that exits when no action stopped it fails the run.
- **Load.** `loadgen.py` runs `--threads` connections with the op weights in `--mix`,
  following `--schedule` (default `steady:120`). It reconnects after every error, and probes a connection idle for 2 s before its next write (a `select 1`), so a Postgres restart in an idle doesn't count against the next segment. A write that fails is never retried or hidden.
- **Profiles.** A schedule is `PROFILE:SECS[:RATE],...`: `idle` (no writes), `trickle` (one
  a minute), `steady` (`--tps`) and `spike` (10× `--tps`). `RATE` is `N` per second, `N/m`
  per minute or `xN` times `--tps`.
- **Steps.** Each `--step` runs in order while the load runs. `a+b` runs `a` and `b` at the
  same time. `until:SEGMENT:OFFSET` waits for a point in the schedule, which places the next
  step at an idle point. A `--scenario` is a named step list with load defaults.
- **Probes.** Every `--probe-secs` while loaded, and a second before each idle segment ends,
  the harness writes one row and times how long it takes to reach its target.
- **Monitor.** It writes `monitor.tsv` every 5 s: worker RSS, ring rows, bytes and age,
  definitions by status, drain failures, held keys, Trellis connections, schema and target
  sizes, the WAL position, and the `backend_xmin` hold of Trellis's sessions.
- **Oracle.** After the load ends, `oracle.sql` (a two-way `EXCEPT` per target) runs every
  10 s until it reports no differences or `--converge-secs` passes.

Each run's events, worker logs, load log, monitor series, `metrics.json` and final
`trellis status` are under `target/soak/logs/<time>-<scenario>/`.

## Operational metrics

`metrics.json` has, per schedule segment: the write rate achieved, probe latency, WAL bytes
(and the hourly rate after a settling time), the longest `backend_xmin` hold, idle-in-transaction
sessions, and WARN and ERROR lines grouped by message. Across segments, it compares each first
write after an idle with the steady-state latency. The run prints a summary and puts a short
form in the logbook note.

A run with an idle segment is gated on these thresholds (`GATE_*` in `chaos.py`); a breach fails
the run (outcome `fail`) with a message naming the metric and segment:

- first write after an idle at most 2.0x the steady p50;
- idle WAL at most 6.5 MB/h once settled;
- `backend_xmin` hold at most 2 s once settled, in any segment except one with `long-txn` or `pg-dump`;
- no WARN in an idle segment with no injected fault (WARNs in a faulted idle are not gated).

## Outcome

`setup` and `run` each reserve a logbook entry (kind `soak`; pass `--lane` and `--issue`) and
wait for the box to be free. It finishes the entry with the rows the load wrote. It exits 0 when the oracle
converged and every threshold held, and 1 otherwise. The thresholds are an unexpected worker
exit, a definition not `live` at the end, `--max-rss-mb`, `--max-age-secs` and the profile gates. It exits 2 when
the harness itself failed and 64 on a usage error. Use `--no-logbook` only for trial runs.

## Upgrades and full volumes

- **Upgrades.** `handover:NEW_BIN` tests an upgrade under load. Set up and run with the old
  binary (`setup --bin OLD` and `run --bin OLD`); the step starts `NEW_BIN` beside the old
  workers, kills them with `kill -9` and waits for the new worker to take over.
- **Full volumes.** `volume-full` needs a cluster whose data directory is a small tmpfs, with
  WAL elsewhere: `setup --volume-mb 700 --accounts 10000 --orders 100000`. That tmpfs stays
  mounted for later runs; `sudo umount target/soak/vol` frees it.

## Checks

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s soak -p 'test_*.py'
ruff check soak && ruff format --check soak
```
