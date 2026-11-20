# Chaos and soak harness

Runs Trellis workers on a scratch Postgres under load, injects faults, and then checks every
target against a recompute. It runs by hand on a dev box, not in CI. It needs Postgres binaries
on `PATH`, `psycopg2`, `systemd-run --user`, and passwordless `sudo -n` for the blackhole,
tablespace and volume actions.

```sh
soak/chaos.py setup                                    # once: build, seed, define, snapshot
soak/chaos.py run --scenario smoke --lane f --issue 2188
soak/chaos.py run --step sleep:60 --step 'pg-restart:immediate+kill9:w2' --load-secs 300
soak/chaos.py actions                                  # every action and scenario
soak/chaos.py stop                                     # after an interrupted run
```

## What a run does

- **Cluster.** `setup` puts a Postgres cluster in `target/soak/` (`--dir`, `$TRELLIS_SOAK_DIR`).
  It seeds `schema.sql` (100k accounts, 1M orders by default), applies the six transforms in
  `defs.txt`, waits for them to go live and snapshots the data directory. Trellis runs as the
  role `trellis`, which isn't a superuser. The harness itself uses the cluster's superuser.
- **Restore.** Every `run` stops the load and the workers by pid file, then stops Postgres,
  and only then restores the snapshot.
- **Workers.** Each worker runs under `systemd-run --scope -p MemoryMax=6G` and serves
  `/metrics` on port 59501, 59502 and so on. `w1` connects over TCP so `blackhole` reaches it.
  A worker that exits when no action stopped it fails the run.
- **Load.** `loadgen.py` runs `--threads` connections at `--tps`, with the op weights in
  `--mix`, for `--load-secs`. It reconnects after every error.
- **Steps.** Each `--step` runs in order while the load runs. `a+b` runs `a` and `b` at the
  same time. A `--scenario` is a named step list with load defaults.
- **Monitor.** It writes `monitor.tsv` every 5 s: worker RSS, ring rows, bytes and age,
  definitions by status, drain failures, held keys, Trellis connections, and schema and target
  sizes.
- **Oracle.** After the load ends, `oracle.sql` (a two-way `EXCEPT` per target) runs every
  10 s until it reports no differences or `--converge-secs` passes.

Each run's events, worker logs, load log, monitor series and final `trellis status` are
under `target/soak/logs/<time>-<scenario>/`.

## Outcome

`run` reserves a logbook entry (kind `soak`; pass `--lane` and `--issue`) and waits for the box
to be free. It finishes the entry with the rows the load wrote. It exits 0 when the oracle
converged and every threshold held, and 1 otherwise. The thresholds are an unexpected worker
exit, a definition not `live` at the end, `--max-rss-mb` and `--max-age-secs`. It exits 2 when
the harness itself failed and 64 on a usage error. Use `--no-logbook` only for trial runs.

## Upgrades and full volumes

- **Upgrades.** `handover:NEW_BIN` tests an upgrade under load. Set up and run with the old
  binary (`setup --bin OLD` and `run --bin OLD`); the step starts `NEW_BIN` beside the old
  workers, kills them with `kill -9` and waits for the new worker to take over.
- **Full volumes.** `volume-full` needs a cluster whose data directory is a small tmpfs, with
  WAL elsewhere: `setup --volume-mb 700 --accounts 10000 --orders 100000`.

## Checks

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s soak -p 'test_*.py'
ruff check soak && ruff format --check soak
```
