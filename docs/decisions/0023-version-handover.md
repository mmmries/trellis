---
status: accepted
date: 2026-11-02
deciders: Michael Ries
---

# Version Handover

An instance's background processes (the staging worker and the drain threads,
in any number of processes) share one catalog, one staging ring and one claim
model. Two releases of Trellis may read the catalog differently, generate
different capture functions, or build and drain a definition differently, and
nothing in the claim model keeps them apart: claims are per segment bucket, not
per definition, and one staging leader generates every capture function. A
deploy that starts the new release beside the old one, which is what
[Rolling deploys](../embedding.md#rolling-deploys) describes, would run two
engines over one instance.

This ADR settles what a process does when another release is live on its
instance: it stands by until the other release is gone, and the calls of its
binary that change the instance's state refuse meanwhile. Two versions never
stage, drain or change state side by side.

## The rules

1. **Every background process registers its version.** `worker_registry`
   (migration V9) holds one row per background process, a staging-only one
   (`drain_threads = 0`) included, with the version of its binary (the crate
   version, the exact string), its roles (`staging`, `drain`), whether it
   stands by, and the backend pid of the session it holds the staging-worker
   lock on (`staging_pid`). It writes the row when it starts and on every pass
   after that (one per standby poll, 5 seconds), and removes it on a clean
   shutdown. A row is live while its heartbeat is within the reclaim TTL
   (30 seconds), the window `has_live_drain_workers` reads, which counts only
   the rows whose drain threads run.
2. **Another version is live** when a live row has a different version or none
   (a drain worker of a release before 0.2.0), unless that process stands by
   itself; or when the staging-worker lock is held and its holder is not a row
   of this version (a staging-only leader of a release before 0.2.0 holds the
   lock and registers nothing). A row names its staging session before the
   session takes the lock, so whoever sees the lock held finds its holder's
   row; the holder counts by its row's version whether or not its heartbeat is
   fresh.
3. **Migrations never run beside a live process.** With migrations pending,
   any live row or a held staging lock means a release older than the
   migrations is running: `migrate` (and `trellis apply`, which migrates first)
   refuses with the error of rule 6, and a background process waits. With
   nothing pending and another version live, `migrate` changes nothing: it
   runs its checks, the catalog floor and the refusal of a changed migration
   or a gap in the history among them
   ([ADR-0024](0024-compatibility-floors-and-rollback.md)),
   and succeeds, or refuses when the attach would write (a
   grammar-version pause, [ADR-0018](0018-grammar-versioning.md), or a changed
   marker). With migrations pending and nothing live, a background process
   migrates: its pass runs `migrate`, the ordinary one (the identity and
   attach checks, the catalog floor, the refusal of a changed migration or a
   gap, the takeover work), and takes over in the same pass. The takeover
   work (the grammar-version pauses, the recorded-key backfill, the
   numeric-partials conversion, a queued restore) runs once per binary
   version, whether or not migrations are pending: `migrate` records its
   version on the marker (`attached_version`) in the work's own transaction,
   so a kill leaves all of it or none, and a restore queue is its own marker
   until it is restored. A background process that would take over with
   nothing pending runs `migrate` first in the same way when the marker
   records another version or a restore is queued. With nothing pending,
   `migrate` runs no migration runner, so a role that can only read and write
   Trellis's tables runs the work that writes rows, and stands by as below for
   work that alters tables (a conversion, a restore). `migrate` takes
   the handover lock exclusive and decides again under it, so it never runs
   beside a process that went live since the pass decided, and two processes
   that find the migrations pending at once run them one after the other: the
   second finds nothing left. When `migrate` refuses (the process's role
   can't alter Trellis's tables, the history doesn't fit, the floor), the
   process stands by, says why, and tries again a minute later (at the next
   standby poll when the refusal was a fault, such as a lost connection). An
   explicit `migrate` still works, before or instead of that (`trellis run`
   migrates before it starts its pipeline, a deploy step, a release task): it
   is idempotent, and leaves a background process of its version nothing to
   run. A
   define-only handle runs no background process, so nothing migrates for it
   but its own `migrate`.
4. **Standby.** While another version is live, or this binary's migrations are
   pending and it can't run them, a background process stages nothing, drains
   nothing, dispatches no builds and runs no `self_check` job. A leader
   releases the staging-worker lock first. It checks again every standby poll
   and takes over as soon as nothing holds it back. It waits rather than exiting, so an orchestrator
   doesn't crash-loop it. Its pass runs under a per-instance advisory lock, so
   two processes starting at once decide in turn: the second sees the first,
   and a process that stands by holds no other version back, so two versions
   never wait on each other.
5. **Exact version match only.** Any difference in the version string is
   another version. There are no compatibility ranges or lists (any `0.2.x`,
   say): they would again mix two versions' behaviour in ways that are hard to
   reason about.
6. **State-changing calls refuse while another version is live**, from any
   handle, a define-only one included: `apply` (define, `ALTER`, `PAUSE`,
   `RESUME`, `DROP`), `request_backfill`, `release_key`,
   `detect_capture_disruptions` and `self_check`. The error's code is
   `version_handover`, and its message names this binary's version, the other
   versions and their worker ids. Read-only calls (`status`, the listings, the
   health checks, `watermark_token`, `await_converged`) keep working.
   The call checks before it starts, and again in each of its write
   transactions, under the instance's handover lock taken shared as the
   transaction's first lock. `migrate` and every handover pass take that lock
   exclusive, so neither a takeover nor a migration can land between a
   transaction's check and its commit. The handover lock's key is part of the
   cross-version protocol and never changes. A call of several transactions
   that another version overtakes between two of them refuses on the next
   one, with the earlier ones committed, as a process stopped mid-call leaves
   them; each step is idempotent, so a retry under the new release finishes
   the call.
7. **Visible.** `status()` carries `version_handover` while another version is
   live, naming this binary's version and the other processes by worker id and
   version, and `migrations_pending` while this binary's migrations haven't
   run, naming them and, when this handle's background process tried and was
   refused, why; a read that can't read the catalog until they run refuses
   with them (`version_handover`). A background process logs when it starts
   standing by, why, and when it takes over, naming the migrations it ran
   first, or saying it ran the takeover work. `migrations_pending` also
   carries a refusal of the takeover work alone, with no migrations named. A
   process killed without a clean shutdown leaves its row
   live for up to the reclaim TTL, which delays the takeover by as much; the
   message says so.
8. **Symmetric.** The rules hold for a rollback between two releases that both
   implement them. The older release meets the compatibility floors
   ([ADR-0024](0024-compatibility-floors-and-rollback.md)) as it attaches: it
   refuses a catalog whose floor is above its version, and pauses each
   definition whose floor is. A release before 0.2.0 has no floors; its
   `migrate` refuses any database a later release has migrated.

A schema that has never been migrated has no history and nothing live on it:
it reads as nothing pending, so `migrate` creates it, and a background process
started first fails as it reads the catalog, as it does without this ADR.

## A cross-version deploy

Start the new version: its processes stand by, its state-changing calls refuse,
and capture never stops (the triggers keep staging changes in the
application's own transactions). Stop the old version: within one standby poll
of its clean shutdown (or the reclaim TTL, for a process killed without one),
the new version takes over. One of its background processes first runs its
pending migrations, if any, and its takeover work, in the same pass; no
deploy step has to.

## Rejected

* **Document only.** [Rolling deploys](../embedding.md#rolling-deploys)
  promises a new worker can start beside the old one with no operator step, so
  the default deploy path would mix versions unguarded.
* **Refuse and exit.** An orchestrator crash-loops the process; standing by
  gives the same safety and lets the documented rolling deploy work.
* **Pause definitions.** It pauses the whole instance for a process-level
  condition and needs a false-positive study
  ([ADR-0027](0027-operating-principles.md), rule 3). Standing by and refusing
  calls pause nothing.
* **Each version drains what it knows.** A release's behaviour toward
  definitions it can't parse is fixed (it pauses them at attach), claims are
  per segment bucket rather than per definition, one staging leader generates
  every capture function, and definitions both versions know would be written
  by two engines. It needs an epic-sized change to the claim and progress
  model, for a benefit only long-lived mixed fleets see.

## Consequences

* Migration V9 is the first since 0.1.0: the migration set is no longer
  byte-identical to 0.1.0's (its V1 to V8 are), and 0.1.0 can't attach to a
  database migrated past it.
* During a cross-version deploy the new version does nothing until the old one
  is gone, and its `apply` changes refuse until then. A release that carries a
  migration can't have it run until every process of the old version has
  stopped; then the new version's background processes run it themselves.
* A background process's role needs the rights `migrate` needs, or the
  migrations wait for a role that has them to run `migrate`.
* A stale row left by a killed process delays the takeover by up to the
  reclaim TTL.
* Every state-changing call reads the registry and `pg_locks` before it
  starts and again in each of its write transactions, with the handover lock
  held shared, and every background process writes its row once per standby
  poll. A call's transaction waiting on a lock holds the handover lock shared
  meanwhile, so a pass or `migrate` that queues behind it waits as long
  (rarely seconds).
* A call that another version overtakes part way refuses with its earlier
  transactions committed.
