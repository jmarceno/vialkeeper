# Turso storage backend

Backend-owned layout and controls for `VialKeeper.Storage.Turso`, the default
storage engine. Product contracts are described in
[Operations.md](../../../../Operations.md) and
[README.md](../../../../README.md).

Turso speaks the SQLite dialect, so this backend reuses every SQL module under
`lib/vial_keeper/storage/sqlite/` through the engine driver behaviour
(`VialKeeper.Storage.SQLite.Driver`). `VialKeeper.Storage.Turso.Adapter` only
selects the engine (create, open, artifact name, ownership lease path,
capability probe); open adapters are `VialKeeper.Storage.SQLite.Adapter`
structs whose `driver` is `VialKeeper.Storage.Turso.Driver`. The sections the
SQLite document marks *shared* (ownership lease, sequence reservations, write
transactions, integrity probes, diagnostics) apply here unchanged; see
[../sqlite/BACKEND.md](../sqlite/BACKEND.md).

## Bundle artifact

```text
notes.vialkeeper/
├── turso.db   # revisions, indexes, views, jobs, metadata
├── blobs/     # attachment bytes (product path; not SQL)
└── tmp/       # incomplete uploads and Tantivy search generations
```

`db_meta.storage_engine` is `turso`. A bundle holding a `database.sqlite3`
artifact, or whose `db_meta` names another engine, does not open here
(`unsupported_format`), and Turso does not create a database next to another
engine's artifact. The file starts with the SQLite header, but its MVCC
marker makes it unreadable by SQLite, and the other way round.

## Driver

The driver is the `native/vial_turso` NIF (Rust over the `turso` crate,
pinned to 0.8.1) with the same function set and term encoding as
`native/vial_sqlite`. Every call that touches the database runs on a dirty IO
scheduler and blocks on one shared two-thread tokio runtime;
`query_inline/3` always answers `:contended`, so point statements take the
dirty path too. `serialize/1` is unsupported. `cancel/1` is a no-op: Turso has
no interrupt handle, so a running statement finishes on its own.

All connections that one OS process opens on one file share one
`turso::Database` object, because MVCC state lives there. A closed connection
drops its reference, and a new file at a path whose old file was deleted gets
a fresh database object. A `:memory:` database is private to its connection.

## Connection setup

Every read-write connection runs, in order, `PRAGMA journal_mode = 'mvcc'`,
`PRAGMA synchronous = NORMAL` and `PRAGMA foreign_keys = ON`, and open fails
with `unsupported_format` unless `journal_mode` reads back `mvcc`. Reader
connections set `PRAGMA query_only = 1` (Turso accepts it and refuses writes)
and `foreign_keys`. The busy timeout is 2000 ms. SQLite-only pragmas
(`locking_mode`, `wal_autocheckpoint`, `cache_size`, `temp_store`) are not
set; Turso reports `locking_mode` as `exclusive` and does not accept `normal`.

## MVCC and conflicts

Concurrent write transactions begin with `BEGIN CONCURRENT`. Conflicts are
detected per row: two open transactions that write the same row conflict, and
inserts of different rows into `documents`, `revisions` and `changes` do not.
The NIF maps Turso's `Busy`/`BusySnapshot` errors, and any error whose message
mentions a conflict, to `{:error, :write_conflict}`. Turso has already rolled
the transaction back at that point; the following `ROLLBACK` reports that no
transaction is active, which the transaction code ignores. The writer pool
retries the command with a fresh sequence reservation (`min(50, 2^attempt)` ms
plus jitter) and emits `[:vial_keeper, :writer, :write_conflict]` telemetry for
every conflict.

Turso refuses DDL inside `BEGIN CONCURRENT`, so serial write transactions
(index and view catalog changes, compaction, imports) use `BEGIN IMMEDIATE`.
They run alone behind the writer pool's barrier; a concurrent commit that
overlaps one (only the sequence ledger's reservation write can) gets
`:write_conflict` and is retried. A serial `run/2` itself retries a conflict up
to 8 times.

The backend reports `max_writers: 16` and
`sequence_persistence: :separate_connection` for disk databases, so the writer
pool runs up to 16 document writes at once (`writer_pool_size` caps it).

## Log sidecar

While a database is open, Turso keeps two sidecars next to `turso.db`:
`turso.db-log` (the MVCC logical log) and `turso.db-wal` (the page WAL that a
checkpoint writes through). A clean close of the owner runs
`PRAGMA wal_checkpoint(TRUNCATE)`, which leaves both empty, closes the
connection and removes empty `-wal` and `-log` files, so a closed bundle is a
single `turso.db` plus `blobs/` and `tmp/`. The checkpoint blocks readers and
writers, so it only runs at close and in exclusive maintenance.

After a crash, keep `turso.db` together with its `-log` and `-wal` files;
reopening replays the log. Commits whose `COMMIT` returned survive a killed
process. Under `synchronous = NORMAL`, an OS or power failure can lose the
last committed transactions.

## Offline copy and backup manifests

Copy the complete closed `.vialkeeper` directory, as for SQLite. Turso has no
immutable read-only open, so `VialKeeper.Storage.SQLite.BackupManifest` opens a
closed `turso.db` read-write with `query_only` set and removes the empty
sidecars that open created after it closes. A bundle with live sidecars or a
`.lease` is refused as not closed.

## Capability probe

Startup validates the configured engine only. The Turso probe needs no FTS
(full-text search is Tantivy): it checks `SELECT sqlite_version()`, then that
a temporary file under the system temp directory accepts
`journal_mode = mvcc` and a `BEGIN CONCURRENT` / `COMMIT` round trip.

## Build

`native/vial_turso` builds in release mode in every Mix environment (an
unoptimized build is too slow even for tests) and takes about 8 minutes from
cold on 4 cores; CI caches its `target` directory. Rustler also copies the
library artifacts of Turso's own `cdylib` dependencies into `priv/native`; the
release step `VialKeeper.ReleaseSteps.prune_native_artifacts/1` removes them.
