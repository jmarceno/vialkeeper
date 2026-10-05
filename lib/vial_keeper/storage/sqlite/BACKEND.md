# SQLite storage backend

Backend-owned layout and controls for the `VialKeeper.Storage.SQLite`
implementation. Product contracts (bundle portability, ownership, integrity
rules, public errors) are described in [Operations.md](../../../../Operations.md)
and [README.md](../../../../README.md). Replacing this backend means
implementing the storage ports under `lib/vial_keeper/storage/ports/` plus a
backend module registered like `VialKeeper.Storage.SQLite.Adapter`.

## Engines and drivers

The SQL modules in this directory serve two SQLite-dialect engines: SQLite
itself and Turso (the product default, see
[../turso/BACKEND.md](../turso/BACKEND.md)). Everything engine-specific sits
behind `VialKeeper.Storage.SQLite.Driver`: the NIF statement calls, connection
pragmas, the concurrent-write `BEGIN` statement, the artifact and sidecar file
names, writer capabilities and the startup probe. A connection handle is
`{driver, ref}`, and an open `Adapter` struct records its `driver`, so every
port and SQL module runs unchanged on either engine.
`VialKeeper.Storage.SQLite.Driver.Rusqlite` is the SQLite driver. A host picks
one engine with `[storage].engine` in `host.toml`
(`VialKeeper.Storage.Engines`).

Sections marked *shared* apply to both engines; the rest is SQLite-only.

## Bundle artifact

Inside an `.vialkeeper` directory the SQLite backend stores:

```text
notes.vialkeeper/
├── database.sqlite3   # revisions, indexes, views, jobs, metadata
├── blobs/             # attachment bytes (product path; not SQL)
└── tmp/               # incomplete uploads and Tantivy search generations
```

The artifact filename and SQL schema are owned by this backend. Generic
runtime code opens the selected backend with the bundle root only.

## Ownership lease (shared)

Single-owner admission is a storage capability. The SQLite implementation
holds an exclusive transaction on `<artifact-path>.lease`, always through the
SQLite driver, whatever the engine. A second owner fails with
`database_in_use` (HTTP 409, retryable).

Safe recovery:

1. Confirm no live VialKeeper process owns the database.
2. After a crash, a leftover `.lease` file with no live exclusive lock can be
   reopened normally. Do not delete `.lease` while another process may hold
   the lock.
3. Prefer letting the crashed BEAM die, then retry open.
4. Never delete or rewrite `database.sqlite3` to “clear” a lease.

## Offline copy

Copy the complete closed `.vialkeeper` directory. Ignore `.lease`. Close
checkpoints the write-ahead log into `database.sqlite3` and removes empty
`-wal`/`-shm` sidecars, so a clean closed bundle is a single database file
plus `blobs/` and `tmp/`.

Do not copy an active crash-recoverable bundle piecemeal: keep
`database.sqlite3` together with any live `-wal` and `-shm` files until
recovery finishes. Reopening a crashed bundle replays the WAL automatically.

## Open WAL and snapshot readers

While a disk database is open, the writer connection uses WAL and
`synchronous=NORMAL`. Classified product reads open additional readonly
connections (`query_only`) against the same artifact; each logical read holds
one deferred snapshot. Memory SQLite and the in-process memory backend do not
open extra connections.

`synchronous=NORMAL` keeps the WAL consistent on application crash and fsyncs
it at checkpoints (close and auto-checkpoint); a power/OS failure can lose the
last committed transactions, which SQLite replays or discards on reopen. This
is an explicit durability trade-off: `FULL` added one WAL fsync per commit and
capped write throughput at disk fsync latency.

Disk writers set `wal_autocheckpoint=16384` (64 MiB at the default 4 KiB page
size) so ordinary commits are not stalled by the SQLite default 4 MiB
checkpoint fsync. Close still runs `wal_checkpoint(TRUNCATE)`. A power/OS
failure can therefore lose a larger suffix of unsynced WAL frames than the
SQLite default; application-crash consistency is unchanged.

Close order is drain running writer-pool writes, drain in-flight snapshots,
close writer-slot connections, close readers, checkpoint the writer
(`wal_checkpoint(TRUNCATE)`), close the writer, then remove empty `-wal`/`-shm`
sidecars. Exclusive commands (compact, integrity, rebuild, live-digest, blob
cleanup) drain snapshots before the writer runs, then resume the reader pool.
Runtime code never names sidecar files; this backend document does because it
owns the artifact.

## Sequence reservations (shared)

Change sequences come from the runtime sequence ledger, not from write
transactions. `db_meta.sequence_reserved_through` stores the highest sequence
the ledger may have handed out. Before handing out a sequence above it, the
ledger raises it by a block of 4096 in a short `BEGIN IMMEDIATE` transaction
on a writer connection of its own (`persist_sequence_reservation`, the only
statement that writes the column, always `max(current, new)`). The column is
therefore at or above every sequence stored in `changes`,
`documents.update_sequence` and `revisions.insertion_sequence`, and a restart
after a crash continues above it: sequences are never reused, and unused ones
are permanent holes. Integrity checks the retention floor and peer positions
against this column.

Storage used without a ledger (offline tooling and storage-level callers) has
one writer; there a write raises the column inside its own write transaction,
so a rolled-back write leaves no hole. The value is cached per connection and
forgotten on rollback.

The driver reports `max_writers` and `sequence_persistence`. SQLite reports
`max_writers` from the test-only `:sqlite_max_writers` setting (default 1) and
`sequence_persistence: :separate_connection` for disk databases (`:none` for
`:memory:`). `open_writer/1` opens an extra read-write connection
(`role: :writer_slot`) for writer slots and the ledger; its close only clears
that connection's caches and closes it, and never checkpoints or removes
sidecars.

## Write transactions (shared)

Serial write transactions (`run/2`) begin with `BEGIN IMMEDIATE`; DDL (index
and view catalog changes) only runs there. Concurrent write transactions
(`run_concurrent/2`, writer slots) begin with the driver's `begin_concurrent/0`
statement. On SQLite that is `BEGIN IMMEDIATE` too, so extra SQLite writers
serialize on the write lock and never conflict. A statement or commit that the
driver reports as `{:error, :write_conflict}` is normalized to a retryable
`:write_conflict` error, even when a SQL module wrapped the statement error
(`Connection.take_conflict/1`). `run_concurrent/2` returns it to the writer
pool, which retries the command; `run/2` retries it itself up to the driver's
`serial_conflict_retries/0` (0 on SQLite).

No storage SQL uses a returning clause: a plain insert plus
`Connection.last_insert_rowid/1` (or a keyed read-back for multi-row inserts)
gives the same result and avoids a large per-statement cost in the Turso
binding.

## Statement scheduling

The SQLite driver is the `native/vial_sqlite` NIF (Rust over `rusqlite` with
bundled SQLite), called only through `Connection`. Each statement is one NIF
call that prepares from the connection's statement cache, binds, steps to
completion and returns the rows. Statements normally run on a dirty IO
scheduler. Point statements on the single-document write path (key lookups
and single-row writes, `Connection.point_query/3` and `point_execute/3`) run
on the calling scheduler while the owner holds its `BEGIN IMMEDIATE` write
transaction: there they cannot wait on a lock, and their SQLite work is
smaller than a dirty-scheduler hop. `BEGIN`, `COMMIT` (which may checkpoint),
reads outside a write transaction and every other statement stay on dirty
schedulers.

The driver installs its own busy handler (2000 ms default, polled in short
sleeps) so `Connection.close/1` and `Connection.interrupt/1` wake a caller
waiting on another connection's lock.

## Integrity probes (shared)

Product integrity rules run over normalized domain facts. The SQLite backend
additionally reports engine probes (foreign keys, required tables). Failures
surface as `integrity_violation`. Full-text indexes are Tantivy generations
outside SQLite under the bundle `tmp/search/indexes/` directory; integrity
records them as external rather than comparing FTS rows.

## Format recognition

Open recognizes Version 1 before applying persistent pragmas. Empty files and
non-SQLite bytes are rejected without opening the engine. A SQLite file is
opened, then `application_id`, `user_version`, `db_meta` versions, required
tables and `db_meta.storage_engine` (the engine that created the bundle) are
confirmed. Only then does open set `journal_mode=WAL`. A bundle created by
another engine, or holding only another engine's artifact, is
`unsupported_format` ("bundle was created by the turso storage engine"); there
is no conversion between engines. Header fields
are not trusted from the raw file because page 1 may still live in WAL after a
crash. A foreign, future, or partial schema is `unsupported_format` and is not
rewritten. There is no in-place migrator in V1.

## Diagnostics (shared)

`VialKeeper.Diagnostics.runtime/0` includes an opaque selected-backend
capability map. SQLite version, compile options, FTS5, and transaction probes
are backend diagnostics, not the product identity model.

## Backend replacement checklist

```text
[ ] Implement storage port families (lifecycle, transaction, ownership,
    document/revision facts, change log, local records, retention records,
    index/candidate search, view state, derived state, attachment metadata,
    inspection)
[ ] Change log: `sequence_high_water/1` returns the persisted reserved-through
    value; `persist_sequence_reservation/2` raises it to at least the given
    value in its own transaction; `read_page/4` returns only rows with
    `since < sequence <= through`; never allocate sequences in a write
[ ] Lifecycle: `open_writer/1` and `close_writer/1` for an extra read-write
    connection (or `{:error, :unsupported_writers}`); `reset_writer_caches/1`;
    `capabilities/1` reports `max_writers` (positive integer) and
    `sequence_persistence` (`:separate_connection` or `:none`)
[ ] Transaction: `run_concurrent/2` (equal to `run/2` with one writer); report
    a write-write conflict as a retryable `:write_conflict` error
[ ] Own bundle artifact layout under the `.vialkeeper` root
[ ] Provide ownership acquire/release with typed in-use errors
[ ] Provide capability validation used at application startup
[ ] Keep product algorithms in shared services; backend code only maps facts
[ ] Add physical tests under test/physical/<backend>/
[ ] Register the backend module for runtime selection
```
