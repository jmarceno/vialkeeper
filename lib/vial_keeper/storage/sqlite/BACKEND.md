# SQLite storage backend

Backend-owned layout and controls for the default `VialKeeper.Storage.SQLite`
implementation. Product contracts (bundle portability, ownership, integrity
rules, public errors) are described in [Operations.md](../../../../Operations.md)
and [README.md](../../../../README.md). Replacing this backend means
implementing the storage ports under `lib/vial_keeper/storage/ports/` plus a
backend module registered like `VialKeeper.Storage.SQLite.Adapter`.

## Bundle artifact

Inside an `.vialkeeper` directory the SQLite backend stores:

```text
notes.vialkeeper/
├── database.sqlite3   # revisions, indexes, views, jobs, metadata
├── blobs/             # attachment bytes (product path; not SQL)
└── tmp/               # incomplete uploads
```

The artifact filename and SQL schema are owned by this backend. Generic
runtime code opens the selected backend with the bundle root only.

## Ownership lease

Single-owner admission is a storage capability. The SQLite implementation
holds an exclusive transaction on `<bundle-path>.lease`. A second owner fails
with `database_in_use` (HTTP 409, retryable).

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
`synchronous=FULL`. Classified product reads open additional readonly
connections (`query_only`) against the same artifact; each logical read holds
one deferred snapshot. Memory SQLite and the in-process memory backend do not
open extra connections.

Close order is drain in-flight snapshots, close readers, checkpoint the writer
(`wal_checkpoint(TRUNCATE)`), close the writer, then remove empty `-wal`/`-shm`
sidecars. Exclusive commands (compact, integrity, rebuild, live-digest, blob
cleanup) drain snapshots before the writer runs, then resume the reader pool.
Runtime code never names sidecar files; this backend document does because it
owns the artifact.

## Integrity probes

Product integrity rules run over normalized domain facts. The SQLite backend
additionally reports engine probes (foreign keys, required tables). Failures
surface as `integrity_violation`. Full-text indexes are Elixir posting lists
outside SQLite; integrity records them as external rather than comparing FTS
rows.

## Diagnostics

`VialKeeper.Diagnostics.runtime/0` includes an opaque selected-backend
capability map. SQLite version, compile options, FTS5, and transaction probes
are backend diagnostics, not the product identity model.

## Backend replacement checklist

```text
[ ] Implement storage port families (lifecycle, transaction, ownership,
    document/revision facts, change log, local records, retention records,
    index/candidate search, view state, derived state, attachment metadata,
    inspection)
[ ] Own bundle artifact layout under the `.vialkeeper` root
[ ] Provide ownership acquire/release with typed in-use errors
[ ] Provide capability validation used at application startup
[ ] Keep product algorithms in shared services; backend code only maps facts
[ ] Add physical tests under test/physical/<backend>/
[ ] Register the backend module for runtime selection
```
