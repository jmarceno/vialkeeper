# ElixirDB

ElixirDB is a Version 1 revisioned document database implemented as a single
supervised Elixir application. Each registered database is one SQLite file;
documents, complete revision histories, tombstones, changes, local
replication metadata, and logical indexes live in that file.

## Runtime

The checked-in baseline is Elixir 1.20.2 on Erlang/OTP 29.0.4. Dependencies
are pinned in `mix.lock`. The default HTTP listener binds to loopback at
`127.0.0.1:4000`; set `ELIXIR_DB_ROOT` to choose the database root.

```sh
mix deps.get
mix check.fast
mix check.full
mix run --no-halt
```

Operator procedures (start/stop, registration, offline copy, leases, integrity,
replication job states, and host-limit troubleshooting) live in
[docs/operations.md](docs/operations.md).

The public protocol is rooted at `/v1`. Database and document operations use
JSON envelopes, and document IDs are carried in request bodies. SQL and
backend-specific query syntax are never accepted from clients.

## Offline portability

Stop the server, close the database, and copy the database file with ordinary
operating-system tools. The copied file remains inert until it is explicitly
registered with the destination server. The `.lease` companion is transient
and is not part of authoritative database state.

## Revision and replication model

Every mutation creates an immutable SHA-256 revision over canonical JSON.
Physical leaves and conflict branches are retained. One-shot replication
transfers complete root-to-leaf chains and preserves revision IDs, ancestry,
tombstones, and deterministic winner selection. Configuration, indexes, jobs,
and checkpoints are local to each database and do not cross the wire.

## Registration and copying

Databases are created below the configured root and are registered in the
routing-only registrations.json manifest. Existing files are admitted only
through POST /v1/registrations with a relative path; registration validates
the SQLite markers, schema, UUID, and integrity before routing traffic to it.
Unregistering removes routing metadata but never deletes the database file.

For an offline copy, stop writes, close the database with
POST /v1/databases/:uuid/close, copy the single .db file, and register the
copy at the destination. A .lease file is transient ownership state and is
not authoritative database data.

## Replication jobs and limits

Replication job definitions, checkpoints, revisions, tombstones, changes, and
logical indexes are stored in the database file. Runtime worker state is
transient. Enabled continuous jobs resume after restart; one-shot workers end
in completed, failed, or cancellation state. Use the replication endpoints
under /v1/databases/:uuid/replications to inspect and control jobs.

Host limits bound request bodies, document IDs and bodies, bulk operations,
query scans/results/time, changes batches, replication batches, open databases,
and concurrent workers. Limit violations use resource_limit; ownership,
checkpoint, and revision races use stable conflict codes. Backend exception
names and SQL text are not part of the public error contract.

## Operational checks

`ElixirDB.Diagnostics.runtime/0` reports the Elixir, OTP, Exqlite, SQLite,
compile-option, and protocol versions recorded for a release. The maintenance
HTTP endpoint `/v1/databases/:uuid/integrity-check` validates the SQLite file.
