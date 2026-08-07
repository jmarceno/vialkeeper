# Elixir Replicated Document Database

## Version 1 Technical Specification

**Status:** Implementation-ready Version 1 specification
**Implementation language:** Elixir
**Storage engine:** SQLite through Exqlite, isolated behind a storage adapter
**Storage abstraction:** Engine-neutral domain contracts with a Version 1 SQLite adapter
**Public data model:** Revisioned JSON documents
**Public transport:** HTTP with JSON payloads
**Replication model:** CouchDB-inspired, independently implemented
**Database portability:** One self-contained file while cleanly offline
**Compatibility target:** No CouchDB or PouchDB compatibility commitment

---

# 1. Purpose

This specification defines a document database implemented as a stateless Elixir server that manages independently portable SQLite database files.

The system SHALL provide:

* JSON document storage.
* Document creation, retrieval, replacement, and deletion.
* Deterministic immutable document revisions.
* Conflict detection and preservation.
* A durable changes feed.
* Incremental replication between database instances.
* Continuous and one-shot replication.
* Indexed structured JSON queries and full-text search.
* Database-specific configuration stored inside each database file.
* Replication jobs and checkpoints stored inside the relevant database files.
* One canonical file per logical database while the database is cleanly offline.
* Normal operating-system copy, move, rename, backup, and restore of closed database files.
* File-based host configuration co-located with the database root.
* Bearer-token authentication for the HTTP API.
* Transport-layer encryption for non-loopback access.

The implementation SHALL prioritize:

1. Correctness.
2. Durability.
3. Predictable behavior.
4. Recoverability.
5. Implementation simplicity.
6. Performance optimization.

Version 1 SHALL NOT attempt to implement a new physical storage engine.

---

# 2. Normative terminology

The terms **MUST**, **MUST NOT**, **SHALL**, **SHALL NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** define binding requirements.

Every normative requirement identified by an ID such as `ARCH-001` or `REPL-004` MUST have corresponding automated validation before Version 1 is released.

---

# 3. Primary design constraints

## `DESIGN-001` — Elixir server

The database server MUST be implemented in Elixir.

Elixir processes MAY hold transient runtime state, including:

* Open database connections.
* Prepared statements.
* Process registries.
* Request queues.
* Replication workers.
* Backoff timers.
* Parsed configuration.
* Query plans.

No transient process state may be required to reconstruct authoritative database state after a restart.

## `DESIGN-002` — Stateless server

The server SHALL be considered stateless when:

* All document data resides in database files.
* All revision data resides in database files.
* All database-specific configuration resides in database files.
* All persistent replication jobs reside in database files.
* All replication checkpoints reside in database files.
* All logical index definitions reside in database files.
* Runtime caches and processes can be discarded and recreated.
* No central server database is required to restore or interpret an individual database file.

The server MAY maintain a host-local registration manifest mapping managed database UUIDs to paths. This manifest is routing metadata only: it MUST be reconstructible by re-registering database files and MUST NOT contain document, revision, query-index, or replication-job state.

Version 1 host-level configuration is limited to:

* Database root location.
* Registration manifest location, when not derived from the database root.
* Network listeners.
* Authentication tokens and authentication enablement.
* Transport-layer encryption certificates and enablement.
* Logging.
* Observability export endpoint and sampling (see Section 20.5).
* Resource ceilings.
* Retention and compaction resource bounds.
* Shutdown timeout.

## `DESIGN-003` — Document API only

Clients MUST NOT submit SQL.

The public data interface SHALL consist of document-oriented operations:

* Put.
* Get.
* Delete.
* Bulk write.
* Bulk get.
* Changes.
* Query.
* Index management.
* Replication management.

SQLite SHALL remain an internal implementation detail.

## `DESIGN-004` — Replication-first architecture

Replication MUST be part of the initial data model.

Revision identifiers, conflicts, deletion semantics, changes sequencing, checkpoints, and internal storage MUST be designed for replication before public CRUD behavior is finalized.

Replication MUST NOT be added later as an external synchronization layer over ordinary mutable records.

## `DESIGN-005` — Correctness before optimization

Version 1 SHALL prefer simple serialized database access and complete revision bodies over more complex optimizations.

The following SHALL NOT block Version 1:

* Maximum read concurrency.
* Revision delta compression.
* Binary transport.
* Specialized blob storage.
* Advanced distributed ownership.
* Full CouchDB query compatibility.

## `DESIGN-006` — Replicated and local database state

Version 1 replication MUST transfer only document revision state:

* Document IDs.
* Revision IDs and complete revision bodies.
* Revision ancestry.
* Tombstones.
* Conflict branches.

The following state MUST remain local to each database file and MUST NOT be transferred by the replication protocol:

* Database UUID and local update sequence.
* Database configuration.
* Structured and full-text index definitions.
* Derived structured and full-text index data.
* Replication job definitions.
* Replication checkpoints and histories.
* Maintenance metadata.

Ordinary operating-system copying of an offline database file preserves both replicated and local state. Protocol replication preserves only document revision state.

## `DESIGN-007` — OTP release packaging

Version 1 production and staging hosts MUST run ElixirDB as an assembled OTP
release produced by the project’s Mix release pipeline (`MIX_ENV=prod mix
release` / `mix release.build`). The release artifact includes the BEAM
applications and a pinned ERTS suitable for the build OS/ABI.

Mix project commands (`mix run`, `mix test`, and related tasks) are permitted
only for local development and continuous integration. They MUST NOT be the
production process entrypoint.

The release pipeline MUST record release metadata consumed by
`ElixirDB.Diagnostics.runtime/0`, including application version, Elixir
version, OTP version, Exqlite version, and SQLite runtime version and compile
options. Release metadata MUST NOT depend on VCS state.

Operational start, stop, remote console, and evaluation MUST use the release
scripts under `bin/elixir_db` (`start`, `daemon`, `stop`, `remote`, `eval`,
`pid`). Host configuration for the release is supplied through the host
configuration file evaluated at startup (see `CONFIG-001`); the
`ELIXIR_DB_ROOT` environment variable locates the database root that contains
this file.

---

# 4. System architecture

```text
Client
  │
  │ HTTPS/JSON (TLS for non-loopback access)
  │ Authorization: Bearer <token>
  ▼
Elixir Server
  │
  ├── terminates TLS when enabled
  ├── authenticates the bearer token
  ├── validates protocol input and resource limits
  ├── resolves the logical database
  ├── locates or starts its DatabaseOwner process
  ├── validates document and query operations
  ├── starts and supervises replication workers
  └── returns project-owned protocol responses
        │
        ▼
One SQLite file per logical database
```

## 4.1 Supervision structure

The server SHOULD use the following logical supervision structure:

```text
ApplicationSupervisor
├── DatabaseCatalog
├── DatabaseRegistry
├── DatabaseSupervisor
│   └── DatabaseRuntimeSupervisor
│       ├── DatabaseOwner
│       └── ChangeNotifier
├── ReplicationSupervisor
│   └── ReplicationWorker
└── HTTPSserver (TLS termination + bearer-token authentication)
```

The exact module names are implementation details.

## `ARCH-001` — Database owner process

Each open database MUST have exactly one `DatabaseOwner` process.

The owner process MUST:

* Own the SQLite connection.
* Own all prepared statements associated with that connection.
* Serialize all database operations.
* Apply database pragmas.
* Execute transactions.
* Allocate local update sequences.
* Coordinate shutdown.
* Coordinate replication writes.
* Coordinate maintenance operations.

## `ARCH-002` — One connection per open database

Version 1 MUST use one SQLite connection per open logical database.

All reads and writes for one database SHALL be serialized through its owner process.

This is an intentional Version 1 simplification.

It provides:

* Deterministic ordering.
* No connection-level write contention.
* Straightforward transaction ownership.
* Straightforward prepared-statement ownership.
* Simple shutdown and portability guarantees.
* Concurrency across separate databases through separate Elixir processes.

Parallel readers for the same database are deferred until measurements demonstrate that they are required.

## `ARCH-003` — Explicit registration and lazy opening

The server MUST manage an explicit set of registered database files.

The server MUST NOT recursively scan arbitrary files and automatically adopt every recognized database it finds.

The server SHALL:

1. Register databases created through the public API automatically.
2. Require an explicit registration operation for database files copied or placed into the database root.
3. Store only the database path and expected UUID in the host-local registration manifest.
4. Inspect registered databases during startup only as required to validate identity and resume enabled continuous replication jobs.
5. Start a `DatabaseOwner` when the database is accessed or requires an active replication job.
6. Close idle databases according to a configured host policy.
7. Recreate the owner process when the database is accessed again.

Loss of the registration manifest MUST NOT make a database file unreadable or incomplete. The file SHALL become available again after explicit re-registration.

## `ARCH-004` — Single active owner

The server MUST prevent two owner processes from opening the same logical database concurrently.

Before opening the storage adapter, the server MUST acquire an exclusive cross-process ownership lease for the canonical database path. Failure to acquire the lease MUST return `database_in_use` without opening the database.

The lease mechanism MAY use transient lock files or operating-system advisory locking. Lease artifacts are not authoritative database state and MUST be removable after verifying that no owner process remains.

Two registered files with the same internal database UUID MUST be treated as a duplicate-identity error.

The server MUST NOT silently allow both to participate in replication.

## `ARCH-005` — Storage adapter boundary

All document, revision, changes, replication, and query domain code MUST access persistence through an engine-neutral storage adapter contract.

SQLite-specific concerns MUST remain inside the Version 1 SQLite adapter, including:

* Exqlite connections and prepared statements.
* SQL statements and table names.
* SQLite transaction primitives.
* Pragmas.
* SQLite JSON functions and JSONB.
* Expression indexes.
* FTS5 virtual tables.
* SQLite error codes.

SQLite row identifiers, SQL fragments, JSONB blobs, and virtual-table details MUST NOT cross the adapter boundary.

The storage adapter MUST expose project-owned operations for:

* Database lifecycle and metadata.
* Atomic transactions.
* Document and revision persistence.
* Changes-feed reads and writes.
* Local records and checkpoints.
* Logical structured indexes.
* Logical full-text indexes.
* Query execution.
* Integrity verification.
* Compact retention and retention maintenance state.

A storage-adapter conformance suite MUST define the observable behavior required from any future engine implementation.

## `ARCH-006` — Bounded admission and backpressure

Requests MUST NOT be allowed to accumulate without a bound in a database owner mailbox.

Each database MUST have a bounded admission queue in front of its owner process. When the queue is full, new operations MUST fail with the retryable `database_overloaded` error.

Replication workers, HTTP requests, maintenance operations, and index operations MUST use the same admission mechanism.

## `ARCH-007` — Non-blocking wait operations

Long-poll changes requests, streamed changes requests, replication waits, and heartbeat timers MUST NOT hold the SQLite connection, an open transaction, or the database owner call stack while waiting.

After reading the current sequence, a waiting operation SHALL subscribe to a transient change notifier. A committed document mutation SHALL publish the new sequence after commit. The waiter SHALL then perform another bounded changes read through the owner.

A compact-retention operation that advances the retention floor MUST publish a
maintenance notification even though it allocates no document changes
sequence. Replication and changes waiters MUST re-read the source identity and
retention floor after that notification.

Closing a database MUST terminate its waiters with a retryable `database_closed` event.

## `ARCH-008` — Adapter transaction boundary

Only the database owner MAY hold a storage-adapter handle or start an adapter transaction.

Domain services and replication workers MUST submit storage-neutral commands to the owner. Adapter-private handles, row identifiers, SQL values, transaction objects, and native query plans MUST NOT escape the owner or adapter boundary.

## `ARCH-009` — Required adapter capabilities

A Version 1 storage adapter MUST implement all of these capability groups:

* Database create, open, close, identity, and format validation.
* Atomic local mutations and atomic replication imports.
* Winning-document, specific-revision, leaf, and ancestry reads.
* Bounded changes reads and local sequence allocation.
* Missing-revision comparison and complete revision-chain retrieval.
* Compare-and-swap local records for checkpoints.
* Persistent replication-job storage.
* Structured index create, delete, list, rebuild, explain, and query.
* Full-text index create, delete, list, rebuild, explain, and query.
* Domain and physical integrity verification.
* Compact retention: revision-history, conflict-branch, and tombstone trimming with stable-frontier gating.
* Offline single-file portability.

Version 1 MUST fail database opening when the active adapter lacks any required capability. It MUST NOT silently disable query, full-text, replication, or durability behavior.

---

# 5. Physical storage

## `STORE-001` — Version 1 SQLite adapter

The Version 1 storage adapter MUST use SQLite as its authoritative physical storage engine.

SQLite SHALL provide:

* Atomic transactions.
* Crash recovery.
* File-format stability.
* Internal B-tree storage.
* Internal indexes.
* Constraint enforcement.
* JSON extraction.
* Expression indexes.
* FTS5 full-text indexes.

The storage-neutral domain layer SHALL provide:

* Document semantics.
* Revision semantics.
* Conflict handling.
* Query grammar.
* Replication.
* Configuration behavior.
* Public protocol behavior.

Changing the storage engine in a future version MUST NOT require changing public document, revision, replication, or query semantics.

## `STORE-002` — Exqlite

The SQLite adapter MUST access SQLite using Exqlite directly.

Ecto MUST NOT be used inside the core storage adapter.

Direct Exqlite access is required to preserve explicit control over:

* Connection ownership.
* Transaction boundaries.
* Prepared statements.
* Pragmas.
* Internal SQL generation.
* Error mapping.
* Database shutdown.

Exqlite SHALL be pinned to an explicitly tested release rather than a floating version range. The bundled SQLite version and compile options MUST be recorded in the project’s release metadata.

The pinned SQLite build MUST be version 3.45.0 or later and MUST include FTS5. Startup validation MUST verify the runtime version and FTS5 availability before any database is accepted.

## `STORE-003` — Rollback journal mode

Every database connection MUST apply:

```text
PRAGMA journal_mode = DELETE;
PRAGMA synchronous = EXTRA;
PRAGMA foreign_keys = ON;
PRAGMA locking_mode = NORMAL;
PRAGMA trusted_schema = OFF;
```

A bounded `busy_timeout` MAY also be configured, although normal operation should not create internal contention because each database has one owning connection.

WAL mode MUST NOT be used.

In `DELETE` mode, SQLite deletes the rollback journal when a transaction successfully completes. `synchronous=EXTRA` adds a directory synchronization after that journal is removed, providing stronger durability for `DELETE` mode than `FULL` on filesystems where a recently removed journal could otherwise reappear after power loss.

## `STORE-004` — Durable and derived state

All authoritative logical state for a database MUST reside inside its SQLite file, including:

* Database UUID and format versions.
* Documents, revision trees, tombstones, and changes-feed state.
* Database configuration.
* Logical structured and full-text index definitions.
* Replication jobs and checkpoints.
* Maintenance metadata.

SQLite structured indexes, FTS5 tables, and JSONB projections MAY also reside inside the file, but they are derived adapter state. They MUST be rebuildable from authoritative logical state and MUST NOT define document, revision, replication, or public query semantics.

Version 1 MUST NOT require:

* An external index directory.
* An external changes log.
* An external configuration file.
* An external replication database.
* A separate attachment file.
* A server-level catalog required for restoration.

## `STORE-005` — Adapter conformance

The SQLite adapter MUST pass the complete storage-adapter conformance suite.

The suite MUST validate atomicity, revision persistence, changes ordering, checkpoint behavior, structured indexes, full-text indexes, close-and-reopen behavior, and offline single-file portability without relying on SQLite-specific assertions outside adapter tests.

---

# 6. Offline single-file portability

## 6.1 Definition

An **offline-portable database** is a database for which:

* All active transactions have completed or been rolled back.
* The SQLite connection has been closed.
* No process has the database open.
* No hot rollback journal exists.
* No database operation is in progress.

## `FILE-001` — Single canonical file

Each logical database MUST have one canonical database file.

In the offline-portable state, that file MUST be sufficient to restore the complete logical database.

## `FILE-002` — Standard operating-system operations

When the database is offline-portable, the user MUST be able to use ordinary operating-system operations to:

* Copy the file.
* Move the file.
* Rename the file.
* Back up the file.
* Restore the file.
* Place the file in another compatible server installation.

No server command, export procedure, database-specific copy utility, or conversion process may be required.

## `FILE-003` — No mandatory export mode

The server MUST NOT require an export mode for database portability.

The canonical closed database file is the portable artifact.

A future convenience command MAY automate a normal copy, but it MUST NOT establish a separate export format or become required for correctness.

## `FILE-004` — Location independence

Moving or renaming the database file MUST NOT change:

* Database UUID.
* Document IDs.
* Database history epoch.
* Revision IDs.
* Conflicts.
* Local changes sequence.
* Replication checkpoints.
* Replication job definitions.
* Database configuration.
* Logical index definitions.

The external database name MAY be derived from its filename or configured alias, but its logical identity MUST come from the UUID stored inside the file.

## `FILE-005` — Clean shutdown

Normal server shutdown MUST:

1. Stop accepting new operations.
2. Allow or cancel active replication batches.
3. Complete or roll back active transactions.
4. Finalize prepared statements.
5. Close every database connection.
6. Confirm that no active rollback journal remains.
7. Mark the database owner as stopped.

No explicit export or checkpoint step is required.

## `FILE-006` — Abnormal shutdown boundary

The one-file portability guarantee applies after normal database close or successful crash recovery.

If an application, operating system, or machine crash leaves a hot rollback journal, the main database file and journal MUST remain together until SQLite performs recovery.

Copying, moving, renaming, or deleting only the main file while a hot journal is present is unsupported because the journal may contain information required to restore the database to a consistent state.

After SQLite has recovered and closed the database, normal one-file portability applies again.

## `FILE-007` — No live-copy guarantee

Version 1 does not guarantee that copying a database while it is open or actively processing operations produces a valid transactional snapshot.

Live-copy support is not required.

---

# 7. Database identity and registration

## `LIFE-001` — Database UUID

Every database MUST have a randomly generated permanent UUID.

The UUID MUST:

* Be created with the database.
* Be stored inside the database.
* Survive renaming and relocation.
* Be used in replication identity.
* Remain independent of the filename.

## `LIFE-002` — Database root

The server MUST operate within a configured database root or an equivalent constrained locator.

Clients MUST NOT submit arbitrary filesystem paths.

Database routing MUST prevent:

* Path traversal.
* Symlink escape.
* Opening unrelated SQLite files.
* Overwriting unrelated files.
* Resolving paths outside the configured root.

## `LIFE-003` — Recognized file format

The database file MUST include:

* A project-specific SQLite `application_id`.
* A physical file-format version.
* A logical schema version.
* A database UUID.
* A database history epoch.
* A revision algorithm version.
* A JSON canonicalization version.
* A protocol compatibility version.

The server MUST reject:

* Ordinary SQLite files not created by this project.
* Unsupported file-format versions.
* Unsupported logical schema versions.
* Invalid or duplicate database identities.

## `LIFE-004` — Registration and startup inspection

The host-local registration manifest MUST define which database files belong to the running server.

The server MUST NOT recursively auto-discover or automatically adopt unregistered files.

On startup, the server SHALL inspect only registered database files. It MUST validate each file’s UUID and history epoch and read enough database state to identify enabled continuous replication jobs. Databases without active work SHOULD remain closed after inspection.

A database copied into the configured root MUST remain inert until it is explicitly registered. Registration MUST validate the file before adding its path and UUID to the manifest.

## `LIFE-005` — Copied databases

A copied database retains its original UUID.

Placing two copies with the same UUID under the same server instance MUST produce a duplicate identity error.

Running divergent copies with the same UUID as independent replication endpoints is unsupported, even when they are hosted by different servers. A copied file represents backup or relocation until an explicit clone operation assigns a new UUID.

Creating an independent logical clone requires an explicit future clone operation that assigns a new UUID and resets replication identity. Copying the file alone is a backup or relocation operation, not a clone.

## `LIFE-006` — Version 1 format handling

Version 1 MUST create and open only the exact Version 1 physical format and logical schema it implements.

It MUST reject any different format or schema version. Version 1 SHALL NOT perform in-place schema or file-format migrations.

## `LIFE-007` — Registration manifest format

The Version 1 registration manifest MUST be a versioned UTF-8 JSON document containing only:

```json
{
  "version": 1,
  "databases": [
    {
      "uuid": "database-uuid",
      "path": "relative/path/to/database-file"
    }
  ]
}
```

Manifest paths MUST be normalized paths relative to the configured database root. Absolute paths, parent traversal, and symlink escape MUST be rejected.

Manifest updates MUST use write-to-temporary-file, file flush, and atomic rename. A failed manifest update MUST leave the previous manifest valid.

A missing file or UUID mismatch MUST mark the registration as `unavailable`; it MUST NOT cause the server to silently remove or replace the entry.

## `LIFE-008` — Public database identity

Public protocol operations MUST address databases by their internal UUID, never by filename or filesystem path.

The registration path is host-local routing metadata. Moving a closed file requires updating or recreating its registration, but it does not change the database UUID.

## `LIFE-009` — Close eligibility

A database may enter the offline-portable state only when it has:

* No admitted or queued operations.
* No active transaction.
* No active index build or maintenance operation.
* No active replication batch.
* No enabled continuous replication worker requiring the database to remain open.
* No registered changes waiter that has not been terminated.

---

# 8. Internal logical schema

The exact SQL schema is private, but it MUST implement the following logical stores.

| Store               | Purpose                                                       |
| ------------------- | ------------------------------------------------------------- |
| `metadata`          | Database identity, versions, sequence counters, configuration |
| `documents`         | Current winning revision and materialized winning body        |
| `revisions`         | Complete immutable revision records and ancestry              |
| `changes`           | Ordered local changes feed                                    |
| `local_records`     | Non-replicated local metadata and replication checkpoints     |
| `replication_jobs`  | Persistent replication definitions                            |
| `index_definitions` | Logical structured and full-text index definitions            |

## `SCHEMA-001` — Metadata

The metadata store MUST contain at least:

* Database UUID.
* Database history epoch.
* File-format version.
* Logical schema version.
* Revision algorithm version.
* Canonicalization version.
* Current local update sequence.
* Retention floor and compaction epoch.
* Database configuration.
* Creation metadata.

## `SCHEMA-002` — Documents

The logical documents store MUST contain:

* Document ID.
* Winning revision ID.
* Winning body as canonical JSON text.
* Winning deletion state.
* Last local update sequence.

The winning body MUST be materialized.

Normal reads and JSON queries MUST NOT require reconstruction through revision deltas.

The SQLite adapter MAY add a stable numeric row key, derived JSONB, or other opaque physical columns. Such columns MUST remain adapter-private and rebuildable where derived.

## `SCHEMA-003` — Revisions

Each revision record MUST contain:

* Document ID.
* History ID.
* Revision ID.
* Generation.
* Parent revision ID or `null`.
* Content digest.
* Deletion state.
* Canonical body.
* Local insertion sequence.

Every retained revision MUST store its complete body.

Compact retention (Section 18) MAY remove entire revision records at or below the stable frontier; a removed revision MUST NOT remain individually retrievable.

Revision delta storage is deferred.

## `SCHEMA-004` — Changes

Each changes record MUST contain:

* Local sequence.
* Document ID.
* Current winning revision at that sequence.
* The physical leaf revision set at that sequence, including each leaf’s history ID and deletion state.
* Winning deletion status.
* Change origin for diagnostics.

The change origin MUST NOT affect revision identity.

## `SCHEMA-005` — Local records

Local records MUST NOT replicate.

They SHALL be used for:

* Replication checkpoints.
* Replication history.
* Replication-peer leases and safe source positions.
* Compaction boundaries for truncated and purged histories.
* Local maintenance state.
* Other explicitly local metadata.

## `SCHEMA-006` — Replication jobs

A persistent replication job MUST contain:

* Job ID.
* Owning local database UUID.
* Direction.
* Counterpart endpoint reference.
* One-shot or continuous mode.
* Enabled state.
* Retry policy.
* Batch configuration.
* Protocol options.
* Last diagnostic state.

Runtime process identifiers, sockets, and timers MUST NOT be stored.

## `SCHEMA-007` — Index definitions

Each logical index definition MUST include:

* Stable logical index ID.
* Human-readable name.
* Index type: `structured` or `full_text`.
* Ordered storage-neutral field definitions.
* Expected field types and sort directions where applicable.
* Storage-neutral tokenization settings where applicable.
* Canonical definition digest.
* Lifecycle state.
* Definition version.

Adapter-private index names, virtual-table names, native options, and rebuild metadata MUST be stored as opaque adapter metadata and MUST NOT be part of the logical index contract.

## `SCHEMA-008` — Replication scope

Only the `documents`, `revisions`, and document-derived `changes` state participate in protocol replication.

Metadata, local records, replication jobs, index definitions, and physical index structures MUST remain local and MUST NOT appear in revision transfer payloads.

---

# 9. JSON document contract

## `JSON-001` — Document form

A document body MUST be a JSON object.

Top-level arrays and scalar values MUST be rejected as document bodies.

## `JSON-002` — JSON profile

Document bodies MUST conform to the I-JSON interoperability profile.

The implementation MUST reject:

* Duplicate object keys.
* Invalid Unicode.
* Non-finite numbers.
* Unsupported numeric values.
* Malformed JSON.
* Excessive nesting.
* Documents exceeding configured size limits.

I-JSON and JSON Canonicalization Scheme provide defined interoperability and deterministic serialization constraints suitable for revision hashing.

## `JSON-003` — Number model

Version 1 JSON numbers MUST use the IEEE 754 binary64 value model required by RFC 8785 canonicalization.

The server MUST reject:

* Values that overflow to infinity.
* Non-zero values that underflow to zero during parsing.
* Integer literals outside the inclusive safe range `-9007199254740991` through `9007199254740991`.

Negative zero SHALL canonicalize to `0`. Revision identity is based on the canonical numeric value, not the original lexical spelling.

## `JSON-004` — Canonical storage

Accepted document bodies MUST be converted to RFC 8785 JSON Canonicalization Scheme form before:

* Revision hashing.
* Storage-neutral integrity comparison.
* Replication transmission.

The authoritative storage-neutral representation MUST be RFC 8785 canonical UTF-8 JSON.

The SQLite adapter MUST retain canonical JSON text for revision verification and replication. It MAY additionally maintain SQLite JSONB as a derived query-acceleration representation.

SQLite JSONB MUST NOT be used in revision identifiers, public protocol payloads, replication payloads, or storage-adapter contracts. Any JSONB representation MUST be rebuildable from canonical JSON and MAY be discarded without data loss.

## `JSON-005` — Metadata separation

The user document body MUST NOT contain protocol-controlled metadata.

The following values SHALL be carried in the document envelope rather than body fields:

* Document ID.
* Revision ID.
* Deletion state.
* Conflict list.
* Local sequence.
* Replication metadata.

The public protocol MAY visually present Couch-style names such as `_id` and `_rev`, but they remain protocol metadata rather than user-controlled body values.

## `JSON-006` — Document size

Every database MUST define a maximum document size.

A host-level maximum MUST cap any less restrictive database-level setting.

Version 1 does not provide a blob or attachment API. Documents MUST NOT be used as an unrestricted large-binary transport.

---

# 10. Document identity

## `DOC-001` — Document ID

Every document MUST have one stable document ID.

A document ID MUST:

* Be valid UTF-8.
* Be non-empty.
* Fit within a configured maximum byte length.
* Exclude the NUL character and Unicode control characters.
* Exclude the reserved `_system/` prefix.
* Remain unchanged for the life of the document.

Document IDs are transported as JSON values rather than URL path segments, so they MAY contain `/`, `?`, `#`, spaces, and other valid Unicode characters.

When the server generates a document ID, it MUST generate a lowercase UUID version 4 string.

Renaming a document means creating a new document and deleting the previous one.

## `DOC-002` — Creation

A document creation request MUST:

* Provide a document ID or request server-generated identity.
* Omit a parent revision.
* Fail when an existing non-deleted document uses the same ID.
* Produce generation `1`.
* Generate a new random history ID for the document history.

Recreating an ID whose winner is deleted MUST always begin a fresh history
with generation `1`, a new history ID, and no parent, even when the old
tombstone remains stored locally or at another peer. The old deleted history
may temporarily coexist with the new root as a separate history until
compact retention removes it. A fresh history ID prevents a new generation-1
revision from colliding with an old generation-1 revision that is still
retained by another peer.

---

# 11. Revision model

## `REV-001` — Immutable revisions

Every successful document mutation MUST create an immutable revision.

A revision MUST never be modified after creation.

## `REV-002` — Revision identifier

A Version 1 revision ID SHALL use:

```text
<generation>-<lowercase-sha256-hex>
```

The digest input SHALL be the RFC 8785 canonical representation of:

```json
{
  "version": 1,
  "document_id": "document-id",
  "history_id": "history-id",
  "parent_revision": "parent-revision-or-null",
  "deleted": false,
  "body": {}
}
```

For a tombstone revision, `deleted` MUST be `true` and `body` MUST be JSON `null`. A deleted revision MUST NOT carry a user document body.

The generation SHALL be:

* `1` when no parent exists.
* Parent generation plus one otherwise.

The digest MUST NOT include:

* Timestamp.
* Local update sequence.
* Database UUID.
* Server identity.
* Request ID.
* Replication job ID.
* Replication direction.

The history ID MUST be generated once for an initial or fresh-history root and
MUST be inherited unchanged by every descendant revision.

## `REV-003` — Revision verification

When a revision is received through replication, the target MUST recalculate and validate its revision digest.

If an existing document ID and revision ID are received with different history ID, content, ancestry, or deletion state, the operation MUST fail as an integrity violation.

## `REV-004` — Local updates

A normal local replacement MUST provide the expected parent revision.

When the supplied revision is not the current winning revision, the request MUST fail with a conflict response.

A recreation whose current winner is deleted MUST use the winning tombstone as
an expected-state precondition, but its new revision is a parentless
generation-1 root with a new history ID (`DOC-002`).

Normal CRUD operations MUST NOT silently create sibling conflict branches.

## `REV-005` — Replicated updates

Replication MUST be able to insert revisions while preserving:

* Revision ID.
* History ID.
* Generation.
* Parent relationship.
* Deletion state.
* Body.

Replication MAY create sibling branches.

## `REV-006` — Revision tree

All revisions belonging to one history of a document form a revision tree.

Revisions with different history IDs do not share ancestry. A document MAY
temporarily contain more than one history root when a deleted history is still
retained at one peer or has been purged at another. Such roots form a history
forest until the old history is compacted or discarded; they participate in
the same deterministic winner selection.

A revision with no known child is a physical leaf revision. A physical leaf whose deletion state is false is a live leaf revision.

A document has an active conflict when it has more than one live leaf revision. Deleted physical leaves remain part of revision history but do not count as active conflicts.

A revision whose parent revision record has been removed by compact retention is a truncated revision. A truncated revision keeps its original revision ID and parent reference, remains a physical leaf unless it has a stored child, and participates normally in winner selection and conflict counting.

A document history whose entire tree has been removed by compact retention no
longer exists on that database; a later creation of the same document ID
begins a fresh history (`DOC-002`). A compacted history boundary MUST record
enough history ID and generation information to reject its old revisions if a
peer sends them after compaction. The boundary contains no revision body.

## `REV-007` — Conflict preservation

All physical leaves MUST remain stored until removed by compact retention (`MAINT-005`, `MAINT-007`).

Selecting a winner MUST NOT destroy or overwrite losing branches.

A normal document read SHALL return:

* The winning revision.
* The winning body.
* Active conflict revision identifiers when explicitly requested.

Specific losing and deleted revisions MUST remain individually retrievable until removed by compact retention (`MAINT-005`); a removed revision MUST return `revision_not_found`.

## `REV-008` — Winning revision

The winning revision MUST be selected deterministically using this order:

1. A non-deleted leaf outranks a deleted leaf.
2. A higher generation outranks a lower generation.
3. Equal-generation leaves are ordered by digest.
4. The lexicographically greatest digest wins.

The result MUST be independent of replication direction and arrival order.

This follows the central Couch-style conflict model: conflicting branches are retained until compact retention removes them, and all peers deterministically select the same visible winner.

## `REV-009` — Deletion

Deleting a document MUST create a new tombstone revision.

Deletion MUST NOT physically remove the revision tree.

A tombstone MUST:

* Reference its parent revision.
* Have a deterministic revision ID.
* Appear in the changes feed.
* Replicate normally.
* Participate in winner selection.
* Remain stored in Version 1 until removed by stable-frontier compact retention (`MAINT-007`).

Deleting only the winning live leaf while other live conflict leaves exist MAY cause another live leaf to become the winner. Removing all live branches requires the explicit conflict-resolution operation.

## `REV-010` — Explicit conflict resolution

Version 1 MUST provide one atomic conflict-resolution operation.

The request MUST contain:

* Document ID.
* The exact set of expected current live leaf revision IDs.
* A chosen live parent revision when producing a surviving document body.
* Either a replacement body or an instruction to delete all live branches.

When producing a surviving document body, the transaction MUST:

1. Verify that the expected live-leaf set exactly matches current state.
2. Create one normal child revision from the chosen live parent using the replacement body.
3. Create one tombstone child for every other live leaf.
4. Recalculate the winner and materialized document.
5. Append one local changes entry for the resulting document state.

When deleting all live branches, the transaction MUST create a tombstone child for every current live leaf and append one local changes entry.

A stale or incomplete expected live-leaf set MUST fail with `revision_conflict` and create no revisions.

---

# 12. Transaction invariants

## `TX-001` — Atomic logical mutation

A document mutation MUST atomically perform all applicable steps:

```text
validate request
→ canonicalize body
→ calculate revision ID
→ insert revision
→ update revision-tree state
→ calculate winner
→ update materialized winning document
→ update structured and full-text indexes
→ allocate local sequence
→ append changes record
→ commit
```

If any step fails, none of the mutation may become visible.

## `TX-002` — Acknowledgment

The server MUST NOT acknowledge a successful mutation until SQLite reports a successful transaction commit.

## `TX-003` — Index consistency

All structured-index and full-text-index updates for the materialized winning document MUST occur in the same transaction as the winning-document update.

A committed state MUST NOT expose an index entry that refers to an uncommitted document state.

## `TX-004` — Bulk writes

A Version 1 bulk-write request MUST be atomic as a complete request.

If one operation fails validation or integrity checks, the entire batch MUST roll back.

Bulk request size MUST be bounded.

## `TX-005` — Local sequence cardinality

A transaction MUST allocate at most one local changes sequence for each affected document, regardless of how many revisions for that document are inserted by the transaction.

A replication import that inserts several ancestors or conflict branches for one document MUST publish one changes entry describing the final committed leaf set and winner.

Configuration, index-definition, replication-job, checkpoint, and maintenance changes MUST NOT allocate document changes sequences.

## `TX-006` — Idempotent mutation retries

For a normal put or delete, the server MUST calculate the candidate revision before rejecting a stale parent.

If the exact candidate revision already exists with identical ancestry and content:

* The server MUST return success when that revision is still the current winner.
* The server MUST return `revision_conflict` with `operation_already_committed: true` when the revision exists but a later or conflicting revision has changed the current state.

For conflict resolution, a retry MUST return success with `replayed: true` when every revision the request would create already exists identically and the resulting live-leaf set is still current. A partially matching replay MUST fail with `revision_conflict` or `integrity_violation`; it MUST NOT create only the missing subset.

An existing revision ID with different ancestry, deletion state, or body MUST return `integrity_violation`.

---

# 13. Changes feed

## `CHANGE-001` — Local sequence

Every committed logical document change MUST receive a monotonically increasing local sequence.

A sequence is meaningful only within its originating database.

It MUST NOT be:

* Included in a revision ID.
* Compared across databases.
* Reused after allocation.
* Imported as the target’s sequence during replication.

## `CHANGE-002` — Replication visibility

A changes record MUST expose every physical leaf revision present for the changed document at that sequence, including deleted leaves, so a replication worker can transfer all conflict and tombstone branches.

The leaf set is a snapshot of the document state at the entry's sequence. It
is not rewritten after compact retention. A peer that cannot start from the
entry's sequence MUST bootstrap from a current snapshot instead.

## `CHANGE-003` — Changes operations

The changes interface MUST support:

* Changes after a sequence.
* Bounded batches.
* A terminal sequence for one-shot processing.
* Waiting for new changes.
* Continuous streaming.
* Inclusion of leaf revision identifiers.
* Inclusion of deletion state.

## `CHANGE-004` — Repetition

Consumers MUST tolerate repeated change entries.

Replication processing MUST be idempotent.

Changes-feed repetition and checkpoint restart behavior are normal parts of Couch-style replication design.

## `CHANGE-005` — Change entry contract

Every changes entry MUST use the storage-neutral form:

```json
{
  "sequence": 42,
  "document_id": "document-id",
  "winning_revision": "3-digest",
  "deleted": false,
  "leaf_revisions": [
    {
      "revision": "3-digest",
      "history_id": "history-id",
      "deleted": false
    },
    {
      "revision": "2-other-digest",
      "history_id": "history-id",
      "deleted": true
    }
  ]
}
```

`leaf_revisions` MUST be ordered lexicographically by revision ID. `deleted` describes the winning revision. When every physical leaf is deleted, `winning_revision` remains the deterministic winning tombstone and `deleted` is true.

## `CHANGE-006` — Bounded response contract

A bounded changes read MUST return:

```json
{
  "results": [],
  "last_sequence": 42,
  "has_more": false
}
```

`last_sequence` MUST be the last sequence examined, or the supplied `since` value when no entry was returned. When `since` is below the database's retention floor, the read MUST fail with `history_truncated` and include the source database UUID, history epoch, retention floor, and compaction epoch in error details. It MUST NOT silently advance `since`, because doing so could make replication skip a required history boundary. `has_more` MUST indicate whether additional committed document changes existed when the read transaction completed for a non-truncated read.

## `CHANGE-007` — Race-free waiting

A waiting changes operation MUST use this sequence:

1. Read changes after the requested sequence.
2. When no changes are available, subscribe to the change notifier.
3. Re-read the current sequence after subscription.
4. Wait only when the sequence remains unchanged.
5. On notification, perform another bounded changes read.

This order MUST prevent a commit between the initial read and subscription from being missed.

If the requested sequence is below the retention floor, the operation MUST
terminate with `history_truncated` rather than waiting or silently advancing
the requested sequence.

## `CHANGE-008` — Streaming events

A streamed changes response MUST use newline-delimited JSON events with these types:

* `change`: contains one normal changes entry.
* `caught_up`: contains the current sequence after all available entries have been emitted.
* `heartbeat`: contains no database state and keeps the connection active.
* `closed`: indicates that the database was closed or the server is shutting down.
* `error`: contains the normal public error envelope and terminates the stream.

A stream requested below the retention floor MUST emit `error` with
`history_truncated` and terminate without emitting a synthetic catch-up event.

## `CHANGE-009` — Scope

The changes feed MUST contain only committed document revision mutations. Database configuration, index operations, replication jobs, checkpoints, and maintenance operations MUST NOT appear in it.

---

# 14. Query model

## `QUERY-001` — Query language

Version 1 MUST implement a restricted Mango-inspired JSON selector language.

It SHALL NOT claim complete compatibility with:

* CouchDB Mango.
* MongoDB query syntax.
* PouchDB Find.

Mango provides the appropriate model of declarative JSON selectors, separately defined indexes, query execution, and query explanation.

## `QUERY-002` — Query target

Queries MUST operate on:

* The current winning revision.
* Non-deleted documents.
* The materialized canonical document body.

Conflict branches are not included in normal query results.

## `QUERY-003` — Supported values

Version 1 indexed predicates MUST support:

* Null.
* Boolean.
* Number.
* String.

Arrays and objects MAY be returned through projection but SHALL NOT participate in ordered Version 1 indexes.

Missing fields MUST remain distinguishable from fields containing JSON `null`.

## `QUERY-004` — Supported operators

Version 1 MUST support:

* Implicit equality.
* `$eq`.
* `$gt`.
* `$gte`.
* `$lt`.
* `$lte`.
* `$in`.
* `$exists`.
* `$and`.

Every field reference MUST be an RFC 6901 JSON Pointer. The empty pointer referring to the complete document is not valid for an index or predicate.

Comparison operators MUST compare only values of the same JSON scalar type. A missing field does not equal JSON `null`. `$exists` MUST take a Boolean operand. `$in` MUST contain a non-empty bounded array of scalar values.

`$and` MUST contain a non-empty array of selector objects. Multiple field entries in one selector object are also combined with logical AND.

The following are deferred:

* `$or`.
* `$not`.
* `$ne`.
* `$nin`.
* Regular expressions.
* Array-element matching.
* Element-match operators.
* Geospatial operators.

## `QUERY-005` — Query request

A query request MAY contain:

```json
{
  "selector": {
    "/type": "task",
    "/status": {
      "$in": ["open", "blocked"]
    },
    "/priority": {
      "$gte": 3
    }
  },
  "sort": [
    {
      "path": "/priority",
      "direction": "desc"
    },
    {
      "path": "/created_at",
      "direction": "asc"
    }
  ],
  "fields": [
    "/title",
    "/status",
    "/priority"
  ],
  "limit": 50,
  "bookmark": "opaque-value",
  "index": "tasks-by-status-priority"
}
```

## `QUERY-006` — Logical indexes

Clients SHALL define indexes using JSON.

Example:

```json
{
  "name": "tasks-by-status-priority",
  "type": "structured",
  "fields": [
    {
      "path": "/status",
      "type": "string",
      "direction": "asc"
    },
    {
      "path": "/priority",
      "type": "number",
      "direction": "desc"
    },
    {
      "path": "/created_at",
      "type": "string",
      "direction": "asc"
    }
  ]
}
```

The server MUST:

1. Parse and validate every JSON Pointer.
2. Validate field types and directions.
3. Canonicalize the storage-neutral definition.
4. Generate a stable logical index identifier from its definition digest.
5. Ask the active storage adapter to build the physical index.
6. Store the logical definition and opaque adapter metadata inside the database.
7. Never expose or accept raw SQL or backend index expressions.

Index names MUST be unique within one database. Recreating the same canonical definition under the same name MUST return the existing index idempotently. Reusing a name for a different definition MUST fail with `index_name_conflict`.

## `QUERY-007` — SQLite implementation

Canonical document bodies SHALL be retained as JSON text.

The SQLite adapter MAY maintain a derived JSONB column for the materialized winning body and SHOULD benchmark both representations before selecting the expression-index source. JSONB is an internal SQLite optimization and MUST remain rebuildable from canonical JSON.

The query subsystem SHOULD use SQLite JSON functions and expression indexes over the selected winning-body representation.

SQLite supports JSON extraction from text and indexes over deterministic expressions, making this query model implementable without a separate query engine.

Generated expressions MUST include JSON type checks where required to prevent incorrect comparison between different JSON types.

## `QUERY-008` — Internal SQL generation

All query values MUST use bound parameters.

Client-supplied field paths MUST pass a strict parser.

Raw client text MUST NOT be concatenated directly into SQL expressions or identifiers.

Internal index names MUST be generated from validated canonical definitions rather than user-provided names.

## `QUERY-009` — Index selection

The planner MUST choose indexes deterministically.

Selection priority SHALL be:

1. A valid explicitly requested index.
2. Longest equality-compatible field prefix.
3. Compatible range field.
4. Compatible sort fields.
5. Stable logical index ID as final tie-breaker.

When an explicitly requested index is missing or incompatible, the query MUST fail with `invalid_index_hint`; the planner MUST NOT silently choose a different index.

## `QUERY-010` — Sort

After leading equality-constrained index fields are removed, the query sort MUST match a prefix of the remaining index fields in either:

* The declared direction of every used field; or
* The exact inverse direction of every used field.

Mixed partial reversal is not supported.

The document ID MUST be used as the final deterministic tie-breaker.

## `QUERY-011` — Full scans

A query without a usable index MAY execute only when the number of candidate documents is below the configured scan threshold.

Above that threshold, the server MUST return `index_required`.

The server MUST NOT unpredictably perform unbounded full-database scans.

## `QUERY-012` — Pagination

Primary pagination MUST use opaque bookmarks.

A bookmark MUST bind to:

* Query fingerprint.
* Selected logical index ID and definition digest.
* Database local update sequence captured for the first page.
* Last storage-neutral ordering key or adapter-private full-text cursor.
* Last document ID.
* Sort direction.
* Protocol version.

Changing the selector, sort, projection, or index MUST invalidate the bookmark.

When the database local update sequence differs from the sequence encoded in the bookmark, the server MUST return `bookmark_stale`. Version 1 does not provide a multi-request snapshot that survives intervening document mutations.

Bookmarks MUST be self-contained; the server MUST NOT retain per-bookmark cursor state. The outer bookmark SHALL be base64url-encoded canonical JSON containing:

* Bookmark format version.
* Protocol major version.
* Query fingerprint.
* Logical index ID and definition digest.
* Database local update sequence.
* Sort direction.
* Last document ID.
* A storage-neutral ordering key or opaque adapter cursor.
* SHA-256 checksum of the canonical bookmark payload excluding the checksum field.

The adapter cursor MUST be treated as an opaque JSON value by the public protocol layer. A bookmark is not encrypted or authenticated and MUST be fully validated before use.

## `QUERY-013` — Query explanation

Every query MUST support an explanation operation reporting:

* Selected index.
* Candidate indexes.
* Rejected-index reasons.
* Full-scan status.
* Sort compatibility.
* Selector constraints.
* Expected pagination strategy.
* Examined document and index-entry counts when available.

## `QUERY-014` — Version 1 full-text search

Version 1 MUST provide full-text search through logical full-text indexes.

The SQLite adapter MUST implement full-text indexes with FTS5. The server MUST fail startup validation when the pinned SQLite runtime does not provide FTS5 rather than silently degrading search behavior.

## `QUERY-015` — Full-text index definition

A full-text index MUST be defined through a storage-neutral JSON contract.

Example:

```json
{
  "name": "notes-full-text",
  "type": "full_text",
  "fields": ["/title", "/body"],
  "tokenization": {
    "strategy": "unicode_words_v1",
    "diacritics": "remove"
  }
}
```

Version 1 MUST support exactly one word-tokenization strategy: `unicode_words_v1`.

`unicode_words_v1` MUST have these storage-neutral semantics:

* Unicode character classification is based on Unicode 6.1.
* Characters in general categories `L*`, `N*`, and `Co` are token characters.
* All other characters are separators.
* Each contiguous run of token characters forms one token.
* Token matching is case-insensitive according to Unicode 6.1 case-folding rules.
* `diacritics: "preserve"` preserves Latin-script diacritics.
* `diacritics: "remove"` removes Latin-script diacritics, including characters represented by one code point with multiple diacritics.
* One or more string field paths may participate in an index.
* Relevance ordering is adapter-provided, with deterministic document-ID tie-breaking.

Storage-engine tokenizer names and native options MUST NOT appear in public contracts, persisted logical index definitions, or storage-adapter interfaces.

Every storage adapter MUST implement the exact `unicode_words_v1` behavior and pass shared tokenization conformance fixtures. The SQLite adapter SHALL map it to FTS5 `unicode61` using the default categories `L* N* Co`, `remove_diacritics 0` for `preserve`, and `remove_diacritics 2` for `remove`.

Arrays, objects, case-sensitive word search, prefix-query syntax, custom tokenization strategies, stemming, synonyms, and language-specific analyzers are deferred.

## `QUERY-016` — FTS5 physical representation

The SQLite adapter SHOULD use one contentless-delete FTS5 virtual table per logical full-text index.

The FTS5 row ID MUST map to a stable internal numeric document key maintained by the SQLite adapter. Search results SHALL join back to the materialized winning-document store through that key.

Full-text index entries MUST represent only current non-deleted winning revisions. A transaction that changes the winning revision MUST update every affected full-text index before commit.

FTS5 tables are derived index state. They MUST remain inside the canonical database file and MUST be rebuildable from materialized winning documents and logical index definitions.

## `QUERY-017` — Full-text query contract

Clients MUST use a project-owned search contract rather than raw FTS5 `MATCH` syntax.

Example:

```json
{
  "search": {
    "index": "notes-full-text",
    "text": "replication checkpoint",
    "mode": "all"
  },
  "selector": {
    "/type": "note"
  },
  "limit": 50,
  "bookmark": "opaque-value"
}
```

Version 1 search modes MUST include:

* `all`: every parsed term must match.
* `any`: at least one parsed term must match.
* `phrase`: the normalized phrase must match.

The storage adapter MUST escape and compile the project-owned search request into its engine-specific query representation. The SQLite adapter SHALL compile it into FTS5 query syntax. Raw storage-engine operators, backend-specific column filters, SQL fragments, and FTS5 syntax MUST NOT be accepted from clients.

Search results MUST be ordered by the active storage adapter’s relevance ranking unless an explicitly supported order is requested. Exact score values and relative relevance ordering are adapter-specific and are not guaranteed to remain identical across different storage engines. For the same adapter version, index definition, query, and database state, result ordering MUST be deterministic. Equal-ranking results MUST be ordered by document ID.

A full-text search MAY be combined with a structured selector. Both predicates MUST apply to the same current winning document.

## `QUERY-018` — Full-text integrity

The server MUST provide a rebuild operation for each full-text index.

Integrity validation MUST detect missing, stale, and extra full-text entries by comparing index state with current winning documents.

Full-text index creation, winner changes, tombstones, conflict winner changes, and index deletion MUST be covered by transactional tests.

## `QUERY-019` — Index lifecycle

Version 1 index creation, deletion, and rebuild operations MUST be synchronous, serialized maintenance operations.

While such an operation runs, later database operations remain queued subject to the normal bounded admission limit; they do not execute concurrently through another connection.

An index MUST become visible only after its complete physical structure and logical definition commit atomically. On failure, the previous committed index state MUST remain usable and no partially built index may be selected.

## `QUERY-020` — Query response consistency

Each query request MUST run against one committed database state and return only current non-deleted winning revisions from that state.

When no projection is requested, each query result MUST contain the full document envelope.

When `fields` is supplied, each query result MUST contain `id`, `revision`, and a `fields` object keyed by the requested JSON Pointers:

```json
{
  "id": "document-id",
  "revision": "3-digest",
  "fields": {
    "/title": "Example",
    "/status": "open"
  }
}
```

A missing field MUST be omitted from the `fields` object. It MUST NOT be converted to JSON `null`. JSON object member order is not part of the contract.

## `QUERY-021` — Index extraction behavior

Document writes MUST NOT fail merely because an indexed path is missing or contains a value of a different type from the logical index definition.

For a structured index:

* A document contributes an index entry only when every required indexed path exists and matches its declared scalar type.
* A non-matching document remains stored and may still be found through another compatible index or a permitted bounded scan.
* The planner MUST NOT use such an index for an `$exists` predicate when doing so would omit existing values of other types.

For a full-text index:

* Missing and JSON `null` fields contribute no text.
* String fields contribute their string value.
* Boolean, number, array, and object values contribute no text.
* A document with no contributing text MAY be absent from the physical full-text index.

These extraction rules are storage-neutral and MUST be covered by the adapter conformance suite.

---

# 15. Replication model

## `REPL-001` — Independent protocol

The project MUST implement its own replication protocol based on CouchDB’s replication model.

It MUST NOT claim wire compatibility with CouchDB or PouchDB.

## `REPL-002` — Replication objective

Replication SHALL transfer missing revisions while preserving:

* Document IDs.
* History IDs.
* Revision IDs.
* Revision ancestry.
* Tombstones.
* Conflicting leaves.
* Deterministic winner selection.

Replication MUST NOT overwrite or discard a branch merely because another branch currently wins.

## `REPL-003` — Convergence

When writes stop and successful bidirectional replication completes, both databases MUST:

* Select the same winning revision and materialized body for every document history visible to both peers.
* Preserve every current live leaf whose history has not been compacted at either endpoint.
* Permit one endpoint to retain additional historical revisions, deleted leaves, or old history roots that the other endpoint has compacted, provided the difference is covered by a recorded retention floor and history boundary.
* Never reintroduce a revision or history that the receiving endpoint has already retired by compact retention.

Active peers whose leases have not expired MUST be able to complete incremental
replication without losing a committed change. A peer whose source checkpoint
is below the source retention floor, whose history epoch differs, or whose
lease has expired MUST bootstrap from a current snapshot; transparent
multi-master rejoin is not promised after that boundary.

Local sequences and local checkpoint records may differ.

## `REPL-004` — Required primitives

The internal replication interface MUST provide equivalents of:

* Database identity and current sequence.
* Changes after sequence.
* Retention floor, history epoch, and compaction epoch.
* Paginated compact-history-boundary retrieval.
* Missing-revision comparison.
* Bulk revision retrieval.
* Current-state snapshot/bootstrap retrieval.
* Revision insertion with preserved identifiers.
* Local checkpoint read.
* Local checkpoint write.
* Durable target commit confirmation.

These are the essential stages of the CouchDB replication protocol: read changes, determine missing revisions, retrieve them, insert them with their existing history, and checkpoint progress.

## `REPL-005` — Source, target, and endpoint references

A persistent replication job MUST be owned by one local database and MUST be unidirectional.

Its direction MUST be either:

* `push`: the owning local database is the source.
* `pull`: the owning local database is the target.

Bidirectional replication requires two independent jobs.

An endpoint reference MUST use one of these storage-neutral forms:

```json
{
  "kind": "local",
  "database_uuid": "database-uuid"
}
```

```json
{
  "kind": "remote",
  "base_url": "http://host:port",
  "database_uuid": "database-uuid",
  "auth_token": "bearer-token"
}
```

The expected remote database UUID MUST be stored and verified during every replication handshake. Changing a remote `base_url` without changing the expected database UUID MUST NOT create a new replication identity.

A remote `base_url` MUST use `http` or `https`, MUST NOT contain embedded credentials, and MUST NOT contain a query string or fragment. It identifies the server base before the Version 1 `/v1` paths.

A remote endpoint reference to a target with authentication enabled MUST carry an `auth_token` string. The source worker MUST present that value as the `Authorization: Bearer` credential on every replication wire call to that target (`AUTH-003`). A remote endpoint reference to an unauthenticated target MUST omit `auth_token`. The field holds a raw bearer token, not a digest; it MUST be stored as local state in the owning database (see `REPL-013`) and MUST NOT participate in replication. A local endpoint reference MUST omit `auth_token`.

## `REPL-006` — Replication ID

A replication ID MUST be a SHA-256 digest over a canonical structure containing:

* Source database UUID.
* Target database UUID.
* Direction.
* One-shot or continuous mode.
* Replication protocol major version.
* Filter definition and version when filters are implemented.

Network addresses, worker identity, and retry counts MUST NOT affect replication identity.

## `REPL-007` — Checkpoints

Checkpoints MUST be stored as non-replicating local records on both source and target.

A checkpoint MUST use this storage-neutral form:

```json
{
  "version": 1,
  "replication_id": "sha256-digest",
  "checkpoint_version": 7,
  "session_id": "session-uuid",
  "source_history_epoch": "history-epoch",
  "source_compaction_epoch": 3,
  "source_sequence": 42,
  "safe_source_sequence": 42,
  "installed_source_compaction_epoch": 3,
  "history": [
    {
      "session_id": "session-uuid",
      "source_history_epoch": "history-epoch",
      "source_sequence": 42,
      "documents_read": 10,
      "revisions_written": 8,
      "completed_at": "RFC3339-timestamp"
    }
  ]
}
```

Checkpoint history MUST retain at most the ten most recent completed sessions.

`source_history_epoch` and `source_sequence` identify the applied source
position. `source_compaction_epoch` and
`installed_source_compaction_epoch` identify the retention fence observed by
the target. A newer source compaction epoch is valid for incremental
replication when `source_sequence` is at or above the source retention floor;
the target MUST install that fence before acknowledging it. A checkpoint from
another history epoch MUST NOT be used for incremental replication.

`safe_source_sequence` MUST be no greater than `source_sequence`. It is the
target's durable safe-position report and MAY advance only when the target has
no unacknowledged local mutation whose causal context is earlier than that
position. A normal checkpoint does not by itself advance the stable
compaction frontier.

At startup, the replication worker MUST find the newest session entry shared by
both endpoint histories. When no common entry exists, replication MAY restart
from source sequence `0` only when the source retention floor is zero and the
source history epoch matches. Otherwise replication MUST request a current
snapshot/bootstrap instead of reading a sequence that the source has already
discarded.

Checkpoint writes MUST use compare-and-swap with `checkpoint_version`. A writer MUST supply the version it observed and increment it by exactly one. A stale write MUST fail with the retryable `checkpoint_conflict` error and MUST NOT replace newer progress.

A retry made after a successful write but before its response was received MUST return success when the currently stored checkpoint is byte-for-byte equivalent to the requested replacement, even though the request’s expected version is now one behind. Any other stale expected version MUST return `checkpoint_conflict`.

## `REPL-008` — Batch algorithm

Each replication batch MUST execute:

```text
read source changes
→ collect candidate leaf revision IDs
→ ask target which revisions are missing
→ retrieve missing revisions and required ancestry
→ validate revision digests
→ insert missing revisions at target
→ commit target transaction
→ update target checkpoint
→ update source checkpoint
→ continue
```

## `REPL-009` — Commit ordering

The target document transaction MUST commit before either endpoint advances its checkpoint beyond that batch.

The target MUST NOT advertise a `safe_source_sequence` or
`installed_source_compaction_epoch` beyond the state it has durably committed
and fenced. A source MUST record those reports in the peer ledger only after
the corresponding checkpoint write succeeds.

Checkpoint writes across two databases are not atomic.

A failure between the two checkpoint writes MUST result only in repeated work.

It MUST NOT cause a committed source change to be skipped.

## `REPL-010` — Idempotency

The following operations MUST be idempotent:

* Reading changes.
* Comparing revisions.
* Fetching revision bodies.
* Inserting an existing identical revision.
* Reprocessing a batch.
* Writing an equivalent checkpoint.
* Restarting a replication worker.

## `REPL-011` — One-shot replication

One-shot replication MUST:

1. Read or capture a source terminal sequence and source history epoch.
2. Process changes through that sequence.
3. If the source reports `history_truncated`, switch to the explicit snapshot/bootstrap path.
4. Commit a final checkpoint containing the same source history epoch and compaction epoch.
5. Return a completed status.

Changes committed after the captured terminal sequence belong to a later replication.

## `REPL-012` — Continuous replication

Continuous replication MUST reuse the one-shot batch algorithm.

After reaching the current source sequence, the worker SHALL wait for additional changes and continue.

Before waiting and after every wake-up, the worker MUST re-read the source
history epoch and retention floor. A compaction notification MUST wake the
worker so it can continue from a valid checkpoint or enter the
snapshot/bootstrap path; it MUST NOT remain asleep waiting for a document
sequence that has already crossed a retention boundary.

It MUST support:

* Cancellation.
* Restart.
* Retry.
* Exponential backoff.
* Bounded jitter.
* Network interruption.
* Source restart.
* Target restart.

## `REPL-013` — Persistent jobs

Persistent replication jobs MUST reside in the owning local database and MUST NOT replicate.

Enabled continuous jobs MUST resume after server restart without client traffic.

This requires startup inspection of explicitly registered databases.

Version 1 replication requests and job definitions MUST NOT carry credentials in the URL or in any field other than the endpoint reference's `auth_token`. The `auth_token` is the sole permitted credential and is stored as local, non-replicating job state.

A job MUST expose one of these states:

* `disabled`: persisted but not eligible to run.
* `idle`: enabled and scheduled to start.
* `running`: actively processing a batch.
* `waiting`: continuous job caught up and waiting for source changes.
* `backoff`: paused after a retryable failure.
* `completed`: one-shot job reached its captured terminal sequence.
* `failed`: stopped after a non-retryable failure.

The persisted job definition MUST remain separate from transient runtime state.

## `REPL-014` — Filtering

Version 1 replication MUST replicate the complete document revision state of the source database.

Selective replication, document filters, selector filters, and document-ID subsets are deferred.

## `REPL-015` — Protocol handshake

Before reading changes, a replication worker MUST obtain this identity information from both endpoints:

```json
{
  "database_uuid": "database-uuid",
  "history_epoch": "history-epoch",
  "current_sequence": 42,
  "retention_floor": 17,
  "compaction_epoch": 3,
  "retention_boundary_digest": "sha256-digest",
  "retention_mode": "stable_frontier",
  "replication_protocol_major": 1,
  "revision_algorithm_version": 1,
  "canonicalization_version": 1
}
```

The worker MUST reject:

* A database UUID different from the configured endpoint reference.
* Equal source and target database UUIDs.
* A replication protocol major-version mismatch.
* A revision algorithm mismatch.
* A canonicalization-version mismatch.

A mismatch between a checkpoint's `source_history_epoch` and the current
source history epoch MUST NOT be treated as a protocol incompatibility. It
invalidates the affected incremental checkpoint and requires
snapshot/bootstrap; the source and target databases normally have different
history epochs.

A source or peer history position that regresses relative to the durable
checkpoint or peer ledger MUST require the same bootstrap path. It MUST NOT
renew a lease or accept new revision imports as if the old history continued.

The listed protocol, revision-algorithm, canonicalization, and endpoint
identity failures are non-retryable until configuration or software changes.

## `REPL-016` — Missing-revision and transfer model

Version 1 SHALL use complete root-to-leaf revision chains rather than revision deltas.

When an incremental read returns `history_truncated`, or when the source
history epoch does not match the checkpoint, the target MUST use a bounded,
paginated snapshot/bootstrap transfer containing the current winning state,
all retained physical leaves, history IDs, and the source retention boundary.
It MUST NOT restart from sequence zero against a truncated source. A target
with unacknowledged local mutations MUST preserve them in its local durable
state and require an explicit rebase/export decision before replacing its
replicated state; bootstrap MUST NOT silently discard local writes.

When the source compaction epoch is newer than the target's installed source
compaction epoch, or its boundary digest differs, the target MUST retrieve and
durably install all compact-history-boundary pages before it reports the
source's installed epoch and digest. Boundary pages MUST be idempotent,
resumable, and bound to the source history epoch and boundary digest. A target
MUST NOT acknowledge a compaction epoch merely because it observed the source
identity response.

For each changes batch:

1. The source provides the physical leaf revision IDs and history IDs recorded for every changed document.
2. The target reports which exact leaf revision IDs are absent and which are
   already covered by a local compact-history boundary. A leaf whose chain
   later proves to begin at a retired branch root is also treated as compacted
   stale state rather than imported.
3. For every missing leaf, the source returns the complete ordered chain from the root revision through that leaf. When compact retention has removed ancestors, the source returns the chain beginning at the nearest retained revision and MUST mark it truncated (`MAINT-007`). A chain MUST be marked truncated exactly when any revision's parent record is absent at the source. A chain rooted at a fresh-history revision after purge (`DOC-002`) has parent `null`, a new history ID, and is never truncated. The target MUST accept a truncated chain only when the chain's source history epoch matches the handshake and checkpoint and the chain's history boundary is not retired at the target. A source or target retention boundary MUST NOT be bypassed by replaying an old chain.
4. The target idempotently ignores revisions it already stores and inserts the remaining revisions in parent-before-child order.

Sending complete chains may repeat existing ancestors. This is an accepted Version 1 trade-off for protocol simplicity and integrity. Truncated chains are the compaction counterpart of that trade-off: they stop repeating ancestors at or below the retention boundary at the cost of serving history from the first retained revision.

A transferred revision MUST use:

```json
{
  "document_id": "document-id",
  "history_id": "history-id",
  "revision_id": "3-digest",
  "generation": 3,
  "parent_revision": "2-parent-digest",
  "deleted": false,
  "body": {}
}
```

A tombstone MUST have `deleted: true` and `body: null`.

## `REPL-017` — Target import transaction

A target revision-import request MUST be atomic as a complete bounded request.

The target MUST:

1. Validate chain ordering, generations, parents, canonical bodies, and revision digests.
2. Reject any committed state that would contain a dangling parent reference, except that a chain marked truncated MAY introduce a truncated revision whose parent record is absent (`MAINT-007`, `REV-006`), and a chain rooted at a fresh-history revision with a new history ID and parent `null` MAY coexist with a deleted history already stored at the target (`DOC-002`, `REV-006`). A revision whose history ID and generation are below a target's recorded compaction boundary, or whose chain begins at a retired branch root, MUST be ignored as a compacted stale replay and MUST NOT allocate a new local changes sequence. An import containing only compacted stale revisions MUST succeed as a no-op. Any other dangling parent MUST be rejected atomically.
3. Treat an existing identical revision as a no-op.
4. Reject an existing revision ID with different history ID, content, ancestry, or deletion state as `integrity_violation`.
5. Recalculate every affected document’s physical leaves, live leaves, winner, materialized body, and indexes.
6. Allocate one local changes sequence per affected document.
7. Commit before reporting durable success.

## `REPL-018` — Worker exclusivity and cancellation

A server MUST run at most one active worker for a replication ID.

Starting a duplicate worker MUST fail with `replication_already_running`.

Cancellation MUST occur between bounded transactions. A cancellation MUST NOT interrupt an already committing target transaction or advance a checkpoint beyond committed target state.

Retryable failures SHALL enter `backoff` using exponential delay with bounded jitter. Non-retryable protocol, identity, or integrity failures SHALL enter `failed`.

---

# 16. Server and database configuration

## `CONFIG-001` — Host configuration

Host configuration MUST be supplied as a single TOML file located inside the
configured database root. The `ELIXIR_DB_ROOT` environment variable locates the
database root; the host configuration file MUST reside at
`<database_root>/host.toml`.

The file MAY require host-level configuration for:

* Database root and registration manifest location.
* Listener addresses.
* Authentication tokens and enablement (`AUTH-001`).
* TLS certificates and enablement (`TLS-001`).
* Maximum open databases.
* Maximum concurrent replication workers.
* Absolute document size.
* Absolute request size.
* Logging.
* Observability export configuration (see Section 20.5, `OBSV-001`).
* Retention and compaction resource bounds.
* Shutdown timeout.

A human-edited file MUST satisfy all of the following:

* It MUST NOT be hidden by default. The filename `host.toml` MUST be used.
* It MUST be editable in any plain-text editor without significant risk of
  parse failure from insignificant whitespace, as TOML is not
  whitespace-sensitive for values.
* Comments using `#` MUST be honored.

Configuration precedence SHALL be:

```text
built-in defaults
→ values in <database_root>/host.toml
```

When `host.toml` is absent on startup, the server MUST create it with the
standard fully commented template before reading it, so that the file exists as
a visible, editable artifact from first run. The creation MUST use the same
write-to-temporary-file, flush, and atomic-rename discipline required of the
registration manifest (`LIFE-007`). When `host.toml` is present, the server
MUST read it verbatim and MUST NOT overwrite an existing file under any
circumstance, so that operator edits are preserved across copies and
relocations of the database root.

The server MUST reject `host.toml` with field-level errors: unknown keys,
wrong value types, and out-of-range values MUST each name the offending key.

The compiled built-in defaults and the shipped template MUST hold identical
values; a conformance test MUST assert this so the two cannot drift.

## `CONFIG-001a` — Moveable host configuration

Copying the database root to another host MUST carry host configuration,
authentication tokens, and TLS material along with the database files. The
copy MUST be usable on the destination after pointing `ELIXIR_DB_ROOT` at it;
no further transfer of configuration, tokens, or certificates is permitted.

Certificate and key file paths declared in `host.toml` MUST be relative to the
database root unless absolute, so that a copied root remains self-contained.

## `CONFIG-002` — Database configuration

The logical database configuration object MUST contain:

* Configuration schema version.
* Maximum document and document-ID sizes, bounded by host limits.
* Query scan threshold and result limits, bounded by host limits.
* Changes batch and wait limits, bounded by host limits.
* Default replication batch and retry settings, bounded by host limits.
* Retention and compaction settings, bounded by host limits.

Database identity, format versions, replication jobs, checkpoints, and logical index definitions are stored inside the same database file but are separate state stores, not fields of the configuration object.

## `CONFIG-003` — Client-supplied options

Clients MAY supply ephemeral options for individual operations, including:

* Query limit.
* Changes batch size.
* Replication batch size.
* Request timeout within host bounds.

Ephemeral options MUST NOT become durable configuration unless an explicit configuration operation stores them.

## `CONFIG-004` — Precedence

Configuration precedence at load time is fixed by `CONFIG-001`. Operational
precedence at request time SHALL be:

```text
host resource and safety limits
→ database configuration
→ permitted request-level options
```

A database or client request MUST NOT weaken host-enforced safety limits.

## `CONFIG-005` — Listener safety

The default listener configuration MUST bind to loopback interfaces. Loopback
access with no authentication and no TLS is permitted and is the
out-of-the-box default.

The server MUST refuse to start when the listener binds a non-loopback
interface and neither authentication (`AUTH-001`) nor TLS (`TLS-001`) is
enabled. This failsafe prevents an open server from reaching the network by
accident.

An operator who accepts the risk MAY override the failsafe by setting
`[security] allow_insecure_remote = true` in `host.toml`. The override MUST be
explicit; it MUST NOT be implied by any other configuration.

## `CONFIG-006` — Storage-neutral database configuration

Version 1 database configuration MUST be a versioned JSON object containing only storage-neutral behavior and limits, grouped under:

* `documents`: document-size and identifier limits.
* `queries`: default and maximum result limits and bounded-scan threshold.
* `changes`: default and maximum batch sizes and wait limits.
* `replication`: default batch document count, batch byte limit, and retry policy.
* `retention`: the stable-frontier compact-retention policy defined by `MAINT-006`.

SQLite pragmas, FTS5 options, native index names, JSONB settings, and other adapter-specific tuning MUST NOT appear in public or persisted logical database configuration.

Adapter tuning belongs to adapter release configuration and MUST preserve the logical behavior defined by this specification.

## `CONFIG-007` — Host configuration portability

Host configuration, authentication tokens, and TLS certificate material MUST
reside inside the database root alongside the database files and the
registration manifest. Ordinary operating-system copying of the database root
directory MUST carry all of them to another host with no further setup beyond
pointing `ELIXIR_DB_ROOT` at the copy.

This keeps the single-portability-unit property defined by `DESIGN-006`,
`FILE-002`, and `CONFIG-001a` intact: a database file already carries its own
configuration, indexes, jobs, and checkpoints; the root directory carries
server-level configuration, tokens, and certificates in the same manner.

---

# 16a. Authentication

## `AUTH-001` — Bearer-token authentication

The server MAY authenticate HTTP API access using a single bearer-token
scheme. When `[auth] enabled = true` in `host.toml`, every `/v1` request MUST
present a valid `Authorization: Bearer <token>` header. When authentication is
disabled, the server MUST NOT require credentials.

Version 1 defines exactly one credential model: a single bearer token grants
full access to the HTTP API. Usernames, roles, sessions, cookies, OAuth,
per-database credentials, and fine-grained authorization are not part of
Version 1.

## `AUTH-002` — Token storage and comparison

Tokens MUST be stored in `host.toml` as SHA-256 hexadecimal digests, never as
raw token text. The server MUST compute the SHA-256 digest of the presented
credential and constant-time compare it against each stored digest.

Multiple digests MAY be listed to support rotation. An empty token list with
authentication enabled MUST cause startup to fail.

Token generation MUST be provided by the `bin/elixir_db token` release
command, which MUST print the raw token once and its SHA-256 digest. Token
rotation MUST be documented as an add-new, remove-old procedure over restarts.
Version 1 MUST NOT provide a runtime revocation endpoint; rotation is achieved
by editing `host.toml` and restarting.

## `AUTH-003` — Replication source credentials

A replication source MUST authenticate to a target that has `AUTH-001` enabled
by sending the `Authorization: Bearer` credential declared in the remote
endpoint reference's `auth_token` field (`REPL-005`) on every replication wire
call. The target MUST validate that credential through the same authentication
mechanism as direct client requests.

A pull job whose source requires authentication MUST likewise carry
`auth_token` so the source accepts its read calls.

## `AUTH-004` — Authentication failures

A missing, malformed, or non-matching credential MUST fail with the stable
`unauthorized` error (`API-016`) and HTTP status 401. The error MUST NOT
reveal whether the failure was a missing header, a malformed header, or a
wrong token; the message MUST be identical across all three cases.

## `AUTH-005` — Local-loopback exemption

When the listener is loopback and `[auth] enabled = false`, the server MUST
serve requests without credentials. This is the out-of-the-box default for
local development and in-process embedding.

---

# 16b. Transport-layer security

## `TLS-001` — TLS listener

When `[tls] enabled = true` in `host.toml`, the server MUST serve the public
API over HTTPS on a single listener. Version 1 MUST NOT run a parallel
plaintext listener; the single listener is either HTTP or HTTPS, not both.

TLS termination MUST use the certificate and key referenced by `[tls]
certfile` and `[tls] keyfile`, resolved relative to the database root unless
absolute. Paths MUST be inside the database root; absolute paths escaping the
root MUST be rejected at startup.

## `TLS-002` — TLS material portability

Certificate and key files MUST reside inside the database root so that a copy
of the root carries a working HTTPS listener. Operator responsibility for
certificate validity (subject alternative names, issuer, expiry) across hosts
is out of scope; Version 1 loads whatever PEM files are referenced and
reports load failures through standard error envelopes.

## `TLS-003` — Replication and TLS

A remote endpoint `base_url` using the `https` scheme (`REPL-005`) MUST cause
the source's outbound replication calls to use TLS. The source MUST validate
the target certificate using the Erlang/OTP default CA store; pinning,
custom CA bundles, and certificate mutual-authentication are not part of
Version 1.

---

# 17. Public protocol

## `API-001` — Transport

Version 1 MUST use HTTP with JSON request and response bodies.

Streaming operations MAY use:

* Chunked JSON.
* Newline-delimited JSON.
* HTTP compression.

## `API-002` — Versioning

The public protocol MUST use an explicit major version.

Protocol versioning MUST remain separate from:

* Database schema version.
* Database file-format version.
* Revision algorithm version.

## `API-003` — Required database operations

The protocol MUST expose:

* Create database.
* Register an existing database file.
* Unregister a closed database without deleting its file.
* List registered databases.
* Read database information.
* Read database configuration.
* Update permitted database configuration.
* Close database.
* Run integrity verification.
* Run compact retention.

No export operation is required.

## `API-004` — Required document operations

The protocol MUST expose:

* Get document.
* Get specific revision.
* Put document.
* Delete document.
* List conflicts.
* Bulk get.
* Atomic bulk write.

## `API-005` — Required changes operations

The protocol MUST expose:

* Changes after sequence.
* Bounded changes.
* Long-poll changes.
* Streamed changes.

## `API-006` — Required query operations

The protocol MUST expose:

* Create a structured or full-text index.
* Delete an index.
* List indexes.
* Explain query.
* Execute query.

## `API-007` — Required replication operations

The protocol MUST expose:

* Create replication job.
* Start one-shot replication.
* Start continuous replication.
* Read replication status.
* Disable replication.
* Cancel active replication.
* Delete replication job.

## `API-008` — Response envelopes

Every non-streaming success MUST use:

```json
{
  "request_id": "request-id",
  "data": {}
}
```

Every public failure MUST use:

```json
{
  "request_id": "request-id",
  "error": {
    "code": "stable_error_code",
    "message": "Human-readable message",
    "retryable": false,
    "details": {}
  }
}
```

The server MAY accept a caller-provided `X-Request-ID` header after validating its size and characters. Otherwise it MUST generate a request ID.

Database IDs, document IDs, expected revisions, observed revisions, validation failures, and operation-specific context MUST appear inside `error.details` when applicable.

SQLite error messages and backend exception names MUST NOT define the public contract.

## `API-009` — URI and content conventions

All Version 1 endpoints MUST be rooted at `/v1`.

Database-scoped endpoints MUST use the database UUID in the URI:

```text
/v1/databases/{database_uuid}
```

Document IDs MUST be carried in JSON request bodies, not URI path segments.

Non-streaming requests and responses MUST use `application/json`. Streamed changes MUST use `application/x-ndjson`, with one complete JSON object per line.

Version 1 request objects MUST reject unknown top-level fields rather than silently ignoring them. All diagnostic timestamps MUST use UTC RFC 3339 strings and MUST NOT affect revision or replication identity.

## `API-010` — Database-management endpoints

The following method and path contracts are normative:

| Method   | Path                                            | Operation                                                  |
| -------- | ----------------------------------------------- | ---------------------------------------------------------- |
| `POST`   | `/v1/databases`                                 | Create and automatically register a database               |
| `POST`   | `/v1/registrations`                             | Register an existing closed database file by relative path |
| `DELETE` | `/v1/registrations/{database_uuid}`             | Unregister a closed database without deleting its file     |
| `GET`    | `/v1/databases`                                 | List registrations and runtime states                      |
| `GET`    | `/v1/databases/{database_uuid}`                 | Read database information                                  |
| `GET`    | `/v1/databases/{database_uuid}/config`          | Read database configuration                                |
| `PUT`    | `/v1/databases/{database_uuid}/config`          | Replace permitted database configuration fields            |
| `POST`   | `/v1/databases/{database_uuid}/close`           | Close an eligible database                                 |
| `POST`   | `/v1/databases/{database_uuid}/integrity-check` | Run domain and adapter integrity checks                    |
| `POST`   | `/v1/databases/{database_uuid}/compact`          | Run compact retention                                       |

Registration accepts only a normalized path relative to the configured database root.

## `API-011` — Document envelopes and endpoints

A returned document MUST use:

```json
{
  "id": "document-id",
  "revision": "3-digest",
  "deleted": false,
  "body": {},
  "conflicts": ["2-other-digest"]
}
```

`conflicts` MUST be omitted unless explicitly requested. It contains active conflicting live revision IDs only.

The following endpoints are normative:

| Method | Path                                                 | Request                                                         |
| ------ | ---------------------------------------------------- | --------------------------------------------------------------- |
| `POST` | `/v1/databases/{database_uuid}/documents/get`        | `{ "id": "...", "revision": null, "include_conflicts": false }` |
| `POST` | `/v1/databases/{database_uuid}/documents/put`        | `{ "id": "...", "if_revision": null, "body": {} }`              |
| `POST` | `/v1/databases/{database_uuid}/documents/delete`     | `{ "id": "...", "if_revision": "..." }`                         |
| `POST` | `/v1/databases/{database_uuid}/documents/resolve`    | Conflict-resolution request defined by `REV-010`                |
| `POST` | `/v1/databases/{database_uuid}/documents/bulk-get`   | Ordered array of get requests                                   |
| `POST` | `/v1/databases/{database_uuid}/documents/bulk-write` | Ordered array of put, delete, or resolve operations             |

For creation, `if_revision` MUST be omitted or JSON `null`. For update and recreation after deletion, it MUST contain the expected winning revision.

A put, delete, or resolve success MUST return the resulting winning revision, local sequence, and `replayed` Boolean.

A normal get of a document whose winner is deleted MUST return `document_not_found` and include the winning tombstone revision in error details. A specific-revision get MAY return a deleted revision with `deleted: true` and `body: null`.

Bulk-get results MUST preserve request order and MAY contain per-item errors. Bulk-write is atomic and MUST return one top-level error rather than partial mutation results when any operation fails.

## `API-012` — Changes endpoints

The bounded or long-poll endpoint MUST be:

```text
POST /v1/databases/{database_uuid}/changes
```

Its request MUST use:

```json
{
  "since": 0,
  "limit": 500,
  "wait_ms": 0
}
```

`wait_ms: 0` performs an immediate bounded read. A positive `wait_ms` performs the race-free waiting procedure in `CHANGE-007` when no changes are immediately available.

The streaming endpoint MUST be:

```text
POST /v1/databases/{database_uuid}/changes/stream
```

Its request MUST contain `since`, bounded batch size, and heartbeat interval. Its response MUST follow `CHANGE-008`.

## `API-013` — Index and query endpoints

The following endpoints are normative:

| Method   | Path                                                       | Operation                                    |
| -------- | ---------------------------------------------------------- | -------------------------------------------- |
| `POST`   | `/v1/databases/{database_uuid}/indexes`                    | Create a structured or full-text index       |
| `GET`    | `/v1/databases/{database_uuid}/indexes`                    | List logical indexes and states              |
| `DELETE` | `/v1/databases/{database_uuid}/indexes/{index_id}`         | Delete an index                              |
| `POST`   | `/v1/databases/{database_uuid}/indexes/{index_id}/rebuild` | Rebuild an index                             |
| `POST`   | `/v1/databases/{database_uuid}/query`                      | Execute a structured or full-text query      |
| `POST`   | `/v1/databases/{database_uuid}/query/explain`              | Explain planning without returning documents |

A query response MUST contain `documents`, `bookmark` when another page is available, the database sequence used for the page, and the selected logical index ID. Projected results MUST use the pointer-keyed `fields` form defined by `QUERY-020`.

## `API-014` — Replication-job endpoints

Replication creation MUST accept:

```json
{
  "persist": true,
  "mode": "continuous",
  "direction": "push",
  "endpoint": {
    "kind": "remote",
    "base_url": "http://host:port",
    "database_uuid": "database-uuid",
    "auth_token": "bearer-token"
  },
  "enabled": true,
  "batch": {},
  "retry": {}
}
```

The optional `auth_token` is the only credential field permitted on the
endpoint; it MUST be omitted for local endpoints and for remote endpoints that
do not require authentication, and MUST be present when the remote endpoint
requires authentication (`AUTH-003`). It is stored as local, non-replicating
job state.

`persist: false` is valid only with `mode: "one_shot"` and starts an unpersisted worker immediately. A persistent job receives a server-generated lowercase UUID version 4 job ID.

The following endpoints manage local persistent or one-shot replication workers:

| Method   | Path                                                          | Operation                                     |
| -------- | ------------------------------------------------------------- | --------------------------------------------- |
| `POST`   | `/v1/databases/{database_uuid}/replications`                  | Apply the replication creation contract above |
| `GET`    | `/v1/databases/{database_uuid}/replications`                  | List jobs and runtime states                  |
| `GET`    | `/v1/databases/{database_uuid}/replications/{job_id}`         | Read one job and runtime state                |
| `POST`   | `/v1/databases/{database_uuid}/replications/{job_id}/start`   | Start an eligible job                         |
| `POST`   | `/v1/databases/{database_uuid}/replications/{job_id}/cancel`  | Cancel the active worker                      |
| `POST`   | `/v1/databases/{database_uuid}/replications/{job_id}/enable`  | Enable persistent execution                   |
| `POST`   | `/v1/databases/{database_uuid}/replications/{job_id}/disable` | Disable persistent execution                  |
| `DELETE` | `/v1/databases/{database_uuid}/replications/{job_id}`         | Delete an inactive job                        |

## `API-015` — Replication wire endpoints

Remote replication MUST use only these database-scoped primitives:

| Method | Path                                                                     | Purpose                               |
| ------ | ------------------------------------------------------------------------ | ------------------------------------- |
| `GET`  | `/v1/databases/{database_uuid}/replication/identity`                     | Handshake information from `REPL-015` |
| `POST` | `/v1/databases/{database_uuid}/replication/boundaries`                    | Return compact-history-boundary pages |
| `POST` | `/v1/databases/{database_uuid}/replication/changes`                      | Bounded changes read                  |
| `POST` | `/v1/databases/{database_uuid}/replication/revisions/diff`               | Report missing and compacted leaf revisions |
| `POST` | `/v1/databases/{database_uuid}/replication/revisions/get`                | Return complete chains or snapshot pages |
| `POST` | `/v1/databases/{database_uuid}/replication/revisions/put`                | Atomically import revision chains     |
| `GET`  | `/v1/databases/{database_uuid}/replication/checkpoints/{replication_id}` | Read a local checkpoint               |
| `PUT`  | `/v1/databases/{database_uuid}/replication/checkpoints/{replication_id}` | Compare-and-swap a checkpoint         |

The revision-diff request MUST use:

```json
{
  "documents": [
    {
      "document_id": "document-id",
      "leaf_revisions": [
        {
          "revision": "3-digest",
          "history_id": "history-id"
        },
        {
          "revision": "2-other-digest",
          "history_id": "history-id"
        }
      ]
    }
  ]
}
```

The compact-history-boundary request and response MUST be storage-neutral and
pageable. The request MUST identify the source history epoch, desired
compaction epoch, and an opaque page cursor. A response MUST use this shape:

```json
{
  "source_history_epoch": "history-epoch",
  "compaction_epoch": 3,
  "boundary_digest": "sha256-digest",
  "next_page": null,
  "boundaries": [
    {
      "document_id": "document-id",
      "history_id": "history-id",
      "minimum_retained_generation": 3,
      "retired": false,
      "retired_branch_roots": ["3-removed-branch-root"]
    }
  ]
}
```

`retired: true` means the complete history has been purged. Otherwise the
boundary identifies the minimum retained generation; lower generations and
descendants of `retired_branch_roots` are compacted stale state. The page
digest MUST cover the complete ordered boundary set for the source compaction
epoch. `minimum_retained_generation` is `null` when `retired` is true.

The response MUST return the same document grouping with `missing_revisions`
and `compacted_revisions`. A leaf covered by a target history boundary MUST be
reported as compacted rather than missing, so the source does not repeatedly
replay a history the target has intentionally retired.

The revision-get response MUST use:

```json
{
  "chains": [
    {
      "document_id": "document-id",
      "history_id": "history-id",
      "leaf_revision": "3-digest",
      "revisions": []
    }
  ]
}
```

The revision-get request MAY select `bootstrap: true` instead of a leaf list.
In that mode the source MUST return bounded pages of current document
histories and the source retention boundary. Bootstrap pages MUST be
idempotent and MUST be safe to resume or repeat before the target commits the
resulting checkpoint.

A chain whose history begins at a compact-retention boundary MUST include `"truncated": true`; a chain rooted at a fresh-history revision after purge (`DOC-002`) MUST include a new `history_id` and `"truncated": false`. Every revision in a truncated chain MUST still carry its original history ID, revision ID, generation, parent reference, deletion state, and body.

The revision-put request uses the same `chains` form. A successful response MUST report documents changed, revisions newly inserted, and the target’s resulting local sequence.

A checkpoint PUT MUST contain `expected_checkpoint_version` and the complete replacement checkpoint.

## `API-016` — Stable error registry

Version 1 MUST define at least these public error codes and HTTP statuses:

| Code                          | HTTP | Retryable          |
| ----------------------------- | ---: | ------------------ |
| `unauthorized`                | 401  | No                 |
| `invalid_request`             | 400  | No                 |
| `invalid_bookmark`            | 400  | No                 |
| `database_not_registered`     | 404  | No                 |
| `database_not_found`          | 404  | No                 |
| `document_not_found`          | 404  | No                 |
| `revision_not_found`          | 404  | No                 |
| `history_truncated`           | 410  | No                 |
| `index_not_found`             | 404  | No                 |
| `database_in_use`             | 409  | Yes                |
| `database_not_closable`       | 409  | Yes                |
| `duplicate_database_uuid`     | 409  | No                 |
| `revision_conflict`           | 409  | No                 |
| `bookmark_stale`              | 409  | Yes                |
| `replication_incompatible`    | 409  | No                 |
| `replication_already_running` | 409  | Yes                |
| `checkpoint_conflict`         | 409  | Yes                |
| `unsupported_format`          | 409  | No                 |
| `index_name_conflict`         | 409  | No                 |
| `invalid_index_hint`          | 422  | No                 |
| `index_required`              | 422  | No                 |
| `integrity_violation`         | 422  | No                 |
| `payload_too_large`           | 413  | No                 |
| `resource_limit`              | 422  | No                 |
| `database_overloaded`         | 429  | Yes                |
| `database_closed`             | 503  | Yes                |
| `database_unavailable`        | 503  | Yes                |
| `internal_error`              | 500  | Depends on details |

Additional error codes MAY be added without changing the envelope, but existing code meanings MUST remain stable for protocol major version 1.

---

# 18. Maintenance

Version 1 compact retention is a bounded-history replication protocol. It is
not age-based pruning and it MUST NOT use the local current sequence or one
replication-job checkpoint as a safe deletion point.

A **source position** is the tuple `(database_uuid, history_epoch, sequence)`.
The `history_epoch` identifies one database history incarnation. It is created
with the database and MUST change when a database is discarded, recreated, or
has its replication history explicitly reset. Compacting within one history
epoch does not change the epoch. A sequence remains local to its originating
database and MUST never be reused within an epoch.

A clean relocation or backup copy that continues the same logical database
MAY retain its history epoch. A restore that rolls the logical database back
to an older state MUST rotate to a new history epoch before it participates in
replication; peers MUST treat the old epoch as a discarded source history.

The **compaction epoch** is a local monotonically increasing fence. It MUST
advance whenever the retention floor advances and MUST remain unchanged for a
no-op run. A peer's installed compaction epoch means that the peer has
observed and durably installed the source's corresponding retention boundary;
it is separate from the source history epoch and does not participate in
revision identity.

A **safe peer position** for a source is the highest source sequence that the
peer has durably applied and for which it has no unacknowledged local mutation
whose parent or causal context is earlier than that position. A normal received
checkpoint is not automatically a safe peer position. A peer MUST advance its
safe position only after its pending pre-frontier mutations have been
replicated or otherwise resolved locally. This prevents compaction from
removing a parent that an offline peer still needs for a local write.

The **stable frontier** for a database is the minimum safe peer position for
the database's current history epoch across every known, non-expired
replication peer, bounded by the database's current sequence. A peer with no
safe-position report contributes sequence zero. The frontier is local
maintenance state; it MUST be computed from durable peer reports and MUST NOT
be advanced from an unverified indirect claim. Its sequence MUST never move
backwards.

Each known peer MUST have a local lease record containing its database UUID,
history epoch, last safe position, last installed compaction epoch, and lease
expiry. A successful replication handshake renews the lease and may advance
the recorded position. A peer that is not present in the local peer ledger is
a new peer and MUST bootstrap; it MUST NOT require the database to retain
history from before its registration. A new peer MUST NOT enter the
stable-frontier minimum until bootstrap commits; its initial safe position is
the source retention floor and its installed compaction epoch is the source
compaction epoch returned by that bootstrap. A peer whose lease expires is removed
from the stable-frontier minimum. If it later returns, it MUST bootstrap from
a current snapshot or be explicitly recreated; automatic transparent
multi-master rejoin after expiry is not guaranteed.

The storage-neutral peer-ledger record for a local source MUST contain at
least:

```json
{
  "peer_database_uuid": "peer-uuid",
  "peer_history_epoch": "peer-history-epoch",
  "source_database_uuid": "database-uuid",
  "source_history_epoch": "history-epoch",
  "safe_source_sequence": 42,
  "installed_source_compaction_epoch": 3,
  "last_seen_at": "RFC3339-timestamp",
  "lease_expires_at": "RFC3339-timestamp"
}
```

Peer-ledger records are local state. They MUST NOT be transferred as document
revision state or used as a substitute for the source/target checkpoint CAS.

A peer reporting a different history epoch, or a lower safe position or
installed compaction epoch than the durable ledger previously recorded, MUST
be treated as a rollback or replacement, MUST NOT renew the old lease, and
MUST bootstrap before it can rejoin the stable frontier.

Compaction does not require a network-wide consensus round or a live
connection to every peer. A database uses its latest durable reports. A stale
report can delay compaction but MUST NOT make it unsafe. The membership ledger
and expiration policy are therefore part of the correctness boundary: a
database MUST NOT silently omit a known non-expired peer from the minimum.

The multi-master guarantee is intentionally bounded by those local leases. A
network partition can preserve safety by stopping the frontier, but it cannot
provide indefinite automatic merge after one side expires the other. Changes
created after an asymmetric expiry require snapshot/bootstrap plus explicit
rebase or export handling.

## `MAINT-001` — Integrity checks

The server MUST provide a maintenance operation that validates:

* SQLite structural integrity.
* Required internal tables.
* Metadata consistency.
* Revision ancestry.
* Winning-revision references.
* Changes-feed references.
* Replication checkpoint structure.
* Logical structured-index definitions.
* Logical full-text-index definitions.
* Adapter-specific physical structured and full-text index consistency.
* Storage-adapter conformance invariants.
* Compact-retention metadata consistency, including the history epoch, stable frontier reports, retention floor, compaction epoch, trimmed counts, and a monotonically non-decreasing compaction counter.
* Peer leases and safe positions: expired peers MUST NOT constrain a frontier, and a non-expired peer MUST NOT be silently absent from it.
* Truncated revisions: every revision whose parent record is absent MUST correspond to a documented compact-retention boundary, and every changes entry above the retention floor MUST reference retained leaf revisions.
* Purged-history boundaries: every retired history ID and retired branch root MUST have a recorded compaction epoch and boundary until the peer-installation conditions in `MAINT-007` are satisfied.

## `MAINT-002` — SQLite vacuum

A maintenance operation MAY run SQLite `VACUUM`.

`VACUUM` is not required for file portability.

The operation MUST run only while the database remains under its owner process.

Compact retention MAY be followed by physical space reclamation; the reclamation mechanism is an adapter concern (for example, SQLite `VACUUM` in the Version 1 adapter).

## `MAINT-003` — Revision retention

Version 1 MUST retain complete bodies for all retained revisions.

Stable-frontier compact retention (`MAINT-005` through `MAINT-009`) MAY remove
only history at or below the local stable frontier. It MUST NOT remove a
revision merely because it is old. Until a compact operation removes a
revision, its complete body MUST remain stored and retrievable.

Time-based deletion independent of the stable frontier is not permitted.

## `MAINT-004` — Database close

The server MUST support closing an individual idle database without stopping the complete server.

After a successful close, the file enters the offline-portable state.

## `MAINT-005` — Compact retention operation

The server MUST provide one atomic compact-retention operation per database.

The operation MUST:

* Run as a serialized maintenance operation through the database owner and share the bounded admission mechanism with all other operations (`ARCH-001`, `ARCH-006`).
* Be idempotent; running it when there is nothing to remove MUST succeed as a no-op.
* Compute a monotonic stable frontier from the durable peer ledger without contacting peers during the operation.
* Commit atomically: revision-history trimming, changes-feed trimming, retention-floor advancement, compaction-epoch advancement, and compact-history-boundary updates MUST commit or roll back together.
* Be implemented behind the storage adapter as a storage-neutral capability; any physical space reclamation is an adapter concern and MUST NOT define the operation's contract.
* Remove only what `MAINT-007` and `MAINT-008` permit.
* Preserve document IDs, history IDs, revision IDs and ancestry of retained revisions, the winning revision of every surviving document, materialized winning documents, structured and full-text indexes, replication jobs, checkpoints, peer leases, database configuration, and the local changes sequence. Physical leaves, losing branches, tombstones, and old history roots are preserved only to the extent `MAINT-007` requires.
* Allocate no document changes sequences (`TX-005`).
* Record its outcome as local maintenance metadata: a compaction counter, the previous and resulting retention floors, the stable frontier used, the compaction epoch, the highest changes sequence removed, and the counts of revisions, history boundaries, and changes entries removed.

The compaction counter MUST start at zero and increment by one on every run
that advances the retention floor or removes at least one revision record,
history boundary, or changes entry; a no-op run MUST NOT increment it. The
counter makes compacted state observable: a difference between a database and
its peers in leaf, conflict, tombstone, or retained-history counts MUST be
attributable to the recorded retention floors, history boundaries, compaction
counters, and trim statistics. Any difference not explained by that metadata
indicates an error (`MAINT-001`).

The operation MUST NOT be required for the correctness of reads, writes, queries, indexes, or replication; it exists to reclaim storage only.

A specific-revision read of a removed revision MUST return `revision_not_found`. A bounded changes read whose `since` is below the retention floor MUST return `history_truncated` as defined by `CHANGE-006`. Nothing else in the public protocol changes after compaction.

## `MAINT-006` — Retention configuration

The database configuration object MUST contain a storage-neutral `retention`
group (`CONFIG-006`) bounded by host-level ceilings, containing:

* `mode`: either `disabled` or `stable_frontier`. `disabled` means that the
  database MUST never remove revisions, tombstones, history boundaries, or
  changes entries through compact retention.
* `history_depth`: a non-negative integer; the number of parent generations
  above the winning revision that an eligible surviving history retains. Zero
  permits removal of every non-winning revision and every older winning
  ancestor once the stable frontier permits it.
* `peer_expiry_ms`: a positive duration used to expire a peer lease when the
  source has not observed a successful replication handshake. Expiration is a
  local policy decision and MUST be recorded with the peer ledger.
* `schedule`: a storage-neutral interval after which the server MAY run the
  operation automatically, or an explicit disabled value; a disabled or
  absent schedule means the operation runs only on explicit request.

Absent `retention` configuration MUST mean `mode: disabled`, preserving full
retention. Enabling stable-frontier compaction is an explicit operator action;
the server MUST NOT compact merely because a schedule exists while the mode is
disabled.

## `MAINT-007` — Revision-history trimming

The operation MUST NOT remove:

* Any revision state for a document whose most recent local changes entry is
  above the resulting retention floor.
* The winning revision of any surviving document.
* A retained history boundary needed to reject a stale chain from a
  non-expired peer.

For a document whose most recent local changes entry is at or below the
resulting retention floor, the operation MAY remove:

* Winning ancestors older than the configured `history_depth`.
* Every frontier-settled losing branch, including its leaves.
* A frontier-settled deleted history in its entirety, including its tombstone and every
  remaining ancestor, when every physical leaf in the history is deleted.
  The document history then no longer exists on this database (`REV-006`,
  `DOC-002`).

A removed revision loses its complete record, including its body. Its history
ID remains represented by retained ancestry or by a compact-history boundary;
the boundary need not retain every removed revision ID. A retained revision
whose parent record was removed is a truncated revision (`REV-006`). A
boundary MUST retain the history ID, the minimum retained generation, retired
branch roots, or a retired-history marker, and MUST NOT retain a revision body.

A boundary MAY be removed only after every known non-expired peer has reported
an installed compaction epoch at least as new as the boundary's compaction
epoch. An expired peer does not prevent boundary removal and MUST bootstrap if
it later returns. Until that condition holds, the boundary is the local fence
that prevents a retained peer from replaying an old history after compaction.
A peer MAY retain the fenced physical rows until its own later compact run, but
it MUST install the boundary before reporting the compaction epoch and MUST
exclude those rows from future replication.

Every recreation uses a new history ID rather than creating a child of a
tombstone that another peer may already have purged. Revisions in the old
history remain valid only until the stable frontier and history-boundary rules
remove them.

Removing revisions MUST NOT change the materialized winning document,
deterministic winner selection, or any query index of a surviving document.
An active conflict of a frontier-settled document MAY disappear when compact retention
removes a losing branch; winner selection remains deterministic on every peer
that retains the corresponding history state (`REV-008`).

## `MAINT-008` — Changes-feed trimming and the retention floor

The retention floor MUST be the resulting stable-frontier sequence for the
current database history epoch. It MUST be derived from all known
non-expired-peer safe positions, not merely from persistent push jobs. Pull
jobs and unpersisted jobs contribute through their peer safe-position reports;
an absent or unacknowledged peer report contributes sequence zero. A database
with no known non-expired peers MAY advance its floor to its current sequence;
new peers then bootstrap instead of receiving old feed history.

When stable-frontier compaction advances the floor, the operation MUST remove
every changes entry at or below the new floor. There is deliberately no
"latest entry per document" exception: a document whose latest change is
older than the floor is discovered through snapshot/bootstrap, not through an
artificial historical feed row. Entries above the floor MUST remain exact
snapshots of the leaf set at their original sequence and MUST NOT be rewritten
without allocating a new sequence.

The retention floor is the highest source sequence for which the changes feed
and revision history are no longer guaranteed to be available. A changes read
or replication request below that floor MUST return `history_truncated` and
MUST include the source history epoch, floor, and compaction epoch. The local
changes sequence MUST remain monotonically increasing and MUST NOT be reused
or renumbered (`CHANGE-001`).

## `MAINT-009` — Compaction scheduling and eligibility

The server MUST run the operation on explicit request and MAY run it automatically when the configured schedule applies.

A scheduled run MUST be an ordinary admitted maintenance operation. It MUST
NOT run concurrently with any other operation through the owner, MUST defer
while the database is closing, and MUST be idempotent so overlapping or
repeated triggers are harmless. It MUST use the latest local peer ledger and
MUST NOT perform a network-wide coordination round. When no safe frontier is
available beyond the current retention floor, the run MUST be a no-op.

---

# 19. PouchDB and CouchDB usage

## `REF-001` — CouchDB

CouchDB SHALL be used as the behavioral reference for:

* Revision trees.
* Conflicts.
* Tombstones.
* Changes feeds.
* Missing-revision discovery.
* Checkpoints.
* One-shot replication.
* Continuous replication.

The implementation SHALL remain independent.

## `REF-002` — PouchDB source port

PouchDB MUST NOT be ported line by line into Elixir.

Its implementation includes architectural concerns specific to:

* JavaScript.
* Browsers.
* IndexedDB.
* Node.js adapters.
* LevelDB-style adapters.
* CouchDB compatibility.
* Plugin packaging.

Those implementation structures are not the target architecture.

## `REF-003` — PouchDB test suite

The PouchDB test suite SHALL be used as a supplemental validation source.

It SHALL NOT be the sole release criterion.

PouchDB separates core integration, find, map/reduce, migration, browser, and adapter-specific testing, and its test infrastructure supports multiple backing servers. This makes it useful for extracting behavioral cases but not sufficient as a complete proof for this independently scoped implementation.

## `REF-004` — Selected validation

The project MUST identify and adapt relevant upstream cases covering:

* Put and get.
* Update conflicts.
* Deletes.
* Bulk operations.
* Revision determinism.
* Changes feeds.
* Conflict branches.
* Replication interruption.
* Replication retry.
* Checkpoint recovery.
* Query behavior within the supported subset.
* Full-text indexing and search behavior where an upstream case matches the project-owned contract.

Every excluded upstream test MUST have a documented reason, such as:

* Deferred feature.
* Browser-adapter behavior.
* CouchDB-specific API behavior.
* Map/reduce.
* Attachments.
* Unsupported query operator.
* Compatibility behavior not adopted by this specification.

## `REF-005` — Differential testing

Where semantics intentionally overlap, randomized operation sequences SHOULD be executed against:

* The Elixir implementation.
* A pinned PouchDB version.
* A pinned CouchDB version.

Results SHALL be compared at the behavioral level rather than raw API structure.

---

# 20. Input safety and resource limits

## `SEC-001` — Required limits

The server MUST enforce bounded values for:

* Document size.
* JSON nesting depth.
* Bulk-operation count.
* Bulk request size.
* Query result count.
* Query execution time.
* Full-scan document count.
* Changes batch size.
* Replication batch size.
* Concurrent replication jobs.
* Open databases.
* Request body size.
* Document ID size.

## `SEC-002` — SQL isolation

Clients MUST NOT be able to provide:

* SQL fragments.
* SQLite identifiers.
* SQLite functions.
* SQLite collations.
* Raw JSON paths used directly in SQL.
* Index expressions.

All internal SQL MUST be generated by trusted project code.

## `SEC-003` — Filesystem isolation

Database routing MUST prevent:

* Path traversal.
* Symlink escape.
* Arbitrary file opening.
* Arbitrary database creation outside the configured root.
* Overwriting unrelated files.
* Attaching arbitrary SQLite databases.

## `SEC-004` — Untrusted files

Before accepting an existing file as a project database, the server MUST validate:

* File type.
* SQLite application ID.
* Supported format version.
* Required schema.
* Database UUID.
* File location.
* Internal integrity.

---

# 20.5. Observability

## `OBSV-001` — OpenTelemetry-native observability

Version 1 observability SHALL be emitted through the OpenTelemetry (OTel) standard, not through ad hoc logging or bare `:telemetry.execute/3` calls.

The implementation plan's observability section ([Implementation_Plan_V1.md §11](Implementation_Plan_V1.md)) is authoritative for module layout, instrumentation sites, dependencies, and rollout phasing. This section fixes the normative, externally observable contract.

### `OBSV-002` — Required signals

Every Version 1 installation MUST emit the following signal families when export is enabled. The signal names are part of the operational contract and MUST remain stable for protocol major version 1.

Each of the ten operational events defined in the implementation plan and this specification MUST be emitted as one OpenTelemetry span and, where the plan or specification specifies a metric, one counter or histogram:

```text
elixir_db.database.open       (span + counter)
elixir_db.database.command    (span + histogram)
elixir_db.database.compact    (span + counter + histogram)
elixir_db.database.overload   (counter only)
elixir_db.changes.read        (span + histogram)
elixir_db.query.execute       (span + histogram)
elixir_db.index.build         (span + histogram)
elixir_db.replication.batch   (span + histogram)
elixir_db.replication.checkpoint (span + counter)
elixir_db.http.request        (span + histogram)
```

Measurements MUST include monotonic duration for each span and histogram and a bounded count where the plan defines one (changes entries returned, query candidates examined, revisions written, revisions removed).

### `OBSV-003` — Attribute allow-list

Span and metric attributes MUST be drawn exclusively from a project-owned allow-list. Permitted attributes are limited to:

* `db.uuid`
* `command.type` (the normalized command atom)
* `error.code` (the stable `ElixirDB.Error` code atom)
* `outcome` (`:ok`, `:rejected`, or `:replayed`)
* `http.method`, `http.route` (route template, never the raw path), `http.status_code`
* `index_id`, `index_type`
* `replication.id`, `endpoint` (`:source` or `:target`)
* Bounded counts defined by the plan

The following MUST NOT appear in any span or metric attribute:

* Document bodies.
* Document IDs.
* Search text.
* Revision IDs and revision bodies.
* Complete remote URLs containing private path data.
* SQLite error messages and backend exception names.

Storage-engine details remain behind the storage adapter boundary and MUST NOT leak into telemetry any more than into public protocol responses.

### `OBSV-004` — Opt-in export

Telemetry export MUST be disabled by default. When no collector endpoint is configured, the server MUST start, serve traffic, and make zero network connections to any collector. Enabling export MUST be an explicit host configuration action, not a default behavior.

A configured exporter MUST transport traces and metrics over OTLP. Propagation across the HTTP and replication boundaries MUST use W3C Trace Context.

### `OBSV-005` — Trace context continuity

A replication worker MUST preserve trace context across its asynchronous phase tasks so that a single replication batch, its endpoint calls, and any remote-server HTTP handling share one trace.

Inbound HTTP requests SHALL accept a W3C `traceparent` so an external caller's trace continues into the server. Outbound remote-replication HTTP requests SHALL inject the current context. A one-shot remote replication between two Version 1 servers MUST produce spans on both servers under a single `trace_id`.

### `OBSV-006` — Span status policy

Span status MUST distinguish unexpected internal failures from expected application outcomes:

* `internal_error` and unmapped adapter failures SHALL set span status to ERROR.
* Registered domain errors (`revision_conflict`, `document_not_found`, `database_in_use`, `checkpoint_conflict`, and all others in the `API-016` registry) SHALL leave span status UNSET and instead carry `error.code` as an attribute.

This keeps error-rate signals reserved for genuine internal failures.

### `OBSV-007` — Low overhead

Instrumentation MUST be in-process and non-blocking. Export SHALL be asynchronous and bounded. Hot paths — document mutation, changes reads, query execution — MUST NOT allocate per event beyond what the OpenTelemetry API requires. A node with export disabled MUST behave identically to a node with no OpenTelemetry dependencies reachable.

---

# 21. Implementation gates

These phases define mandatory capability and validation gates, not the detailed implementation plan.

The separate implementation plan SHALL define modules, files, function seams, call stacks, test locations, and single-PR sequencing without changing the behavior frozen by this specification.

## Phase 0 — Frozen behavioral and protocol contracts

### Deliverables

* Final replicated-versus-local state classification.
* Engine-neutral storage adapter capability and command contracts.
* Storage-adapter conformance suite structure.
* I-JSON and binary64 number validator.
* RFC 8785 canonicalizer.
* RFC 6901 JSON Pointer parser.
* Pure document, revision-tree, tombstone, and active-conflict model.
* Winner-selection and explicit conflict-resolution models.
* Pure changes-entry and local-sequence model.
* Replication ID, complete revision-chain, checkpoint, and worker-state models.
* Versioned HTTP request, response, streaming-event, and error schemas.
* Self-contained bookmark codec contract.
* Host configuration file schema, authentication token-digest contract, and TLS option contract.
* Language-neutral conformance fixtures.
* Requirement-to-test mapping.

### Validation

* Canonicalization and numeric-boundary vectors.
* Deterministic revision vectors, including tombstones.
* Invalid JSON, duplicate-key, Unicode, and numeric rejection.
* Randomized revision-tree and winner property tests.
* Active-conflict creation, resolution, delete-all, and replay tests.
* Changes-entry ordering and one-sequence-per-document fixtures.
* Replication ID and checkpoint compare-and-swap vectors.
* JSON Pointer escaping and invalid-pointer tests.
* HTTP schema tests, including unknown-field rejection.
* Bookmark checksum, query-binding, and stale-sequence tests.

### Exit condition

No storage or HTTP implementation may define its own semantics after Phase 0. All pure models and language-neutral protocol fixtures MUST pass first.

---

## Phase 1 — SQLite adapter and lifecycle foundation

### Deliverables

* SQLite lifecycle subset of the storage adapter.
* Exqlite integration and runtime capability validation.
* Database creation, identity, fixed Version 1 schema marker, and required pragmas.
* Per-database runtime supervisor, owner, notifier, and bounded admission queue.
* Exclusive cross-process ownership lease.
* Versioned registration manifest with atomic updates.
* Database registry and register, unregister, list, open, and close operations.
* Lazy opening and startup inspection of registered databases.
* Clean shutdown and unavailable-registration handling.

### Validation

* Create, open, close, and reopen.
* Competing-process ownership lease rejection.
* Bounded admission and `database_overloaded` behavior.
* Race-free notifier subscription without missed commits.
* Manifest interruption before and during atomic replacement.
* Missing file and UUID-mismatch registrations remain `unavailable`.
* Explicit registration before opening a copied file.
* Manifest reconstruction by re-registering files.
* Clean shutdown and individual database close eligibility.
* Normal OS copy, relocation, registration, and reopen under another server.
* UUID and all authoritative local state remain preserved.
* Unsupported SQLite and non-Version 1 files are rejected.
* SQLite version and FTS5 capability validation.
* Hot-journal recovery before offline portability.
* Lifecycle and portability subset of the adapter conformance suite.

### Exit condition

A cleanly closed file MUST be independently movable and restorable without export, and no second process may obtain ownership while the first owner is active.

---

## Phase 2 — Documents and revisions

### Deliverables

* Document creation, retrieval, replacement, deletion, and specific-revision reads.
* Revision-tree and physical/live leaf persistence.
* Deterministic winner projection.
* Active-conflict inspection.
* Atomic conflict resolution with surviving-body and delete-all modes.
* Idempotent put, delete, resolve, and atomic bulk-write retries.
* Materialized winning-document updates.

### Validation

* First, linear, deleted, and recreated histories.
* Stale local updates and integrity mismatches.
* Replicated sibling branches and active-conflict detection.
* Deleted leaves do not count as active conflicts.
* Winner determinism under every insertion order.
* Deleting the winner while another live leaf exists.
* Conflict resolution with a chosen branch.
* Conflict resolution deleting every live branch.
* Exact live-leaf-set compare-and-swap.
* Successful replay and partial-replay rejection.
* Atomic bulk rollback.
* Randomized stored model versus the Phase 0 pure model.

### Exit condition

Every stored revision-tree, winner, tombstone, and conflict-resolution result MUST match the Phase 0 model.

---

## Phase 3 — Changes and replication primitives

### Deliverables

* One-sequence-per-affected-document allocation.
* Exact physical-leaf changes entries.
* Bounded, long-poll, and NDJSON streamed changes.
* Race-free notifier integration.
* Missing exact leaf-revision comparison.
* Complete root-to-leaf revision-chain retrieval with history IDs.
* Atomic revision-chain import.
* Compare-and-swap checkpoint storage and bounded history.
* Retention-floor and snapshot/bootstrap primitives.

### Validation

* Monotonic sequences and no exposure after rollback.
* Multiple revisions for one document produce one changes entry.
* Configuration, indexes, jobs, and checkpoints never enter the changes feed.
* Physical leaf and tombstone sets are complete and ordered.
* No missed change between read and notifier subscription.
* Stream `caught_up`, heartbeat, close, and error events.
* Repeated chain retrieval and import.
* Existing identical revisions are no-ops.
* Different content under one revision ID is rejected.
* Dangling parent chains are rejected atomically unless marked truncated by compact retention or rooted at a fresh-history revision after purge with a new history ID.
* Checkpoint common-history selection, history-epoch changes, compare-and-swap conflicts, and lost-response replay.
* History gaps switch to snapshot/bootstrap rather than restarting below the retention floor.

### Exit condition

Every changes, chain-transfer, import, and checkpoint primitive MUST be deterministic, bounded, and idempotent.

---

## Phase 4 — Local replication

### Deliverables

* Local endpoint references and handshake.
* Replication and session IDs.
* One-shot and continuous workers.
* Complete-chain batch algorithm.
* Two-sided checkpoint reconciliation.
* Worker exclusivity, cancellation, backoff, and runtime states.
* Local source and target endpoint adapters.

### Validation

* UUID, protocol, revision-algorithm, and canonicalization mismatch rejection.
* Equal source and target UUID rejection.
* Empty-to-populated and populated-to-empty replication.
* Repeated replication and duplicate-worker rejection.
* Interrupted change read, chain retrieval, import, target checkpoint, and source checkpoint.
* Failure between checkpoint writes repeats work only.
* Missing and divergent checkpoint histories.
* Cancellation at every transaction boundary.
* Concurrent edits, conflicts, tombstones, and winner changes.
* Database configuration, indexes, jobs, and checkpoints do not replicate.
* Bidirectional convergence after writes stop.

### Exit condition

Fault injection at every transition MUST cause repetition at worst and MUST never skip a committed revision or transfer local-only state.

---

## Phase 5 — HTTP and remote replication

### Deliverables

* `/v1` routing and content-type enforcement.
* Success, error, and NDJSON stream envelopes.
* Database, registration, document, changes, maintenance, and replication-wire endpoints.
* Stable public error mapping.
* Remote source and target endpoint adapters.
* Request IDs, compression, deadlines, retry classification, and tracing.
* Host configuration loading from `<database_root>/host.toml` with first-run template creation and field-level validation.
* Bearer-token authentication plug and the `unauthorized` error code.
* TLS listener when enabled, and the non-loopback failsafe guard.
* Replication source presentation of `auth_token` to authenticated targets.

### Validation

* Contract tests for every implemented method, path, request, response, and status.
* Unknown top-level fields are rejected.
* Document IDs round-trip only through JSON bodies.
* Per-item bulk-get errors and atomic bulk-write errors.
* Long-poll and streamed changes over HTTP.
* Two independent servers using only Version 1 wire endpoints.
* Connection interruption, truncation, duplicate requests, and server restart.
* Slow source, slow target, and timeout followed by retry.
* Malformed identities, revision chains, checkpoints, and oversized payloads.
* Backend errors never leak through the public envelope.
* First run in an empty root creates a fully commented `host.toml`; a subsequent run never overwrites it.
* `host.toml` with unknown keys, wrong types, or out-of-range values fails startup with a field-level error.
* The shipped template and compiled defaults hold identical values.
* A valid bearer token succeeds; missing, malformed, and wrong tokens all return `unauthorized` with identical messages.
* An authenticated target rejects a source that omits or sends the wrong `auth_token`, and accepts a source that sends the right one.
* A non-loopback listener without authentication and without TLS fails to start, and the explicit override permits it.
* TLS-enabled serving and outbound `https` replication both use TLS.

### Exit condition

Remote replication MUST pass the local convergence suite, and all implemented HTTP contracts MUST match the frozen Phase 0 schemas.

---

## Phase 6 — Persistent replication jobs

### Deliverables

* Persistent and unpersisted replication creation contracts.
* Local and remote counterpart endpoint references.
* Stored definitions separate from transient runtime states.
* Startup inspection through the registration manifest.
* Automatic enabled-job restart.
* Enable, disable, start, cancel, and delete behavior.
* Runtime state and diagnostics reporting.

### Validation

* Persistent job and unpersisted one-shot request validation.
* Restart resumes enabled continuous jobs from registered databases.
* Disabled and failed jobs do not start automatically.
* Runtime transitions through idle, running, waiting, backoff, completed, and failed.
* Unregistered copied databases remain inert.
* Explicit registration exposes copied job definitions.
* Database copy retains local jobs and checkpoints without protocol replication.
* Remote URL changes preserving UUID retain replication identity.

### Exit condition

Enabled continuous jobs MUST resume without client traffic, while job definitions and runtime states remain correctly separated.

---

## Phase 7 — Queries and indexes

### Deliverables

* RFC 6901 selector, sort, projection, and index field parsing.
* Storage-neutral structured and full-text index definitions.
* SQLite expression-index and FTS5 physical implementations.
* Deterministic structured planner and adapter full-text planner.
* Storage-neutral tokenization and search modes.
* Synchronous atomic index create, delete, and rebuild.
* Pointer-keyed projections.
* Self-contained keyset and adapter-cursor bookmarks.
* Query explain and bounded-scan enforcement.
* Query and index HTTP endpoints.

### Validation

* JSON Pointer escaping, nested fields, missing versus null, and scalar type distinctions.
* Documents with missing or mistyped indexed fields remain writable and follow extraction rules.
* Compound index equality prefixes, ranges, sorting, full reversal, and mixed-reversal rejection.
* Planner determinism and explicit invalid-index-hint rejection.
* Bookmark continuation, checksum validation, query binding, index binding, and stale-sequence rejection.
* Full-scan threshold enforcement.
* Field, index-name, query-value, bookmark, and backend-query injection attempts.
* Shared `unicode_words_v1` fixtures covering Unicode 6.1 token categories, case folding, token boundaries, and both diacritic modes.
* SQLite `unicode61` mappings with `remove_diacritics 0` and `2` satisfy the same fixtures.
* Full-text index creation, deletion, rebuild, and integrity detection.
* Winner, conflict-resolution, and tombstone changes update all derived indexes atomically.
* Adapter-specific deterministic relevance order and document-ID tie-breaking.
* Structured selector combined with full-text search.
* Pointer-keyed projection and missing-field omission.
* Mango differential fixtures for overlapping structured-query behavior.

### Exit condition

Every accepted structured query MUST use a compatible index or permitted bounded scan; every full-text query MUST use a compatible logical full-text index without exposing backend semantics.

---

## Phase 8 — Hardening and release

### Deliverables

* Complete storage-adapter conformance suite.
* Complete Version 1 HTTP contract suite.
* Selected PouchDB behavior suite and CouchDB differential fixtures.
* Crash, ownership-lease, admission, and notifier fault tests.
* Long-running local and remote replication tests.
* Revision, query, bookmark, protocol, and JSON fuzzing.
* Offline portability and registration-recovery tests.
* Compact retention with stable-frontier gating, bounded peer leases, history-boundary fencing, settled conflict-branch and tombstone removal, and truncated-chain/bootstrap replication.
* OTP release pipeline (`mix release.build`), release metadata artifact, and operational documentation.
* Version 1 format declaration.

### Required end-to-end scenario

1. Start each server with an empty database root and confirm a fully commented `host.toml` is created on first run.
2. Enable bearer-token authentication on the target, generate a token with `bin/elixir_db token`, and confirm replication into it succeeds only when the source endpoint carries the matching `auth_token`.
3. Create and register two databases through the Version 1 HTTP API.
4. Write documents independently and retry a mutation after losing its response.
5. Create divergent revisions through replication.
6. Replicate in both directions and verify active conflicts.
7. Resolve one conflict to a surviving body and another by deleting all live branches.
8. Verify changes entries contain the final physical leaf sets.
9. Create structured and full-text indexes and exercise pointer-keyed projections.
10. Execute paginated queries, mutate the database, and verify the old bookmark becomes stale.
11. Restart both servers during continuous replication and resume through checkpoint reconciliation.
12. Verify local configuration, index definitions, jobs, and checkpoints did not protocol-replicate.
13. Cleanly stop both servers.
14. Copy one database root directory, including `host.toml`, tokens, certificates, and database files, using ordinary OS file copy.
15. On a fresh host, point `ELIXIR_DB_ROOT` at the copy and start the server with no further setup.
16. Verify the relocated server serves authenticated, TLS-protected traffic using the copied material, and that its databases, UUIDs, documents, histories, conflicts, queries, local jobs, and checkpoints are intact.

### Release condition

Version 1 is ready only when:

* The entire test suite passes with zero failures.
* The complete storage-adapter conformance suite passes.
* The complete Version 1 HTTP contract suite passes.
* Every end-to-end scenario passes with zero failures.
* Every crash-recovery, ownership-lease, admission, and notifier scenario passes.
* Every replication fault-injection scenario passes.
* Tests prove that protocol replication transfers document revision state only.
* Offline single-file portability and registration recovery pass.
* Every derived structured and full-text index can be rebuilt from authoritative state.
* Compact retention tests pass: only history at or below the stable frontier is removed, surviving winners and their configured ancestry remain, pruned revisions and purged tombstones are unreachable, stale peer replays are fenced, recreation after purge begins a new history, the changes sequence stays monotonic, and valid peers replicate through truncated chains while expired peers bootstrap.
* The OTP release builds (`MIX_ENV=prod mix release.build`) and emits release metadata through `bin/elixir_db eval`.
* Copying a populated database root to a fresh host and pointing `ELIXIR_DB_ROOT` at it yields a working server carrying its host configuration, tokens, and TLS material.
* A non-loopback listener refuses to start without authentication or TLS unless the explicit override is set.
* `bin/elixir_db token` produces token-plus-digest pairs, and authentication compares digests constant-time.
* No ignored failure represents a product defect.
* Every excluded upstream test has a documented scope reason.
* All requirement IDs are mapped to validation.

---

# 22. Version 1 acceptance criteria

## Storage

* Each cleanly closed database is one independently portable file.
* WAL mode is prohibited and no export command is required.
* Normal OS copy, relocation, registration, and reopen of a closed file work.
* Abnormal-shutdown recovery completes before single-file portability is asserted.
* An exclusive ownership lease prevents concurrent cross-process opening.
* The registration manifest is atomically replaceable and reconstructible.
* Derived JSONB, structured indexes, and full-text indexes are rebuildable from authoritative logical state.
* Compact retention is atomic, removes only history at or below the stable frontier, never removes the winning revision of a surviving document, fences stale history replays, and never renumbers the changes sequence.

## Documents

* Documents use the specified I-JSON, binary64, and RFC 8785 model.
* Revisions and winners are deterministic.
* Local stale writes fail and committed retries are idempotent.
* Physical leaves, live leaves, and active conflicts remain distinct.
* Replicated branches and tombstones are preserved until removed by compact retention.
* Explicit conflict resolution atomically keeps one branch or deletes all live branches.

## Transactions and changes

* Acknowledged writes are committed and partial mutations are never visible.
* Bulk writes and replication imports are atomic.
* Structured and full-text indexes change atomically with the winning document.
* Each affected document receives at most one local changes sequence per transaction.
* Changes entries contain the complete final physical-leaf set.
* Configuration, indexes, jobs, checkpoints, and maintenance operations never enter the document changes feed.

## Queries

* Clients cannot submit SQL or backend full-text syntax.
* All field references use RFC 6901 JSON Pointers.
* Structured and full-text indexes use storage-neutral logical contracts.
* `unicode_words_v1` has fixed cross-adapter tokenization semantics.
* Missing and mistyped indexed fields follow the defined extraction rules without rejecting document writes.
* Structured planning and sort compatibility are deterministic.
* Full scans are bounded and incompatible explicit index hints fail.
* Bookmarks are self-contained, query-bound, index-bound, checksummed, and stale after a document mutation.
* Exact relevance scores and cross-adapter relevance order are not portable API guarantees.

## Replication

* Replication transfers document revisions, ancestry, conflicts, and tombstones only.
* Configuration, indexes, jobs, checkpoints, and other local state do not protocol-replicate.
* Handshakes verify endpoint UUID and semantic-version compatibility.
* Complete root-to-leaf chains preserve history IDs and revision IDs and reject dangling ancestry; compact retention may replace them with marked truncated chains, while expired or truncated checkpoints use snapshot/bootstrap transfer.
* Repeated imports and checkpoint retries are safe.
* Checkpoint disagreement causes replay rather than skipped data.
* One-shot replication stops at a captured sequence.
* Continuous replication resumes after restart and bidirectional replication converges after writes stop.

## Protocol

* All Version 1 routes, envelopes, content types, streams, and stable error codes follow Section 17.
* Unknown top-level request fields are rejected.
* Document IDs remain JSON values rather than URI path segments.
* Backend exception names and storage-engine details do not leak through public responses.

## Server state

* Durable logical database and replication state resides in the database file.
* Runtime processes, waiters, and caches can be discarded and reconstructed.
* Per-database admission is bounded and waiting changes requests do not retain a connection or transaction.
* The registration manifest contains routing metadata only and can be rebuilt by re-registering files.
* No central server database is required to restore or interpret an individual file.

## Host configuration, authentication, and transport security

* Host configuration lives in `<database_root>/host.toml`, is created from a template on first run, and is never overwritten once present.
* Copying the database root carries host configuration, authentication tokens, and TLS material to another host with no further setup beyond `ELIXIR_DB_ROOT`.
* A non-loopback listener without authentication and without TLS fails to start unless the operator sets the explicit override.
* Authentication digests are SHA-256, constant-time compared, and the `bin/elixir_db token` command produces token-plus-digest pairs.
* A replication source authenticates to an authenticated target via the endpoint `auth_token`, and outbound calls to `https` endpoints use TLS.
* Authentication failures return `unauthorized` with an identical message regardless of cause.

---

# 23. Deferred items

The following items are excluded from Version 1 and MUST remain tracked.

## `DEF-001` — Large blobs and attachments

Version 1 has no first-class blob API.

A future version MAY add:

* A separate blob file associated with the database.
* A compartment inside a larger container format.
* An append-only value log.
* Content-addressed blob storage.

Future design MUST define:

* Blob identity.
* Revision-manifest hashing.
* Streaming.
* Replication.
* Garbage collection.
* Copy and restore behavior.
* Atomicity between document revisions and blob references.
* Effects on the one-file database requirement.

The future attachment design MUST integrate with stable-frontier compaction:
removing a revision row MUST NOT by itself delete attachment bytes that are
still referenced by a retained winner, conflict leaf, snapshot/bootstrap
page, or durable pending mutation. Attachment garbage collection may use the
same peer fences, but its reference and recovery rules belong to the future
attachment specification.

The decision between one physical file and a database-plus-blob pair MUST be explicit before implementation.

## `DEF-002` — Cap’n Proto

Cap’n Proto is deferred for:

* Client transport.
* Server-to-server transport.
* Database storage.
* Revision serialization.

HTTP/JSON remains the Version 1 protocol.

Cap’n Proto may be reconsidered only after profiling proves that JSON serialization or HTTP framing is a significant bottleneck.

## `DEF-003` — Selective replication

Deferred selective replication features include:

* Document-ID filters.
* JSON selector filters.
* Named filters.
* Partial database replication.
* Replication projections.

Any future filter definition MUST become part of replication identity.

## `DEF-004` — Full Mango compatibility

Deferred query features include:

* `$or`.
* `$not`.
* `$ne`.
* `$nin`.
* Regular expressions.
* Array-element indexes.
* Object comparison.
* Geospatial queries.
* Aggregation pipelines.
* Joins.
* Custom collations.

## `DEF-005` — Map/reduce and executable query code

Version 1 MUST NOT execute:

* JavaScript.
* User-supplied query functions.
* Stored map/reduce code.
* Design-document functions.

## `DEF-006` — PouchDB source port

A line-by-line PouchDB port is rejected as the Version 1 implementation strategy.

PouchDB remains a behavioral reference and supplemental test source.

## `DEF-007` — CouchDB and PouchDB compatibility

Full API, wire-protocol, storage-format, and client compatibility are not Version 1 goals.

A test-only compatibility facade MAY be created if useful for upstream tests, but it MUST NOT define the production API.

## `DEF-008` — Tombstone collection, purge, and automatic pruning

Stable-frontier compact retention (`MAINT-005` through `MAINT-009`) is part of Version 1 and MAY remove history, tombstones, and conflict branches at or below the recorded retention floor.

The following maintenance behaviors remain deferred:

* Time-based tombstone expiration independent of peer expiry and the stable frontier.
* Automatic pruning or compaction without operator configuration or explicit request.
* Revision depth limits beyond the configured `history_depth`.
* Garbage collection policies beyond the stable-frontier gate and peer-expiry rules.

Version 1 retains complete bodies for all retained revisions and removes only what `MAINT-007` permits.

## `DEF-009` — Revision deltas

Version 1 stores complete bodies for every retained revision; compact retention removes whole revision records rather than compressing bodies.

Future delta compression may be an internal optimization, but complete revision semantics MUST remain unchanged.

## `DEF-010` — Live database file copying

Version 1 guarantees ordinary file copying only when the database is offline-portable.

Copying an open or active database without coordination is deferred and not required.

## `DEF-011` — Parallel readers per database

Version 1 serializes operations through one connection.

A bounded read pool may be introduced after profiling demonstrates a meaningful requirement and tests preserve shutdown, transaction, and consistency behavior.

## `DEF-012` — Multi-node ownership

Version 1 does not support two server nodes opening the same database file concurrently.

Future distribution MUST use:

* Explicit ownership assignment.
* Ownership handoff.
* Independent files connected through replication.
* A separately specified coordination system.

## `DEF-013` — Network filesystems

Database files on NFS, SMB, distributed filesystems, or object-storage mounts are unsupported in Version 1.

## `DEF-014` — Custom physical storage engine

Replacing SQLite with CubDB or a custom append-only engine is deferred.

It may be reconsidered only after a demonstrated SQLite limitation justifies ownership of:

* Atomic commits.
* Recovery.
* File format.
* Indexes.
* Compaction.
* Integrity tooling.
* Migration tooling.
* Platform-specific filesystem behavior.

## `DEF-015` — Native encryption at rest

Version 1 MAY rely on host-level or volume encryption.

Native per-database encryption requires a separate design for:

* Key storage.
* Key rotation.
* Recovery.
* Replication.
* Migration.
* File portability.
* Lost-key behavior.

## `DEF-016` — Cross-database transactions

Each database remains an independent transaction and replication unit.

Atomic transactions spanning multiple database files are deferred.

## `DEF-017` — Independent cloning

Copying a database preserves its UUID and represents backup or relocation.

A future clone operation may:

* Copy the complete file.
* Assign a new database UUID.
* Reset replication checkpoints.
* Disable or rewrite replication jobs.
* Preserve document and revision identities.

## `DEF-018` — Schema and file-format migrations

Version 1 does not perform migrations.

A future migration specification MUST be storage-engine neutral at the domain level and MUST define:

* Supported source and target format versions.
* Preservation of database UUIDs, document IDs, revision IDs, revision ancestry, local sequences, checkpoints, and logical index identities.
* Atomic failure behavior.
* Backup and rollback expectations.
* Adapter-specific physical migration steps hidden behind the storage adapter.
* Cross-engine migration behavior when moving away from SQLite.

## `DEF-019` — Extended authentication and authorization

Version 1 provides single bearer-token authentication (`AUTH-001`) and
single-certificate TLS (`TLS-001`). The following are deferred:

* Usernames, roles, and fine-grained authorization.
* Per-database or per-collection credentials.
* Sessions, cookies, refresh tokens, and OAuth.
* Certificate mutual-authentication (mTLS) for clients or replication peers.
* Custom CA bundles, certificate pinning, and automatic certificate rotation.
* A runtime token-revocation endpoint and a dedicated secret-management store.

A future security specification MUST define these concerns independently from document, replication, and storage semantics.

## `DEF-020` — Advanced full-text tokenization and matching

Version 1 supports only the fixed `unicode_words_v1` tokenization strategy and the `all`, `any`, and `phrase` search modes.

Deferred full-text behavior includes:

* Case-sensitive word search.
* Prefix and wildcard query syntax.
* Custom token-character or separator configuration.
* Tokenization based on newer Unicode versions.
* Trigram and substring search.
* Stemming, synonyms, and language-specific analyzers.
* Portable cross-adapter relevance-ranking equivalence.

Any future strategy MUST receive a new storage-neutral strategy identifier and conformance fixture set. Existing `unicode_words_v1` semantics MUST NOT change.

---

# 24. Permanently out of scope

Items in this section are architectural exclusions, not deferred features.

## `OUT-001` — SQLite WAL mode

WAL mode MUST never be used for user database files.

The database portability model requires a cleanly closed database to be represented by one canonical file without required WAL or shared-memory sidecars. The SQLite adapter MUST use rollback-journal `DELETE` mode.

## `OUT-002` — Mandatory export workflow

Database portability MUST never depend on an export command, conversion step, or server-generated portable format.

A convenience copy command MAY be added, but ordinary operating-system copying of a cleanly closed canonical file MUST remain sufficient.

---

# 25. Final Version 1 definition

Version 1 SHALL be:

> A stateless Elixir document-database server packaged as an OTP release, with frozen storage-neutral domain and HTTP contracts plus a compartmentalized SQLite storage adapter. Each logical database is one SQLite file using rollback-journal `DELETE` mode. The system stores canonical revisioned JSON documents, retains the winning revision and configured ancestry of each surviving document, and permits explicit stable-frontier compact retention to remove history, conflict branches, tombstones, and old changes entries at or below a bounded retention floor. It distinguishes active conflicts, provides atomic conflict resolution, reports explicit history gaps, and performs checkpointed Couch-inspired replication by transferring complete revision chains or marked truncated chains, with snapshot/bootstrap for peers that cross a retention boundary. Protocol replication transfers document revision state only; database configuration, logical indexes, replication jobs, checkpoints, peer leases, and maintenance state remain local to each database file. Structured queries use RFC 6901 field references and deterministic bounded planning. Full-text search uses the fixed storage-neutral `unicode_words_v1` contract, implemented by the Version 1 SQLite adapter through FTS5. A cleanly closed database file can be copied, moved, renamed, backed up, and restored with ordinary operating-system file operations without an export command.

The server provides explicit database registration, exclusive per-file ownership, bounded per-database admission, non-blocking changes waiters, supervised replication workers, the versioned `/v1` HTTP protocol, optional bearer-token authentication, and optional TLS. Production hosts run the assembled release (`bin/elixir_db`); Mix is reserved for development and CI.

Host configuration, authentication tokens, and TLS material live alongside the database files inside the database root in a single editable TOML file, so copying the root directory relocates a complete, working server. The registration manifest is reconstructible non-authoritative routing metadata. The database file remains the complete durable unit.

SQLite provides Version 1 physical transactions and derived indexes behind the storage adapter. The project-owned contracts define document, revision, conflict, changes, query, full-text, replication, configuration, lifecycle, and protocol behavior independently of SQLite.
