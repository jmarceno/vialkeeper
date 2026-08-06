# Elixir Replicated Document Database

## Version 1 Detailed Implementation Plan

**Status:** Ready for implementation
**Authoritative architecture:** `Arch V2`
**Delivery model:** One integration branch and one final pull request
**Primary runtime:** Elixir 1.20.2 on Erlang/OTP 29.0.4
**Primary storage adapter:** SQLite through Exqlite

---

# 1. Purpose and authority

This document translates `Arch V2` into an executable implementation sequence.

`Arch V2` remains authoritative for externally observable behavior. This plan defines:

* Project structure.
* Module boundaries.
* OTP supervision topology.
* Storage-adapter seams.
* Initial SQLite schema.
* Call paths.
* Type-checking strategy.
* Test architecture.
* Parallel work tracks.
* Phase gates.
* Integration order.

An implementation agent MUST NOT reinter*pre*t an architectural requirement because a different implementation appears easier. Any contradiction discovered during implementation MUST be resolved in `Arch V2` before code proceeds.

The implementation SHALL be delivered through one final pull request. Parallel agents MAY work in separate branches or worktrees, but their work MUST merge into a single integration branch before review.

---

# 2. Guiding implementation principles

## 2.1 Use the BEAM as the concurrency boundary

Concurrency SHALL be organized around independent database and replication processes rather than shared mutable state.

The main units of isolation are:

* One supervised runtime per open database.
* One SQLite-owning process per open database.
* One supervised state-machine process per active replication.
* One process per HTTP request or stream, supplied by the HTTP server.
* Short-lived supervised tasks for bounded cross-database work.

Concurrency across databases is expected and encouraged.

Operations inside one database remain serialized because Version 1 deliberately uses one SQLite connection per database.

## 2.2 Prefer crash isolation over defensive global state

A failure in one database runtime MUST NOT terminate:

* Other database runtimes.
* Unrelated replication workers.
* The HTTP listener.
* The registration catalog.

A failure in one replication worker MUST NOT close either endpoint database.

Unexpected failures SHALL terminate the smallest responsible process and allow its supervisor to decide whether to restart it.

Non-retryable domain failures, such as an unsupported database format, MUST be returned as data and MUST NOT trigger restart loops.

## 2.3 Keep domain semantics pure

Revision calculation, winner selection, conflict detection, query normalization, checkpoint reconciliation, and protocol validation MUST be implemented as deterministic pure functions wherever possible.

Processes coordinate state and failures. They MUST NOT become the only place where business rules exist.

## 2.4 Keep storage details behind one boundary

Only the SQLite adapter and its tests may know about:

* SQL.
* Table names.
* Exqlite statements.
* SQLite row IDs.
* JSON1/JSONB details.
* Expression-index syntax.
* FTS5 tables and queries.
* SQLite error codes.

The domain, HTTP, and replication layers operate only on project-owned structs and result tuples.

## 2.5 Build correctness around a small number of strong suites

The test suite SHALL avoid duplicating the same behavior in many low-value unit tests.

The implementation is organized around six strong test pillars:

1. Pure contract and property tests.
2. Storage-adapter conformance tests.
3. Runtime concurrency and supervision tests.
4. Replication state-machine and fault tests.
5. HTTP contract tests.
6. End-to-end portability and convergence tests.

Private helper functions do not require dedicated tests when their behavior is fully covered through one of these suites.

---

# 3. Language and runtime baseline

## 3.1 Versions

Pin the development and CI toolchain to:

```text
Elixir 1.20.2
Erlang/OTP 29.0.4
```

Use a checked-in `mise.toml`, `.tool-versions`, or equivalent repository-standard toolchain file.

The release pipeline MUST record:

* Elixir version.
* OTP version.
* Exqlite version.
* SQLite runtime version.
* SQLite compile options.
* Git commit.

A secondary CI smoke job MAY run on the newest supported OTP 28 maintenance release, but OTP 29 is the authoritative Version 1 development target.

## 3.2 Elixir 1.20 gradual typing strategy

Elixir 1.20 gradually type-checks all programs through compiler inference, without requiring user annotations.

Version 1 MUST take advantage of this by writing code that exposes useful information to the compiler:

* Use structs instead of unconstrained maps after input validation.
* Use separate function clauses for distinct variants.
* Use precise patterns and guards.
* Use tagged tuples instead of overloaded return values.
* Avoid broad catch-all clauses until all valid variants have explicit clauses.
* Avoid passing arbitrary keyword lists through domain layers.
* Convert external `dynamic()` data into validated project structs at the boundary.
* Keep functions short enough that inferred narrowing remains visible.
* Avoid `Map.get/3` for required domain fields after construction.

Compiler warnings MUST fail CI:

```text
mix compile --warnings-as-errors
```

No type warning may be suppressed without a written explanation in the source and an issue tracking its removal.

## 3.3 Typespecs and Dialyzer

Current Elixir set-theoretic inference and Erlang-style typespecs are separate systems.

Use `@type`, `@opaque`, `@callback`, and `@spec` for:

* Public domain structs.
* Storage-adapter callbacks.
* Endpoint-adapter callbacks.
* Public library functions.
* Complex result tuples.
* Documentation generated by ExDoc.

Add Dialyxir as a development and test dependency and require a clean Dialyzer run.

Typespecs MUST NOT be presented as enforcement by Elixir’s new compiler type system. They are documentation and a secondary static-analysis layer.

Dialyzer ignore files SHOULD remain empty. Any required ignore entry MUST include the warning text, rationale, owner, and removal condition.

## 3.4 Compiler configuration

Configure:

```elixir
elixirc_options: [module_definition: :interpreted]
```

Keep this option only if the initial compile benchmark confirms that it improves clean and incremental compilation without tool incompatibility. It does not change generated BEAM files and is intended only to reduce compilation work.

Define repository checks:

```text
mix check.fast
mix check.full
```

`check.fast` SHALL run:

```text
mix format --check-formatted
mix compile --warnings-as-errors
mix test --warnings-as-errors --exclude slow
```

`check.full` SHALL run:

```text
mix format --check-formatted
mix compile --warnings-as-errors
mix xref graph --format cycles --label compile-connected --fail-above 0
mix test --warnings-as-errors
mix dialyzer
```

---

# 4. Project form and dependencies

## 4.1 Single OTP application

Use one supervised Mix application, not an umbrella.

Suggested application and module namespace:

```text
application: :elixir_db
namespace: ElixirDB
```

An umbrella would add release, configuration, dependency, and test boundaries without providing a deployment benefit in Version 1.

Internal namespaces provide sufficient isolation.

## 4.2 Runtime dependencies

Use a deliberately small dependency set:

| Dependency  | Purpose                                  |
| ----------- | ---------------------------------------- |
| `exqlite`   | SQLite access and bundled SQLite runtime |
| `plug`      | HTTP connection and routing contract     |
| `bandit`    | Supervised Plug HTTP server              |
| `req`       | Remote replication HTTP client           |
| `telemetry` | Low-overhead runtime instrumentation     |

Development and test dependencies:

| Dependency    | Purpose                          |
| ------------- | -------------------------------- |
| `stream_data` | Property and model-based testing |
| `dialyxir`    | Dialyzer Mix integration         |

Pin Exqlite to the exact version validated by the project because its bundled SQLite runtime is part of the database compatibility baseline.

The initial expected versions are:

```text
exqlite 0.39.0
plug 1.20.3
bandit 1.12.4
req 0.7.2
telemetry 1.4.2
stream_data 1.4.0
dialyxir 1.4.7
```

The lockfile is authoritative. Version changes require the full adapter, protocol, and portability suites.

## 4.3 Explicit non-dependencies

Do not add these in Version 1 without a proven requirement:

* Phoenix.
* Ecto.
* Broadway.
* GenStage.
* Oban.
* Mox.
* A generic validation framework.
* A runtime type-checking framework.
* A second JSON library.
* A CouchDB client.
* A PouchDB port.

Use behaviours and real in-process test components instead of broad mocking frameworks.

## 4.4 JSON implementation

Use Elixir’s built-in `JSON` module for ordinary protocol encoding and as the parser engine.

Do not use its default object decoder directly for authoritative input because duplicate keys must be rejected.

Implement:

```text
ElixirDB.JSON.StrictDecoder
ElixirDB.JSON.Canonical
ElixirDB.JSON.Pointer
```

`StrictDecoder` SHALL use `JSON.decode/3` custom decoder callbacks to:

* Detect duplicate object keys before map construction.
* Enforce the binary64 number model.
* Reject unsafe integers.
* Reject overflow and non-zero underflow.
* Enforce nesting and size limits.
* Preserve strings exactly.

`Canonical` SHALL be a project-owned RFC 8785 serializer.

Do not rely on an unverified third-party JCS implementation for revision identity. Existing implementations may be used as research references only.

The canonicalizer MUST pass:

* RFC 8785 vectors.
* RFC 8785 number examples.
* Official reference implementation fixtures.
* Negative-zero tests.
* UTF-16 property ordering tests.
* Cross-runtime fixture verification generated by at least one independent implementation.

---

# 5. Repository structure

Use this initial structure:

```text
mix.exs
mise.toml
config/
  config.exs
  dev.exs
  test.exs
  runtime.exs

lib/elixir_db.ex
lib/elixir_db/application.ex
lib/elixir_db/config.ex
lib/elixir_db/error.ex
lib/elixir_db/telemetry.ex

lib/elixir_db/domain/
  database_info.ex
  document.ex
  revision.ex
  leaf.ex
  change.ex
  index_definition.ex
  query.ex
  bookmark.ex
  replication_endpoint.ex
  replication_job.ex
  checkpoint.ex

lib/elixir_db/json/
  strict_decoder.ex
  canonical.ex
  pointer.ex

lib/elixir_db/revisions/
  id.ex
  tree.ex
  winner.ex
  conflict_resolution.ex

lib/elixir_db/storage/
  adapter.ex
  commands.ex
  results.ex

lib/elixir_db/storage/sqlite/
  adapter.ex
  connection.ex
  schema.ex
  statements.ex
  documents.ex
  revisions.ex
  changes.ex
  local_records.ex
  replication_jobs.ex
  structured_indexes.ex
  full_text_indexes.ex
  query_compiler.ex
  integrity.ex

lib/elixir_db/runtime/
  database_catalog.ex
  database_registry.ex
  database_supervisor.ex
  database_runtime_supervisor.ex
  database_owner.ex
  database_admission.ex
  change_notifier.ex
  file_lease.ex
  registration_manifest.ex

lib/elixir_db/documents.ex
lib/elixir_db/changes.ex

lib/elixir_db/query/
  selector.ex
  normalizer.ex
  planner.ex
  projection.ex
  bookmark_codec.ex
  full_text.ex

lib/elixir_db/replication/
  endpoint.ex
  local_endpoint.ex
  remote_endpoint.ex
  remote_transport.ex
  id.ex
  checkpoint_reconciler.ex
  worker.ex
  worker_supervisor.ex
  worker_registry.ex
  job_manager.ex

lib/elixir_db/http/
  router.ex
  request.ex
  response.ex
  error_mapper.ex
  body_reader.ex
  stream_writer.ex
  routes/
    databases.ex
    documents.ex
    changes.ex
    indexes.ex
    replications.ex
    replication_wire.ex

priv/sqlite/
  schema_v1.sql

priv/fixtures/
  canonical_json/
  revision_ids/
  tokenization/
  protocol/

 test/
  support/
    temp_database.ex
    adapter_case.ex
    model_generators.ex
    fault_adapter.ex
    test_server.ex
    eventual.ex
  contract/
  storage_adapter/
  runtime/
  replication/
  http/
  end_to_end/
```

Module files may be split further when they exceed one clear responsibility. Do not create generic `Utils`, `Helpers`, or `Common` modules.

---

# 6. Domain representation and typing rules

## 6.1 Struct construction

Every domain struct MUST:

* Use `@enforce_keys` for required fields.
* Expose a `new/1` or `from_wire/1` constructor.
* Return `{:ok, struct}` or `{:error, ElixirDB.Error.t()}`.
* Reject unknown fields when constructed from external input.
* Contain only validated values.
* Avoid storing backend-specific state.

After construction, internal code may pattern-match directly on the struct without repeating boundary validation.

## 6.2 Result convention

Use these return forms consistently:

```elixir
{:ok, value}
{:error, %ElixirDB.Error{}}
```

Use `:ok` only when no value is meaningful.

Do not return bare strings, atoms, SQLite codes, or exceptions as domain errors.

Unexpected programmer errors SHOULD raise and crash the responsible process. Expected validation, conflict, overload, unavailable, and protocol failures MUST return structured errors.

## 6.3 Error struct

`ElixirDB.Error` SHALL contain:

```text
code
message
retryable
http_status
details
cause
```

`cause` is internal and MUST never be serialized directly.

Every error constructor SHALL be centralized so code and HTTP mappings cannot drift.

## 6.4 Command structs

Do not send arbitrary tuples or maps to `DatabaseOwner`.

Define storage-neutral command structs under `ElixirDB.Storage.Commands`, including:

```text
CreateDocument
PutDocument
DeleteDocument
ResolveConflict
BulkWrite
GetDocument
GetRevision
ReadChanges
DiffRevisions
GetRevisionChains
ImportRevisionChains
GetCheckpoint
PutCheckpoint
CreateIndex
DeleteIndex
RebuildIndex
ExecuteQuery
ExplainQuery
IntegrityCheck
```

This improves compiler inference, documentation, logging, and fault-test targeting.

---

# 7. OTP supervision and runtime topology

## 7.1 Application tree

Use this application-level tree:

```text
ElixirDB.Supervisor (:one_for_one)
├── ElixirDB.Runtime.DatabaseRegistry
├── ElixirDB.Runtime.DatabaseSupervisor (DynamicSupervisor)
├── ElixirDB.Replication.WorkerRegistry
├── ElixirDB.Replication.WorkerSupervisor (DynamicSupervisor)
├── ElixirDB.TaskSupervisor
├── ElixirDB.Runtime.DatabaseCatalog
└── Bandit
```

`DatabaseRegistry` SHALL use unique keys by database UUID.

`WorkerRegistry` SHALL use unique keys by replication ID.

The HTTP listener starts last so the catalog and supervisors are ready before accepting traffic.

## 7.2 Per-database tree

Each open database SHALL run under:

```text
DatabaseRuntimeSupervisor (:rest_for_one)
├── FileLease
├── DatabaseOwner
├── DatabaseAdmission
└── ChangeNotifier
```

Ordering is intentional:

* If `FileLease` fails, every later child restarts.
* If `DatabaseOwner` fails, admission state and waiters restart with it.
* If only `ChangeNotifier` fails, the database connection remains valid and only waiters reconnect.

The database runtime child spec SHALL use `restart: :transient`:

* Unexpected crashes restart it.
* An intentional close exits with `:shutdown` and remains closed.
* A non-retryable open failure marks the registration unavailable instead of creating a restart loop.

## 7.3 Database owner

`DatabaseOwner` is the only process allowed to hold the SQLite adapter handle.

Responsibilities:

* Open and validate the database.
* Apply pragmas.
* Prepare fixed statements.
* Execute one command at a time.
* Start and end transactions.
* Publish committed sequence changes.
* Finalize statements and close the adapter.

`DatabaseOwner` MUST NOT:

* Wait for network responses.
* Sleep for retry delays.
* Hold long-poll clients.
* Run replication state machines.
* Parse HTTP requests.
* Own registration metadata.

## 7.4 Bounded admission

Implement admission using an atomics-backed token counter owned by `DatabaseAdmission`.

`DatabaseAdmission` registers an opaque token reference in `DatabaseRegistry` metadata.

Call flow:

```text
caller
→ Registry lookup
→ atomic try_acquire
→ GenServer.call(DatabaseOwner, command, deadline)
→ atomic release in after block
```

When no token is available, return `database_overloaded` without sending a message to `DatabaseOwner`.

This bounds the owner mailbox by the configured in-flight limit instead of placing an unbounded queue in another process mailbox.

The initial default SHOULD be conservative, such as 128 admitted operations per database, and remain host-configurable.

## 7.5 Cross-process file lease

Implement the lease with a transient companion SQLite lock file:

```text
<database-file>.lease
```

`FileLease` SHALL:

1. Open the lease SQLite file.
2. Set zero busy timeout.
3. Start and retain an exclusive transaction for its lifetime.
4. Fail immediately with `database_in_use` when the transaction cannot be acquired.
5. Roll back and close on termination.

This uses SQLite’s cross-platform process locking while keeping lease state non-authoritative.

The lease file may remain after a crash. Its existence alone does not indicate ownership; the live exclusive transaction does.

Portability tests MUST prove that the database file remains complete without the lease file.

## 7.6 Change notifier

`ChangeNotifier` maintains transient subscribers and the latest published sequence.

Use monitors so subscribers are removed automatically.

The notifier MUST NOT retain change bodies. It only wakes waiters, which read durable entries through `DatabaseOwner`.

Implement the race-free subscribe-and-recheck sequence from `Arch V2`.

## 7.7 Replication worker

Implement `ElixirDB.Replication.Worker` using `:gen_statem`, not a large `GenServer` callback.

States:

```text
idle
handshake
read_changes
diff
fetch_chains
import
checkpoint_target
checkpoint_source
waiting
backoff
completed
failed
```

The explicit state machine SHALL make transition, cancellation, and fault tests direct and deterministic.

The worker MUST hold no SQLite handle. It calls endpoint behaviours.

## 7.8 Bounded parallel tasks

Use `Task.Supervisor.async_stream_nolink/4` for bounded work across independent databases, including:

* Startup inspection of registered files.
* Multi-database integrity checks.
* Test setup and convergence verification.

Never use unbounded `Task.async_stream` defaults for user-controlled collections.

Do not use `PartitionSupervisor` in Version 1 unless profiling demonstrates a shared-process bottleneck. Database UUIDs already provide natural process partitioning.

---

# 8. Storage adapter contract

## 8.1 Adapter module

Define one public behaviour:

```text
ElixirDB.Storage.Adapter
```

The adapter handle SHALL be an opaque adapter-owned term.

Callback groups:

### Lifecycle

```text
create(path, creation_options)
open(path, open_options)
close(handle)
identity(handle)
integrity_check(handle, options)
```

### Documents and revisions

```text
get_document(handle, request)
get_revision(handle, request)
apply_local_mutation(handle, command)
apply_bulk_mutation(handle, command)
resolve_conflict(handle, command)
```

### Changes and replication primitives

```text
read_changes(handle, request)
diff_revisions(handle, request)
get_revision_chains(handle, request)
import_revision_chains(handle, request)
```

### Local state

```text
get_local_record(handle, namespace, key)
put_local_record_cas(handle, request)
list_replication_jobs(handle)
put_replication_job(handle, job)
delete_replication_job(handle, job_id)
```

### Indexes and queries

```text
create_index(handle, definition)
delete_index(handle, index_id)
rebuild_index(handle, index_id)
list_indexes(handle)
execute_query(handle, query)
explain_query(handle, query)
```

All callbacks return project-owned structs and `ElixirDB.Error` values.

## 8.2 Conformance suite

The adapter suite MUST be executable against any adapter module using a shared test macro:

```text
use ElixirDB.Storage.AdapterCase, adapter: ElixirDB.Storage.SQLite.Adapter
```

The suite SHALL own all storage-neutral expectations.

SQLite-specific tests SHALL cover only physical details, such as pragmas, SQL constraints, FTS5 mapping, and crash recovery.

## 8.3 No transaction callback leakage

Do not expose a generic callback such as:

```text
transaction(handle, function)
```

That would allow domain code to depend on adapter handles and transaction mechanics.

Expose complete atomic domain commands instead.

---

# 9. Initial SQLite physical design

## 9.1 File markers

Use:

```text
PRAGMA application_id = 0x45584442
PRAGMA user_version = 1
```

`0x45584442` corresponds to the project marker `EXDB`.

The logical metadata row independently records all format and algorithm versions.

## 9.2 Table policy

Use SQLite `STRICT` tables for every fixed internal table.

Use:

* Foreign keys.
* Unique constraints.
* `CHECK` constraints for Boolean fields and generations.
* Explicit `NOT NULL` declarations.
* Canonical JSON text for authoritative JSON values.

Do not use `AUTOINCREMENT`.

Documents are not physically removed in Version 1, so a normal `INTEGER PRIMARY KEY` provides a stable adapter-private document key.

## 9.3 Fixed tables

### `db_meta`

One row containing:

```text
id = 1
database_uuid
file_format_version
logical_schema_version
revision_algorithm_version
canonicalization_version
replication_protocol_major
current_sequence
created_at
config_json
```

### `documents`

```text
doc_key INTEGER PRIMARY KEY
document_id TEXT UNIQUE NOT NULL
winning_revision TEXT
winning_body_json TEXT
winning_deleted INTEGER NOT NULL
update_sequence INTEGER NOT NULL
```

Allow a temporary null winner only inside the transaction that creates the initial revision. Committed state MUST always satisfy the document invariants.

### `revisions`

```text
doc_key INTEGER NOT NULL
revision_id TEXT NOT NULL
generation INTEGER NOT NULL
parent_revision TEXT
digest TEXT NOT NULL
deleted INTEGER NOT NULL
body_json TEXT
insertion_sequence INTEGER NOT NULL
is_leaf INTEGER NOT NULL
PRIMARY KEY (doc_key, revision_id)
```

Constraints:

* Generation is positive.
* Root revisions have no parent and generation 1.
* Tombstones have null body.
* Live revisions have a body.
* Parent references belong to the same document.

Indexes:

```text
(doc_key, is_leaf)
(doc_key, is_leaf, deleted, generation, revision_id)
(doc_key, parent_revision)
```

### `changes`

```text
sequence INTEGER PRIMARY KEY
doc_key INTEGER NOT NULL
document_id TEXT NOT NULL
winning_revision TEXT NOT NULL
winning_deleted INTEGER NOT NULL
leaf_set_json TEXT NOT NULL
origin TEXT NOT NULL
```

`leaf_set_json` is canonical JSON in the storage-neutral ordered form.

### `local_records`

```text
namespace TEXT NOT NULL
record_key TEXT NOT NULL
record_version INTEGER NOT NULL
value_json TEXT NOT NULL
PRIMARY KEY (namespace, record_key)
```

### `replication_jobs`

```text
job_id TEXT PRIMARY KEY
definition_json TEXT NOT NULL
enabled INTEGER NOT NULL
last_diagnostic_json TEXT
```

Runtime state is not stored here.

### `index_definitions`

```text
index_id TEXT PRIMARY KEY
name TEXT UNIQUE NOT NULL
index_type TEXT NOT NULL
definition_digest TEXT NOT NULL
definition_json TEXT NOT NULL
lifecycle_state TEXT NOT NULL
adapter_metadata_json TEXT NOT NULL
```

## 9.4 Transaction policy

All writes SHALL use `BEGIN IMMEDIATE`.

The adapter MUST allocate sequences through one metadata update inside the same transaction.

For multiple documents in one transaction:

* Sort affected document IDs lexicographically before sequence allocation.
* Allocate one consecutive sequence per affected document.
* Produce deterministic changes ordering independent of request map ordering.

Prepared statements are owned and reused only by `DatabaseOwner`.

Dynamic DDL for indexes remains adapter-internal and is validated before interpolation.

## 9.5 JSONB decision

Do not store derived JSONB in the first correctness implementation.

Start with canonical JSON text and expression indexes.

After Phase 7 passes, run an explicit benchmark comparing text and JSONB for:

* Indexed equality queries.
* Residual predicates.
* Index creation.
* Large document reads.
* Write amplification.

Add JSONB only when it produces a material benefit and all conformance tests remain unchanged.

## 9.6 FTS5 design

Create one contentless-delete FTS5 virtual table per logical full-text index.

Physical table names SHALL derive only from the index digest, for example:

```text
fts_<first-24-hex-digest-characters>
```

Never interpolate the client-provided index name.

Use the document key as FTS row ID.

Before implementing the complete FTS layer, create a focused SQLite feasibility test proving:

* Contentless-delete creation.
* Insert, update, and delete behavior.
* Transaction rollback.
* Rebuild inside one transaction.
* `unicode61` tokenization mappings required by `unicode_words_v1`.
* Deterministic BM25 ordering with document-ID tie-breaking.

If contentless-delete cannot satisfy atomic rebuild behavior on the pinned SQLite version, use an ordinary contentless table with explicit FTS delete commands. Do not alter the logical search contract.

---

# 10. HTTP implementation

## 10.1 Server and routing

Use Bandit supervised by the application supervisor and a plain Plug router.

Do not introduce Phoenix.

Split route modules by resource and forward from the root router.

## 10.2 Strict request decoding

Do not use a default JSON body parser for Version 1 requests.

`ElixirDB.HTTP.BodyReader` SHALL:

1. Enforce content type.
2. Read the body in bounded chunks.
3. Stop when the host request limit is exceeded.
4. Decode through `StrictDecoder`.
5. Return `invalid_request` or `payload_too_large` through the standard envelope.

Route modules convert decoded maps into domain request structs immediately.

## 10.3 Response encoding

Use built-in `JSON.encode_to_iodata!/1` for ordinary responses.

Canonical JSON is required only where `Arch V2` requires identity, hashing, persisted logical JSON, or bookmark checksums.

## 10.4 Remote replication client

Use Req behind:

```text
ElixirDB.Replication.RemoteTransport
```

Disable Req’s automatic retry layer for replication requests:

```text
retry: false
```

The replication state machine owns retry classification, delay, and cancellation. Two independent retry layers would make checkpoint and fault behavior difficult to reason about.

Set explicit:

* Connect timeout.
* Receive timeout.
* Overall operation deadline.
* Maximum response body size.
* Accepted content type.

Do not use raising Req functions in production paths.

## 10.5 Streaming changes

The Bandit request process owns the client socket and stream lifecycle.

It SHALL:

1. Read currently available changes.
2. Write NDJSON `change` events using `Plug.Conn.chunk/2`.
3. Emit `caught_up`.
4. Subscribe to `ChangeNotifier` without retaining a database admission token.
5. Emit heartbeats while idle.
6. Reacquire admission only for bounded changes reads.
7. Stop on disconnect, close, deadline, or error.

A slow client MUST not block `DatabaseOwner`; it only blocks its own request process.

---

# 11. Observability

Define project telemetry events before implementing runtime logic.

Use prefixes such as:

```text
[:elixir_db, :database, :open]
[:elixir_db, :database, :command]
[:elixir_db, :database, :overload]
[:elixir_db, :changes, :read]
[:elixir_db, :query, :execute]
[:elixir_db, :index, :build]
[:elixir_db, :replication, :batch]
[:elixir_db, :replication, :checkpoint]
[:elixir_db, :http, :request]
```

Measurements SHOULD include duration and bounded counts.

Metadata MAY include:

* Database UUID.
* Command type.
* Logical index ID.
* Replication ID.
* Error code.

Never include:

* Document bodies.
* Search text.
* Revision bodies.
* Complete remote URLs containing private path data.

Logging SHALL use Logger metadata and stable event names, not ad hoc interpolated messages.

---

# 12. Test architecture

## 12.1 Pillar 1 — Pure contracts and properties

Location:

```text
test/contract/
```

Covers:

* Strict JSON decoding.
* JCS canonicalization.
* Revision IDs.
* Revision trees and winner selection.
* Active conflicts.
* Conflict resolution.
* JSON Pointer parsing.
* Query normalization.
* Replication IDs.
* Checkpoint reconciliation.
* Bookmark encoding.
* Tokenization fixtures.

Use StreamData for generated revision trees, JSON values, pointers, and operation sequences.

These tests SHALL run asynchronously and without SQLite.

## 12.2 Pillar 2 — Adapter conformance

Location:

```text
test/storage_adapter/
```

Run every storage-neutral storage behavior against a real temporary SQLite file.

One test file may exercise a complete behavior family rather than one file per callback.

Required families:

```text
lifecycle
mutations
conflicts
changes
revision_transfer
checkpoints
jobs
structured_indexes
full_text_indexes
integrity
portability
```

## 12.3 Pillar 3 — Runtime and supervision

Location:

```text
test/runtime/
```

Use actual supervisors and monitored processes.

Test:

* Owner uniqueness.
* Lease exclusion across OS processes.
* Admission saturation and release.
* Owner crash and restart.
* Waiter termination.
* Catalog recovery.
* Registration atomicity.
* Independent database isolation.

Do not use arbitrary sleeps. Use monitors, barriers, telemetry events, or explicit test hooks.

## 12.4 Pillar 4 — Replication state machine

Location:

```text
test/replication/
```

Implement a thin fault-injecting endpoint wrapper around real endpoint implementations.

Inject failure before and after each state transition:

```text
handshake
changes read
diff
chain fetch
import commit
target checkpoint
source checkpoint
wait subscription
```

The core assertion is small:

> Every injected retryable failure may repeat work but may never skip a committed revision.

Use generated operation histories for convergence properties.

## 12.5 Pillar 5 — HTTP contract

Location:

```text
test/http/
```

Start an actual Bandit server on an ephemeral port.

Use Req as the client.

Cover:

* Every method/path pair.
* Request and response schemas.
* Unknown-field rejection.
* Content types.
* Status and error mapping.
* Bulk semantics.
* NDJSON streams.
* Remote replication wire calls.

Do not mock Plug or Req.

## 12.6 Pillar 6 — End-to-end scenarios

Location:

```text
test/end_to_end/
```

Keep this suite small and expensive.

Required scenarios:

1. Full local lifecycle and offline copy.
2. Two-server remote convergence with restart.
3. Conflict creation and both resolution modes.
4. Structured and full-text query correctness after replication.
5. Crash recovery with hot rollback journal.
6. Copied database registration and derived-index rebuild.

Mark expensive cases `@tag :slow`.

## 12.7 Test determinism

Every randomized failure MUST report:

* Seed.
* Generated operation history.
* Database paths.
* Fault transition.

CI SHALL preserve failing temporary databases as artifacts when possible.

Use virtual or injected clocks for retry and diagnostic logic. Do not make tests wait for real exponential backoff.

---

# 13. Parallel-agent execution protocol

## 13.1 Integration ownership

Assign one integrator for the complete effort.

Only the integrator may change these shared files after Phase 0 freezes them:

```text
Arch V2
ElixirDB.Error
ElixirDB.Storage.Adapter
core domain structs
protocol fixture schemas
application supervision topology
```

Agents propose shared-contract changes through a handoff note rather than editing those files independently.

## 13.2 Worktree model

Each parallel track receives:

* One branch or worktree.
* Explicit file ownership.
* Required input contracts.
* Required output tests.
* A handoff checklist.

Each track MUST finish with:

```text
mix check.fast
```

The integrator runs `mix check.full` after merging tracks.

## 13.3 Merge discipline

Within one phase:

1. Merge contract or seam changes first.
2. Rebase implementation tracks onto those contracts.
3. Merge tests before or with implementations.
4. Run the phase gate.
5. Do not begin dependent phase work until the gate passes.

Agents MAY start research or fixture preparation for the next phase, but MUST NOT implement against unfrozen seams.

## 13.4 Parallelism labels

This plan uses:

* **SERIAL** — must complete before dependent work starts.
* **PARALLEL** — safe for separate agents with disjoint file ownership.
* **INTEGRATION** — integrator-only merge and gate work.

---

# 14. Phase 0 — Bootstrap and frozen contracts

## Objective

Create a compiling project and freeze every cross-track contract before storage and process implementations diverge.

## SERIAL — Repository bootstrap

Create:

```text
mix.exs
config files
application module
formatter config
CI workflow
check aliases
version pinning
```

Add dependencies and verify the pinned SQLite version and compile options through a temporary Mix task.

## PARALLEL Track A — Domain and errors

Own:

```text
lib/elixir_db/error.ex
lib/elixir_db/domain/
```

Implement structs, constructors, typespecs, and stable errors.

## PARALLEL Track B — JSON and revision identity

Own:

```text
lib/elixir_db/json/
lib/elixir_db/revisions/id.ex
priv/fixtures/canonical_json/
priv/fixtures/revision_ids/
```

Implement strict decoding, canonicalization, pointers, and revision vectors.

## PARALLEL Track C — Pure revision and conflict model

Own:

```text
lib/elixir_db/revisions/tree.ex
lib/elixir_db/revisions/winner.ex
lib/elixir_db/revisions/conflict_resolution.ex
```

No process or storage code.

## PARALLEL Track D — Adapter and endpoint behaviours

Own:

```text
lib/elixir_db/storage/adapter.ex
lib/elixir_db/storage/commands.ex
lib/elixir_db/storage/results.ex
lib/elixir_db/replication/endpoint.ex
```

Define callbacks and opaque boundary types.

## PARALLEL Track E — Test infrastructure

Own:

```text
test/support/
test/contract/
```

Build generators, fixtures, temporary directories, and contract macros.

## INTEGRATION

Freeze:

* Struct shapes.
* Error registry.
* Adapter callbacks.
* Endpoint callbacks.
* JSON fixtures.
* Revision and conflict behavior.

## Key tests

* All official JCS vectors.
* Duplicate JSON key rejection.
* Binary64 boundary behavior.
* Generated revision-tree insertion-order independence.
* Conflict-resolution model properties.
* Compiler and Dialyzer clean baseline.

## Exit gate

```text
mix check.full
```

No storage or HTTP code starts before this gate passes.

---

# 15. Phase 1 — SQLite lifecycle and supervised database runtimes

## Objective

Open, own, close, copy, register, and recover Version 1 database files through supervised runtimes.

## SERIAL — SQLite feasibility spike

Before broad implementation, prove in executable tests:

* Required pragmas are accepted.
* `STRICT` tables work.
* FTS5 is present.
* Transactional DDL works as expected.
* The companion lease database excludes another OS process and releases after process death.
* Hot rollback-journal recovery works.

Failures here must adjust physical implementation choices before other tracks depend on them.

## PARALLEL Track A — SQLite lifecycle

Own:

```text
lib/elixir_db/storage/sqlite/connection.ex
lib/elixir_db/storage/sqlite/schema.ex
lib/elixir_db/storage/sqlite/adapter.ex
priv/sqlite/schema_v1.sql
```

Implement create, open, validate, close, and identity.

## PARALLEL Track B — Runtime supervision

Own:

```text
lib/elixir_db/runtime/database_supervisor.ex
lib/elixir_db/runtime/database_runtime_supervisor.ex
lib/elixir_db/runtime/database_owner.ex
lib/elixir_db/runtime/database_admission.ex
lib/elixir_db/runtime/change_notifier.ex
lib/elixir_db/runtime/file_lease.ex
```

Use a temporary adapter stub until Track A merges.

## PARALLEL Track C — Catalog and manifest

Own:

```text
lib/elixir_db/runtime/database_catalog.ex
lib/elixir_db/runtime/database_registry.ex
lib/elixir_db/runtime/registration_manifest.ex
```

Keep catalog reads in ETS or Registry metadata so the catalog GenServer is not a request bottleneck.

## PARALLEL Track D — Lifecycle conformance tests

Own:

```text
test/storage_adapter/lifecycle_test.exs
test/runtime/
test/end_to_end/offline_copy_test.exs
```

## Call stack — Open database

```text
HTTP or internal caller
→ DatabaseCatalog.open(uuid)
→ registration lookup
→ DynamicSupervisor.start_child(DatabaseRuntimeSupervisor)
→ FileLease acquires companion lease
→ DatabaseOwner opens SQLite adapter
→ adapter validates application_id, versions, pragmas, integrity
→ Admission and ChangeNotifier start
→ runtime registered ready
→ caller receives database info
```

## Call stack — Close database

```text
caller
→ DatabaseCatalog.close(uuid)
→ runtime checks close eligibility
→ stop admission
→ terminate waiters
→ cancel or reject active work
→ DatabaseOwner finalizes and closes SQLite
→ FileLease releases
→ runtime exits :shutdown
→ registration remains
```

## Exit gate

* Two OS processes cannot own the same file.
* One database crash does not affect another.
* A cleanly closed file copies and opens elsewhere.
* Manifest interruption never destroys the prior manifest.
* Adapter lifecycle suite passes.

---

# 16. Phase 2 — Documents, revisions, and conflicts

## Objective

Persist the complete revision model and expose atomic local mutations.

## PARALLEL Track A — SQLite document persistence

Own:

```text
lib/elixir_db/storage/sqlite/documents.ex
lib/elixir_db/storage/sqlite/revisions.ex
lib/elixir_db/storage/sqlite/statements.ex
```

Implement physical leaf updates, winner materialization, and sequence allocation.

## PARALLEL Track B — Domain command service

Own:

```text
lib/elixir_db/documents.ex
```

Validate command structs, canonicalize bodies before admission, and map domain results.

## PARALLEL Track C — Conflict-resolution persistence

Own isolated functions in SQLite revision modules plus dedicated conformance tests.

Do not duplicate winner logic in SQL. Load the bounded leaf metadata needed by the pure winner module and persist its result.

## PARALLEL Track D — Model-based tests

Generate operation histories and compare:

```text
pure model state
versus
SQLite adapter state
```

Operations include create, update, delete, replicated sibling insertion, resolve, and replay.

## Call stack — Put

```text
HTTP decoder
→ PutDocument.new
→ canonicalize body
→ calculate candidate revision
→ DatabaseAdmission.with_token
→ DatabaseOwner command
→ SQLite BEGIN IMMEDIATE
→ verify current winner or exact replay
→ insert revision
→ update parent leaf flag
→ calculate physical/live leaves and winner
→ update documents row
→ allocate sequence
→ insert change
→ COMMIT
→ publish sequence to ChangeNotifier
→ return result
```

## Call stack — Resolve conflict

```text
validated ResolveConflict
→ owner transaction
→ load exact current live leaves
→ compare expected set
→ pure conflict-resolution plan
→ insert surviving child and loser tombstones
→ update leaf flags
→ pure winner selection
→ update materialized document
→ one sequence and one change entry
→ commit and notify
```

## Exit gate

Random generated histories produce identical revision trees, winners, active conflicts, tombstones, and replay results in the pure model and SQLite adapter.

---

# 17. Phase 3 — Changes and replication primitives

## Objective

Implement durable changes, chain transfer, atomic import, and checkpoints before building replication workers.

## PARALLEL Track A — Changes storage and notifier integration

Own:

```text
lib/elixir_db/storage/sqlite/changes.ex
lib/elixir_db/changes.ex
```

Implement bounded reads, `has_more`, and exact leaf sets.

## PARALLEL Track B — Revision diff and chain retrieval

Own adapter functions that:

* Compare exact leaf IDs.
* Return complete parent-first chains.
* Bound response documents, revision count, and bytes.

## PARALLEL Track C — Atomic chain import

Own target import transaction and validation.

Reuse the same pure winner and revision-ID modules as local mutations.

## PARALLEL Track D — Checkpoint local records

Own:

```text
lib/elixir_db/storage/sqlite/local_records.ex
lib/elixir_db/replication/checkpoint_reconciler.ex
```

Implement versioned compare-and-swap and lost-response replay.

## PARALLEL Track E — Stream tests

Implement notifier race tests and NDJSON event model tests without HTTP first.

## Call stack — Long-poll changes

```text
caller reads changes
→ none available
→ subscribe notifier
→ re-read current sequence
→ if changed: unsubscribe and read
→ otherwise wait without admission token
→ notification or timeout
→ bounded read
```

## Call stack — Import chains

```text
ImportRevisionChains command
→ validate all chains before write
→ begin transaction
→ insert missing revisions parent-first
→ collect affected documents
→ calculate final leaves and winners
→ update materialized documents and indexes
→ allocate one sequence per affected document
→ append one change per affected document
→ commit
→ publish highest new sequence
```

## Exit gate

Every primitive is bounded, deterministic, idempotent, and independently testable without a replication worker.

---

# 18. Phase 4 — Local replication state machine

## Objective

Replicate between two local database endpoints using only the frozen endpoint behaviour.

## SERIAL — State-machine skeleton

The integrator creates all state names, events, transition results, and cancellation points before parallel work.

## PARALLEL Track A — Local endpoint

Own:

```text
lib/elixir_db/replication/local_endpoint.ex
```

Translate endpoint calls into database-owner commands.

## PARALLEL Track B — Worker state machine

Own:

```text
lib/elixir_db/replication/worker.ex
lib/elixir_db/replication/id.ex
```

No HTTP code.

## PARALLEL Track C — Worker supervision and registry

Own:

```text
lib/elixir_db/replication/worker_supervisor.ex
lib/elixir_db/replication/worker_registry.ex
```

Enforce one active worker per replication ID.

## PARALLEL Track D — Fault suite

Own state-transition fault injection and generated convergence histories.

## Call stack — One batch

```text
worker handshake
→ source changes after checkpoint
→ target diff
→ source fetch complete chains
→ target atomic import
→ target checkpoint CAS
→ source checkpoint CAS
→ next batch or terminal state
```

## Supervision behavior

* Retryable endpoint errors move the worker to `backoff`.
* A timer event returns it to handshake or the interrupted safe stage.
* Non-retryable identity or integrity errors move to `failed`.
* Unexpected worker exceptions crash only the worker; the supervisor restarts it and it reconciles checkpoints.
* Cancellation is handled between bounded endpoint calls.

## Exit gate

Fault injection at every transition proves no committed source revision can be skipped.

---

# 19. Phase 5 — HTTP API and remote replication

## Objective

Expose the Version 1 protocol and make a remote endpoint behaviorally equivalent to the local endpoint.

## PARALLEL Track A — HTTP infrastructure

Own:

```text
lib/elixir_db/http/router.ex
lib/elixir_db/http/request.ex
lib/elixir_db/http/response.ex
lib/elixir_db/http/error_mapper.ex
lib/elixir_db/http/body_reader.ex
```

## PARALLEL Track B — Database and document routes

Own their route modules and contract tests.

## PARALLEL Track C — Changes and streaming routes

Own stream writer, disconnect handling, and heartbeat behavior.

## PARALLEL Track D — Replication wire routes

Own only wire primitives, not job-management routes.

## PARALLEL Track E — Remote endpoint

Own:

```text
lib/elixir_db/replication/remote_endpoint.ex
lib/elixir_db/replication/remote_transport.ex
```

Remote endpoint results MUST be indistinguishable from local endpoint results to the worker.

## PARALLEL Track F — HTTP contract suite

Use two real Bandit instances and actual SQLite files.

## Call stack — Remote endpoint call

```text
replication worker
→ RemoteEndpoint callback
→ RemoteTransport builds Req request
→ remote Bandit/Plug route
→ strict request validation
→ remote database admission
→ remote DatabaseOwner
→ adapter command
→ standard response envelope
→ RemoteTransport validates envelope
→ project result returned to worker
```

## Exit gate

Run the complete local replication fault and convergence suite again with one endpoint remote, then both endpoints remote.

---

# 20. Phase 6 — Persistent replication jobs

## Objective

Store job definitions locally and resume enabled continuous jobs after restart.

## PARALLEL Track A — Job persistence

Own:

```text
lib/elixir_db/storage/sqlite/replication_jobs.ex
```

## PARALLEL Track B — Job manager

Own:

```text
lib/elixir_db/replication/job_manager.ex
```

Responsibilities:

* Validate job creation.
* Generate job IDs.
* Separate persisted definition from runtime state.
* Start and stop workers.
* Report combined status.

## PARALLEL Track C — Startup recovery

Extend `DatabaseCatalog` startup inspection using bounded supervised tasks.

Do not permanently open databases with no enabled jobs.

## PARALLEL Track D — Job HTTP routes and tests

Own job-management routes, not wire routes.

## Call stack — Startup resume

```text
Application starts
→ DatabaseCatalog loads manifest
→ bounded tasks inspect registered database metadata/jobs
→ enabled continuous job found
→ owning database runtime opened
→ WorkerSupervisor starts worker by replication ID
→ worker reconciles endpoint checkpoints
→ running or waiting state
```

## Exit gate

Restart behavior, copied job preservation, inert unregistered files, and local-only job semantics all pass.

---

# 21. Phase 7 — Structured queries and full-text search

## Objective

Implement storage-neutral query semantics over SQLite expression indexes and FTS5.

Structured and full-text work can proceed largely in parallel after shared index structs and lifecycle rules are frozen.

## SERIAL — Shared query contract

Freeze:

* Normalized selector form.
* Planner input and output structs.
* Logical index definitions.
* Projection shape.
* Bookmark payload.
* Explain result.

## PARALLEL Track A — Selector and planner

Own:

```text
lib/elixir_db/query/selector.ex
lib/elixir_db/query/normalizer.ex
lib/elixir_db/query/planner.ex
lib/elixir_db/query/projection.ex
```

Use pure functions and generated planner fixtures.

## PARALLEL Track B — Structured SQLite indexes

Own:

```text
lib/elixir_db/storage/sqlite/structured_indexes.ex
lib/elixir_db/storage/sqlite/query_compiler.ex
```

Compile validated JSON Pointers into internal SQLite paths. Client text must never become an identifier or unvalidated SQL fragment.

## PARALLEL Track C — Full-text domain

Own:

```text
lib/elixir_db/query/full_text.ex
priv/fixtures/tokenization/
```

Implement `unicode_words_v1` conformance fixtures independent of FTS5.

## PARALLEL Track D — FTS5 adapter

Own:

```text
lib/elixir_db/storage/sqlite/full_text_indexes.ex
```

Map the domain contract to `unicode61` and FTS5 queries.

## PARALLEL Track E — Bookmarks

Own:

```text
lib/elixir_db/query/bookmark_codec.ex
lib/elixir_db/domain/bookmark.ex
```

Self-contained, checksummed, query-bound, and sequence-bound.

## PARALLEL Track F — Query HTTP routes and contract tests

Own index/query routes after domain request structs are frozen.

## Call stack — Structured query

```text
HTTP map
→ Query.new and normalize
→ planner selects logical index
→ bookmark validation
→ admission token
→ owner ExecuteQuery
→ SQLite adapter compiles bound SQL
→ one read transaction
→ results projected
→ next key encoded into bookmark
→ response
```

## Call stack — Full-text query

```text
validated FullText query
→ logical index lookup
→ storage-neutral term parsing
→ admission token
→ adapter maps terms to escaped FTS5 query
→ FTS5 match and rank
→ optional structured residual selector
→ deterministic document-ID tie-break
→ adapter cursor bookmark
```

## Index creation

Index creation and rebuild are synchronous owner commands.

Since one connection serializes all operations, no second connection is required to preserve an old index during rebuild. Use one SQLite transaction so rollback restores the prior committed state.

## Exit gate

* Planner property tests pass.
* Structured and FTS extraction semantics pass adapter conformance.
* Tokenization fixtures match `unicode_words_v1`.
* No backend syntax leaks through HTTP.
* Bookmark behavior is deterministic and stale after mutation.
* Index updates remain atomic with winner changes.

---

# 22. Phase 8 — Hardening, release, and implementation closure

## Objective

Prove that the complete system satisfies `Arch V2` under crashes, concurrency, copying, and retries.

## PARALLEL Track A — Crash and durability matrix

Test process kill and VM kill around:

* Before transaction.
* During SQLite work.
* After commit before response.
* Before notifier publication.
* During checkpoint writes.
* During manifest replacement.

## PARALLEL Track B — Long-running replication

Run randomized concurrent writers on two servers, periodic process restarts, network interruption, and final convergence.

## PARALLEL Track C — Query and JSON fuzzing

Fuzz:

* Strict JSON.
* Canonicalization.
* JSON Pointers.
* Selectors.
* Bookmarks.
* FTS text escaping.
* Revision-chain payloads.

## PARALLEL Track D — Operational documentation

Document:

* Starting and stopping.
* Database root.
* Registration.
* Offline copy procedure.
* Lease recovery.
* Integrity checking.
* Replication job states.
* Limits.
* Troubleshooting stable error codes.

## INTEGRATION — Final validation

Run:

```text
mix check.full
mix test --only slow
```

Run the architecture end-to-end scenario on Linux and at least one of macOS or Windows.

Validate ordinary OS file copy with the server stopped.

Record the final SQLite compile options and file-format declaration.

## Release gate

The implementation is complete only when:

* Every `Arch V2` requirement ID maps to at least one test.
* All six test pillars pass.
* No compiler warning remains.
* Dialyzer is clean.
* No ignored or quarantined test represents a product defect.
* The full remote replication suite converges after injected failures.
* The copied offline file opens independently without sidecars.
* The final pull request contains architecture, implementation, and operational documentation.

---

# 23. Critical call-path ownership map

| Call path                      | Primary owner    | Must not contain            |
| ------------------------------ | ---------------- | --------------------------- |
| HTTP decode → domain request   | HTTP layer       | SQL, Exqlite terms          |
| Domain request → owner command | Domain service   | Plug connections            |
| Owner command → adapter        | Runtime owner    | Network waits               |
| Adapter mutation → commit      | SQLite adapter   | HTTP errors                 |
| Commit → notify                | Owner/notifier   | Document bodies in messages |
| Replication worker → endpoint  | Replication      | SQLite handles              |
| Remote endpoint → HTTP         | Remote transport | Worker state mutation       |
| Query planner → adapter plan   | Query domain     | FTS5 or SQL syntax          |
| Adapter query → results        | SQLite adapter   | Public bookmark encoding    |

---

# 24. Agent parallelism summary

The safest maximum parallelism is:

| Phase   | Safe parallel tracks             |
| ------- | -------------------------------: |
| Phase 0 | 5 after repository bootstrap     |
| Phase 1 | 4 after SQLite feasibility spike |
| Phase 2 | 4                                |
| Phase 3 | 5                                |
| Phase 4 | 4 after state skeleton           |
| Phase 5 | 6                                |
| Phase 6 | 4                                |
| Phase 7 | 6 after shared query contract    |
| Phase 8 | 4                                |

Adding more agents than the listed tracks is likely to create overlapping ownership and reduce correctness.

The critical path remains:

```text
Phase 0 contracts
→ Phase 1 lifecycle
→ Phase 2 revision persistence
→ Phase 3 replication primitives
→ Phase 4 local replication
→ Phase 5 remote protocol
→ Phase 6 persistent jobs
→ Phase 7 queries
→ Phase 8 hardening
```

Parallelism should shorten work inside each phase, not bypass its gate.

---

# 25. First implementation actions

The first implementation session SHALL perform only these actions:

1. Create the supervised Mix project.
2. Pin Elixir, OTP, and dependencies.
3. Add `check.fast` and `check.full`.
4. Create the source and test directory skeleton.
5. Add the application supervisor with placeholder children.
6. Implement `ElixirDB.Error`.
7. Implement strict JSON and canonicalization fixtures.
8. Freeze domain structs and adapter behaviours.
9. Run the Phase 0 gate.

Do not begin SQLite schema implementation until revision identity and canonical JSON fixtures are stable. Revision IDs become durable protocol state and are the most expensive behavior to change later.