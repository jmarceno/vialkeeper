# ElixirDB operations

Practical runbook for a Version 1 ElixirDB host. Operational behavior is defined by the CONFIG / LIFE / REPL / QUERY / MAINT sections of `Architecture.md`; module names below identify the implementation boundaries that realize that contract.

Production and staging hosts run an assembled OTP release. `mix` is for
local development and CI only.

## Build the release

On a machine with the pinned Elixir/OTP toolchain (see `mise.toml`):

```sh
export MIX_ENV=prod
mix release.build
```

The assembled tree is `_build/prod/rel/elixir_db/`. It includes ERTS and the
application BEAMs. Copy that directory to the target host (same OS/ABI as the
build machine). `ElixirDB.Diagnostics.runtime/0` reports the Mix application
version and runtime/SQLite identity from the assembled BEAMs; it does not
read VCS metadata.

## Start and stop

```sh
export ELIXIR_DB_ROOT=/var/lib/elixirdb

# first start: the directory and host.toml are created automatically
/opt/elixir_db/bin/elixir_db daemon    # background
# or: /opt/elixir_db/bin/elixir_db start   # foreground
```

Control a running release:

```sh
/opt/elixir_db/bin/elixir_db pid
/opt/elixir_db/bin/elixir_db remote    # remote console
/opt/elixir_db/bin/elixir_db stop
```

`ELIXIR_DB_ROOT` locates the database root (the single directory holding
`host.toml`, `registrations.json`, and the `*.elixirdb` bundle directories). On
first run in an empty root, a fully commented `host.toml` is created; it is
never overwritten once present.

All host configuration lives in `<database_root>/host.toml` — a single visible,
editable TOML file. Default listener binds **loopback only**
(`127.0.0.1:4000`). Binding to a non-loopback interface requires
authentication or TLS to be enabled (see below, and `CONFIG-005`); the server
refuses to start otherwise.

* `[listener]` — `ip` and `port`.
* `[limits]` — host-enforced admission, open-database, body, batch, replication-transfer, live-subscription, local-view, and materialized-view ceilings.
* `[admission]` — per-database owner service weights and reserved queue slots for foreground, subscription, replication, and background maintenance work.
* `[federation]` — bounded ad-hoc cross-database query policy plus named virtual saved queries.
* `[web_ui]` — enables/disables the embedded offline administration console.
* `[auth]` — bearer-token authentication (see Authentication).
* `[tls]` — TLS listener enablement and cert/key paths (see Transport-layer
  security).
* `[security] allow_insecure_remote` — explicit override of the loopback
  failsafe.
* `[observability] otlp_endpoint` — OTLP collector endpoint; empty means no
  exporter and no network (OBSV-004).

Stop with `bin/elixir_db stop` (or SIGTERM to the release OS process). The
catalog closes open database runtimes; each runtime rolls back its companion
`.lease` transaction on terminate (`ElixirDB.Runtime.FileLease`).

## Authentication

Bearer-token authentication (`AUTH-001`) gates the HTTP API and every state-bearing administration-console request. When `[auth] enabled = true` in `host.toml`, every `/v1` request and every `/ui/fragments/...` or `/ui/actions/...` request must present a valid `Authorization: Bearer <token>` header. The inert `/ui` shell and its fixed embedded `/ui/assets/...` resources remain anonymously retrievable so a browser can collect the bearer token locally; they expose no database/server state.

Tokens are stored as SHA-256 hex digests (never raw token text). Generate a
token and its digest with the release command:

```sh
/opt/elixir_db/bin/elixir_db token
# token:  <64-char hex>      (use as the Bearer value)
# digest: <64-char hex>      (paste into host.toml)
```

Paste the **digest** into `host.toml`:

```toml
[auth]
enabled = true
tokens  = ["<digest>"]
```

Restart the release. Clients send the raw token:

```sh
curl -H "Authorization: Bearer <token>" http://127.0.0.1:4000/v1/databases
```

Multiple digests may be listed to support rotation. To rotate with zero
downtime: add the new digest alongside the old, restart, then remove the old
digest and restart. There is no runtime revocation endpoint; rotation is done
by editing `host.toml` and restarting. Authentication failures return
`unauthorized` (HTTP 401) with an identical message regardless of whether the
header was missing, malformed, or wrong (`AUTH-004`).

A replication source authenticates to a target with auth enabled by carrying an
`auth_token` in the remote endpoint reference (`AUTH-003`); see the replication
endpoints documentation.

## Transport-layer security

When `[tls] enabled = true` in `host.toml`, the listener serves HTTPS only on
a single listener (no parallel plaintext port). `certfile` and `keyfile` are
resolved relative to the database root:

```toml
[tls]
enabled  = true
certfile = "cert.pem"
keyfile  = "key.pem"
```

Place `cert.pem` and `key.pem` inside the database root so that copying the
root relocates a working HTTPS listener (`TLS-002`). A quick self-signed pair
for local testing:

```sh
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout "$ELIXIR_DB_ROOT/key.pem" -out "$ELIXIR_DB_ROOT/cert.pem" \
  -subj "/CN=localhost"
```

Use a real CA (Let's Encrypt, an internal CA) for production. Replication
sources connect over TLS when the endpoint `base_url` uses the `https` scheme
(`TLS-003`).

### Development only

From a source checkout, `mix run --no-halt` starts the same supervision tree
for interactive work. Do not use Mix as the production entrypoint.

## Database root and registration manifest

Every durable database bundle lives under the configured **database root**
(`ElixirDB.Config.database_root/0`, `LIFE-002`). Clients never submit absolute
filesystem paths; create/register accept **relative** paths that must not
traverse (`..`), escape the root, or cross symlinks.

Each logical database is one self-contained `.elixirdb` directory (`STORE-004`,
`FILE-001`):

```text
notes.elixirdb/
├── database.sqlite3
├── blobs/
│   └── <digest-prefix>/<sha256>[.raw|.zst]
└── tmp/
```

`database.sqlite3` stores transactional metadata, including database kind, revisions, manifests, configuration, indexes, local declarative-view definitions/state, jobs, checkpoints, and maintenance state. A derived database additionally stores its complete materialized-view definition, source checkpoints, contribution state, and generated result documents there. `blobs/` stores immutable content-addressed attachment bytes. `tmp/` contains incomplete uploads and installation files and is not authoritative.

The **registration manifest** (`ElixirDB.Runtime.RegistrationManifest`) is a
routing-only UTF-8 JSON document (`LIFE-007`):

```json
{
  "version": 1,
  "databases": [
    {"uuid": "…", "path": "relative/path.elixirdb", "database_kind": "ordinary"}
  ]
}
```

Writes use temp file → fsync → atomic rename. A failed write leaves the previous manifest intact. `database_kind` is only a reconstructible routing hint; SQLite metadata inside the bundle is authoritative whenever a bundle is registered/opened. Unregistered `.elixirdb` bundles under the root stay inert; the server does **not** auto-adopt them (`LIFE-004`).

## Registering and unregistering databases

| Action                   | API / module                                                            | Notes                                                                  |
| ------------------------ | ----------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| Create                   | `POST /v1/databases` or `DatabaseCatalog.create/2`                      | Creates the `.elixirdb` bundle, writes identity, adds a manifest entry |
| Register existing bundle | `POST /v1/registrations` `{"path":"…"}` or `DatabaseCatalog.register/1` | Opens briefly to validate format/UUID/layout, then routes traffic      |
| List / info              | `GET /v1/databases`, `GET /v1/databases/:uuid`                          | Public identity is the UUID, never the path (`LIFE-008`)               |
| Unregister               | `DELETE /v1/registrations/:uuid` or `DatabaseCatalog.unregister/1`      | Removes routing metadata **only**; the bundle directory is kept        |
| Close                    | `POST /v1/databases/:uuid/close` or `DatabaseCatalog.close/1`           | Required before unregister or offline copy (`LIFE-009`)                |

Duplicate UUID registration returns `duplicate_database_uuid`. A missing file
after registration surfaces as `unavailable` / `database_unavailable` rather
than silently dropping the manifest entry.

## Offline copy, move, and restore

1. Stop writes; ensure no continuous replication worker requires the DB open. If the database is a source for an enabled materialized federated view, disable that materialization first. If it is itself a derived database, disable its materializer first.
2. `POST /v1/databases/:uuid/close` (or close via the catalog). Active live-query subscriptions are terminated with closed-stream semantics; queued owner work is rejected/drained by database admission. Local view builders stop with the database runtime. Confirm no attachment upload, download, installation, or GC remains active.
3. Copy the complete closed `.elixirdb` directory with ordinary OS tools
   (`FILE-002`). The portable unit is the bundle directory, not a lone SQLite
   file.
4. Do **not** treat `.lease` as authoritative state — it is transient ownership.
5. At the destination root, place the bundle and `POST /v1/registrations` with
   the relative path. Registration re-validates SQLite markers, schema, UUID,
   and required bundle layout.
6. Reopen by addressing the UUID (`POST` document routes auto-open via the
   catalog, or call `DatabaseCatalog.open/1`).

A copy retains the original UUID (`LIFE-005`). Two copies with the same UUID under one host are rejected. Copying is backup/relocation, not cloning. Do not copy an active or crash-recoverable bundle piecemeal: keep the SQLite rollback journal with `database.sqlite3` until recovery finishes.

Automatically created materialized federated views use `_derived/<slug>--<short-uuid>.derived.elixirdb`. Both `_derived/` and the suffix are operator hints only: internal `database_kind = derived` metadata is authoritative. A clean derived bundle can be moved or renamed outside `_derived/` and re-registered normally; its last committed result and all materialization state remain in the bundle.

## Attachments

Uploads use raw `application/octet-stream` bytes:

```text
POST /v1/databases/:uuid/attachments/upload
POST /v1/databases/:uuid/attachments/get
```

The upload response contains the validated SHA-256 `blob`, original `length`,
and `expires_at`. Reference it in `Documents.put/2` (or the document HTTP
route):

```elixir
{:ok, %{blob: digest}} = ElixirDB.Attachments.upload_stream(uuid, [bytes])

{:ok, _} =
  ElixirDB.Documents.put(uuid, %{
    id: "note-1",
    body: %{"title" => "Hello"},
    attachments: %{
      "source.txt" => %{blob: digest, content_type: "text/plain"}
    }
  })
```

`ElixirDB.Attachments.open_stream/2` and the attachment GET route stream
original bytes after metadata resolution. Attachment names are metadata, not
filesystem paths. Uploads durably install bytes before a revision can
reference them; a pending protection record prevents premature collection.

### Attachment limits

Database configuration exposes independent limits:

* `max_attachment_bytes` — maximum original, uncompressed size of one upload.
* `max_concurrent_attachment_reads` — simultaneous download streams.
* `max_concurrent_attachment_writes` — simultaneous upload or replication-write
  streams.

Host ceilings bound database values. Limits are enforced by
`AttachmentCoordinator`, independently of database-owner admission. Admission
overruns return retryable `attachment_overloaded` before streaming starts.
Oversize uploads terminate with `payload_too_large` and do not create a
revision reference. Changing limits does not rewrite stored blobs.

### Attachment GC and integrity

A blob is live when a retained revision manifest references it or an unexpired
pending record protects it. Compact retention can remove a manifest; physical
deletion happens afterward through attachment GC under the exclusive
coordinator barrier. GC releases SQLite and owner resources before deleting
files, is idempotent, and can also reclaim expired pending uploads when compact
is a no-op.

`POST /v1/databases/:uuid/integrity-check` checks SQLite integrity, schema,
revision and index state, attachment manifests, and physical blob
availability. Unreferenced physical blobs are reclaimable orphans rather than
evidence that a committed revision is missing bytes. A crash during GC may
leave such orphaned bytes; rerun GC after recovery.

## Lease recovery (`database_in_use`)

Each open database holds an exclusive SQLite transaction on a companion
`<path>.lease` file (`ElixirDB.Runtime.FileLease`, `ARCH-004`): busy timeout is
zero, so a second owner fails immediately with `database_in_use` (HTTP 409,
retryable).

Safe recovery:

1. Confirm no live ElixirDB release process owns the database (OS process list /
   `bin/elixir_db pid`). A healthy owner always holds the lease while open.
2. If the previous process crashed and left a stale `.lease` **file** but no
   live SQLite exclusive lock, a new open can succeed — FileLease opens the
   lease DB and takes `BEGIN EXCLUSIVE`. Do not delete a `.lease` while another
   host process may still hold the lock.
3. Only remove a leftover `.lease` file after you are certain no process has the
   database open. Prefer letting the crashed BEAM release die and retry open.
4. Never delete or rewrite the main `database.sqlite3` to “clear” a lease.

## Integrity checking

`POST /v1/databases/:uuid/integrity-check` (or
`Adapter.integrity_check/2` / `ElixirDB.Storage.SQLite.Integrity`) runs
`MAINT-001` checks:

* SQLite `integrity_check` and `foreign_key_check`
* Required Version 1 tables
* Revision identity, ancestry, and leaf markers
* Attachment manifests and physical blob verification
* Materialized document winners
* Changes-feed leaf/winner references
* Physical structured / full-text index consistency

Unreferenced physical blobs are reclaimable orphans rather than evidence that a
committed revision is missing bytes. Failures return `integrity_violation`.
Rebuild a damaged logical index with the index rebuild endpoint after
investigating the reported details.

`ElixirDB.Diagnostics.runtime/0` reports Elixir/OTP/SQLite/protocol versions for
release notes; it is not a substitute for per-database integrity checks. From a
running release:

```sh
/opt/elixir_db/bin/elixir_db eval 'IO.inspect(ElixirDB.Diagnostics.runtime(), pretty: true)'
```

## Replication job states

Persistent jobs live in `database.sqlite3`; workers are transient (`REPL-013`, `JobManager`, `Replication.Worker`). The worker state machine remains the authority for the complete session. Independent revision-chain fetches and missing attachment transfers execute beneath it as bounded supervised transfer tasks.

Observed states include:

| State                                     | Meaning                                                                |
| ----------------------------------------- | ---------------------------------------------------------------------- |
| `idle`                                    | Registered / waiting to start                                          |
| `handshake` / `install_boundaries`        | Endpoint compatibility and retention-boundary preparation              |
| `bootstrap`                               | Snapshot/bootstrap transfer when incremental history is unavailable    |
| `read_changes` / `diff`                   | Determine the bounded source work and missing target revisions         |
| `transfer`                                | Concurrent bounded chain fetch/blob transfer before the import barrier |
| `import`                                  | One atomic target revision import after all required bytes are durable |
| `checkpoint_target` / `checkpoint_source` | Durable checkpoint advancement in required order                       |
| `report_peer`                             | Durable safe-position/peer report                                      |
| `waiting`                                 | Continuous job caught up                                               |
| `backoff`                                 | Retryable failure; will retry with jittered delay                      |
| `completed`                               | One-shot reached terminal sequence                                     |
| `failed`                                  | Non-retryable failure or cancelled                                     |

Database replication configuration includes `max_concurrent_chain_fetches`, `max_concurrent_blob_transfers`, and `max_transfer_bytes_in_flight`, all bounded by host ceilings. The byte budget is based on original logical attachment lengths and controls concurrent transfer admission; it does not change attachment identity, physical compression, or the replication batch contract.

Inspect and control jobs under `/v1/databases/:uuid/replications`. Enabled continuous jobs resume after catalog startup inspection; one-shot workers end in `completed` or `failed`. Cancellation is cooperative: transfer tasks stop/cancel safely, while an already committing import or checkpoint operation is allowed to finish (`REPL-018`, `REPL-019`).

## Database owner admission

Every open database has one bounded admission scheduler in front of its single SQLite-owning `DatabaseOwner`. Waiting work remains in that scheduler rather than accumulating in the owner mailbox. At most one owner-execution permit is active per database.

The scheduler uses four trusted origin classes:

* `foreground` — direct client document/query/changes and interactive administrative work.
* `subscription` — live-query snapshots, shared changes reads, and sequence-specific revision batches.
* `replication` — local endpoint metadata/revision/checkpoint work for active replication sessions.
* `maintenance` — automatically scheduled compact-retention and other explicit background maintenance.

`[admission]` in `host.toml` defines positive service weights and reserved queue slots. Scheduling is deterministic weighted round-robin with FIFO order inside each class. Reserved slots prevent one flooded class from consuming every admission position needed by another class. The total active-plus-queued count remains bounded by `[limits] admission_limit`.

Slow attachment streams, replication network/blob transfers, changes waits, live-query waits, and heartbeats do not hold an owner permit after their short metadata operation completes. `database_overloaded` means the bounded admission capacity applicable to the request is full; it is retryable.

## Live query subscriptions

Live structured subscriptions use:

```text
POST /v1/databases/:uuid/query/stream
```

with an NDJSON response. The request contains `query.selector`, optional `query.fields`, and optional `heartbeat_ms`. Live subscriptions deliberately do not accept sort, limit, bookmark, explicit index, or full-text search.

Initial matching documents are emitted as `snapshot` events followed by `caught_up`. Later commits emit `upsert` when a document enters or remains in the result set and changes, and `remove` when a member becomes deleted or stops matching. A retained-history gap produces `reset` followed by a complete replacement snapshot and `caught_up`.

Each database has one shared subscription hub that reads each changes batch once and resolves the exact winning revision identified by each changes entry before fan-out. Each client subscription is a supervised transient process with a bounded membership set and bounded delivery credit. A slow client that exhausts its event capacity receives retryable `subscription_overloaded`; other subscriptions continue unaffected. Subscription runtime state is not stored in `database.sqlite3` and is not restored after reopen.

## Local declarative map/reduce views

Local views are database-local derived state. Their definitions, materialized rows, reducer state, and `indexed_through` checkpoints live inside the owning `database.sqlite3` and do not protocol-replicate.

Manage them under:

```text
POST   /v1/databases/:uuid/views
GET    /v1/databases/:uuid/views
DELETE /v1/databases/:uuid/views/:view_id
POST   /v1/databases/:uuid/views/:view_id/rebuild
POST   /v1/databases/:uuid/views/:view_id/query
```

View definitions are declarative: optional structured selector, scalar path/literal key/value expressions, and optional fixed reducer `_count`, `_sum`, `_min`, `_max`, or `_stats`. There is no JavaScript/Elixir/custom executable map/reduce code.

One supervised builder per view follows the database changes sequence and persists its progress. Notifier delivery only wakes builders; a restart resumes from durable `indexed_through`. View query consistency modes are `stale_ok`, `update_after`, and `consistent`. A `consistent` wait releases owner/admission resources while waiting and is bounded by database/host view limits.

A rebuild scans current winning documents in bounded pages, catches up changes after its captured start sequence, and atomically activates the rebuilt generation. A history gap restarts the rebuild rather than inventing missing changes. Materialized rows are rebuildable; source document revisions remain authoritative.

## Cross-database federation

Ad-hoc federation executes a bounded structured query over an explicit set of **distinct logical database UUIDs**:

```text
POST /v1/federation/query
```

The supported query subset is selector, fields, sort, limit, and bookmark. Full-text search, explicit index hints, joins, writes, cross-database transactions, and live federation are not part of this contract.

The coordinator runs independent ordinary source queries above the per-database runtimes. It never holds one database owner/admission resource while waiting for another. Results are globally ordered deterministically and identify every document by both source database UUID and document ID.

There is no atomic snapshot across databases. Each success reports the exact ordered `{database_uuid, sequence}` source vector used. Federation bookmarks are self-contained and bind to that vector; if any source has changed when a later page is requested, continuation returns `bookmark_stale` rather than mixing states.

Federation is strict: one unavailable/overloaded/failed source fails the request. Source count, concurrency, aggregate candidates, and execution time are bounded by `[federation]` in `host.toml`.

Named virtual federated queries that have no derived database are stored only in `host.toml` as `[[federation.saved_query]]` definitions and are available at:

```text
GET  /v1/federation/saved-queries
POST /v1/federation/saved-queries/execute
```

They are operator configuration: list/execute through HTTP, edit in `host.toml`, then restart. No separate federation catalog/database/file exists.

## Materialized federated views and derived databases

A materialized federated view is one derived `.elixirdb` bundle containing its complete definition, ordered source set, per-source history/checkpoints, contribution rows, rebuild state, exact reducer state, and generated result documents.

Lifecycle/status endpoints are:

```text
POST /v1/materialized-views
GET  /v1/materialized-views
GET  /v1/materialized-views/:derived_uuid
POST /v1/materialized-views/:derived_uuid/refresh
POST /v1/materialized-views/:derived_uuid/rebuild
POST /v1/materialized-views/:derived_uuid/enable
POST /v1/materialized-views/:derived_uuid/disable
```

The materializer reads source databases independently, releases those owner/admission resources, then applies one bounded source batch atomically in the derived database. Contribution changes, affected generated result revisions, and that source checkpoint commit together. No transaction spans source and derived databases.

Source history gaps enter a bounded progressive rebuild. While rebuilding, affected committed generated documents may transition as source contribution pages are replaced/pruned/caught up; status remains `rebuilding` and MUST NOT be treated as `current` until convergence. Other sources remain independently valid.

A locally materialized derived database is externally read-only for its generated documents: public put/delete/bulk/conflict-resolution, attachment mutation, and replication import are rejected with `derived_database_read_only`. It remains readable/queryable/indexable, can host local views, and may be a replication **source**.

If a source is unavailable, the last committed derived result stays readable and status reports stale/source-unavailable. Enabled materializers are continuous dependencies: disable them before closing the derived DB or one of their source DBs.

## Embedded offline administration console

When `[web_ui] enabled = true` (the default), open:

```text
http://127.0.0.1:4000/ui
```

or the corresponding configured HTTPS listener.

The console is server-rendered and HTMX-based. HTMX, project CSS, and the tiny bearer-header bootstrap are vendored and compiled into BEAM modules; the production release serves them from `/ui/assets/...`. There are no CDN, remote-font, analytics, or other runtime Internet dependencies, and the release does not require a frontend/static source directory at runtime.

With authentication disabled, the console loads directly. With bearer auth enabled, the inert shell/assets remain reachable but contain no database/server state. Enter the raw bearer token in the console: it is kept only in browser `sessionStorage` and attached to state-bearing HTMX requests as the same `Authorization: Bearer` header used by `/v1`. The server creates no UI cookie/session and the token is never put in a URL. Closing the tab/session or using logout clears the browser-held token.

The console operates through the same application facades as `/v1`: database/document/query/index/view/federation/materialized-view/replication/maintenance/observability behavior is not reimplemented for HTML. Status screens use bounded HTMX polling where needed rather than a second WebSocket/SSE subsystem.

## Host limits and error troubleshooting

Host limits are configured in `:elixir_db, :host_limits` (see `config/config.exs`). Important keys include:

* `admission_limit` — total active plus queued owner operations per open database (`database_overloaded`).
* `max_open_databases`, `max_replication_workers`.
* replication chain-fetch, blob-transfer, batch, retry, and logical in-flight-byte ceilings.
* live-query active-subscription, membership, buffered-event, and heartbeat ceilings.
* local-view definition count, changes-batch, and consistent-wait ceilings.
* materialized-view source-count, source-concurrency, batch-document, and retry-delay ceilings.
* `max_request_bytes`, `max_document_bytes`, `max_document_id_bytes`.

Ad-hoc federation has its own `[federation]` bounds for source count, concurrent source work, aggregate candidates, and execution time. These remain host-level because no single source database owns an ad-hoc federated request.
* `max_bulk_operations`, `max_changes_batch`, `max_query_results`.
* `max_json_nesting_depth`.

Stable public error codes (see `ElixirDB.Error`) that operators hit most often:

| Code                                        | Typical cause                                           |
| ------------------------------------------- | ------------------------------------------------------- |
| `invalid_request`                           | Unknown JSON fields, bad path, schema shape             |
| `database_in_use`                           | Lease held / second owner                               |
| `database_not_closable`                     | Active work / open waiters / continuous job             |
| `database_overloaded`                       | Per-database owner admission capacity is saturated      |
| `subscription_overloaded`                   | Live subscription count or delivery buffer is saturated |
| `view_not_found` / `view_name_conflict`     | Local-view lifecycle lookup/name collision              |
| `view_not_caught_up`                        | Bounded consistent-view wait expired                    |
| `derived_database_read_only`                | External mutation/import attempted on derived result DB |
| `attachment_overloaded`                     | Attachment read/write or GC admission is saturated      |
| `attachment_not_found`                      | Document revision has no named attachment               |
| `attachment_blob_not_found`                 | Referenced physical blob or metadata is unavailable     |
| `database_unavailable` / `database_closed`  | Missing bundle, closed runtime (retryable when closed)  |
| `duplicate_database_uuid`                   | Two registrations for one UUID                          |
| `revision_conflict` / `checkpoint_conflict` | CAS / leaf-set races                                    |
| `resource_limit` / `payload_too_large`      | Host, DB, or attachment size caps                       |
| `integrity_violation`                       | Failed integrity check or corrupt revision              |
| `replication_already_running`               | Worker exclusivity on the same replication id           |

Backend exception names and SQL text are not part of the public contract; rely
on the versioned error envelope (`code`, `message`, `retryable`, `details`).

## Observability

ElixirDB ships an OpenTelemetry (OTel) pipeline covering the Plan §11
telemetry events: each event is emitted as an OTel span and/or metric, and
trace context propagates across the HTTP boundary and into replication
workers. Collection is **opt-in** — a host with no collector configured
behaves exactly as before.

### Enabling collection

Set `otlp_endpoint` in `host.toml` before starting the release:

```toml
[observability]
otlp_endpoint = "http://localhost:4318"
```

```sh
/opt/elixir_db/bin/elixir_db daemon
```

The OTLP exporter uses the HTTP protobuf protocol (`:http_protobuf`) and
sends traces and metrics to that endpoint. Spans are batched and exported
asynchronously; a periodic metric reader exports every 30 seconds.
Instrumentation never blocks the hot path.

When `otlp_endpoint` is empty or unset, no exporter is wired and **no
network connection to any collector is attempted** (`OBSV-004`). The app
starts and serves traffic exactly as before; instrumentation calls are safe
no-ops. The gate lives in `config/runtime.exs`.

### Span and metric catalog

Span and metric names come verbatim from Plan §11 and are part of the
operational contract (`OBSV-003`):

| Event                  | OTel span                             | Metric                                                     |
| ---------------------- | ------------------------------------- | ---------------------------------------------------------- |
| Database open          | `elixir_db.database.open`             | `elixir_db.database.open.count` (counter)                  |
| Database command       | `elixir_db.database.command`          | `elixir_db.database.command.duration` (histogram)          |
| Admission overload     | — (counter only)                      | `elixir_db.database.overload.count` (counter)              |
| Changes read           | `elixir_db.changes.read`              | `elixir_db.changes.read.duration` (histogram)              |
| Query execute          | `elixir_db.query.execute`             | `elixir_db.query.execute.duration` (histogram)             |
| Index build            | `elixir_db.index.build`               | `elixir_db.index.build.duration` (histogram)               |
| Replication batch      | `elixir_db.replication.batch`         | `elixir_db.replication.batch.duration` (histogram)         |
| Replication transfer   | `elixir_db.replication.transfer`      | `elixir_db.replication.transfer.duration` (histogram)      |
| Replication checkpoint | `elixir_db.replication.checkpoint`    | `elixir_db.replication.checkpoint.count` (counter)         |
| Subscription update    | `elixir_db.query.subscription.update` | `elixir_db.query.subscription.update.duration` (histogram) |
| Subscription open      | —                                     | `elixir_db.query.subscription.open` (counter)              |
| Subscription overload  | —                                     | `elixir_db.query.subscription.overload` (counter)          |
| View update            | `elixir_db.view.update`               | `elixir_db.view.update.duration` (histogram)               |
| View query             | `elixir_db.view.query`                | `elixir_db.view.query.duration` (histogram)                |
| Federation query       | `elixir_db.federation.query`          | `elixir_db.federation.query.duration` (histogram)          |
| Derived view batch     | `elixir_db.derived_view.batch`        | `elixir_db.derived_view.batch.duration` (histogram)        |
| Admission wait         | —                                     | `elixir_db.database.admission.wait` (histogram)            |
| HTTP request           | `elixir_db.http.request`              | `elixir_db.http.request.duration` (histogram)              |
| Attachment read        | `elixir_db.attachment.read`           | `elixir_db.attachment.read.count` / `.read.duration`       |
| Attachment write       | `elixir_db.attachment.write`          | `elixir_db.attachment.write.count` / `.write.duration`     |
| Attachment GC          | `elixir_db.attachment.gc`             | `elixir_db.attachment.gc.count` / `.gc.duration`           |

`database.overload` is a counter increment, not a span: overload is not a
unit of work. `error.code` attributes use the stable error code atom from
`ElixirDB.Error` (e.g. `:revision_conflict`), never the backend message.

### Attribute allow-list

A single module (`ElixirDB.Observability.Attributes`) owns the allow-list;
anything not listed here is never attached to a span or metric:

* `db.uuid` — database UUID, never the filesystem path
* `command.type` — normalized command atom (`:put`, `:get`, …)
* `error.code` — stable error code atom from `ElixirDB.Error`
* `outcome` — e.g. `:ok`, `:rejected`, `:replayed`
* `http.method`, `http.route` (the route template, never the raw path),
  `http.status_code`
* `index_id`, `index_type`
* `replication.id`, `endpoint` (`:source` | `:target`)
* `admission.class` (`foreground`, `subscription`, `replication`, `maintenance`)
* `subscription.event` (`snapshot`, `upsert`, `remove`, `reset`, `caught_up`)
* `view.id`, `derived_view.id`
* `federation.source_count`, `view.consistency`
* Bounded counts: `entries`, `examined`, `revisions_written`
* `finch.duration` — set by the telemetry bridge only; the bounded numeric
  duration of an outbound Finch request

Document bodies, document IDs, attachment names, digests, attachment bytes,
search text, revision bodies, and full remote URLs are **never** recorded.
Document IDs are excluded even though they are not secret: they are
unbounded-cardinality customer data.

### Error → span status policy

| Error class                          | Span status | Notes                                                             |
| ------------------------------------ | ----------- | ----------------------------------------------------------------- |
| `:internal_error`                    | ERROR       | Surface real failures                                             |
| All other registered domain errors   | UNSET       | Expected application outcomes; rely on the `error.code` attribute |
| Adapter error-normalization fallback | ERROR       | Unknown backend failure                                           |

Expected domain errors (`revision_conflict`, `database_in_use`, …) never
mark a span ERROR, so error-rate dashboards alert only on true internal
failures.

### Trace context propagation

* **Inbound HTTP.** W3C `traceparent`/`tracestate` headers are extracted
  on every request, so an external caller's trace continues into ElixirDB
  as the parent of the `elixir_db.http.request` span.
* **Outbound replication.** Requests to remote endpoints inject the
  current span context, so a push job's trace spans both servers under a
  single `trace_id`.
* **Dependency bridge.** `:telemetry` events from Finch (HTTP client) are
  bridged into OTel child spans (`finch.request`), giving outbound
  replication request timing without extra instrumentation. Bandit (HTTP
  server) is intentionally NOT bridged — the `elixir_db.http.request` server
  span already covers inbound requests; bridging Bandit too would double-span
  every request.