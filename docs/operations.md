# ElixirDB operations

Practical runbook for a Version 1 ElixirDB host. Behaviour matches the
CONFIG / LIFE / REPL / MAINT sections of `Architecture.md` and the modules
under `lib/elixir_db/`.

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
`host.toml`, `registrations.json`, and the `*.db` files). On first run in an
empty root, a fully commented `host.toml` is created; it is never overwritten
once present.

All host configuration lives in `<database_root>/host.toml` — a single visible,
editable TOML file. Default listener binds **loopback only**
(`127.0.0.1:4000`). Binding to a non-loopback interface requires
authentication or TLS to be enabled (see below, and `CONFIG-005`); the server
refuses to start otherwise.

* `[listener]` — `ip` and `port`.
* `[limits]` — host-enforced admission, open-database, body, and batch caps.
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

Bearer-token authentication (`AUTH-001`) gates the HTTP API. When
`[auth] enabled = true` in `host.toml`, every request must present a valid
`Authorization: Bearer <token>` header.

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

Every durable database file lives under the configured **database root**
(`ElixirDB.Config.database_root/0`, `LIFE-002`). Clients never submit absolute
filesystem paths; create/register accept **relative** paths that must not
traverse (`..`), escape the root, or cross symlinks.

The **registration manifest** (`ElixirDB.Runtime.RegistrationManifest`) is a
routing-only UTF-8 JSON document (`LIFE-007`):

```json
{
  "version": 1,
  "databases": [
    {"uuid": "…", "path": "relative/path.db"}
  ]
}
```

Writes use temp file → fsync → atomic rename. A failed write leaves the
previous manifest intact. Unregistered `.db` files under the root stay inert;
the server does **not** auto-adopt them (`LIFE-004`).

## Registering and unregistering databases

| Action | API / module | Notes |
| --- | --- | --- |
| Create | `POST /v1/databases` or `DatabaseCatalog.create/2` | Creates the SQLite file, writes identity, adds a manifest entry |
| Register existing file | `POST /v1/registrations` `{"path":"…"}` or `DatabaseCatalog.register/1` | Opens briefly to validate format/UUID, then routes traffic |
| List / info | `GET /v1/databases`, `GET /v1/databases/:uuid` | Public identity is the UUID, never the path (`LIFE-008`) |
| Unregister | `DELETE /v1/registrations/:uuid` or `DatabaseCatalog.unregister/1` | Removes routing metadata **only**; the `.db` file is kept |
| Close | `POST /v1/databases/:uuid/close` or `DatabaseCatalog.close/1` | Required before unregister or offline copy (`LIFE-009`) |

Duplicate UUID registration returns `duplicate_database_uuid`. A missing file
after registration surfaces as `unavailable` / `database_unavailable` rather
than silently dropping the manifest entry.

## Offline copy, move, and restore

1. Stop writes; ensure no continuous replication worker requires the DB open.
2. `POST /v1/databases/:uuid/close` (or close via the catalog).
3. Copy **only** the single `.db` file with ordinary OS tools (`FILE-002`).
4. Do **not** treat `.lease` as authoritative state — it is transient ownership.
5. At the destination root, place the file and `POST /v1/registrations` with the
   relative path. Registration re-validates SQLite markers, schema, and UUID.
6. Reopen by addressing the UUID (`POST` document routes auto-open via the
   catalog, or call `DatabaseCatalog.open/1`).

A copy retains the original UUID (`LIFE-005`). Two copies with the same UUID
under one host are rejected. Copying is backup/relocation, not cloning.

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
4. Never delete or rewrite the main `.db` to “clear” a lease.

## Integrity checking

`POST /v1/databases/:uuid/integrity-check` (or
`Adapter.integrity_check/2` / `ElixirDB.Storage.SQLite.Integrity`) runs
`MAINT-001` checks:

* SQLite `integrity_check` and `foreign_key_check`
* Required Version 1 tables
* Revision identity, ancestry, and leaf markers
* Materialized document winners
* Changes-feed leaf/winner references
* Physical structured / full-text index consistency

Failures return `integrity_violation`. Rebuild a damaged logical index with the
index rebuild endpoint after investigating the reported details.

`ElixirDB.Diagnostics.runtime/0` reports Elixir/OTP/SQLite/protocol versions for
release notes; it is not a substitute for per-database integrity checks. From a
running release:

```sh
/opt/elixir_db/bin/elixir_db eval 'IO.inspect(ElixirDB.Diagnostics.runtime(), pretty: true)'
```

## Replication job states

Persistent jobs live in the database file; workers are transient
(`REPL-013`, `JobManager`, `Replication.Worker`). Observed states include:

| State | Meaning |
| --- | --- |
| `idle` | Registered / waiting to start |
| `handshake` … `checkpoint_source` | Mandated batch phases |
| `waiting` | Continuous job caught up |
| `backoff` | Retryable failure; will retry with jittered delay |
| `completed` | One-shot reached terminal sequence |
| `failed` | Non-retryable failure or cancelled |

Inspect and control jobs under `/v1/databases/:uuid/replications`. Enabled
continuous jobs resume after catalog startup inspection; one-shot workers end
in `completed` or `failed`. Cancellation is cooperative between phases — an
in-flight bounded transaction is allowed to finish (`REPL-018`).

## Host limits and error troubleshooting

Host limits are configured in `:elixir_db, :host_limits` (see
`config/config.exs`). Important keys:

* `admission_limit` — concurrent admitted ops per open DB (`database_overloaded`)
* `max_open_databases`, `max_replication_workers`
* `max_request_bytes`, `max_document_bytes`, `max_document_id_bytes`
* `max_bulk_operations`, `max_changes_batch`, `max_query_results`
* `max_replication_batch_documents` / `_bytes`, retry caps
* `max_json_nesting_depth`

Stable public error codes (see `ElixirDB.Error`) that operators hit most often:

| Code | Typical cause |
| --- | --- |
| `invalid_request` | Unknown JSON fields, bad path, schema shape |
| `database_in_use` | Lease held / second owner |
| `database_not_closable` | Active work / open waiters / continuous job |
| `database_overloaded` | Admission saturation |
| `database_unavailable` / `database_closed` | Missing file, closed runtime (retryable when closed) |
| `duplicate_database_uuid` | Two registrations for one UUID |
| `revision_conflict` / `checkpoint_conflict` | CAS / leaf-set races |
| `resource_limit` / `payload_too_large` | Host or DB config caps |
| `integrity_violation` | Failed integrity check or corrupt revision |
| `replication_already_running` | Worker exclusivity on the same replication id |

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

| Event | OTel span | Metric |
| --- | --- | --- |
| Database open | `elixir_db.database.open` | `elixir_db.database.open.count` (counter) |
| Database command | `elixir_db.database.command` | `elixir_db.database.command.duration` (histogram) |
| Admission overload | — (counter only) | `elixir_db.database.overload.count` (counter) |
| Changes read | `elixir_db.changes.read` | `elixir_db.changes.read.duration` (histogram) |
| Query execute | `elixir_db.query.execute` | `elixir_db.query.execute.duration` (histogram) |
| Index build | `elixir_db.index.build` | `elixir_db.index.build.duration` (histogram) |
| Replication batch | `elixir_db.replication.batch` | `elixir_db.replication.batch.duration` (histogram) |
| Replication checkpoint | `elixir_db.replication.checkpoint` | `elixir_db.replication.checkpoint.count` (counter) |
| HTTP request | `elixir_db.http.request` | `elixir_db.http.request.duration` (histogram) |

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
* Bounded counts: `entries`, `examined`, `revisions_written`
* `finch.duration` — set by the telemetry bridge only; the bounded numeric
  duration of an outbound Finch request

Document bodies, document IDs, search text, revision bodies, and full
remote URLs are **never** recorded. Document IDs are excluded even though
they are not secret: they are unbounded-cardinality customer data.

### Error → span status policy

| Error class | Span status | Notes |
| --- | --- | --- |
| `:internal_error` | ERROR | Surface real failures |
| All other registered domain errors | UNSET | Expected application outcomes; rely on the `error.code` attribute |
| Adapter error-normalization fallback | ERROR | Unknown backend failure |

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
