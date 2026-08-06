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
# optional overrides:
# export ELIXIR_DB_IP=127.0.0.1
# export ELIXIR_DB_PORT=4000
# export ELIXIR_DB_REGISTRATION_MANIFEST=/var/lib/elixirdb/registrations.json
# export ELIXIR_DB_SHUTDOWN_TIMEOUT_MS=30000

mkdir -p "$ELIXIR_DB_ROOT"
/opt/elixir_db/bin/elixir_db daemon    # background
# or: /opt/elixir_db/bin/elixir_db start   # foreground
```

Control a running release:

```sh
/opt/elixir_db/bin/elixir_db pid
/opt/elixir_db/bin/elixir_db remote    # remote console
/opt/elixir_db/bin/elixir_db stop
```

`ELIXIR_DB_ROOT` is required in production. Default listener binds **loopback
only** (`127.0.0.1:4000`, `CONFIG-005`). Override with `ELIXIR_DB_IP` /
`ELIXIR_DB_PORT` or application config:

* `:listener` — `[ip: {127, 0, 0, 1}, port: 4000]`
* `:database_root` — or env `ELIXIR_DB_ROOT` via `config/runtime.exs`
* `:registration_manifest` — defaults to `<database_root>/registrations.json`
* `:shutdown_timeout` — catalog/runtime stop timeout (ms); env
  `ELIXIR_DB_SHUTDOWN_TIMEOUT_MS`
* `:host_limits` — admission, open-database, body, and batch caps

Stop with `bin/elixir_db stop` (or SIGTERM to the release OS process). The
catalog closes open database runtimes; each runtime rolls back its companion
`.lease` transaction on terminate (`ElixirDB.Runtime.FileLease`).

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
