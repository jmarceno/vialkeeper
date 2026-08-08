# ElixirDB operations

Practical runbook for a Version 1 ElixirDB host. Behaviour matches
`Architecture.md` and the modules under `lib/elixir_db/`.

Production and staging hosts run an assembled OTP release. `mix` is for local
development and CI.

## Build, start, and stop

```sh
export MIX_ENV=prod
mix release.build
export ELIXIR_DB_ROOT=/var/lib/elixirdb
/opt/elixir_db/bin/elixir_db daemon
/opt/elixir_db/bin/elixir_db pid
/opt/elixir_db/bin/elixir_db remote
/opt/elixir_db/bin/elixir_db stop
```

The root contains `host.toml`, `registrations.json`, and `.elixirdb` bundle
directories. The first start creates a commented `host.toml` and never
overwrites it. The default listener is loopback (`127.0.0.1:4000`); remote
binding requires authentication or TLS unless the explicit insecure override
is enabled.

`[listener]` configures `ip` and `port`; `[limits]` configures host admission,
body, batch, and worker caps; `[auth]` configures bearer tokens; `[tls]`
configures HTTPS; `[observability] otlp_endpoint` enables OTel export.

## Authentication and TLS

Enable bearer authentication, generate a token, and store its digest:

```toml
[auth]
enabled = true
tokens = ["<sha256-hex-digest>"]
```

```sh
/opt/elixir_db/bin/elixir_db token
curl -H "Authorization: Bearer <token>" \
  http://127.0.0.1:4000/v1/databases
```

For HTTPS, place root-relative certificate material in the root:

```toml
[tls]
enabled = true
certfile = "cert.pem"
keyfile = "key.pem"
```

## Bundles and registration

Every database is one canonical self-contained directory:

```text
notes.elixirdb/
├── database.sqlite3
├── blobs/
│   └── <digest-prefix>/<sha256>[.raw|.zst]
└── tmp/
```

`database.sqlite3` stores transactional metadata, including revisions,
manifests, configuration, indexes, jobs, checkpoints, and maintenance state.
`blobs/` stores immutable content-addressed attachment bytes. `tmp/` contains
incomplete uploads and installation files and is not authoritative.

Create and register paths are relative to `ELIXIR_DB_ROOT`; traversal and
symlink escapes are rejected. The registration manifest is routing metadata
only. Unregistered bundles remain inert.

| Action | API / module | Result |
| --- | --- | --- |
| Create | `POST /v1/databases` or `DatabaseCatalog.create/2` | Creates and registers a bundle |
| Register | `POST /v1/registrations` or `DatabaseCatalog.register/1` | Validates UUID, schema, bundle, and integrity |
| List/info | `GET /v1/databases`, `GET /v1/databases/:uuid` | Uses UUID identity |
| Close | `POST /v1/databases/:uuid/close` or `DatabaseCatalog.close/1` | Drains work and closes the bundle |
| Unregister | `DELETE /v1/registrations/:uuid` | Removes routing metadata only |

## Offline copy, move, and restore

1. Stop writes and continuous work that requires the database open.
2. Close the database through the API or catalog.
3. Ensure no attachment upload, download, installation, or GC is active.
4. Copy, move, rename, back up, or restore the complete closed `.elixirdb`
   directory with ordinary OS tools.
5. Register the relative bundle path at the destination.

The bundle directory is the portable artifact; no export mode is required.
The UUID, revisions, conflicts, changes sequence, configuration, indexes, and
replication state survive relocation. A transient `.lease` file is not
authoritative. Do not copy an active or crash-recoverable bundle piecemeal:
keep the SQLite rollback journal with `database.sqlite3` until recovery
finishes.

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

## Replication and maintenance

Replication jobs, checkpoints, revisions, manifests, changes, and indexes are
durable in the bundle; workers are transient. Inspect jobs under
`/v1/databases/:uuid/replications`. One-shot jobs end in `completed` or
`failed`; continuous jobs resume after restart.

Compact retention is explicit or scheduled. It is gated by the stable frontier,
keeps local sequence numbers monotonic, and may schedule attachment GC.
`ElixirDB.Diagnostics.runtime/0` reports release and SQLite identity; it does
not replace a per-bundle integrity check.

## Errors

| Code | HTTP | Typical cause |
| --- | ---: | --- |
| `database_in_use` | 409 | Another owner holds the lease |
| `database_overloaded` | 429 | Database admission is saturated |
| `attachment_overloaded` | 429 | Attachment read/write or GC admission is saturated |
| `attachment_not_found` | 404 | Document revision has no named attachment |
| `attachment_blob_not_found` | 404 | Referenced physical blob or metadata is unavailable |
| `payload_too_large` | 413 | Request or attachment exceeds its configured limit |
| `database_not_closable` | 409 | Active work prevents close |
| `revision_conflict` | 409 | Conditional mutation or leaf-set race |
| `integrity_violation` | 422 | Bundle integrity check failed |
| `resource_limit` | 413/429 | Host or database cap was exceeded |

Backend exception names and SQL text are not public error contracts; use the
versioned envelope fields `code`, `message`, `retryable`, and `details`.

## Observability

OTel collection is opt-in:

```toml
[observability]
otlp_endpoint = "http://localhost:4318"
```

Attachment operations emit these bounded signals:

| Operation | Span | Signals |
| --- | --- | --- |
| Read/download | `elixir_db.attachment.read` | `.read.count`, `.read.duration` |
| Write/upload | `elixir_db.attachment.write` | `.write.count`, `.write.duration` |
| Garbage collection | `elixir_db.attachment.gc` | `.gc.count`, `.gc.duration` |

Attributes pass through the project-owned privacy allow-list. Database UUID,
operation type, outcome, stable error code, status, and bounded counts may be
recorded; document bodies, document IDs, attachment names, digests, bytes,
search text, and full remote URLs are not.
