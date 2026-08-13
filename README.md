# ElixirDB

ElixirDB is a revisioned JSON document database that runs as one Elixir OTP
application. Each database is a portable `.elixirdb` bundle on disk. Clients
talk JSON over HTTP `/v1`, or call Elixir modules in-process. Clients submit
structured requests rather than backend engine commands.

```text
Your app  ──HTTP /v1──►  ElixirDB host
                │
                ▼
         database root/
           host.toml
           registrations.json
           notes.elixirdb/      # portable database bundle
             <backend data>     # backend-owned durable artifact
             blobs/             # attachment representations (digest.blob)
             tmp/               # incomplete uploads (not authoritative)
```

**Runtime baseline:** Elixir 1.20.2 on Erlang/OTP 29.0.4 (`mise.toml`,
`mix.lock`). Production hosts run an OTP release. Mix is for development and
CI only.

Deploy, auth, TLS, copy/move, leases, and host limits: see
[Operations.md](Operations.md).

---

## What you get

| Capability | Notes |
| ---------- | ----- |
| Documents | Put / get / delete with content-addressed revisions |
| Conflicts | Branches are kept; resolve explicitly |
| Changes feed | Poll or NDJSON stream from a sequence |
| Structured query | Selector predicates, sort, projection, bookmarks |
| Full-text search | Named `full_text` indexes (`unicode_words_v1`) |
| Live subscriptions | NDJSON stream of matching documents |
| Attachments | Upload bytes, reference digests on documents |
| Replication | One-shot or continuous push/pull between databases |
| Local views | Declarative map/reduce (no custom code) |
| Federation | Bounded query across several database UUIDs |
| Materialized views | Derived read-only database from several sources |
| Admin console | Optional offline HTMX UI at `/ui` |

## What it does not do

- No client engine query language or raw full-text query syntax.
- No CouchDB / PouchDB wire compatibility.
- No automatic adoption of bundles dropped under the root (you must register).
- No multi-database transactions or live federation streams.
- No custom JavaScript/Elixir map functions in views.
- No cloning by copy: a copied bundle keeps the same UUID.

---

## Quick start

### HTTP (TypeScript)

Default listener: `http://127.0.0.1:4000` (loopback, auth off). When
`[auth] enabled = true` in `host.toml`, send the raw token from
`bin/elixir_db token`.

```typescript
const baseUrl = "http://127.0.0.1:4000";
const bearerToken = process.env.ELIXIRDB_TOKEN; // only if auth is enabled

type Envelope<T> = {
  request_id: string;
  data?: T;
  error?: { code: string; message: string; retryable: boolean };
};

async function postJson<T>(
  path: string,
  body: unknown,
): Promise<{ status: number; envelope: Envelope<T> }> {
  const headers: Record<string, string> = {
    accept: "application/json",
    "content-type": "application/json",
  };
  if (bearerToken) headers.authorization = `Bearer ${bearerToken}`;

  const response = await fetch(`${baseUrl}${path}`, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });
  return { status: response.status, envelope: (await response.json()) as Envelope<T> };
}

const created = await postJson<{ database_uuid: string }>("/v1/databases", {
  path: "notes.elixirdb",
});
if (created.status !== 201 || !created.envelope.data) {
    throw new Error(created.envelope.error?.message ?? "creation failed");
}
const uuid = created.envelope.data.database_uuid;

const put = await postJson<{ revision: string }>(
  `/v1/databases/${uuid}/documents/put`,
  { id: "note-1", body: { title: "Hello", done: false } },
);
const revision = put.envelope.data!.revision;

const got = await postJson<{ body: { title: string }; revision: string }>(
  `/v1/databases/${uuid}/documents/get`,
  { id: "note-1" },
);
console.log(got.envelope.data?.body.title, got.envelope.data?.revision === revision);

await postJson(`/v1/databases/${uuid}/close`, {});
```

Successful responses: `{"request_id","data"}`. Failures:
`{"request_id","error":{"code","message","retryable",…}}`. Document IDs live
in the JSON body, not the URL path. Unknown JSON fields are rejected.

### Elixir (in-process)

```elixir
alias ElixirDB.Runtime.DatabaseCatalog
alias ElixirDB.Documents

{:ok, %{database_uuid: uuid}} = DatabaseCatalog.create("notes.elixirdb")

{:ok, %{revision: rev1}} =
  Documents.put(uuid, %{id: "note-1", body: %{"title" => "Hello", "done" => false}})

{:ok, %{revision: rev2}} =
  Documents.put(uuid, %{
    id: "note-1",
    if_revision: rev1,
    body: %{"title" => "Hello", "done" => true}
  })

{:ok, %{revision: ^rev2, body: %{"done" => true}}} =
  Documents.get(uuid, %{id: "note-1"})

:ok = DatabaseCatalog.close(uuid)
```

Embed as a Mix dependency:

```elixir
# mix.exs
defp deps do
  [
    {:elixir_db, path: "../elixirdb"}
    # or: {:elixir_db, git: "https://git.example.com/owner/elixirdb.git"}
  ]
end
```

Databases live under `ELIXIR_DB_ROOT` (default `./data`). Paths in create /
register are **relative** to that root.

---

## Documents and revisions

Every write creates an immutable SHA-256 revision over canonical JSON.

```text
put without if_revision     → new document (or new conflict branch)
put with if_revision        → conditional update (CAS)
delete with if_revision     → tombstone
resolve                     → pick a winner among live conflict leaves
```

```typescript
await postJson(`/v1/databases/${uuid}/documents/put`, {
  id: "note-1",
  if_revision: revision,
  body: { title: "Hello", done: true },
});

await postJson(`/v1/databases/${uuid}/documents/delete`, {
  id: "note-1",
  if_revision: revision,
});

// Conflict resolution when several leaves are live
await postJson(`/v1/databases/${uuid}/documents/resolve`, {
  id: "note-1",
  expected_live_revisions: [revA, revB],
  chosen_parent_revision: revA,
  body: { title: "Merged" },
});
```

Bulk helpers: `POST …/documents/bulk-get` and `…/documents/bulk-write`
(JSON arrays).

**Elixir:** `Documents.get/2`, `put/2`, `delete/2`, `resolve/2`, `bulk_get/2`,
`bulk_write/2`.

---

## Queries and indexes

Selectors are storage-neutral. Supported field operators include `$eq`, `$ne`,
`$gt` / `$gte` / `$lt` / `$lte`, `$in` / `$nin`, `$exists`, `$type`,
`$beginsWith`, bounded `$regex`, `$all`, `$elemMatch`, `$size`, `$mod`, plus
`$and` / `$or` / `$nor` / `$not`. A bare JSON value at a path means equality.

```typescript
const query = await postJson<{
  plan_kind: string;
  documents: unknown[];
  results: unknown[];
  bookmark?: string;
}>(`/v1/databases/${uuid}/query`, {
  selector: {
    $or: [{ "/status": "open" }, { "/priority": { $gte: 5 } }],
    "/title": { $beginsWith: "rep" },
  },
  fields: ["/title", "/status"],
  sort: [{ path: "/priority", direction: "desc" }],
  limit: 20,
});

// Same rows appear as both `documents` and `results`.
console.log(query.envelope.data?.plan_kind, query.envelope.data?.documents);

const next = await postJson(`/v1/databases/${uuid}/query`, {
  selector: { "/status": "open" },
  limit: 20,
  bookmark: query.envelope.data?.bookmark,
});
```

Bookmarks are opaque. Send them unchanged. If the plan or database sequence
changed, you get `bookmark_stale` (retryable) — start over.

Explain a plan:

```typescript
await postJson(`/v1/databases/${uuid}/query/explain`, {
  selector: { "/title": { $regex: "^rep" } },
});
```

### Indexes

```typescript
await postJson(`/v1/databases/${uuid}/indexes`, {
  name: "by_status",
  type: "structured",
  fields: ["/status", "/priority"],
});

await postJson(`/v1/databases/${uuid}/indexes`, {
  name: "body_fts",
  type: "full_text",
  fields: ["/title", "/body"],
  tokenization: { strategy: "unicode_words_v1", diacritics: "remove" },
});

await postJson(`/v1/databases/${uuid}/query`, {
  selector: { "/status": "open" },
  search: { index: "body_fts", text: "hello world", mode: "phrase" },
  limit: 20,
});
```

FTS modes: `all`, `any`, `phrase`, `prefix`. List / delete / rebuild:
`GET …/indexes`, `DELETE …/indexes/:index_id`,
`POST …/indexes/:index_id/rebuild`.

**Elixir:** `ElixirDB.Query.execute/2`, `explain/2`, `create_index/2`,
`list_indexes/1`, `delete_index/2`, `rebuild_index/2`.

---

## Changes feed

```typescript
const changes = await postJson<{
  results: unknown[];
  last_sequence: number;
  has_more?: boolean;
}>(`/v1/databases/${uuid}/changes`, {
  since: 0,
  limit: 100,
  wait_ms: 0, // set > 0 to long-poll
});

// NDJSON stream: change | caught_up | heartbeat | closed | error
const stream = await fetch(`${baseUrl}/v1/databases/${uuid}/changes/stream`, {
  method: "POST",
  headers: { "content-type": "application/json", accept: "application/x-ndjson" },
  body: JSON.stringify({ since: 0, limit: 100, heartbeat_ms: 15000 }),
});
```

**Elixir:** `ElixirDB.Changes.read/2`, `wait/2`.

---

## Attachments

1. Upload raw bytes → get a content-addressed `blob` digest.
2. Reference that digest in a document put.
3. Download by document id + attachment name.

Storage encoding is transparent: ingest picks raw or Zstandard per blob and
stores one `digest.blob` file (payload + integrity trailer); downloads always
return the original bytes.

```typescript
const upload = await fetch(`${baseUrl}/v1/databases/${uuid}/attachments/upload`, {
  method: "POST",
  headers: {
    "content-type": "application/octet-stream",
    ...(bearerToken ? { authorization: `Bearer ${bearerToken}` } : {}),
  },
  body: bytes,
});
const { data: blob } = (await upload.json()) as {
  data: { blob: string; length: number; expires_at: string };
};

await postJson(`/v1/databases/${uuid}/documents/put`, {
  id: "note-1",
  body: { title: "Hello" },
  attachments: {
    "source.txt": { blob: blob.blob, content_type: "text/plain" },
  },
});

const download = await fetch(`${baseUrl}/v1/databases/${uuid}/attachments/get`, {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ id: "note-1", revision: null, name: "source.txt" }),
});
```

**Elixir:** `ElixirDB.Attachments.upload_stream/2`, `open_stream/2`. Attachment
names are metadata, not filesystem paths.

---

## Live query subscriptions

```text
POST /v1/databases/:uuid/query/stream   → application/x-ndjson
```

Request accepts only `query.selector`, optional `query.fields`, and
`heartbeat_ms`. **Not allowed:** `sort`, `limit`, `bookmark`, `index`, `search`.

```typescript
const res = await fetch(`${baseUrl}/v1/databases/${uuid}/query/stream`, {
  method: "POST",
  headers: { "content-type": "application/json", accept: "application/x-ndjson" },
  body: JSON.stringify({
    query: {
      selector: { "/type": "task", "/status": "open" },
      fields: ["/title", "/status"],
    },
    heartbeat_ms: 15000,
  }),
});
```

| Event | Meaning |
| ----- | ------- |
| `snapshot` | Initial matching document |
| `caught_up` | Snapshot finished |
| `upsert` | Document entered or changed while matching |
| `remove` | Document left the set or was deleted |
| `reset` | History gap — clear local membership, then expect a new snapshot |
| `heartbeat` | Keepalive |
| `closed` | Database closed |
| `error` | e.g. `subscription_overloaded` (retryable) |

Subscription state is **not** stored in the database. After reopen, clients
must subscribe again.

**Elixir:** `ElixirDB.Query.Subscriptions.open/3`, `next/2`, `close/1`.

---

## Local declarative views

Views are per-database derived state. They do **not** replicate. Definitions
use path/literal expressions and fixed reducers only (`_count`, `_sum`,
`_min`, `_max`, `_stats`).

```typescript
const view = await postJson<{ view_id: string }>(`/v1/databases/${uuid}/views`, {
  name: "scores",
  selector: { "/kind": "task" },
  key: [{ path: "/kind" }],
  value: { path: "/score" },
  reducer: "_sum",
});

await postJson(`/v1/databases/${uuid}/views/${view.envelope.data!.view_id}/query`, {
  consistency: "stale_ok", // or "update_after" | "consistent"
  limit: 50,
});
```

Other routes: `GET …/views`, `DELETE …/views/:view_id`,
`POST …/views/:view_id/rebuild`.

**Elixir:** `ElixirDB.Views.create/2`, `query/3`, `rebuild/2`, `list/1`,
`delete/2`.

---

## Replication (application view)

Replication moves complete revision chains, tombstones, and attachment bytes.
It keeps revision IDs and deterministic winners. Indexes, local views, and
job definitions stay local to each database.

```mermaid
flowchart LR
  A["Source DB"] -->|push or pull| B["Target DB"]
  A -.->|"stays local"| IA["indexes / views / jobs"]
  B -.->|"stays local"| IB["indexes / views / jobs"]
```

```typescript
await postJson(`/v1/databases/${uuid}/replications`, {
  mode: "continuous", // or "one_shot"
  direction: "push",  // or "pull"
  enabled: true,
  endpoint: {
    kind: "remote",
    database_uuid: targetUuid,
    base_url: "https://other-host:4000",
    auth_token: "raw-bearer-if-target-auth-enabled",
  },
});

// Local endpoint: { kind: "local", database_uuid: "…" }
```

Control: `GET …/replications`, `…/:job_id`, `…/start`, `…/cancel`,
`…/enable`, `…/disable`, `DELETE …/:job_id`. Continuous enabled jobs resume
after restart. One-shot jobs end in `completed` or `failed`.

Remote peer HTTP (`/v1/databases/:uuid/replication/…`) sends Zstandard-compressed
JSON (`Content-Encoding: zstd`, `x-elixirdb-uncompressed-length`). Public document
and job APIs stay uncompressed JSON even when a client sends `Accept-Encoding: zstd`.
Attachment payloads transfer as the stored representation byte for byte — raw or
Zstandard as chosen at ingest — using
`application/vnd.elixirdb.blob-representation` without HTTP `Content-Encoding`,
and the target installs them without probing or re-encoding.

**Elixir:** `ElixirDB.Replication.JobManager` (`put/2`, `start/2`, …) and
`ElixirDB.Replication`.

Operator details (job states, transfer limits, peer auth):
[Operations.md](Operations.md).

---

## Shadow databases (application view)

A shadow is a generation-fenced, read-only materialization of one ordinary
source database. The source stores the desired shadow definition and reports
redacted desired/observed state; enabling or changing a definition is
asynchronous and returns `202`.

```typescript
await putJson(`/v1/databases/${sourceUuid}/shadow`, {
  enabled: true,
  location: "worker-a",
  attachment_location: "/srv/elixirdb/cas",
});

const status = await getJson(`/v1/databases/${sourceUuid}/shadow`);
```

The source API accepts only ordinary source databases. Generations and
operation IDs fence replacement and cleanup, and status never exposes bearer
tokens or managed storage paths. Public shadow reads are admitted only after
the worker reports the exact generation ready; the worker control plane is
authenticated separately from the ordinary public API and uses the bounded
Zstandard JSON wire.

Once a generation is ready, eligible point, bulk, and attachment reads default
to eventual routing only while the public source is an ordinary open database.
Send `x-elixirdb-read-consistency: primary` to bypass the
shadow explicitly. Reads are served by the exact generation snapshot when
possible. A lagging document, revision, or attachment miss falls back to the
source for that request and keeps the route. Transport, protocol, identity, or
store failure falls back once, retires only that exact snapshot, notifies
reconciliation, and reports
`x-elixirdb-read-served-by: source`. Shadow-served responses report `shadow`
plus the durable `x-elixirdb-source-watermark`. Attachment downloads use the
same consistency choice and stream from the configured external CAS without
copying attachment bytes into the shadow bundle. A closed or unregistered
source is never served from a shadow.

Operator configuration and the worker lifecycle are documented in
[Operations.md](Operations.md#shadow-control-and-workers).

---

## Cross-database federation

Query several **distinct ordinary** database UUIDs in one request. No writes,
joins, FTS, index hints, or live federation.

```typescript
const page = await postJson<{
  documents: Array<{ id: string; source_database_uuid: string; fields: object }>;
  sources: Array<{ database_uuid: string; sequence: number }>;
  bookmark?: string;
}>("/v1/federation/query", {
  databases: [uuidA, uuidB],
  query: {
    selector: { "/kind": "task" },
    fields: ["/value"],
    sort: [{ path: "/value", direction: "asc" }],
    limit: 50,
  },
});
```

There is no atomic snapshot across databases. The response lists the
`{database_uuid, sequence}` vector used. Continuing with a bookmark after a
source advanced returns `bookmark_stale`.

Named saved queries live in `host.toml` (`[[federation.saved_query]]`). List /
run: `GET /v1/federation/saved-queries`,
`POST /v1/federation/saved-queries/execute` with `{ name, limit?, bookmark? }`.

**Elixir:** `ElixirDB.Federation.query/1`, `ElixirDB.Federation.SavedQueries`.

---

## Materialized federated views

A materialized view is a **derived** `.elixirdb` bundle. Generated documents
are externally read-only (`derived_database_read_only`). You can still query,
index, add local views, and use it as a replication **source**.

```mermaid
flowchart TB
  S1[Source A] --> M[Materializer]
  S2[Source B] --> M
  M --> D["Derived .elixirdb<br/>generated docs"]
```

```typescript
const mv = await postJson<{ database_uuid: string; database_kind: string }>(
  "/v1/materialized-views",
  {
    name: "Sales",
    sources: [sourceUuid],
    map: {
      key: [{ path: "/kind" }],
      value: { path: "/amount" },
    },
    reduce: "_sum",
    enabled: true,
  },
);

const derived = mv.envelope.data!.database_uuid;
await postJson(`/v1/materialized-views/${derived}/refresh`, {});
await postJson(`/v1/materialized-views/${derived}/rebuild`, {});
await postJson(`/v1/materialized-views/${derived}/disable`, {});
```

Disable materialization before closing a source or the derived database.
Bundles are usually created under `_derived/…derived.elixirdb` (path is a
hint; `database_kind = derived` in metadata is authoritative).

**Elixir:** `ElixirDB.MaterializedViews.create/1`, `enable/1`, `disable/1`,
`refresh/1`, `rebuild/1`, `get/1`, `list/0`.

---

## Errors and limits

Public errors use stable codes (`revision_conflict`, `database_overloaded`,
`bookmark_stale`, …). Backend exception names and engine diagnostics are
**not** part of the contract. Prefer `error.retryable` for client retry policy.

Host ceilings live in `host.toml` `[limits]`. Per-database config
(`GET`/`PUT /v1/databases/:uuid/config`) can only be **more** restrictive.
Common caps: document size, query results, changes batch, attachment size,
subscription membership, view counts.

When a limit is hit you typically see `resource_limit`, `payload_too_large`,
`database_overloaded`, `subscription_overloaded`, or
`attachment_overloaded`.

---

## Offline portability (short)

1. Stop writers and continuous jobs that need the DB open.
2. `POST /v1/databases/:uuid/close`.
3. Copy the whole `.elixirdb` directory with normal OS tools.
4. On the destination: place the bundle, then `POST /v1/registrations`
   with `{ "path": "notes.elixirdb" }`.

Do not treat `.lease` as data. Copying keeps the same UUID — two copies on
one host are rejected. Full procedures:
[Operations.md](Operations.md#offline-copy-move-and-restore).

---

## HTTP route map

| Area | Paths |
| ---- | ----- |
| Databases | `POST/GET /v1/databases`, `GET …/:uuid`, `…/config`, `…/close` |
| Registration | `POST /v1/registrations`, `DELETE /v1/registrations/:uuid` |
| Documents | `…/documents/{get,put,delete,resolve,bulk-get,bulk-write}` |
| Changes | `…/changes`, `…/changes/stream` |
| Query | `…/query`, `…/query/explain`, `…/query/stream` |
| Indexes | `…/indexes`, `…/indexes/:id`, `…/indexes/:id/rebuild` |
| Attachments | `…/attachments/upload`, `…/attachments/get` |
| Views | `…/views`, `…/views/:id/{rebuild,query}` |
| Replications | `…/replications` (+ start/cancel/enable/disable) |
| Shadows | `…/shadow` (desired state and redacted status) |
| Shadow control | `/v1/control-plane/capabilities`, generation provision/inspect/destroy/read routes |
| Federation | `/v1/federation/query`, `/v1/federation/saved-queries` |
| Materialized | `/v1/materialized-views` (+ refresh/rebuild/enable/disable) |
| Maintenance | `…/integrity-check`, `…/compact` |
| UI | `/ui` (when `[web_ui] enabled = true`) |

---

## Developing ElixirDB itself

```sh
mix deps.get
mix check.fast    # while iterating
mix check.full    # before handoff
MIX_ENV=prod mix release.build
```

The full gate includes the repository storage-boundary scan. Run it directly
when changing storage, runtime, domain, or product-model code:

```sh
mix storage.boundary_check
```

`ElixirDB.Diagnostics.runtime/0` reports application / Elixir / OTP /
selected-backend / protocol versions from the assembled BEAMs.

Operator runbook: [Operations.md](Operations.md). SQLite backend layout and
controls: [lib/elixir_db/storage/sqlite/BACKEND.md](lib/elixir_db/storage/sqlite/BACKEND.md).
