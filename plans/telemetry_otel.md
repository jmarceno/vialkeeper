# Plan — OpenTelemetry Observability for Version 1

Addresses G2 in [implementation_gaps.md](../implementation_gaps.md): Plan §11 mandates
project telemetry events, but the current `ElixirDB.Telemetry` module only *declares*
nine event prefixes and is never emitted anywhere. We take the opportunity to implement
observability properly against the **OpenTelemetry (OTel)** standard from the start,
rather than wiring the placeholder.

**Goal:** every Plan §11 event is emitted as OTel spans + metrics, trace context
propagates across the HTTP boundary and into replication workers, and the existing
zero-observability footprint is replaced by a correct, low-overhead, opt-in pipeline.

---

## 1. Design principles (carry over from Plan §11)

- **Low overhead.** All instrumentation is in-process; export is asynchronous. Hot paths
  (document mutation, changes read, query execute) must not allocate per-event beyond what
  OTel requires.
- **No secrets in telemetry.** Plan §11 forbids document bodies, search text, revision
  bodies, and complete remote URLs in metadata. The OTel attribute allow-list below
  enforces this statically.
- **Opt-in by default.** The exporter is disabled unless configured. A node with no
  collector configured must behave exactly as today (no network, no crash).
- **Stable event names.** Plan §11's prefixes become OTel span/metric names verbatim;
  they are part of the operational contract.
- **Metadata allow-list is centralized** so a future field addition cannot accidentally
  leak a body.

---

## 2. Dependencies and configuration

### 2.1 Dependencies (`mix.exs`)

Add OpenTelemetry libraries. Pin major versions; the lockfile is authoritative (Plan §4.2).

```elixir
# Runtime — OpenTelemetry
{:opentelemetry_api, "~> 1.4"},     # stable tracing/metrics API (no app by default)
{:opentelemetry, "~> 1.5"},         # SDK, application process; conditionally started
{:opentelemetry_exporter, "~> 1.8"} # OTLP exporter (gRPC+HTTP)
{:telemetry, "1.4.2"}               # KEEP — bridge target, and used by bandit/req/finch
```

Why keep `:telemetry`: Bandit, Plug, Req (via Finch), and db_connection already emit
`:telemetry` events. We bridge those into OTel (§6.3) instead of re-instrumenting them.

Add `:opentelemetry_bandittopher`/a Plug/Route instrumentation helper **only if** a
maintained one exists on hex at integration time; otherwise hand-roll the one router
span (§5.7) — it is ~15 lines.

### 2.2 `config/config.exs`

```elixir
config :opentelemetry, :resource,
  service: %{name: "elixir_db", version: "0.1.0"}

config :opentelemetry, :processors,
  otel_batch_processor: %{
    scheduling_delay_ms: 5000,
    max_queue_size: 2048,
    exporting_timeout_ms: 30_000
  }

config :opentelemetry_exporter,
  otlp_protocol: :http_protobuf,
  otlp_endpoint: "http://localhost:4318"
```

### 2.3 `config/runtime.exs` (opt-in gate)

```elixir
if config_env() == :prod and System.get_env("ELIXIRDB_OTLP_ENDPOINT") do
  config :opentelemetry_exporter, otlp_endpoint: System.fetch_env!("ELIXIRDB_OTLP_ENDPOINT")
  config :opentelemetry, traces_exporter: [:otlp]
  config :opentelemetry, metrics_exporter: [:otlp]
else
  # Default: no export. App still starts so spans are no-ops; no network touched.
  config :opentelemetry, traces_exporter: []
  config :opentelemetry, metrics_exporter: []
end
```

`test/test_helper.exs` sets `traces_exporter: []`, `metrics_exporter: []`, and installs an
in-memory exporter (§7) so tests assert spans without network.

### 2.4 Application children

`lib/elixir_db/application.ex` — add after `ElixirDB.Diagnostics.validate_sqlite!()`:

```elixir
children = [
  ElixirDB.Observability.Supervisor,   # starts OTel app + exporter if configured
  ...existing children...
]
```

The supervisor reads `:opentelemetry` config and decides whether to
`Application.ensure_all_started(:opentelemetry_exporter)`. When unconfigured, it starts
only a lightweight `ElixirDB.Observability.Meter` (a periodic metric reader). This keeps
the "no collector → no network" guarantee.

---

## 3. The nine Plan §11 events → OTel mapping

Each Plan §11 event becomes **one span** plus, where noted, **one metric**. Span names are
stable; metric names use the OTel convention `<prefix>.<name>`.

| Plan §11 event | OTel span name | Metric | Measurements |
|---|---|---|---|
| `[:elixir_db, :database, :open]` | `elixir_db.database.open` | `elixir_db.database.open.count` (counter) | duration; `db.uuid` (hashable), `outcome` |
| `[:elixir_db, :database, :command]` | `elixir_db.database.command` | `elixir_db.database.command.duration` (histogram) | duration; `command.type`, `db.uuid`, `error.code` |
| `[:elixir_db, :database, :overload]` | — (event only) | `elixir_db.database.overload.count` (counter) | `db.uuid` |
| `[:elixir_db, :changes, :read]` | `elixir_db.changes.read` | `elixir_db.changes.read.duration` (histogram) | duration; `db.uuid`, `entries` (bounded count) |
| `[:elixir_db, :query, :execute]` | `elixir_db.query.execute` | `elixir_db.query.execute.duration` (histogram) | duration; `db.uuid`, `index_id`, `examined` (bounded) |
| `[:elixir_db, :index, :build]` | `elixir_db.index.build` | `elixir_db.index.build.duration` (histogram) | duration; `db.uuid`, `index_id`, `index_type` |
| `[:elixir_db, :replication, :batch]` | `elixir_db.replication.batch` | `elixir_db.replication.batch.duration` (histogram) | duration; `replication.id`, `revisions_written` (bounded) |
| `[:elixir_db, :replication, :checkpoint]` | `elixir_db.replication.checkpoint` | counter | duration; `replication.id`, `endpoint` |
| `[:elixir_db, :http, :request]` | `elixir_db.http.request` | `elixir_db.http.request.duration` (histogram) | duration; `http.method`, `http.route`, `http.status_code`, `db.uuid` (optional) |

`error.code` uses the **stable error code atom** from `ElixirDB.Error` (e.g.
`:revision_conflict`), never the SQLite message.

### 3.1 Attribute allow-list (Plan §11 privacy rules)

A single module owns the allow-list. Any field not here MUST NOT be attached:

**Allowed:** `db.uuid`, `command.type` (atom), `error.code` (atom), `outcome` (atom),
`http.method`, `http.route` (the route template, never the path), `http.status_code`,
`index_id`, `index_type`, `replication.id`, `endpoint` (`:source`\|`:target`), and the
bounded counts above.

**Forbidden (enforced by absence in the allow-list):** document bodies, document IDs (PII
risk), search text, revision IDs/bodies, full remote URLs, request bodies. Note: document
IDs are intentionally excluded from telemetry even though they are not secret — they are
unbounded cardinality and customer data.

---

## 4. New module layout

```
lib/elixir_db/observability/
  supervisor.ex            # OTel app/exporter lifecycle
  tracer.ex                # thin wrappers: with_span/3, current_span, inject/extract
  attributes.ex            # the allow-list + sanitization
  instrumentation/
    database.ex            # :open, :command, :overload
    changes.ex             # :read
    query.ex               # :execute, :index :build
    replication.ex         # :batch, :checkpoint
    http.ex                # :request (Plug router span)
  meters.ex                # metric declarations (counters/histograms)
  telemetry_bridge.ex      # :telemetry → OTel for bandit/req/finch
```

`lib/elixir_db/telemetry.ex` (the dead placeholder) is **deleted**; the event list moves
to `Observability.Attributes`/`Instrumentation.*` as real emitters. `grep` for
`ElixirDB.Telemetry` after removal must be empty.

---

## 5. Instrumentation sites (per event)

Each subsection names the exact call site to wrap. Instrumentation MUST be at the
service/owner boundary, never inside the SQLite adapter — spans describe the *system*,
not SQL rows.

### 5.1 `:database, :open` — `lib/elixir_db/runtime/database_catalog.ex`

Wrap the `:open` GenServer call path (entry `def open(uuid)` line 22; the actual open is
the `DynamicSupervisor.start_child` around line 216). Create the span in the caller
process before `GenServer.call`, set `outcome` from the result. On the
`database_unavailable`/`database_in_use` paths, record `outcome: :rejected` and the error
code; these are not exceptions — do not set span status to ERROR, since they are
expected application outcomes.

### 5.2 `:database, :command` — `lib/elixir_db/runtime/database_catalog.ex:command/3`

The central command dispatch (line ~175, the `DatabaseAdmission.with_token(...)` path).
This single wrap covers all owner commands (put/get/delete/resolve/index/import/etc.).
`command.type` comes from the normalized `Storage.Commands` struct — the owner already
pattern-matches on it (`database_owner.ex:45-141`), so add a `command_type/1` helper.
On `{:error, error}`, set `error.code` and span status ERROR only when
`error.code == :internal_error`; expected domain errors (`:revision_conflict`,
`:document_not_found`) stay span status UNSET to avoid alert noise.

### 5.3 `:database, :overload` — `lib/elixir_db/runtime/database_admission.ex`

At the no-token path (line ~56, where `database_overloaded` is returned). This is a
**counter increment, not a span** (overload is not a unit of work). Emit
`elixir_db.database.overload.count` with `db.uuid`.

### 5.4 `:changes, :read` — `lib/elixir_db/changes.ex`

Wrap `Changes.read/3` and the bounded re-reads inside `Changes.wait/3` (the `receive`
block does not hold the owner; only the owner calls are bounded reads). `entries` = length
of `results` returned. The long-poll wait itself is NOT inside this span — only each
bounded read is. (Avoids a span that lasts the whole `wait_ms`.)

### 5.5 `:query, :execute` and `:index, :build` — `lib/elixir_db/storage/sqlite/adapter.ex`

Wrap `execute_query/2` (line ~265) and `create_index`/`rebuild_index` (lines ~247, ~257)
at the **adapter entry**, but emit from the owner process context so the span parent is
the `:command` span. `index_id` and `index_type` are available on the definition/request.
`examined` = the candidate count the runner already computes (`query_runner.ex:84`).

Note: there is a tension with §5's "never inside the SQLite adapter" — resolution is that
the adapter calls into `Observability.Instrumentation.Query` (a service-level helper), not
OTel directly. The adapter still knows nothing about OTel; it knows about a project
callback.

### 5.6 `:replication, :batch` and `:replication, :checkpoint` — `lib/elixir_db/replication.ex`

The batch span wraps one full `read_changes → diff → fetch → import → checkpoint_target`
cycle. The cleanest seam is `handle_phase_result(:checkpoint_source, ...)` completing in
`worker.ex` — emit `:batch` when a batch finishes (success or retryable failure), with
`revisions_written` from the import context. `:checkpoint` wraps each
`checkpoint_target`/`checkpoint_source` `put_checkpoint` call (replication.ex:147, 170).

Carry the worker span context into endpoint calls so remote-end HTTP requests are children
of the batch span (§6.2).

### 5.7 `:http, :request` — `lib/elixir_db/http/router.ex`

Add a top-level `Plug.Builder` plug that starts a span at request entry, sets
`http.method`, `http.route` (use `conn.path_info` mapped to a route template, NOT the raw
path), and on completion records `http.status_code` + duration. Extract incoming trace
context from W3C `traceparent`/`tracestate` headers (§6.1) so a caller's trace continues.

`db.uuid` is attached when the route is database-scoped (extract from path params).

---

## 6. Cross-cutting concerns

### 6.1 Trace context propagation (HTTP inbound)

In the router span plug, before processing the body:
```elixir
:otel_propagator_text_map.extract(to_header_list(conn.req_headers))
```
Uses W3C Trace Context by default. This makes a client-supplied `traceparent` the parent
of the `:http, :request` span, so external callers can trace into ElixirDB. Validate
header sizes (cap at the W3C maximum) before extraction.

### 6.2 Trace context into replication (outbound + worker)

`lib/elixir_db/replication/remote_transport.ex` already builds Req options (line ~7).
Inject the current span context into outgoing requests:
```elixir
headers = :otel_propagator_text_map.inject([])
Req.request(options, headers: headers)
```
On the **remote** server, the router span (§6.1) extracts it, so a push job's trace
spans both servers in one trace. The worker process must **detach and re-attach** the
context across its `Task.Supervisor.async_nolink` phase tasks (`worker.ex:176`): capture
`OpenTelemetry.Ctx.get_current()` before the task, `OpenTelemetry.Ctx.attach/1` inside
it. Without this the async task has no parent and the trace breaks.

### 6.3 `:telemetry` bridge for dependencies

Bandit (`[:bandit, :request, *]`), Req/Finch (`[:finch, *]`), and Plug emit
`:telemetry` events. Install one bridge handler per library that maps them to OTel spans:
`lib/elixir_db/observability/telemetry_bridge.ex` calls
`:telemetry.attach_many/3` in the supervisor. This gives HTTP-client and server timing
"for free" as child spans under the replication/HTTP spans. Detach on shutdown.

### 6.4 Metrics reader

`Observability.Meters` declares counters/histograms via `:otel_metric`. A periodic reader
(default 30s) exports through the OTLP metrics exporter when configured, else is a no-op.
This satisfies Plan §11's "bounded counts" without per-event metric RPCs.

### 6.5 Error → span status policy

| Error class | Span status | Notes |
|---|---|---|
| `:internal_error` | ERROR | surface real failures |
| All other registered errors | UNSET | expected app outcomes; rely on `error.code` attr |
| Adapter `normalize_error` fallback | ERROR | unknown SQLite failure |

This keeps error-rate dashboards meaningful (only true internal failures alert).

---

## 7. Testing

### 7.1 In-memory exporter for tests

`test/support/otel_recorder.ex`: installs an `:otel_span_exporter` that collects spans in
an Agent. `setup` clears it. Helpers: `spans_named(name)`, `span_attr(span, key)`.

### 7.2 Required tests (`test/observability/`)

- `database_open_span_test.exs`: open a DB; assert one `elixir_db.database.open` span with
  `outcome: :ok`; assert a rejected open (swap UUID) emits `outcome: :rejected` and status
  UNSET, not ERROR.
- `command_span_test.exs`: put a document; assert `:command` span child of nothing (no
  HTTP) with `command.type: :put`; a `:revision_conflict` put sets `error.code` and status
  UNSET; an injected `:internal_error` sets status ERROR.
- `overload_metric_test.exs`: saturate admission; assert `overload.count` increments and
  no span is created.
- `privacy_test.exs` (**security gate**): put a document with a distinctive body/search
  text, run a query, then assert NO recorded span/metric attribute contains the body, the
  search text, or the document ID. This enforces the Plan §11 allow-list.
- `http_span_test.exs`: HTTP request through `TestServer`; assert `:http.request` span
  with `http.route` (template), `http.status_code`; assert inbound `traceparent` makes the
  client span the parent.
- `replication_trace_test.exs`: one-shot local replication; assert `:batch` and
  `:checkpoint` spans share a trace; for a remote replication, assert spans on BOTH
  `TestServer` instances share a trace id (validates §6.2 context propagation).
- `telemetry_bridge_test.exs`: trigger a Req call; assert the Finch child span appears
  under the replication span.

### 7.3 Gate

```text
mix check.fast      # includes the new test/observability/ tests
mix check.full      # Dialyzer must stay clean with OTel deps
```

Dialyzer caveat: OTel libraries historically have some unmatched-return noise; if a clean
run is impossible immediately, add *scoped* `plt_add_apps: [:opentelemetry_api]` and
document any ignore with Plan §3.3's required fields (warning text, rationale, owner,
removal condition).

---

## 8. Rollout / migration

1. **Phase A — skeleton (no behavior change):** add deps, config gate (default off),
   supervisor, attributes module, in-memory test exporter. Delete `lib/elixir_db/telemetry.ex`.
   `grep -rn "ElixirDB.Telemetry"` must be empty. CI stays green with export off.
2. **Phase B — instrument the three highest-value events:** `:http.request`,
   `:database.command`, `:database.overload`. Validate with the in-memory exporter tests.
3. **Phase C — remaining events:** `:open`, `:changes.read`, `:query.execute`,
   `:index.build`, `:replication.batch`, `:replication.checkpoint`.
4. **Phase D — propagation:** inbound `traceparent`, outbound Req injection, worker
   cross-task context detach/attach, `:telemetry` bridge.
5. **Phase E — docs:** add an "Observability" section to `docs/operations.md` listing
   collector config (OTLP endpoint env var), the span/metric catalog (§3 table), the
   attribute allow-list, and the error→status policy.

Each phase ends with `mix check.fast`; Phase E ends with `mix check.full`.

---

## 9. Acceptance criteria

- All nine Plan §11 events are emitted as OTel spans/metrics with exactly the attributes
  in the §3.1 allow-list.
- `mix check.full` is green with no ignored Dialyzer warnings representing defects.
- With `ELIXIRDB_OTLP_ENDPOINT` unset, the app starts and serves traffic with **no network
  connections** to any collector and no crash (proven by an env-unset integration test).
- The privacy test (§7.2) passes: no document body, search text, revision body, or
  document ID appears in any recorded span/metric attribute.
- A one-shot **remote** replication between two `TestServer` instances produces spans on
  both servers under a single `trace_id`.
- `docs/operations.md` documents how to enable/configure collection and the full span
  catalog.
- `grep -rn "ElixirDB.Telemetry" lib/ test/` is empty; the placeholder is gone.

---

## 10. Out of scope for this plan

- Logs → OTel logs bridge (Logger already has stable names per Plan §11; a logs exporter
  can be added later without touching spans).
- Custom sampler beyond the SDK default (parent-based / always-on). Configurable samplers
  can follow once a collector is in production.
- Metrics on replication retry/backoff delay distribution (add as a follow-up histogram if
  operational need arises).
