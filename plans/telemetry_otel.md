# Plan — OpenTelemetry Observability for Version 1

Addresses G2 in [implementation_gaps.md](../implementation_gaps.md): Plan §11 mandates
project telemetry events. The current `ElixirDB.Telemetry` module declares nine event
names and exposes a `span/3` wrapper that is **never called anywhere** (only the module's
own `defmodule` line references `ElixirDB.Telemetry`). However, two of the nine events are
**already emitted directly** via bare `:telemetry.execute/3`, outside the placeholder
module (see §5.3 and §5.6); those must be migrated, not re-added. We take the opportunity
to implement observability properly against the **OpenTelemetry (OTel)** standard from the
start, rather than wiring the placeholder.

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
# Runtime — OpenTelemetry. Mark runtime: false so the SDK/exporter apps do NOT
# auto-start; the Observability.Supervisor starts them only when configured.
{:opentelemetry_api, "~> 1.4", runtime: false},  # stable tracing/metrics API
{:opentelemetry, "~> 1.5", runtime: false},       # SDK, application process
{:opentelemetry_exporter, "~> 1.8", runtime: false}, # OTLP exporter (gRPC+HTTP)
{:telemetry, "1.4.2"}               # KEEP — bridge target, and used by bandit/req/finch
```

Why keep `:telemetry`: Bandit, Plug, Req (via Finch), and db_connection already emit
`:telemetry` events. We bridge those into OTel (§6.3) instead of re-instrumenting them.

Add `:opentelemetry_bandittopher`/a Plug/Route instrumentation helper **only if** a
maintained one exists on hex at integration time; otherwise hand-roll the one router
span (§5.7) — it is ~15 lines.

### 2.2 `config/config.exs`

Resource and processor tuning only. **Do not set `otlp_endpoint` here** — a hardcoded
endpoint (even `localhost:4318`) risks a network attempt on misconfiguration and would
weaken the OBSV-004 "zero network when unconfigured" guarantee. The endpoint is set
exclusively inside the `runtime.exs` gate (§2.3).

```elixir
config :opentelemetry, :resource,
  service: %{name: "elixir_db", version: "0.1.0"}

config :opentelemetry, :processors,
  otel_batch_processor: %{
    scheduling_delay_ms: 5000,
    max_queue_size: 2048,
    exporting_timeout_ms: 30_000
  }
```

### 2.3 `config/runtime.exs` (opt-in gate)

Applies to every env. The exporter endpoint and OTLP trace/metric exporters are wired
**only** when `ELIXIRDB_OTLP_ENDPOINT` is present; otherwise both exporter lists stay empty.

```elixir
if System.get_env("ELIXIRDB_OTLP_ENDPOINT") do
  config :opentelemetry_exporter,
    otlp_protocol: :http_protobuf,
    otlp_endpoint: System.fetch_env!("ELIXIRDB_OTLP_ENDPOINT")
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
`Application.ensure_all_started(:opentelemetry_exporter)`. Because the deps are declared
`runtime: false` (§2.1), nothing auto-starts without this explicit call. When unconfigured,
it starts only a lightweight `ElixirDB.Observability.Meter` (a periodic metric reader that
is a no-op with `metrics_exporter: []`). This keeps the "no collector → no network" guarantee.

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
to `Observability.Attributes`/`Instrumentation.*` as real emitters. After removal,
`grep -rn "ElixirDB.Telemetry" lib/ test/` must be empty.

**Note on existing emitters (critical).** The placeholder module is unused, but two of the
nine events are *already* emitted via bare `:telemetry.execute/3` elsewhere in the codebase
— `[:elixir_db, :database, :overload]` at `runtime/database_admission.ex:55` and
`[:elixir_db, :replication, :checkpoint]` at `replication.ex:179-183`. These must be
**migrated** to the new OTel emitters (§5.3, §5.6) and their bare `:telemetry.execute`
calls removed. The existing checkpoint emit also carries `source_sequence` metadata, which
is **not** in the OBSV-003 allow-list and must be dropped on migration. Failing to migrate
these would cause double emission and an allow-list leak. As a gate, after migration no
bare emitter must remain. Use multiline matching — the replication call spans several
lines, so a plain single-line `grep` misses it:
`rg -U --multiline ':telemetry\.execute\(\s*\[\s*:elixir_db' lib/` must be empty.

---

## 5. Instrumentation sites (per event)

Each subsection names the exact call site to wrap. Instrumentation MUST be at the
service/owner boundary, never inside the SQLite adapter — spans describe the *system*,
not SQL rows.

### 5.1 `:database, :open` — `lib/elixir_db/runtime/database_catalog.ex`

Wrap the `:open` GenServer call path (public entry `def open(uuid)` at line 22; the actual
open is the `DynamicSupervisor.start_child` inside `defp open_runtime/2` at line 216).
Create the span in the caller process before `GenServer.call`, set `outcome` from the
result. On the `database_unavailable`/`database_in_use` paths, record `outcome: :rejected`
and the error code; these are not exceptions — do not set span status to ERROR, since they
are expected application outcomes.

### 5.2 `:database, :command` — `lib/elixir_db/runtime/database_catalog.ex`

The central command dispatch. The public API is `command/3` (lines 25-26); the server-side
dispatch is `handle_call({:command, uuid, command}, ...)` at lines 171-181, which runs
`DatabaseAdmission.with_token(uuid, fn -> DatabaseOwner.command(uuid, command) end)` at
line 175. (Module name is `DatabaseAdmission`, singular.) This single wrap covers all owner
commands (put/get/delete/resolve/index/import/etc.).

`command.type` comes from the normalized `Storage.Commands` struct — the owner
pattern-matches on each struct (`database_owner.ex:45-147`, covering `Identity` through
`DeleteJob` plus `Close` and the unknown-command fallback). Since dispatch is by struct
module (e.g. `%ElixirDB.Storage.Commands.PutDocument{}`), add a `command_type/1` helper
that maps the struct module to an atom (e.g. `:put`, `:get`, `:delete`, `:resolve`).

On `{:error, error}`, set `error.code` and span status ERROR only when
`error.code == :internal_error`; expected domain errors (`:revision_conflict`,
`:document_not_found`) stay span status UNSET to avoid alert noise.

`outcome` is sourced from the owner result per `TX-006`: `:ok` on a normal
acknowledged mutation, `:replayed` when the owner returns `%{replayed: true}` (an
idempotent retry that matched an existing identical revision), and left unset/`:ok` on
error. This is how the OBSV-003 `outcome: :replayed` attribute is captured.

### 5.3 `:database, :overload` — `lib/elixir_db/runtime/database_admission.ex`

**Migration, not addition.** This event is already emitted today as a bare
`:telemetry.execute([:elixir_db, :database, :overload], %{count: count}, %{})` at line 55,
inside `defp acquire(counter, limit)` where `database_overloaded` is returned (line 56).
Replace that bare call with the OTel counter increment via
`Observability.Instrumentation.Database`. It remains a **counter increment, not a span**
(overload is not a unit of work). Emit `elixir_db.database.overload.count` with `db.uuid`.
(`db.uuid` is available here via the `acquire` call path; thread it down from
`with_token/2` if not already in scope.)

### 5.4 `:changes, :read` — `lib/elixir_db/changes.ex`

Wrap `Changes.read/2` (`def read(uuid, request \\ %{})`, line 5) and the bounded re-reads
performed by the waiting path. `wait/2` (`def wait(uuid, request)`, line 11) delegates to
the private `wait_request/2`, whose `receive` block (lines 36-50) calls back into `read/2`
on notification, timeout, and close — those are the bounded reads to wrap. `entries` =
length of `results` returned. The long-poll wait itself is NOT inside this span — only each
bounded read is. (Avoids a span that lasts the whole `wait_ms`, and the wait does not hold
the owner connection.)

### 5.5 `:query, :execute` and `:index, :build` — `lib/elixir_db/storage/sqlite/adapter.ex`

Wrap `execute_query/2` (line 265) and `create_index`/`rebuild_index` (lines 247, 257) at
the **adapter entry**, but emit from the owner process context so the span parent is the
`:command` span. `index_id` and `index_type` are available on the definition/request.
`examined` = the candidate count the runner computes (`query_runner.ex`: the `count(*)`
full-scan at line ~84, bound as `examined` at lines 21 and 37 of `execute/2`).

`execute_query/2` already captures `System.monotonic_time(:millisecond)` at line 266 for
its `max_execution_ms` overrun guard — reuse that timing value for the span duration rather
than starting a second monotonic clock.

Note: there is a tension with §5's "never inside the SQLite adapter" — resolution is that
the adapter calls into `Observability.Instrumentation.Query` (a service-level helper), not
OTel directly. The adapter still knows nothing about OTel; it knows about a project
callback.

### 5.6 `:replication, :batch` and `:replication, :checkpoint` — `lib/elixir_db/replication.ex` / `worker.ex`

The batch span wraps one full `read_changes → diff → fetch → import → checkpoint_target`
cycle. The cleanest seam is `handle_phase_result(:checkpoint_source, {:ok, context}, data)`
in `worker.ex` (line 236) — emit `:batch` when a batch finishes (success or retryable
failure), with `revisions_written` from the import context.

`:checkpoint` wraps each `checkpoint_target`/`checkpoint_source` `put_checkpoint` call.
`checkpoint_target/4` is at `replication.ex:147` (the `endpoint_call(target, :put_checkpoint, ...)`
at lines 151-154); `checkpoint_source/3` is at `replication.ex:170` (the
`endpoint_call(source, :put_checkpoint, ...)` at lines 175-178).

**Migration (critical).** `checkpoint_source/3` already emits a bare
`:telemetry.execute([:elixir_db, :replication, :checkpoint], %{documents, revisions},
%{replication_id, source_sequence})` at `replication.ex:179-183`. Replace it with the new
OTel span + counter emitter. Note `source_sequence` is **not** in the OBSV-003 allow-list
— drop it on migration (keep only `replication.id`). `revisions`/`documents` become the
bounded counts; they map to `revisions_written`. Failing to remove the bare call would
cause double emission.

Carry the worker span context into endpoint calls so remote-end HTTP requests are children
of the batch span (§6.2).

### 5.7 `:http, :request` — `lib/elixir_db/http/router.ex`

`HTTP.Router` does `use Plug.Router` with `plug(:match)` / `plug(:dispatch)`; the HTTP
server is Bandit wrapping this plug (`application.ex:9`). Add a top-level `plug` placed
ahead of `plug(:match)` that starts a span at request entry, sets `http.method`,
`http.route` (a route template, NOT the raw `conn.path_info`), and on completion records
`http.status_code` + duration. Extract incoming trace context from W3C
`traceparent`/`tracestate` headers (§6.1) so a caller's trace continues.

**Route-template mapping caveat.** Database-scoped routes are declared with `forward/2`
(`/v1/databases/:uuid/documents` → `Documents`, etc.) plus a few inline `post`/`delete`
macros and a catch-all `match _`. Because routing is forwarded to sub-modules, building the
`http.route` template requires a small lookup from `conn.path_info` (e.g.
`["v1","databases",_uuid,"documents","put"]` → `/v1/databases/:uuid/documents/put`) that
covers both the forwarded and inline routes. The `:uuid` path param is available as the
route template's `:database_uuid`; use it for `db.uuid` when the route is database-scoped.

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
context across its `Task.Supervisor.async_nolink` phase tasks (`worker.ex:176-184`,
`async_phase/1` calling `Task.Supervisor.async_nolink(ElixirDB.TaskSupervisor, fn -> run_phase(...) end)`):
capture `OpenTelemetry.Ctx.get_current()` before the task, `OpenTelemetry.Ctx.attach/1`
inside it. Without this the async task has no parent and the trace breaks.

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

1. **Phase A — skeleton (no behavior change):** add deps (`runtime: false`), config gate
   (default off, no hardcoded endpoint), supervisor, attributes module, in-memory test
   exporter. Delete `lib/elixir_db/telemetry.ex`. At this point
   `grep -rn "ElixirDB.Telemetry" lib/ test/` must be empty. **Also** migrate the two
   pre-existing bare emitters so the placeholder's contract is fully retired:
   `runtime/database_admission.ex:55` (`:database.overload`) and
   `replication.ex:179-183` (`:replication.checkpoint`, dropping `source_sequence`).
   These two become the first real OTel emitters; without this step they would keep firing
   bare `:telemetry.execute` in parallel with the new pipeline. Gate (multiline — the
   replication call spans several lines):
   `rg -U --multiline ':telemetry\.execute\(\s*\[\s*:elixir_db' lib/` must be empty.
   CI stays green with export off.
2. **Phase B — instrument the next highest-value events:** `:http.request` and
   `:database.command`. (`:database.overload` and `:replication.checkpoint` were already
   migrated in Phase A.) Validate with the in-memory exporter tests.
3. **Phase C — remaining events:** `:open`, `:changes.read`, `:query.execute`,
   `:index.build`, `:replication.batch`.
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
- `rg -U --multiline ':telemetry\.execute\(\s*\[\s*:elixir_db' lib/` is empty; the two
  pre-existing bare emitters (`:database.overload`, `:replication.checkpoint`) have been
  migrated to OTel
  and the non-allowlisted `source_sequence` metadata dropped.

---

## 10. Out of scope for this plan

- Logs → OTel logs bridge (Logger already has stable names per Plan §11; a logs exporter
  can be added later without touching spans).
- Custom sampler beyond the SDK default (parent-based / always-on). Configurable samplers
  can follow once a collector is in production.
- Metrics on replication retry/backoff delay distribution (add as a follow-up histogram if
  operational need arises).

## 11. Risk notes and interpretations

- **OBSV-007 "identical to a node with no OTel dependencies reachable"** is read as *no
  network and no observable behavior change*, not literally zero footprint. With
  `:opentelemetry` deps present and `runtime: false`, the SDK starts only when
  `Observability.Supervisor` starts it; with `traces_exporter: []`/`metrics_exporter: []`
  no exporter is wired. Spans are no-ops over a no-op tracer provider. This satisfies the
  intent; a true zero-dep footprint is not achievable while OTel is the mandated standard.
- **Metrics API stability is the single riskiest dependency.** The Erlang OTel metrics
  surface (`:otel_metric`, periodic reader) has historically churned more than tracing.
  Pin `opentelemetry ~> 1.5` (which carries it) and validate the counter/histogram reader
  end-to-end early in Phase B, before building remaining signals on top.
- **Verify bridge sources before relying on them.** Bandit, Plug, and Finch (via Req) are
  confirmed emitters and safe bridge targets. `db_connection` appears in `mix.lock` only
  transitively; exqlite 0.39 uses its own connection path, so do not assume `db_connection`
  emits useful events here — confirm before wiring a handler for it.
- **No `prod.exs` exists**; `config/config.exs` imports `"#{config_env()}.exs"`. Production
  configuration is therefore driven by `runtime.exs` (where the OTLP gate lives), which is
  the correct place for it.
