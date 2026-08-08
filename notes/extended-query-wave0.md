# Extended query Wave 0 — baseline freeze

## Starting commit

- Post-attachment HEAD at gate start: `f9e3076ac1609254985cb6a8b554a97b0a3de70c`
  (Harden attachment integrity and lifecycle handling)
- Wave 0 notes commit parent of this artifact revision: see git history
- Re-verified HEAD when gates were re-run after adversarial review:
  recorded at end of this document under **Gate re-verification**

## Prerequisite gate results

Exact commands re-run on the post-attachment tree (including this notes file;
no query source changes). Results recorded under **Gate re-verification**.

Required commands:

```sh
mix check.full
mix test --warnings-as-errors --only slow
MIX_ENV=prod mix release.build
```

## Attachment milestone evidence

Symbol/path presence (not sufficient alone):

- `AttachmentCoordinator`, `revision_attachments`, `pending_blobs`
- HTTP `attachments/upload`, `replication/blobs`
- `.elixirdb` / `DatabaseBundle` / `database.sqlite3`

Focused attachment/replication suites included in the green full gate and/or
slow suite (proof by automated tests, not symbol search):

- `test/attachments/filesystem_store_test.exs`
- `test/attachments/gc_test.exs`
- `test/attachments/ezstd_gate_test.exs`
- `test/http/attachments_test.exs`
- `test/replication/blob_endpoint_test.exs`
- `test/replication/wave5_test.exs`
- `test/replication/fault_injection_test.exs`
- `test/replication/safe_report_probe_test.exs`
- `test/contract/database_bundle_test.exs`
- `test/observability/attachment_signal_test.exs`
- slow e2e including attachment-bearing paths:
  `test/end_to_end/phase8_scenario_test.exs`,
  `test/end_to_end/offline_copy_test.exs`

## Architecture authority

- Local `docs/` directory is absent (removed; authoritative docs live in UnboundMark).
- Authoritative architecture: UnboundMark **Elixir Replicated Document Database**
  (`36d4783d-b2b4-4b37-8d61-5ef189368861`), revision 12 at Wave 0 review.
- Confirmed present in architecture: extended-query contracts including
  `QUERY-024`–`QUERY-026`, `$beginsWith`, regex safety, array-query boundary,
  plan / candidate semantics.
- No local `docs/Architecture.md` rewrite in Wave 0 (would conflict with
  UnboundMark-only docs policy).

## Post-attachment query seam map (frozen filenames)

### Call topology (actual)

```text
HTTP POST /v1/databases/{uuid}/query | /query/explain
→ ElixirDB.HTTP.Router
→ ElixirDB.HTTP.Routes.Indexes.query/1 | explain/1
→ ElixirDB.HTTP.Schemas (+ Request.call unknown-field rejection)
→ ElixirDB.Query.execute/2 | explain/2
    → ElixirDB.Query.Normalizer.normalize/1
    → limit / search / bookmark-type validation
    → DatabaseCatalog.command(:list_indexes)          # pre-owner index list (Wave 3 removes plan prediction use)
    → BookmarkCodec.decode + sequence/index binding validation
    → DatabaseCatalog.command({:command, :query|:explain_query, prepared})
→ ElixirDB.Runtime.DatabaseAdmission
→ ElixirDB.Runtime.DatabaseOwner
    → Commands.ExecuteQuery | Commands.ExplainQuery
→ ElixirDB.Storage.Adapter (behaviour)
→ ElixirDB.Storage.SQLite.Adapter.execute_query/2 | explain_query/2
    → Observability Instrumentation.Query (execute path)
    → ElixirDB.Storage.SQLite.QueryRunner.execute/2 | explain/2
        → IndexCatalog.list/1
        → ElixirDB.Query.Planner.select_index/2          # Wave 1+ becomes Planner.plan/2
        → QueryCompiler / Indexes.compile_search / FullTextIndexes
        → ElixirDB.Query.Selector.matches?/2            # final evaluator
        → sort / after_id+after_ordering cursor / Projection
→ return to ElixirDB.Query
    → BookmarkCodec.encode next bookmark (execute path only)
→ HTTP response
```

### Frozen modules (exact filenames)

HTTP / request surface:

- `lib/elixir_db/http/router.ex`
- `lib/elixir_db/http/routes/indexes.ex`
- `lib/elixir_db/http/schemas.ex`

Query service:

- `lib/elixir_db/query.ex`
- `lib/elixir_db/query/normalizer.ex`
- `lib/elixir_db/query/selector.ex`
- `lib/elixir_db/query/planner.ex`
- `lib/elixir_db/query/bookmark_codec.ex`
- `lib/elixir_db/query/full_text.ex`
- `lib/elixir_db/query/projection.ex`
- `lib/elixir_db/domain/bookmark.ex`

Commands / runtime:

- `lib/elixir_db/commands.ex`
- `lib/elixir_db/runtime/database_catalog.ex`
- `lib/elixir_db/runtime/database_admission.ex`
- `lib/elixir_db/runtime/database_owner.ex`

Storage:

- `lib/elixir_db/storage/adapter.ex`
- `lib/elixir_db/storage/sqlite/adapter.ex`
- `lib/elixir_db/storage/sqlite/query_runner.ex`
- `lib/elixir_db/storage/sqlite/query_compiler.ex`
- `lib/elixir_db/storage/sqlite/index_catalog.ex`
- `lib/elixir_db/storage/sqlite/indexes.ex` (FTS `compile_search`)
- `lib/elixir_db/storage/sqlite/full_text_indexes.ex`
- `lib/elixir_db/storage/sqlite/structured_indexes.ex`
- `lib/elixir_db/storage/sqlite/index_facade.ex` (create/delete/rebuild facade)

Config / limits:

- `lib/elixir_db/config.ex`
- `lib/elixir_db/host_config.ex`
- `config/test.exs`, `priv/host.toml`

Observability:

- `lib/elixir_db/observability/instrumentation/query.ex`

### Explicit non-active / adjacent modules

- `lib/elixir_db/domain/query.ex` — present on disk; **not** part of the live
  execute/explain call chain above (do not treat as active seam unless
  Wave 1+ audit shows call sites). Wave 0 freezes it as dormant.

## Bookmark ownership (current, pre–Wave 3)

- **Decode / fingerprint / sequence / singular index_id+index_digest validation**
  happen in `ElixirDB.Query` **before** `DatabaseCatalog.command(:query, ...)`,
  after a pre-owner `list_indexes` call.
- **Cursor application** (`after_id` / `after_ordering`) and sort/projection happen
  inside `QueryRunner`.
- **Encode** of the next bookmark happens in `ElixirDB.Query` **after** the owner
  returns, using selected index metadata from the runner result.
- Wave 3 MUST move plan binding validation inside the owner-side runner and stop
  using outside-owner index listing solely to predict the plan (plan §12 / §14).

## `queries.max_execution_ms` enforcement location

- Setting lives in per-database config `queries.max_execution_ms`, capped by host
  `max_query_execution_ms` (`config.ex` / `host_config.ex`).
- **Execute path:** `ElixirDB.Storage.SQLite.Adapter.execute_query/2` records
  monotonic start, runs instrumentation-wrapped `QueryRunner.execute/2`, then
  compares elapsed ms to the configured maximum. Overrun returns `resource_limit`.
  This is a **post-completion** guard today, not mid-execution deadline checks.
- **Explain path:** `explain_query/2` calls `QueryRunner.explain/2` directly and
  is **not** covered by the elapsed-time guard.
- Wave 3 MUST wire the **existing** setting as a monotonic deadline inside
  `QueryRunner` for the execute path (plan §13). Do not add a second timeout
  config field. Explain deadline behavior is out of Wave 0; treat current
  execute-only post-check as the frozen baseline.

## Wave 0 scope

No query behavior changes. Source map, architecture authority confirmation,
attachment evidence pointers, and baseline gate re-verification only.

## Gate re-verification

Re-run after adversarial review fixes to this notes file, on tree containing
post-attachment commit `f9e3076` plus Wave 0 notes only (no query source edits).
Tree at re-run: `61ef43fc97ca38ecbdd73db0da695bedc3443870` with unstaged notes
updates that do not touch `lib/` or `test/`.

| Field | Value |
| --- | --- |
| Post-attachment ancestor | `f9e3076ac1609254985cb6a8b554a97b0a3de70c` |
| Tree under test | `61ef43f` + updated notes (working tree) |
| UTC time of re-run | 2026-08-08 (Wave 0 review fix cycle) |
| `mix check.full` | green — 484 passed (7 properties, 477 tests); credo/ex_dna/reach/dialyzer clean; exit 0 |
| `mix test --warnings-as-errors --only slow` | green — 18 passed, 466 excluded; exit 0 |
| `MIX_ENV=prod mix release.build` | green — release assembled at `_build/prod/rel/elixir_db`; exit 0 |
