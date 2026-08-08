# Extended query Wave 0 — baseline freeze

## Starting commit

- HEAD: `f9e3076ac1609254985cb6a8b554a97b0a3de70c`
- Message: Harden attachment integrity and lifecycle handling
- Working tree: clean at gate start

## Prerequisite gate results

| Command | Result |
| --- | --- |
| `mix check.full` | green (484 passed) |
| `mix test --warnings-as-errors --only slow` | green (18 passed) |
| `MIX_ENV=prod mix release.build` | green |

## Attachment milestone presence

Confirmed present: `AttachmentCoordinator`, `revision_attachments`, `pending_blobs`,
`attachments/upload`, `replication/blobs`, `.elixirdb` / `DatabaseBundle`,
`database.sqlite3`.

## Architecture authority

- Local `docs/` removed (lives in UnboundMark).
- Authoritative architecture: UnboundMark `Elixir Replicated Document Database`
  (`36d4783d-b2b4-4b37-8d61-5ef189368861`), already contains extended-query
  contracts including `QUERY-024`–`QUERY-026`, `$beginsWith`, plan bindings.
- No local Architecture.md rewrite in Wave 0.

## Post-attachment query seam map (frozen filenames)

```
HTTP query route
→ ElixirDB.Query
→ ElixirDB.Query.Normalizer
→ DatabaseCatalog.command
→ DatabaseAdmission
→ DatabaseOwner (Commands.ExecuteQuery / ExplainQuery)
→ Storage.Adapter.execute_query / explain_query
→ ElixirDB.Storage.SQLite.Adapter
→ ElixirDB.Storage.SQLite.QueryRunner
    → IndexCatalog
    → ElixirDB.Query.Planner
    → QueryCompiler / FullTextIndexes / Indexes.compile_search
    → ElixirDB.Query.Selector (final evaluator)
    → sort / bookmark / projection
```

Frozen modules:

- `lib/elixir_db/query.ex`
- `lib/elixir_db/query/normalizer.ex`
- `lib/elixir_db/query/selector.ex`
- `lib/elixir_db/query/planner.ex`
- `lib/elixir_db/query/bookmark_codec.ex`
- `lib/elixir_db/query/full_text.ex`
- `lib/elixir_db/query/projection.ex`
- `lib/elixir_db/domain/bookmark.ex`
- `lib/elixir_db/storage/sqlite/query_compiler.ex`
- `lib/elixir_db/storage/sqlite/query_runner.ex`
- `lib/elixir_db/storage/sqlite/indexes.ex` (FTS `compile_search`)
- `lib/elixir_db/storage/sqlite/full_text_indexes.ex`
- `lib/elixir_db/storage/sqlite/structured_indexes.ex`
- `lib/elixir_db/storage/sqlite/index_catalog.ex`

## `queries.max_execution_ms` enforcement location

Currently enforced **after** `QueryRunner.execute/2` returns in
`ElixirDB.Storage.SQLite.Adapter.execute_query/2` via elapsed-ms comparison.
Wave 3 MUST wire the existing setting as a monotonic deadline inside
`QueryRunner` (plan §13); do not add a second timeout config field.

## Wave 0 scope

No query behavior changes. Source map and baseline only.
