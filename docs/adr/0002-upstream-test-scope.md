# ADR 0002 — Upstream test scope (no PouchDB/CouchDB port)

- **Status:** ACCEPTED
- **Date:** 2026-08-06
- **Decides:** REF-004 selected-validation scope and REF-005 differential-testing scope for Version 1
- **Related IDs:** `REF-001`, `REF-002`, `REF-003`, `REF-004`, `REF-005`, `DEF-006`, `DEF-007`

## Context

`REF-004` requires the project to **identify and adapt relevant upstream cases** across
put/get, update conflicts, deletes, bulk operations, revision determinism, changes feeds,
conflict branches, replication interruption, replication retry, checkpoint recovery, query
behavior in the supported subset, and full-text indexing where an upstream case matches the
project-owned contract. It also requires that **every excluded upstream test has a
documented reason.**

`DEF-006` rejects a line-by-line PouchDB port as the Version 1 implementation strategy, and
`DEF-007` states that full API, wire-protocol, storage-format, and client compatibility are
not Version 1 goals. Version 1 implements an independent replication engine and revision
model; PouchDB/CouchDB remain behavioral references only.

The requirement to *document* the selected/excluded upstream case list (REF-004) is a MUST.
Porting the upstream suites (REF-002/REF-003) or running a differential harness (REF-005)
is not.

## Decision

**Version 1 ships an independent implementation and an independent test suite; it does not
port the PouchDB source (DEF-006), does not run the CouchDB/PouchDB test suites, and does
not run a differential test harness (REF-005) in Version 1.**

Behavioral overlap with CouchDB/PouchDB is covered by the six Version 1 test pillars, each
of which adapts the relevant upstream category rather than importing the upstream case:

| REF-004 required category | Version 1 covering pillar (independent test) |
| --- | --- |
| Put and get | `test/storage_adapter/documents_test.exs`, `test/contract/json_and_revision_test.exs` |
| Update conflicts | `test/storage_adapter/conflicts_test.exs`, `test/contract/revision_adapter_properties_test.exs` |
| Deletes | `test/storage_adapter/documents_test.exs`, `test/storage_adapter/mutations_test.exs` |
| Bulk operations | `test/storage_adapter/mutations_test.exs` (TX-001), `test/storage_adapter/v1_conformance_test.exs` (TX-004/005) |
| Revision determinism | `test/contract/fixtures_test.exs` (REV-002), `test/contract/revision_model_properties_test.exs` (REV-008) |
| Changes feeds | `test/storage_adapter/changes_test.exs`, `test/http/ndjson_changes_test.exs` |
| Conflict branches | `test/storage_adapter/conflicts_test.exs`, `test/storage_adapter/revision_transfer_test.exs` (REPL-016) |
| Replication interruption | `test/replication/fault_injection_test.exs` (DESIGN-005/REPL-008) |
| Replication retry | `test/replication/fault_injection_test.exs`, `test/replication/phase_transitions_test.exs` |
| Checkpoint recovery | `test/storage_adapter/checkpoints_test.exs`, `test/runtime/replication_test.exs` (REPL-011) |
| Query behavior (supported subset) | `test/query/`, `test/storage_adapter/structured_indexes_test.exs` |
| Full-text indexing/search | `test/storage_adapter/full_text_indexes_test.exs` (QUERY-014/015/017) |

### Excluded upstream categories (REF-004 documented reasons)

The following upstream case categories are excluded from Version 1 for the stated REF-004
reasons:

- **Browser-adapter behavior** — PouchDB browser/IndexedDB/WebsQL adapters have no Version 1
  equivalent (the engine is server-only, SQLite-backed).
- **CouchDB-specific API behavior** — CouchDB `_design` docs, `_view`, `_all_docs` semantics,
  `update handlers`, `show`/`list`, and other CouchDB-only endpoints are out of scope
  (DEF-007).
- **Map/reduce** — CouchDB/PouchDB map/reduce views are deferred (Version 1 ships structured
  and full-text indexes only).
- **Attachments** — binary attachments are deferred and not represented in the Version 1
  document model.
- **Unsupported query operators** — selectors are limited to the Version 1 supported subset
  (`$eq`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$and`); upstream cases exercising other
  operators are excluded.
- **Compatibility behavior not adopted by this specification** — wire/storage/client
  compatibility with CouchDB/PouchDB is explicitly out of scope (DEF-007).

## Rationale

- `DEF-006` chose an independent implementation; porting upstream suites would couple the
  release gate to an external test surface that assumes a different storage and API model.
- `DEF-007` excludes wire/protocol/storage compatibility, so differential testing
  (REF-005) would exercise behavior that Version 1 deliberately does not promise.
- The six test pillars provide adversarial, property-based, and fault-injection coverage of
  the behavioral categories REF-004 enumerates, with typed `ElixirDB.Error` contracts that
  upstream suites do not model.

## Consequences

- The Version 1 release gate is fully self-contained: `mix check.full` plus
  `mix test --only slow`, with no external CouchDB/PouchDB dependency or networked harness.
- REF-004's "documented reason" obligation is satisfied by this ADR's excluded-categories
  list; the requirements matrix links it.
- REF-005 (differential testing) remains an unmet **SHOULD**, recorded here as ACCEPTED
  out-of-scope for Version 1.

## Revisit condition

Revisit when differential testing against a pinned PouchDB/CouchDB is needed — for example,
as a pre-2.0 conformance exercise, or if a future version adopts wire/protocol compatibility
(promoting DEF-007 into scope).
