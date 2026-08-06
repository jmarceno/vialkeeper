# Version 1 requirement validation matrix

Architecture.md is authoritative. This matrix is the release-gate index: every
architecture requirement is listed exactly once and points to the implementation
boundary and the automated validation that exercises it. Pure contract tests are
kept separate from SQLite, runtime, replication, and HTTP tests so a failure
identifies the boundary that changed.

| Requirement IDs | Implementation boundary | Automated validation |
| --- | --- | --- |
| DESIGN-001, DESIGN-002, DESIGN-003, DESIGN-004, DESIGN-005, DESIGN-006 | ElixirDB.Application, Runtime.DatabaseCatalog, storage-neutral domain modules | test/contract/v1_contracts_test.exs, test/runtime/replication_test.exs |
| ARCH-001, ARCH-002, ARCH-003, ARCH-004, ARCH-005, ARCH-006, ARCH-007, ARCH-008, ARCH-009 | Runtime.DatabaseRuntimeSupervisor, DatabaseOwner, DatabaseAdmission, Storage.Adapter | test/runtime/replication_test.exs, test/storage_adapter/v1_conformance_test.exs |
| STORE-001, STORE-002, STORE-003, STORE-004, STORE-005 | Storage.SQLite.Adapter, Schema, Connection | test/storage_adapter/documents_test.exs, test/storage_adapter/v1_conformance_test.exs |
| FILE-001, FILE-002, FILE-003, FILE-004, FILE-005, FILE-006, FILE-007 | SQLite schema/lifecycle and Runtime.FileLease | test/storage_adapter/v1_conformance_test.exs, test/runtime/replication_test.exs |
| LIFE-001, LIFE-002, LIFE-003, LIFE-004, LIFE-005, LIFE-006, LIFE-007, LIFE-008, LIFE-009 | Runtime.DatabaseCatalog, RegistrationManifest, FileLease | test/http/v1_http_contract_test.exs, test/runtime/replication_test.exs |
| SCHEMA-001, SCHEMA-002, SCHEMA-003, SCHEMA-004, SCHEMA-005, SCHEMA-006, SCHEMA-007, SCHEMA-008 | priv/sqlite/schema_v1.sql, Storage.SQLite.Schema, local-record/index/job persistence | test/storage_adapter/v1_conformance_test.exs, test/runtime/replication_test.exs |
| JSON-001, JSON-002, JSON-003, JSON-004, JSON-005, JSON-006 | JSON.StrictDecoder, JSON.Canonical, document validation | test/contract/json_and_revision_test.exs, test/contract/v1_contracts_test.exs, test/storage_adapter/v1_conformance_test.exs |
| DOC-001, DOC-002 | Documents, Storage.SQLite.Adapter document validation | test/storage_adapter/documents_test.exs, test/http/v1_http_contract_test.exs |
| REV-001, REV-002, REV-003, REV-004, REV-005, REV-006, REV-007, REV-008, REV-009, REV-010 | Revisions.Id, Winner, ConflictResolution, SQLite revision persistence | test/contract/json_and_revision_test.exs, test/storage_adapter/v1_conformance_test.exs |
| TX-001, TX-002, TX-003, TX-004, TX-005, TX-006 | SQLite transaction boundary, materialized winner/index refresh, bulk preparation/finalization | test/storage_adapter/v1_conformance_test.exs |
| CHANGE-001, CHANGE-002, CHANGE-003, CHANGE-004, CHANGE-005, CHANGE-006, CHANGE-007, CHANGE-008, CHANGE-009 | SQLite changes table, Changes, ChangeNotifier, NDJSON stream | test/storage_adapter/documents_test.exs, test/http/v1_http_contract_test.exs |
| QUERY-001, QUERY-002, QUERY-003, QUERY-004, QUERY-005 | Query.Normalizer, Query, selector compiler | test/contract/v1_contracts_test.exs, test/query/query_test.exs |
| QUERY-006, QUERY-007, QUERY-008, QUERY-009, QUERY-010, QUERY-011, QUERY-012, QUERY-013 | logical index lifecycle and bounded planner in Query and SQLite adapter | test/query/query_test.exs, test/storage_adapter/v1_conformance_test.exs |
| QUERY-014, QUERY-015, QUERY-016, QUERY-017, QUERY-018 | Query.FullText, FTS5 index manager and integrity checks | test/storage_adapter/v1_conformance_test.exs |
| QUERY-019, QUERY-020, QUERY-021 | index rebuild, projection, extraction, and query response consistency | test/query/query_test.exs, test/http/v1_http_contract_test.exs |
| REPL-001, REPL-002, REPL-003, REPL-004, REPL-005 | endpoint boundary and Replication orchestration | test/contract/v1_contracts_test.exs, test/runtime/replication_test.exs |
| REPL-006, REPL-007, REPL-008, REPL-009, REPL-010 | Replication.Id, checkpoint reconciler, endpoint batch algorithm and CAS records | test/contract/v1_contracts_test.exs, test/runtime/replication_test.exs |
| REPL-011, REPL-012, REPL-013 | one-shot/continuous orchestration and Replication.JobManager | test/runtime/replication_test.exs, test/http/v1_http_contract_test.exs |
| REPL-014, REPL-015, REPL-016, REPL-017, REPL-018 | replication request validation, handshake, complete chains, import transaction, worker registry/cancellation | test/contract/v1_contracts_test.exs, test/storage_adapter/v1_conformance_test.exs, test/runtime/replication_test.exs |
| CONFIG-001, CONFIG-002, CONFIG-003, CONFIG-004, CONFIG-005, CONFIG-006 | Config, runtime listener, host/database bounds and persisted storage-neutral config | test/contract/v1_contracts_test.exs, test/storage_adapter/v1_conformance_test.exs |
| API-001, API-002, API-003, API-004, API-005, API-006, API-007, API-008, API-009 | HTTP.Request, BodyReader, Response, protocol version and envelopes | test/http/router_test.exs, test/http/v1_http_contract_test.exs |
| API-010, API-011, API-012, API-013 | database, document, changes, index, query, and maintenance routes in HTTP.Router | test/http/router_test.exs, test/http/v1_http_contract_test.exs |
| API-014, API-015 | replication job and replication wire routes/endpoints | test/runtime/replication_test.exs, test/http/v1_http_contract_test.exs |
| API-016 | stable Error registry and HTTP error mapper | test/contract/v1_contracts_test.exs, test/http/v1_http_contract_test.exs |
| MAINT-001, MAINT-002, MAINT-003, MAINT-004 | adapter integrity, index integrity, owner-scoped close and retained revisions | test/storage_adapter/v1_conformance_test.exs, test/runtime/replication_test.exs |
| SEC-001, SEC-002, SEC-003, SEC-004 | strict JSON, Config/Query limits, generated SQL, safe catalog paths, schema admission | test/contract/v1_contracts_test.exs, test/http/v1_http_contract_test.exs, test/storage_adapter/v1_conformance_test.exs |
| REF-001, REF-002, REF-003, REF-004, REF-005 | revision/changes/replication behavior implemented as independent domain and adapter code | test/contract/v1_contracts_test.exs, test/storage_adapter/v1_conformance_test.exs, test/runtime/replication_test.exs |
| DEF-001, DEF-002, DEF-003, DEF-004, DEF-005, DEF-006, DEF-007, DEF-008, DEF-009, DEF-010, DEF-011, DEF-012, DEF-013, DEF-014, DEF-015, DEF-016, DEF-017, DEF-018, DEF-019, DEF-020 | deferred-scope boundaries documented by the V1 implementation and absence of unsupported routes | test/http/v1_http_contract_test.exs, Architecture.md |
| OUT-001–OUT-002 | release outputs and operational validation | mix check.full, this matrix, README.md |

The matrix intentionally points to the smallest meaningful test boundary. A
passing test still has to prove behavior—mutation, persistence/reload,
replication state, rendered response, or stream output—not only module or
element presence.

Final release commands:

    mix check.full
    mix test --only slow

The full gate includes formatting, warnings-as-errors compilation, cycle
detection, all ExUnit tests, and Dialyzer.
