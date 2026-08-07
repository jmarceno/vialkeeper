# Version 1 requirement proof index

Architecture.md is authoritative. This matrix is the release-gate **proof index**:
each requirement ID points to a **specific automated test** (file, and test/property
name when useful) that asserts the behaviour. Prefer accurate mappings over claiming
coverage that is not present.

Pure contract fixtures stay separate from SQLite, runtime, replication, HTTP, and
end-to-end pillars so a failure identifies the boundary that changed.

Companion docs: [operations.md](operations.md), [adr/0001-strict-decoder.md](adr/0001-strict-decoder.md),
[compile-benchmark.md](compile-benchmark.md).

## Design & architecture

| ID | Proving test |
| --- | --- |
| DESIGN-001 | `test/http/method_path_matrix_test.exs` — `"API-010 through API-015 method/path matrix is reachable"` (Elixir HTTP surface) |
| DESIGN-002 | `test/runtime/replication_test.exs` — `"one-shot replication reuses durable checkpoints and transfers later changes"` (state in DB files, not server process) |
| DESIGN-003 | `test/http/v1_http_contract_test.exs` — `"database, document, integrity, and NDJSON changes routes follow V1 envelopes"` |
| DESIGN-004 | `test/end_to_end/local_convergence_test.exs` — `"two-database local convergence replicates documents from A to B"` |
| DESIGN-005 | `test/replication/fault_injection_test.exs` — `"retryable fault at #{point} may repeat work but never skips source revision"` |
| DESIGN-006 | `test/storage_adapter/checkpoints_test.exs` — `"checkpoint local records support CAS write, replay, and conflict"` |
| DESIGN-007 | `test/runtime/diagnostics_test.exs` — `"runtime/0 records release metadata including the app version"`; Gitea CI `mix release.build` + release `eval` of `Diagnostics.runtime/0` |
| ARCH-001 | `test/runtime/owner_crash_test.exs` — `"owner kill restarts under supervisor; close releases lease; DB usable again"` |
| ARCH-002 | `test/runtime/owner_uniqueness_test.exs` — `"catalog open registers a single owner; second lease fails"` |
| ARCH-003 | `test/runtime/catalog_lifecycle_test.exs` — `"swapped database file UUID marks registration unavailable"` |
| ARCH-004 | `test/runtime/file_lease_os_process_test.exs` — `"separate OS process holding FileLease yields database_in_use in parent"`; also `test/runtime/file_lease_test.exs` |
| ARCH-005 | `test/storage_adapter/v1_conformance_test.exs` (via `ElixirDB.Storage.AdapterCase`) — engine-neutral adapter entry points |
| ARCH-006 | `test/runtime/admission_test.exs` — `"admission saturates at the configured limit"` |
| ARCH-007 | `test/runtime/waiter_termination_test.exs` — `"Changes.wait returns database_closed when the database is closed"` |
| ARCH-008 | `test/storage_adapter/mutations_test.exs` — `"bulk mutations are all-or-nothing"` |
| ARCH-009 | `test/storage_adapter/v1_conformance_test.exs` — suite of required adapter capabilities |

## Storage & files

| ID | Proving test |
| --- | --- |
| STORE-001 | `test/storage_adapter/lifecycle_test.exs` — `"create, close, reopen preserves identity and documents"` |
| STORE-002 | `test/storage_adapter/lifecycle_test.exs` — `"reopen after empty create still validates schema"` (startup SQLite/FTS validation path) |
| STORE-003 | `test/end_to_end/hot_journal_recovery_test.exs` — `"SIGKILL mid-write leaves -journal; reopen recovers; then portable"` (DELETE journal) |
| STORE-004 | `test/storage_adapter/full_text_indexes_test.exs` — `"winner and tombstone refresh update full-text search results"` (durable vs derived) |
| STORE-005 | `test/storage_adapter/v1_conformance_test.exs` + `test/support/adapter_case.ex` |
| FILE-001 | `test/storage_adapter/portability_test.exs` — `"closed-file OS copy without lease reopens with integrity"` |
| FILE-002 | `test/end_to_end/offline_copy_test.exs` — `"offline copy, registration, derived index rebuild, and integrity"` |
| FILE-003 | `test/end_to_end/offline_copy_test.exs` (copy of closed file; no export workflow) |
| FILE-004 | `test/storage_adapter/portability_test.exs` — reopen after OS copy under new path/registration |
| FILE-005 | `test/runtime/owner_crash_test.exs` — close releases lease; DB usable again |
| FILE-006 | `test/end_to_end/hot_journal_recovery_test.exs` — `"SIGKILL mid-write leaves -journal; reopen recovers; then portable"` |
| FILE-007 | `test/runtime/file_lease_os_process_test.exs` — live ownership blocks concurrent open (`database_in_use`) |

## Lifecycle & schema

| ID | Proving test |
| --- | --- |
| LIFE-001 | `test/storage_adapter/lifecycle_test.exs` — identity preserved across reopen |
| LIFE-002 | `test/http/v1_http_contract_test.exs` — database create under configured root |
| LIFE-003 | `test/storage_adapter/lifecycle_test.exs` — `"reopen after empty create still validates schema"` |
| LIFE-004 | `test/runtime/catalog_lifecycle_test.exs` — UUID mismatch → `unavailable` |
| LIFE-005 | `test/end_to_end/offline_copy_test.exs` — copied DB registered and opened |
| LIFE-006 | `test/storage_adapter/lifecycle_test.exs` — Version 1 schema admission |
| LIFE-007 | `test/runtime/manifest_atomicity_test.exs` — `"failed write after prior publish leaves prior manifest bytes intact"` |
| LIFE-008 | `test/http/unknown_fields_and_routes_test.exs` — `"GET /v1/databases and GET /v1/databases/:uuid plus unknown document fields"` |
| LIFE-009 | `test/end_to_end/offline_copy_test.exs` — closed before offline copy |
| SCHEMA-001 | `test/storage_adapter/lifecycle_test.exs` — metadata / identity after create |
| SCHEMA-002 | `test/storage_adapter/documents_test.exs` — `"put, update, delete, specific revision and changes"` |
| SCHEMA-003 | `test/storage_adapter/revision_transfer_test.exs` — `"diff and import transfer a root-to-leaf chain"` |
| SCHEMA-004 | `test/storage_adapter/changes_test.exs` — `"changes are ordered by sequence and advance last_sequence"` |
| SCHEMA-005 | `test/storage_adapter/checkpoints_test.exs` — local-record checkpoint CAS |
| SCHEMA-006 | `test/storage_adapter/replication_jobs_test.exs` — `"replication jobs can be listed, upserted, and deleted"` |
| SCHEMA-007 | `test/storage_adapter/structured_indexes_test.exs` — `"structured index creation, query selection, and delete"` |
| SCHEMA-008 | `test/contract/fixtures_test.exs` — `"replication ID fixtures match ElixirDB.Replication.Id (REPL-006)"` (scope fields in ID) |

## JSON & documents

| ID | Proving test |
| --- | --- |
| JSON-001 | `test/storage_adapter/documents_test.exs` — document body put/get |
| JSON-002 | `test/contract/strict_decoder_test.exs` — describe `"duplicate keys (JSON-002)"`, `"UTF-8 and malformed input (JSON-002)"`, `"fixture-driven reject vectors"` (+ `priv/fixtures/strict_json/rejects.json`) |
| JSON-003 | `test/contract/strict_decoder_test.exs` — describe `"binary64 safe integers (JSON-003)"`, `"float overflow and underflow (JSON-003)"` |
| JSON-004 | `test/contract/fixtures_test.exs` — `"canonical JSON object fixtures match ElixirDB.JSON.Canonical"`; `"RFC 8785 Appendix B number fixtures match ElixirDB.JSON.Canonical"` |
| JSON-005 | `test/contract/json_and_revision_test.exs` — `"revision identity is independent of local sequence"` |
| JSON-006 | `test/contract/strict_decoder_test.exs` — `"rejects bodies exceeding max_bytes"` |
| DOC-001 | `test/storage_adapter/documents_test.exs` — put/get by document id |
| DOC-002 | `test/storage_adapter/documents_test.exs` — `"put, update, delete, specific revision and changes"` |

## Revisions & transactions

| ID | Proving test |
| --- | --- |
| REV-001 | `test/storage_adapter/mutations_test.exs` — `"put replay returns the same revision without advancing sequence"` |
| REV-002 | `test/contract/fixtures_test.exs` — `"revision ID fixtures match ElixirDB.Revisions.Id (REV-002)"` |
| REV-003 | `test/storage_adapter/mutations_test.exs` — `"integrity mismatch is detected after revision corruption"` |
| REV-004 | `test/storage_adapter/documents_test.exs` — `"stale local writes are rejected"` |
| REV-005 | `test/storage_adapter/revision_transfer_test.exs` — `"diff and import transfer a root-to-leaf chain"` |
| REV-006 | `test/contract/revision_adapter_properties_test.exs` — `"adapter and pure model agree on trees, winners, conflicts, tombstones, and replay"` |
| REV-007 | `test/storage_adapter/conflicts_test.exs` — `"sibling imports surface conflicts and resolve with live leaf CAS"` |
| REV-008 | `test/contract/revision_model_properties_test.exs` — `"linear history winner is the tip regardless of shuffle of equal set"` |
| REV-009 | `test/storage_adapter/documents_test.exs` — delete path in `"put, update, delete, specific revision and changes"` |
| REV-010 | `test/storage_adapter/conflicts_test.exs` — `"conflict resolution CAS rejects stale leaf sets after success"`; also `revision_adapter_properties_test` resolve modes |
| TX-001 | `test/storage_adapter/mutations_test.exs` — `"bulk mutations are all-or-nothing"` |
| TX-002 | `test/storage_adapter/documents_test.exs` — successful put returns revision/sequence |
| TX-003 | `test/storage_adapter/full_text_indexes_test.exs` — `"winner and tombstone refresh update full-text search results"` |
| TX-004 | `test/storage_adapter/v1_conformance_test.exs` — `"bulk writes are atomic and allocate one change per affected document"` |
| TX-005 | `test/storage_adapter/v1_conformance_test.exs` — same bulk test (one change / affected doc) |
| TX-006 | `test/storage_adapter/mutations_test.exs` — `"put replay returns the same revision without advancing sequence"`; `"different content under one revision id is rejected"` in `revision_transfer_test.exs` |

## Changes feed

| ID | Proving test |
| --- | --- |
| CHANGE-001 | `test/storage_adapter/changes_test.exs` — `"changes are ordered by sequence and advance last_sequence"` |
| CHANGE-002 | `test/runtime/replication_test.exs` — `"one-shot replication reuses durable checkpoints and transfers later changes"` |
| CHANGE-003 | `test/storage_adapter/changes_test.exs` — read changes after mutations |
| CHANGE-004 | `test/storage_adapter/changes_test.exs` — bounded `since` reads |
| CHANGE-005 | `test/http/ndjson_changes_test.exs` — change event payload shape |
| CHANGE-006 | `test/storage_adapter/changes_test.exs` — `"reject invalid since and oversized limit"` |
| CHANGE-007 | `test/runtime/waiter_termination_test.exs` — waiters wake / terminate safely |
| CHANGE-008 | `test/http/ndjson_changes_test.exs` — `"changes stream emits change, caught_up, heartbeat, closed, and error events"` |
| CHANGE-009 | `test/http/ndjson_changes_test.exs` — stream scoped to one database |

## Query & indexes

| ID | Proving test |
| --- | --- |
| QUERY-001 | `test/contract/v1_contracts_test.exs` — `"query normalization rejects unknown fields and unsupported operators"` |
| QUERY-002 | `test/query/query_test.exs` — `"selector and pointer projection operate on materialized documents"` |
| QUERY-003 | `test/contract/v1_contracts_test.exs` — query normalization value rules |
| QUERY-004 | `test/contract/v1_contracts_test.exs` — unsupported operators rejected |
| QUERY-005 | `test/http/unknown_fields_and_routes_test.exs` — `"index create, query, replication job, and changes reject unknown fields"` |
| QUERY-006 | `test/storage_adapter/structured_indexes_test.exs` — `"structured index creation, query selection, and delete"` |
| QUERY-007 | `test/storage_adapter/structured_indexes_test.exs` — physical selection path |
| QUERY-008 | `test/query/planner_test.exs` + adapter structured index query (generated SQL path) |
| QUERY-009 | `test/query/planner_test.exs` — `"longer equality prefix beats more non-prefix equality matches"`; `"range tier beats sort-only when equality prefixes tie"` |
| QUERY-010 | `test/query/planner_test.exs` — sort-tier / tie-break cases |
| QUERY-011 | `test/query/query_test.exs` — selector without matching index still returns docs |
| QUERY-012 | `test/contract/v1_contracts_test.exs` — `"bookmarks are checksummed and query-bound"` |
| QUERY-013 | `test/http/method_path_matrix_test.exs` — explain route reachable in API-013 matrix |
| QUERY-014 | `test/storage_adapter/full_text_indexes_test.exs` — `"create, delete, and rebuild full-text indexes"` |
| QUERY-015 | `test/contract/fixtures_test.exs` — `"unicode_words_v1 tokenization fixtures match ElixirDB.Query.FullText (QUERY-015)"` |
| QUERY-016 | `test/storage_adapter/v1_conformance_test.exs` — `"structured and full-text indexes are physical and integrity checked"` |
| QUERY-017 | `test/storage_adapter/full_text_indexes_test.exs` — `"unicode_words_v1 matcher post-filters FTS5 over-matches"`; also conformance `"full-text search post-filters with unicode_words_v1 matcher (QUERY-015/017)"` |
| QUERY-018 | `test/storage_adapter/v1_conformance_test.exs` — FTS integrity in structured/FTS integrity test |
| QUERY-019 | `test/storage_adapter/full_text_indexes_test.exs` — create/delete/rebuild; `offline_copy_test` rebuild after registration |
| QUERY-020 | `test/http/v1_http_contract_test.exs` — query route envelopes |
| QUERY-021 | `test/query/planner_test.exs` — `"projection returns body when fields omitted and fields map when listed"` |

## Replication

| ID | Proving test |
| --- | --- |
| REPL-001 | `test/contract/v1_contracts_test.exs` — `"endpoint URLs reject credentials, paths, and unknown fields"` |
| REPL-002 | `test/end_to_end/local_convergence_test.exs` — A→B document convergence |
| REPL-003 | `test/end_to_end/two_server_http_convergence_test.exs` — `"two Bandit servers converge over remote wire across mid-replication restart"` |
| REPL-004 | `test/replication/phase_transitions_test.exs` — `"worker emits mandated phases through checkpoint_source"` |
| REPL-005 | `test/runtime/replication_test.exs` — local endpoint orchestration |
| REPL-006 | `test/contract/fixtures_test.exs` — `"replication ID fixtures match ElixirDB.Replication.Id (REPL-006)"` |
| REPL-007 | `test/contract/fixtures_test.exs` — `"checkpoint reconcile fixtures match CheckpointReconciler (REPL-007)"`; `"checkpoint CAS and wire fixtures execute (REPL-007)"` |
| REPL-008 | `test/replication/fault_injection_test.exs` — per-phase faults never skip committed source revision |
| REPL-009 | `test/storage_adapter/revision_transfer_test.exs` — import ordering / chain integrity |
| REPL-010 | `test/storage_adapter/revision_transfer_test.exs` — `"identical imports are idempotent no-ops"` |
| REPL-011 | `test/runtime/replication_test.exs` — `"one-shot replication reuses durable checkpoints and transfers later changes"` |
| REPL-012 | `test/runtime/replication_test.exs` — continuous path via worker phase/waiting coverage in `fault_injection_test` `"retryable fault at waiting/after_waiting never skips later source revision"` |
| REPL-013 | `test/runtime/replication_test.exs` — `"persistent one-shot jobs report terminal state and converge"` |
| REPL-014 | `test/contract/v1_contracts_test.exs` — replication request validation |
| REPL-015 | `test/contract/fixtures_test.exs` — `"protocol fixtures exist with required wire shapes"` |
| REPL-016 | `test/storage_adapter/revision_transfer_test.exs` — `"dangling parent chains are rejected atomically"` |
| REPL-017 | `test/storage_adapter/revision_transfer_test.exs` — import atomicity tests |
| REPL-018 | `test/runtime/replication_test.exs` — `"cancel between phases finishes current work without brutal_kill"`; `fault_injection_test` — `"worker enters real :completed gen_statem state before stop"` / `":failed"` |

## Config, API, maintenance, security

| ID | Proving test |
| --- | --- |
| CONFIG-001 | `test/host_config_test.exs` — first-run template creation, never-overwrite, field-level errors; `test/observability/no_network_test.exs` — host.toml-driven OTLP gate |
| CONFIG-001a | `test/host_config_test.exs` — `"shipped template decodes to the compiled defaults (no drift)"` + moveable-root cases |
| CONFIG-002 | `test/storage_adapter/v1_conformance_test.exs` — `"closed databases reopen with identity, sequence, and configuration intact"` |
| CONFIG-003 | `test/http/unknown_fields_and_routes_test.exs` — unknown client fields rejected |
| CONFIG-004 | `test/contract/v1_contracts_test.exs` — host/database config contracts |
| CONFIG-005 | `test/application_startup_test.exs` — non-loopback listener failsafe (refuses without auth/tls/override) |
| CONFIG-006 | `test/storage_adapter/v1_conformance_test.exs` — persisted config across reopen |
| CONFIG-007 | `test/host_config_test.exs` — config + tokens + cert material co-located in root; `test/observability/no_network_test.exs` — copyable root |
| AUTH-001 | `test/http/auth_plug_test.exs` — disabled pass-through; enabled requires bearer |
| AUTH-002 | `test/release_commands_test.exs` — `bin/elixir_db token` digest is SHA-256 of raw token; `test/host_config_test.exs` — digest storage/round-trip |
| AUTH-003 | `test/replication/auth_token_transport_test.exs` — remote `auth_token` sent as `Authorization: Bearer` over the wire |
| AUTH-004 | `test/http/auth_plug_test.exs` — missing/malformed/wrong produce indistinguishable errors |
| AUTH-005 | `test/http/auth_plug_test.exs` — loopback no-auth default passes without credentials |
| TLS-001 | `test/application_startup_test.exs` + `test/host_config_test.exs` — single HTTPS listener, in-root cert/key paths |
| TLS-002 | `test/host_config_test.exs` — cert/key relative to root; escaping paths rejected |
| TLS-003 | `test/contract/v1_contracts_test.exs` — `https` endpoint scheme accepted |
| API-001 | `test/http/router_test.exs` — `"database and document endpoints return versioned envelopes"` |
| API-002 | `test/http/router_test.exs` — versioned `/v1` envelopes |
| API-003 | `test/http/method_path_matrix_test.exs` — database management methods |
| API-004 | `test/http/method_path_matrix_test.exs` — document methods |
| API-005 | `test/http/ndjson_changes_test.exs` + method/path matrix changes routes |
| API-006 | `test/http/method_path_matrix_test.exs` — query/index methods |
| API-007 | `test/http/method_path_matrix_test.exs` — replication job + wire methods |
| API-008 | `test/contract/fixtures_test.exs` — `"protocol fixtures exist with required wire shapes"` (`http_envelopes.json`) |
| API-009 | `test/http/unknown_fields_and_routes_test.exs` — `"registration rejects unknown fields"`; `"index create, query, replication job, and changes reject unknown fields"` |
| API-010 | `test/http/method_path_matrix_test.exs` — `"API-010 through API-015 method/path matrix is reachable"` |
| API-011 | `test/http/method_path_matrix_test.exs` (document section of matrix) |
| API-012 | `test/http/ndjson_changes_test.exs` + method/path matrix |
| API-013 | `test/http/method_path_matrix_test.exs` (index/query section) |
| API-014 | `test/http/method_path_matrix_test.exs` (replication-job section) |
| API-015 | `test/http/method_path_matrix_test.exs` (wire section); `two_server_http_convergence_test.exs` for live wire |
| API-016 | `test/http/v1_http_contract_test.exs` — `"HTTP rejects unknown fields and returns the stable error envelope"` |
| MAINT-001 | `test/storage_adapter/mutations_test.exs` — `"integrity mismatch is detected after revision corruption"` |
| MAINT-002 | *PARTIAL* — vacuum API present in method matrix; no dedicated vacuum behaviour assertion found |
| MAINT-003 | *PARTIAL* — retention config exists; no dedicated retention-pruning test located |
| MAINT-004 | `test/runtime/waiter_termination_test.exs` + `owner_crash_test` close path |
| SEC-001 | `test/contract/strict_decoder_test.exs` — nesting/size; `test/runtime/admission_test.exs` — admission |
| SEC-002 | `test/query/planner_test.exs` + structured index tests (compiled paths, not string-concat client SQL) |
| SEC-003 | `test/http/v1_http_contract_test.exs` — relative paths under database root |
| SEC-004 | `test/runtime/catalog_lifecycle_test.exs` — swapped file / UUID mismatch admission |

## Reference, deferred, outcomes

| ID | Proving test / note |
| --- | --- |
| REF-001–REF-005 | Behavioural overlap covered by the revision/changes/replication/query/FTS suites above (independent implementation, DEF-006/007). No PouchDB/CouchDB port or differential harness in Version 1 — selected/excluded upstream categories and REF-004 documented reasons are recorded in [adr/0002-upstream-test-scope.md](adr/0002-upstream-test-scope.md) |
| DEF-001–DEF-020 | Deferred scope: absence of unsupported routes asserted by `test/http/unknown_fields_and_routes_test.exs` / method matrix (non-404 only for normative paths); normative text in Architecture.md |
| OUT-001 | `test/end_to_end/hot_journal_recovery_test.exs` — DELETE journal / `-journal` recovery (WAL out of scope) |
| OUT-002 | `test/end_to_end/offline_copy_test.exs` — closed-file copy, no mandatory export |

## End-to-end release scenarios (cross-cutting)

| Scenario | Proving test |
| --- | --- |
| Architecture §21 Phase 8 (16 steps) | `test/end_to_end/phase8_scenario_test.exs` — `"Architecture §21 Phase 8 scenario with local endpoints"` |
| Two-server HTTP convergence + restart | `test/end_to_end/two_server_http_convergence_test.exs` |
| Offline copy + register + index rebuild | `test/end_to_end/offline_copy_test.exs` |
| Hot rollback-journal crash recovery | `test/end_to_end/hot_journal_recovery_test.exs` |
| Restart convergence | `test/end_to_end/restart_convergence_test.exs` |
| Local convergence | `test/end_to_end/local_convergence_test.exs` |

## Final release commands

```text
mix check.full
mix test --only slow
```

The full gate includes formatting, warnings-as-errors compilation, cycle detection,
ExUnit, and Dialyzer. Property suites use `@moduletag :property`; OS-process lease
tests use `@moduletag :os_process` / `@tag :slow`.
