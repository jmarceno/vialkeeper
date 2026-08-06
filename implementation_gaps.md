# V1 Review — Gaps & Deviations

## Verdict

P0/P1 work closed the original release-bar holes: six-pillar tests, AdapterCase
conformance families, Plan §7.7 replication phases with fault injection, HTTP
route/schema split, SQLite responsibility modules, fixtures, and the requirements
proof index. Residual hygiene IDs below are **CLOSED** or **ACCEPTED-with-ADR**
with proving links; anything still incomplete is marked **PARTIAL**.

### Residual ID checklist (P2 hygiene)

| ID | Status | Proof |
| --- | --- | --- |
| **F1** | **ACCEPTED-with-ADR** | [docs/adr/0001-strict-decoder.md](docs/adr/0001-strict-decoder.md); adversarial suite `test/contract/strict_decoder_test.exs` + `priv/fixtures/strict_json/rejects.json` (not smoke-only) |
| **B1** | **CLOSED** | `ElixirDB.Replication.Worker` Plan §7.7 states; `test/replication/fault_injection_test.exs` — `"worker enters real :completed gen_statem state before stop"` / `":failed"`; `test/replication/phase_transitions_test.exs` — `"worker emits mandated phases through checkpoint_source"` |
| **B2** | **CLOSED** | `test/replication/fault_injection_test.exs` — `"retryable fault at #{point} may repeat work but never skips source revision"` (+ waiting/import FaultEndpoint cases) |
| **D3** | **CLOSED** | Runtime: `file_lease_os_process_test`, `owner_crash_test`, `waiter_termination_test`, `manifest_atomicity_test`, `database_isolation_test`, plus lease/admission/catalog suites. Replication + `test/end_to_end/*` pillars present |
| **D4** | **CLOSED** | `test/contract/revision_model_properties_test.exs`; `test/contract/revision_adapter_properties_test.exs` — `"adapter and pure model agree on trees, winners, conflicts, tombstones, and replay"` |
| **D5** | **CLOSED** | `priv/fixtures/{canonical_json,revision_ids,tokenization,protocol,strict_json}`; proven by `test/contract/fixtures_test.exs` + `strict_decoder_test.exs` |
| **D6** | **CLOSED** | [docs/requirements-matrix.md](docs/requirements-matrix.md) — per-ID proof index |
| **D8** | **CLOSED** | `test/end_to_end/phase8_scenario_test.exs`; companions `hot_journal_recovery_test`, `two_server_http_convergence_test`, `offline_copy_test`, `restart_convergence_test` |
| **C3** | **CLOSED** | Real ownership in `storage/sqlite/{mutations,chains,import,index_catalog,query_runner,revisions,…}.ex` (no longer empty shells) |
| **C4** | **CLOSED** | `lib/elixir_db/storage/sqlite/query_compiler.ex` owns pointer→SQLite compilation (`sqlite_path/1`, `json_expression/1`, `structured_expression/1`) |
| **F5** | **CLOSED** | `lib/elixir_db/http/schemas.ex` + `BodyReader` `:allowed_fields`; `test/http/unknown_fields_and_routes_test.exs` |
| **F6** | **CLOSED** | `lib/elixir_db/storage/results.ex` result structs used by `DatabaseOwner`; Commands normalized at owner boundary |
| **E3** | **CLOSED (drop)** | [docs/compile-benchmark.md](docs/compile-benchmark.md) — do **not** enable `module_definition: :interpreted` |

### Still PARTIAL (honest)

- **MAINT-002 / MAINT-003** — vacuum/retention surfaces exist; dedicated behaviour proofs thin (see requirements matrix).
- Historical narrative in sections A–F below is retained as the original review record; status for the residual IDs above supersedes the outdated “missing suite / 3-state worker” claims.

---

## A. Behavioral / semantic deviations from `Architecture.md` (normative)

**A1. FTS5 is not contentless-delete (`QUERY-016`, Plan §9.6).**
Spec/plan: "one **contentless-delete** FTS5 virtual table per logical full-text index," name `fts_<first-24-hex-digest>`.
Implemented (`storage/sqlite/indexes.ex:145`): a plain FTS5 table with stored columns (`document_key UNINDEXED, content`), name `exdb_f_<index_id>`, maintained by manual delete-then-reinsert. The plan allows a plain-contentless fallback *only if* a contentless-delete feasibility test fails — but there is no evidence that the required Phase 1 FTS5 feasibility spike (`CREATE VIRTUAL TABLE ... contentless`, transaction rollback rebuild, etc.) was ever run or that it failed. Both the table type and the naming scheme deviate.

**A2. The project-owned full-text matcher is dead code; FTS5 owns match semantics (`QUERY-015`, `QUERY-017`).**
Spec: "Clients MUST use a project-owned search contract rather than raw FTS5 MATCH," and the adapter "MUST escape and compile the project-owned search request." `QUERY-015` requires exact `unicode_words_v1` semantics validated by **shared tokenization conformance fixtures**.
Implemented: `Query.FullText.matches?/3` (the storage-neutral matcher) is never called anywhere in `lib/`. The actual search path compiles tokens into an FTS5 `MATCH` string (`indexes.ex:66`) and lets FTS5 decide matches. `FullText.tokens/2` only feeds the query string. Consequence: the authoritative match semantics at query time are FTS5 `unicode61`'s, not the spec's `unicode_words_v1` contract — and there are **no tokenization fixtures** to prove equivalence. This is the single most material semantic deviation.

**A3. Index planner scoring does not match `QUERY-009`.**
Spec priority: (1) explicit requested index, (2) **longest equality-compatible field prefix**, (3) compatible **range** field, (4) compatible sort fields, (5) logical index ID tie-break.
Implemented (`adapter.ex:668` `select_index/3`): score = `equality_count * 100 + sort_count`, where equality counts are set-membership matches *anywhere* in the index fields (not a prefix), and there is no distinct range-field tier. An index with more non-prefix equality matches can outrank one with a longer true prefix — violating the required deterministic priority.

**A4. UUID-on-disk mismatch is not detected at open/startup (`LIFE-007`, `ARCH-003` step 4).**
Spec `LIFE-007`: "A missing file or **UUID mismatch** MUST mark the registration as `unavailable`." `ARCH-003`: startup must "validate identity." Implemented: UUID is validated only at register time; `DatabaseOwner.init` opens by path without comparing the file's stored `database_uuid` against the registry/manifest key. A file whose UUID changed after registration (or a swapped file) opens silently instead of being marked `unavailable`.

**A5. Prepared statements are not owned/reused by the owner (`ARCH-001`, Plan §9.4).**
Spec `ARCH-001`: the owner "MUST Own all prepared statements associated with that connection." Plan §9.4: "Prepared statements are owned and reused only by `DatabaseOwner`." Implemented (`storage/sqlite/connection.ex`): statements are prepared and `Sqlite3.release`'d **on every single call**; there is no owner-held statement cache and no `statements.ex` file. Functionally correct, but violates the explicit MUST and forgoes the intended optimization.

**A6. Replication cancellation brutal-kills the whole task (`REPL-018`).**
Spec: "Cancellation MUST occur between bounded transactions. A cancellation MUST NOT interrupt an already committing target transaction." Implemented (`worker.ex`): cancellation does `Task.shutdown(task, :brutal_kill)` on the entire replication task — a hard kill that can land mid-transaction rather than at a bounded boundary.

**A7. Endpoint behaviour omits the durable-commit-confirm primitive (`REPL-004`).**
Spec lists eight primitives including "Durable target commit confirmation." Implemented (`replication/endpoint.ex`): seven callbacks; commit confirmation is folded into `put_checkpoint`. Minor — arguably acceptable since the plan's §8.1 callback list matches the implementation — but it diverges from the normative `REPL-004` enumeration.

---

## B. Replication worker architecture (Plan §7.7, §12.4, Phase 4)

**B1. The `:gen_statem` uses only 3 states, not the mandated 12.** — **CLOSED.**
Worker now implements Plan §7.7 phase states (including real `:completed` / `:failed` before stop). Proof: `test/replication/phase_transitions_test.exs`, `test/replication/fault_injection_test.exs` (`"worker enters real :completed gen_statem state before stop"`, `":failed"`).

**B2. Per-transition fault injection is therefore impossible (Plan §12.4, Phase 4 exit gate).** — **CLOSED.**
Proof: `test/replication/fault_injection_test.exs` — `"retryable fault at #{point} may repeat work but never skips source revision"` (+ waiting / FaultEndpoint import cases); helper `test/support/fault_endpoint.ex`.

---

## C. Structural deviations from `Implementation_Plan_V1.md` module layout

**C1. HTTP routes collapsed into one monolithic router (Plan §5, §10.1).**
Missing: `lib/elixir_db/http/routes/{databases,documents,changes,indexes,replications,replication_wire}.ex`. The plan mandates "Split route modules by resource and forward from the root router." All routes live in a single 527-line `router.ex`. Endpoint coverage is complete, so this is structural, not functional.

**C2. Query planner and projection are storage-coupled, not storage-neutral (`ARCH-005`, Plan §5, §21 Track A).**
Missing: `lib/elixir_db/query/planner.ex` and `lib/elixir_db/query/projection.ex`. The planning logic (`select_index/3`, `index_score/2`, `structured_sort_compatible?/3`) and projection (`project/2`) live **inside `storage/sqlite/adapter.ex`**. `ARCH-005` requires "All document, revision, changes, replication, **and query domain code** MUST access persistence through an engine-neutral storage adapter contract." Planning/projection are query-domain code currently embedded in the SQLite adapter, so a future engine swap would require re-implementing them.

**C3. SQLite per-responsibility modules are empty delegation shells (Plan §5, §16 Track A, §9).** — **CLOSED.**
Ownership lives in `mutations.ex`, `chains.ex`, `import.ex`, `index_catalog.ex`, `query_runner.ex`, `revisions.ex`, `documents.ex`, etc.; adapter is a thin coordinator. Proof: storage-adapter family tests under `test/storage_adapter/`.

**C4. `query_compiler.ex` is a stub; compilation is duplicated (Plan §21 Track B).** — **CLOSED.**
`Storage.SQLite.QueryCompiler` owns `sqlite_path/1`, `json_expression/1`, `structured_expression/1`. Proof: structured/FTS index and planner suites.

---

## D. Test architecture & conformance suite — the largest gap (Plan §12, §8.2, Phase gates)

**D1. No storage-adapter conformance suite macro (Plan §8.2, `STORE-005`, `ARCH-009`).** — **CLOSED.**
`test/support/adapter_case.ex`; used by `test/storage_adapter/v1_conformance_test.exs` and family tests.

**D2. `test/support/` is entirely missing (Plan §5, §12).** — **CLOSED.**
Helpers present: `temp_database`, `adapter_case`, `model_generators`, `revision_history_model`, `fault_adapter`, `fault_endpoint`, `test_server`, `eventual`.

**D3. Two test pillars are absent; the rest are skeletal.** — **CLOSED.**
Pillars present under `test/{contract,storage_adapter,runtime,replication,http,end_to_end}/`. Runtime proof includes `file_lease_os_process_test`, `owner_crash_test`, `waiter_termination_test`, `manifest_atomicity_test`, `database_isolation_test`. See [docs/requirements-matrix.md](docs/requirements-matrix.md).

**D4. No StreamData property/model-based tests (Plan §12.1, Phase 2 exit gate).** — **CLOSED.**
Proof: `test/contract/revision_model_properties_test.exs`; `test/contract/revision_adapter_properties_test.exs` — `"adapter and pure model agree on trees, winners, conflicts, tombstones, and replay"` (+ `test/support/model_generators.ex`, `revision_history_model.ex`).

**D5. `priv/fixtures/` is entirely missing (Plan §5, §4.4, Phase 0).** — **CLOSED.**
`priv/fixtures/{canonical_json,revision_ids,tokenization,protocol,strict_json}` proven by `test/contract/fixtures_test.exs` and `test/contract/strict_decoder_test.exs`.

**D6. No requirement-ID → test mapping (Plan §21/§22 release gate, §2 of Architecture).** — **CLOSED.**
Proof index: [docs/requirements-matrix.md](docs/requirements-matrix.md). MAINT-002/MAINT-003 remain PARTIAL there.

**D7. No operational documentation (Plan §22 Track D, release gate).** — **CLOSED.**
[docs/operations.md](docs/operations.md).

**D8. The Phase 8 end-to-end scenario (Architecture §21, 16 steps) is not validated.** — **CLOSED.**
Proof: `test/end_to_end/phase8_scenario_test.exs` — `"Architecture §21 Phase 8 scenario with local endpoints"`; also `hot_journal_recovery_test`, `two_server_http_convergence_test`, `offline_copy_test`.

---

## E. Configuration & project-setup deviations

**E1. No `shutdown_timeout` in config (`CONFIG-001`, Plan §5).**
`CONFIG-001` lists "Shutdown timeout" as host config. It is hardcoded (`30000` in `database_catalog.ex:367` `Supervisor.stop(...)`) and not configurable.

**E2. JSON nesting-depth limit is not host-enforced (`SEC-001`).** — **CLOSED.**
`StrictDecoder` reads `Config.host_limits()[:max_json_nesting_depth]`. Proof: `test/contract/strict_decoder_test.exs` — `"uses host-configured nesting depth when option omitted"`.

**E3. `elixirc_options: [module_definition: :interpreted]` not set (Plan §3.4).** — **CLOSED (drop).**
Benchmark: [docs/compile-benchmark.md](docs/compile-benchmark.md). Default `mix compile --force` ≈ 1.35 s wall; interpreted saved ~150–200 ms — not worth enabling. Decision: do **not** set the option.

**E4. No `.tool-versions` (Plan §3.1).**
`mise.toml` is present and pins `elixir 1.20.2` / `erlang 29.0.4`, satisfying "or equivalent" — acceptable. Reporting for completeness.

---

## F. Smaller deviations (reported per your instruction to flag even small ones)

**F1. `StrictDecoder` is a hand-rolled parser, not `JSON.decode/3` callbacks (Plan §4.4).** — **ACCEPTED-with-ADR.**
ADR: [docs/adr/0001-strict-decoder.md](docs/adr/0001-strict-decoder.md). Spike: `JSON.decode/3` callbacks cannot return typed `{:error, Error.t()}`, lack first-class depth/size hooks, and make duplicate-key rejection awkward. Keep hand-rolled decoder. Proof corpus: `test/contract/strict_decoder_test.exs` (JSON-002/003 adversarial suite) + `priv/fixtures/strict_json/rejects.json` — **not** thin smoke tests alone.

**F2. Negative-zero handling split across two layers (`JSON-003`).**
`-0.0` decodes to float `-0.0` and only canonicalizes to `"0"` at `Canonical.encode` time. `JSON-003` ("Negative zero SHALL canonicalize to 0") is ultimately satisfied because revision hashing goes through canonicalization — so this is correct end-to-end, but the invariant lives in the encoder rather than the decoder.

**F3. `error_mapper.ex` is a stub (Plan §5, §6.3).**
It only has `normalize/1` → `internal_error`. The real code→HTTP mapping is centralized in `error.ex` `@registry` (which is actually *better* than the plan's wording and satisfies §6.3's "centralized" requirement). The route module is just vestigial.

**F4. `ChangeNotifier.close` notifies subscribers but does not self-terminate.**
It sends `{:database_closed, uuid}` to subscribers and clears its map; `DatabaseCatalog` separately stops the runtime supervisor. Subscribers are correctly terminated with the retryable `database_closed` event (`ARCH-007`), so behavior is fine — structural only.

**F5. Unknown-field rejection is per-route, not centralized in `BodyReader` (`API-009`).** — **CLOSED.**
Central allow-lists in `lib/elixir_db/http/schemas.ex` passed through `Request` → `BodyReader` `:allowed_fields`. Proof: `test/http/unknown_fields_and_routes_test.exs`.

**F6. `Storage.Commands` structs are unused; `Storage.Results` has no result structs (Plan §6.4).** — **CLOSED.**
`Storage.Results` structs used by `DatabaseOwner`; Commands normalized at the owner boundary (`lib/elixir_db/storage/{commands,results}.ex`).

**F7. `Domain.Bookmark`/`Checkpoint`/`ReplicationJob`/`Query`/`Change`/`Leaf` have no `new/1` constructor (Plan §6.1).**
Plan §6.1: "Every domain struct MUST … Expose a `new/1` or `from_wire/1` constructor." Half the domain structs (Bookmark, Checkpoint, ReplicationJob, Query, Change, Leaf) lack constructors; the bookmark codec operates on plain maps rather than the `Bookmark` struct.

**F8. Checkpoint history dedup is `Enum.uniq_by` then sort+take 10.**
`REPL-007` requires "at most the ten most recent completed sessions." Implemented cap is 10, but the `uniq_by` keying should be verified to match "completed sessions" semantics exactly (it keys by session identity). Likely fine — flagging for explicit review.

---

## Confirmed-correct (selected, for balance)

The following match the spec closely and appear sound:
- **Pragmas** (`STORE-003`): exactly `journal_mode=DELETE, synchronous=EXTRA, foreign_keys=ON, locking_mode=NORMAL, trusted_schema=OFF`; no WAL; rejected on open if not `delete`.
- **SQLite ≥3.45.0 + FTS5 startup validation** (`STORE-002`): `Diagnostics.validate_sqlite!/0`, run at boot.
- **Schema** (§8, §9): all 7 tables, all `STRICT`, with `CHECK`/`FK`/`NOT NULL`; `application_id = 0x45584442` ("EXDB"), `user_version = 1`; `documents`/`revisions`/`changes` column sets and constraints match; tombstone `body IS NULL` enforced in DDL and code.
- **Revision IDs** (`REV-002`): `<gen>-<lowercase-sha256>` over RFC-8785 canonical `{version:1, document_id, parent_revision, deleted, body}`; tombstone forces `body: nil`; generation = parent+1.
- **Winner selection** (`REV-008`): exact order — non-deleted > deleted, higher generation, lexicographically-greatest digest.
- **Conflict resolution** (`REV-010`): exact live-leaf-set CAS, surviving-body mode (child from chosen parent + tombstones for the rest), delete-all mode, replay (`replayed: true`) vs partial-replay rejection.
- **Idempotent retries** (`TX-006`): candidate revision computed before stale-parent rejection; identical-existing → success/`replayed`; same-ID-different-content → `integrity_violation`.
- **Canonicalization** (`JSON-004`): project-owned RFC-8785 serializer, UTF-16 code-unit key ordering, `[:short]` float formatting, `-0.0 → "0"`.
- **Sequence allocation** (`TX-005`, Plan §9.4): one sequence per affected document; affected doc IDs sorted lexicographically before allocation (bulk and import).
- **`BEGIN IMMEDIATE`** for all writes (Plan §9.4); checkpoint CAS with byte-equivalent lost-response replay (`REPL-007`); replication ID = SHA-256 over the canonical fields (`REPL-006`); worker exclusivity via unique Registry key → `replication_already_running` (`REPL-018`).
- **Loopback default listener** (`CONFIG-005`); bounded admission atomics → `database_overloaded` (`ARCH-006`); companion-`.lease` SQLite `BEGIN EXCLUSIVE` with zero busy timeout (`ARCH-004`); atomic manifest write-to-temp+rename (`LIFE-007`).
- **HTTP endpoint surface** (`API-010`–`API-015`): every normative method/path is present; NDJSON stream events `change/caught_up/heartbeat/closed/error` (`CHANGE-008`); stable error registry with correct codes/statuses/retryability (`API-016`).

