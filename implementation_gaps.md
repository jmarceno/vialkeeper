# V1 Review — Gaps & Deviations

## Verdict

The **core data model and storage layer are implemented faithfully** — revision IDs, winner selection, the revision/conflict model, the SQLite schema, pragmas, application ID, sequence allocation, and the HTTP route surface all match the spec. The **HTTP endpoints are essentially complete** (no missing routes).

However, the delivery is **not actually at the V1 release bar** the two documents set. The dominant gaps are: (1) the test architecture and storage-adapter conformance suite are largely absent, (2) several planned modules were collapsed into a monolithic SQLite adapter making query planning storage-coupled, (3) the replication worker is a single-task loop instead of the mandated per-transition state machine, and (4) a handful of behavioral deviations (FTS5 representation, planner scoring, file-lease/UUID checks, prepared-statement ownership).

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

**B1. The `:gen_statem` uses only 3 states, not the mandated 12.**
Spec/Plan §7.7: states `idle, handshake, read_changes, diff, fetch_chains, import, checkpoint_target, checkpoint_source, waiting, backoff, completed, failed`, so that "transition, cancellation, and fault tests [are] direct and deterministic."
Implemented: the worker is `:gen_statem` but transitions only among `idle / running / backoff`. The entire replication runs as **one async task** (`Replication.run/3`); the phase states exist only as literals in `JobManager` ETS guards and are never emitted. `completed`/`failed` are reported at terminate, not as states.

**B2. Per-transition fault injection is therefore impossible (Plan §12.4, Phase 4 exit gate).**
Pillar 4 requires injecting failure "before and after each state transition" (handshake, changes read, diff, chain fetch, import commit, target/source checkpoint, wait subscription) with the core assertion "every injected retryable failure may repeat work but may never skip a committed revision." Because the batch is one opaque task, this fault model cannot be implemented or tested as designed. The Phase 4 exit gate ("fault injection at every transition proves no committed source revision can be skipped") is not satisfiable in the current shape.

---

## C. Structural deviations from `Implementation_Plan_V1.md` module layout

**C1. HTTP routes collapsed into one monolithic router (Plan §5, §10.1).**
Missing: `lib/elixir_db/http/routes/{databases,documents,changes,indexes,replications,replication_wire}.ex`. The plan mandates "Split route modules by resource and forward from the root router." All routes live in a single 527-line `router.ex`. Endpoint coverage is complete, so this is structural, not functional.

**C2. Query planner and projection are storage-coupled, not storage-neutral (`ARCH-005`, Plan §5, §21 Track A).**
Missing: `lib/elixir_db/query/planner.ex` and `lib/elixir_db/query/projection.ex`. The planning logic (`select_index/3`, `index_score/2`, `structured_sort_compatible?/3`) and projection (`project/2`) live **inside `storage/sqlite/adapter.ex`**. `ARCH-005` requires "All document, revision, changes, replication, **and query domain code** MUST access persistence through an engine-neutral storage adapter contract." Planning/projection are query-domain code currently embedded in the SQLite adapter, so a future engine swap would require re-implementing them.

**C3. SQLite per-responsibility modules are empty delegation shells (Plan §5, §16 Track A, §9).**
`documents.ex`, `revisions.ex`, `changes.ex`, `local_records.ex`, `replication_jobs.ex`, `structured_indexes.ex`, `full_text_indexes.ex`, `integrity.ex` are all one-line `@moduledoc false` delegates. All SQL lives in `adapter.ex` (2734 lines) and `indexes.ex` (360 lines). The plan assigns these modules real ownership (e.g., Phase 2 Track A: "`documents.ex`/`revisions.ex`/`statements.ex` — implement physical leaf updates, winner materialization"). The files exist but the responsibilities are centralized.

**C4. `query_compiler.ex` is a stub; compilation is duplicated (Plan §21 Track B).**
It delegates to `Normalizer`; the real pointer→SQLite compilation is duplicated in `adapter.ex` (`json_expression/1`, `sqlite_path/1`) and `indexes.ex` (`structured_expression/1`).

---

## D. Test architecture & conformance suite — the largest gap (Plan §12, §8.2, Phase gates)

**D1. No storage-adapter conformance suite macro (Plan §8.2, `STORE-005`, `ARCH-009`).**
There is no `use ElixirDB.Storage.AdapterCase, adapter: ...`. `STORE-005`/`ARCH-009` require a conformance suite that is engine-neutral and could run against any adapter. The current `v1_conformance_test.exs` is a plain `ExUnit.Case` hardcoded to the SQLite adapter (4 tests).

**D2. `test/support/` is entirely missing (Plan §5, §12).**
All six mandated helpers are absent: `temp_database`, `adapter_case`, `model_generators`, `fault_adapter`, `test_server`, `eventual`. `test_helper.exs` is just `ExUnit.start()`.

**D3. Two test pillars are absent; the rest are skeletal.**
- `test/replication/` — **missing** as a directory (Pillar 4: fault-injecting endpoint wrapper, per-transition fault injection, generated convergence histories).
- `test/end_to_end/` — **missing entirely** (Pillar 6: the 6 required scenarios incl. two-server convergence with restart, offline copy + registration + index rebuild, crash recovery with hot rollback journal).
- `test/runtime/` — 1 file, replication only. Missing the 8 required runtime areas (owner uniqueness, cross-OS-process lease exclusion, admission saturation, owner crash/restart, waiter termination, catalog recovery, registration atomicity, independent-DB isolation).
- `test/storage_adapter/` — 2 files / 6 tests. Plan §12.2 lists 11 required families (lifecycle, mutations, conflicts, changes, revision_transfer, checkpoints, jobs, structured_indexes, full_text_indexes, integrity, portability).
- `test/contract/` — 2 files / 11 tests. Plan §12.1 lists ~13 areas.
- `test/http/` — 2 files / 3 tests. Plan §12.5: every method/path, schemas, bulk semantics, NDJSON streams, remote wire calls.

Total: ~9 test files, ~706 lines — vs. the six-pillar architecture the plan mandates.

**D4. No StreamData property/model-based tests (Plan §12.1, Phase 2 exit gate).**
Phase 2 exit gate: "Random generated histories produce identical revision trees, winners, active conflicts, tombstones, and replay results in the pure model and SQLite adapter." No `model_generators` and no generated-history tests exist.

**D5. `priv/fixtures/` is entirely missing (Plan §5, §4.4, Phase 0).**
No `canonical_json/`, `revision_ids/`, `tokenization/`, or `protocol/` fixtures. Phase 0 requires all official RFC 8785 JCS vectors, RFC 8785 number examples, UTF-16 property-ordering tests, and cross-runtime fixture verification. The Phase 0 exit gate ("All official JCS vectors … MUST pass") cannot be evidenced.

**D6. No requirement-ID → test mapping (Plan §21/§22 release gate, §2 of Architecture).**
Architecture §2: "Every normative requirement identified by an ID … MUST have corresponding automated validation." Release gate: "Every `Arch V2` requirement ID maps to at least one test." Given the thin suite, most `ARCH/STORE/REV/TX/CHANGE/QUERY/REPL/SEC/MAINT` IDs are unmapped.

**D7. No operational documentation (Plan §22 Track D, release gate).**
Release gate: "The final pull request contains architecture, implementation, and operational documentation" (start/stop, database root, registration, offline copy, lease recovery, integrity checking, job states, limits, error-code troubleshooting). Not present.

**D8. The Phase 8 end-to-end scenario (Architecture §21, 16 steps) is not validated.** No end-to-end tests exist; the scenario is a release gate.

---

## E. Configuration & project-setup deviations

**E1. No `shutdown_timeout` in config (`CONFIG-001`, Plan §5).**
`CONFIG-001` lists "Shutdown timeout" as host config. It is hardcoded (`30000` in `database_catalog.ex:367` `Supervisor.stop(...)`) and not configurable.

**E2. JSON nesting-depth limit is not host-enforced (`SEC-001`).**
`SEC-001` requires bounded "JSON nesting depth" as a host limit. It is hardcoded `@default_max_depth 100` in `strict_decoder.ex`, absent from `Config.host_limits()`.

**E3. `elixirc_options: [module_definition: :interpreted]` not set (Plan §3.4).**
Plan §3.4 says keep it "only if the initial compile benchmark confirms … a benefit." No benchmark evidence either way. Minor / conditional.

**E4. No `.tool-versions` (Plan §3.1).**
`mise.toml` is present and pins `elixir 1.20.2` / `erlang 29.0.4`, satisfying "or equivalent" — acceptable. Reporting for completeness.

---

## F. Smaller deviations (reported per your instruction to flag even small ones)

**F1. `StrictDecoder` is a hand-rolled parser, not `JSON.decode/3` callbacks (Plan §4.4).**
Plan §4.4 mandates `StrictDecoder` "SHALL use `JSON.decode/3` custom decoder callbacks." Implementation is a bespoke recursive-descent parser (the `JSON` module is used only to unescape string tokens). Functionally it does enforce duplicate-key rejection, the binary64 model (integer safe-range, overflow rejection, non-zero underflow-to-zero rejection), UTF-8 validity, nesting, and size — so the *behavior* matches; the *mechanism* diverges.

**F2. Negative-zero handling split across two layers (`JSON-003`).**
`-0.0` decodes to float `-0.0` and only canonicalizes to `"0"` at `Canonical.encode` time. `JSON-003` ("Negative zero SHALL canonicalize to 0") is ultimately satisfied because revision hashing goes through canonicalization — so this is correct end-to-end, but the invariant lives in the encoder rather than the decoder.

**F3. `error_mapper.ex` is a stub (Plan §5, §6.3).**
It only has `normalize/1` → `internal_error`. The real code→HTTP mapping is centralized in `error.ex` `@registry` (which is actually *better* than the plan's wording and satisfies §6.3's "centralized" requirement). The route module is just vestigial.

**F4. `ChangeNotifier.close` notifies subscribers but does not self-terminate.**
It sends `{:database_closed, uuid}` to subscribers and clears its map; `DatabaseCatalog` separately stops the runtime supervisor. Subscribers are correctly terminated with the retryable `database_closed` event (`ARCH-007`), so behavior is fine — structural only.

**F5. Unknown-field rejection is per-route, not centralized in `BodyReader` (`API-009`).**
Spec `API-009`: "Version 1 request objects MUST reject unknown top-level fields." `BodyReader` does generic content-type/size/decode checks; unknown-field rejection is done ad hoc per route via `unknown_fields?/2` allow-lists. Behavior is present for the routes that implement it, but it's not a single enforced boundary.

**F6. `Storage.Commands` structs are unused; `Storage.Results` has no result structs (Plan §6.4).**
All 19 command structs exist but are fieldless empty structs with zero consumers — the runtime dispatches tagged tuples (`{:command, :put, request}`) instead. Plan §6.4 prescribes command structs "to improve compiler inference, documentation, logging, and fault-test targeting." They're decorative.

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

