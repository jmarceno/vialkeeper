# Plan — V1 Compliance Fixes

Addresses every non-telemetry open item from [implementation_gaps.md](../implementation_gaps.md).
Items are ordered by severity. Each item is self-contained: an agent can take one item,
make the change, and run its tests without needing the others. Telemetry (G2) is
specified directly in the master plan ([Implementation_Plan_V1.md §11](../Implementation_Plan_V1.md)
and [Architecture.md §20.5](../Architecture.md), `OBSV-001` through `OBSV-007`).

Conventions:
- Run `mix check.fast` after each item (format + warnings-as-errors + non-slow tests).
- Run `mix check.full` after the last item (adds xref cycles + Dialyzer).
- Every spec ID below is defined in `Architecture.md` or `Implementation_Plan_V1.md`.
- File:line references are current as of this plan; re-locate with `grep` before editing.

---

## Item 1 — Fix TX-006 stale-parent replay to check the winner (G1, CORRECTNESS)

**Severity: high.** This is the only item that can produce a wrong durable result.

### Problem
`lib/elixir_db/storage/sqlite/mutations.ex`:
- `candidate_revision/7` (around line 197-203) returns `{:ok, {:replayed, revision}}`
  when the supplied parent is stale AND the identical candidate already exists.
- `apply_local_tx/2` (line 30-32) turns that into an unconditional success
  `{:ok, %{..., replayed: true}}`.
- Bulk mirror: `prepare_bulk_mutation_operation` (around line 359-369).

TX-006 requires: identical revision exists but a later/conflicting revision changed the
current state → MUST return `revision_conflict` with `operation_already_committed: true`.

### Fix
The stale-branch replay must verify the candidate is still the winner, exactly like the
non-stale path already does at `insert_local_revision` (line 247-263).

1. In `candidate_revision/7`, change the `{:ok, existing} -> ... Revisions.same?(...)`
   branch so it does NOT swallow the winner check. Cleanest approach: stop returning a
   sentinel `{:replayed, _}` from the stale branch. Instead return the same
   `revision_conflict` used elsewhere, and let `insert_local_revision` own the single,
   unified replay path (it already checks `doc.winning_revision == candidate.revision_id`
   and emits `operation_already_committed: true` on line 257-260).
   - Concretely: in the stale-parent case where the identical revision exists, fall
     through to the normal `insert_local_revision` path (which re-findds the existing
     revision and applies the correct winner check), rather than short-circuiting to
     `{:replayed, _}`.
2. Apply the same change to `prepare_bulk_mutation_operation` (line ~359-369) so bulk
   puts/deletes use the same unified replay path. The bulk explicit-find branch
   (line 374-395) is already correct — use it as the reference.
3. Remove the now-unused `{:replayed, candidate}` clause in `apply_local_tx` (line 30-32)
   if it becomes unreachable. If kept for clarity, ensure it can only be reached after a
   winner check.

### Tests
Add to `test/storage_adapter/mutations_test.exs` (model after the existing
`"put replay returns the same revision without advancing sequence"`):

- `"stale-parent retry of a superseded revision returns revision_conflict with operation_already_committed"`:
  1. Put doc A → rev1.
  2. Put doc A (if_revision rev1) → rev2. (now winner = rev2)
  3. Retry the rev1→rev2 put but with `if_revision: rev1` and the **same body** that
     produced rev2. Expect `revision_conflict` with
     `details.operation_already_committed == true`, NOT success.
- `"stale-parent retry that is still the winner replays successfully"`:
  1. Put doc A → rev1.
  2. Retry the same create (idempotent) — expect success `replayed: true`.
- Bulk equivalent in `test/storage_adapter/v1_conformance_test.exs` or `mutations_test.exs`:
  a bulk-write containing a stale-parent superseded retry fails the batch atomically
  (TX-004) with `operation_already_committed: true`.

### Gate
`mix test test/storage_adapter/mutations_test.exs` then `mix check.fast`.

---

## Item 2 — Make `internal_error` retryable "depends on details" (G3, API-016)

**Severity: medium.** Spec/API contract fidelity.

### Problem
`lib/elixir_db/error.ex:43` hardcodes `internal_error: {500, true}`. API-016 says
`internal_error` retryable is "**Depends on details**". The current registry cannot
express per-details non-retryable internal errors.

### Fix
Keep the registry default but allow callers to flag a non-retryable internal error.

1. Add a sentinel registry value for `internal_error` that signals "depends on details",
   e.g. `internal_error: {500, :depends}`.
2. In `new/4`, when `code == :internal_error` and no explicit `:retryable` opt is given:
   - default `retryable` to `false` (the safer default for an unknown internal failure),
   - but keep the handful of internal-error call sites that are genuinely transient
     (e.g. SQLite `SQLITE_BUSY`-flavored paths, if any) explicitly opt in via
     `retryable: true`.
3. Audit every `ElixirDB.Error.internal_error(...)` call site (`grep -rn "internal_error("
   lib/`) and decide retryable per site. Most (adapter `normalize_error`, unexpected
   phase result, etc.) should be `retryable: false`.
4. `Error.public/1` already serializes whatever `retryable` boolean is set — no envelope
   change needed.

### Tests
- `test/http/v1_http_contract_test.exs`: assert a triggered `internal_error` returns
  `retryable: false` by default (or the documented per-site value).
- Keep existing `internal_error` registry test green; update it to the new default.

### Gate
`mix test test/http/ test/contract/` then `mix check.fast`.

---

## Item 3 — Remove dead `ElixirDB.HTTP.ErrorMapper` (G4, hygiene)

**Severity: low.** Zero functional impact; pure cleanup.

### Problem
`lib/elixir_db/http/error_mapper.ex` has zero call sites in `lib/` or `test/`. The real
mapping lives in `error.ex` `@registry`.

### Fix
1. `git rm lib/elixir_db/http/error_mapper.ex`.
2. Confirm no references remain: `grep -rn "ErrorMapper" lib/ test/ config/` → empty.
3. Plan §5 lists `error_mapper.ex` in the module tree — that line is now aspirational;
   either drop it from the plan or leave it (the plan is a historical artifact). No code
   change beyond the deletion.

### Tests
No behavior change. `mix check.fast` must still pass (it will).

### Gate
`mix check.fast`.

---

## Item 4 — Change QUERY-011 scan-threshold boundary to "below" (G5)

**Severity: low.** Spec-wording fidelity; behavior change is one row at the boundary.

### Problem
`lib/elixir_db/storage/sqlite/query_runner.ex:88` uses `count <= scan_threshold`, but
QUERY-011 says "only when the number of candidate documents is **below** the configured
scan threshold." Same inclusive comparison at line 164 (`enforce_scan_limit`).

### Fix
1. Change `count <= scan_threshold` → `count < scan_threshold` at query_runner.ex:88.
2. Change the post-hoc `enforce_scan_limit` comparison at line 164 the same way
   (`examined <= threshold` → `examined < threshold`) so the two gates agree.
3. Sanity-check the semantics: with `scan_threshold = 1000`, a database with exactly 1000
   candidate docs must now require an index (`index_required`), not scan.

### Tests
In `test/query/query_test.exs` or `test/storage_adapter/structured_indexes_test.exs`:
- `"full scan is permitted only below scan_threshold"`: seed exactly `scan_threshold`
  documents with no matching index and assert `index_required`. Seed `scan_threshold - 1`
  and assert the scan succeeds.

### Gate
`mix test test/query/ test/storage_adapter/` then `mix check.fast`.

---

## Item 5 — Replace bare `Pointer.parse` match with an error path (G6)

**Severity: low.** Defensive; unreachable from HTTP today.

### Problem
`lib/elixir_db/storage/sqlite/query_compiler.ex:15`:
`{:ok, tokens} = Pointer.parse(path)` crashes with `MatchError` on an invalid path.
HTTP paths are pre-validated by the Normalizer, so this is internal-misuse only.

### Fix
1. Make `sqlite_path/1` return `{:ok, iodata()} | {:error, Error.t()}`:
   ```elixir
   def sqlite_path(path) do
     case Pointer.parse(path) do
       {:ok, tokens} -> {:ok, build_path(tokens)}
       {:error, error} -> {:error, error}
     end
   end
   ```
2. Thread the result through `json_expression/1` and `structured_expression/1` (they feed
   index DDL and query SQL). Both already run inside the adapter transaction boundary.
3. Update callers in `indexes.ex` (`create_structured`, line ~136) and `query_compiler`
   consumers to handle the error tuple instead of assuming success.

### Tests
- Existing tokenization / structured-index / planner suites cover the happy path.
- Add one unit test asserting an internally-invalid pointer returns an `Error`, not a
  crash (e.g. in `test/storage_adapter/structured_indexes_test.exs`).

### Gate
`mix test test/storage_adapter/structured_indexes_test.exs test/query/` then
`mix check.fast`.

---

## Item 6 — Document the REF-004 scope decision (G10, MUST → ADR)

**Severity: low (process).** REF-004 is a MUST, but the project deliberately chose an
independent implementation (DEF-006/007). The MUST is to *document* the exclusion, not to
port the suite.

### Problem
No documented selected/excluded PouchDB/CouchDB upstream-case list exists.

### Fix
1. Add `docs/adr/0002-upstream-test-scope.md` recording:
   - Decision: Version 1 implements an independent replication engine (DEF-006) and does
     not ship a PouchDB/CouchDB port or differential test harness.
   - Rationale: wire/storage compatibility with CouchDB/PouchDB is explicitly out of scope
     (DEF-007); behavioral overlap is covered by the six test pillars (revision model,
     changes, replication fault injection, convergence).
   - Excluded-upstream reasons (per REF-004's required categories): browser-adapter
     behavior, CouchDB-specific API, map/reduce, attachments, deferred query operators,
     deferred selective replication.
   - Revisit condition: when differential testing is needed (e.g. pre-2.0).
2. Update `docs/requirements-matrix.md` REF-001–REF-005 row to link the ADR instead of
   the bare "no separate suite" note.

### Tests
None (documentation).

### Gate
`mix check.fast` (formatting only).

---

## Item 7 — Record release metadata in CI (G11, Plan §3.1)

**Severity: low.** Plan §3.1 release-pipeline fidelity.

### Problem
Plan §3.1 mandates the release record Elixir/OTP/Exqlite/SQLite version, SQLite compile
options, and git commit. `Diagnostics.runtime/0` captures all except git commit; CI does
not publish any of it.

### Fix
1. Add git commit to `Diagnostics.runtime/0`:
   ```elixir
   git_commit: System.get_env("ELIXIRDB_GIT_REF") || git_sha_from_file() || "unknown"
   ```
   (Prefer reading a build-time file written by the release step over shelling out to
   `git` at runtime.)
2. In `.github/workflows/ci.yml`, after a successful `mix check.full`, emit the metadata
   as a build artifact: run `mix run -e 'IO.inspect(ElixirDB.Diagnostics.runtime())'` and
   capture `${{ github.sha }}` into a `release-metadata.json` artifact via
   `actions/upload-artifact@v4`. Gate it on a `release` or `workflow_dispatch` input if
   you do not want it on every CI run.

### Tests
- `test/contract/...` or a small unit test asserting `Diagnostics.runtime/0` includes a
  `:git_commit` key.

### Gate
`mix check.fast`; manually trigger the workflow once and download the artifact.

---

## Item 8 — Optional: implement MAINT-002 VACUUM (G8, MAY)

**Severity: optional.** MAINT-002 is a MAY. Skip unless desired.

### If implementing
1. Add `Storage.Commands.Vacuum` and an `Adapter.vacuum/1` callback
   (`VACUUM` runs outside a transaction; the owner calls it with no in-flight txn).
2. Expose via the existing integrity-check route module as a separate
   `POST /v1/databases/{uuid}/vacuum` (new route) OR fold into integrity-check options.
   A new route requires updating the API-010 matrix and `method_path_matrix_test.exs`.
3. `MAINT-002`: "MUST run only while the database remains under its owner process" — the
   owner-serialized command path already guarantees this.

### If not implementing
No code change; the gap doc already records this as MAY/descoped.

### Gate
`mix check.fast` (and the method/path matrix test if a route is added).

---

## Item 9 — Strengthen replication worker state assertions (G7, test nuance)

**Severity: low.** Behavioral gap is none; the states exist. This hardens the proof.

### Problem
`:idle` and `:backoff` have no literal assertion in `test/replication/`; the 8 active
phases are asserted only via `phase_hook` logs.

### Fix
In `test/replication/phase_transitions_test.exs` (or a new
`test/replication/worker_states_test.exs`), drive a worker with the `state_notify` option
(as the `:completed`/`:failed` tests already do) and assert the full emitted-state
sequence includes `:handshake ... :checkpoint_source` plus a `:backoff` entry on an
injected retryable fault. `:idle` is the initial state before `:start` — assert it via
`(:gen_statem.call(pid, :get_state)` or by observing the first `state_notify`).

### Tests
This item *is* the test.

### Gate
`mix test test/replication/` then `mix check.fast`.

---

## Sequencing

Items are independent. Suggested order if doing them in one pass:

1. Item 1 (TX-006 correctness) — first, highest value.
2. Item 3 (delete ErrorMapper) — trivial, clears noise.
3. Items 4 & 5 (QUERY-011 boundary, query_compiler match) — small, same area.
4. Item 2 (internal_error retryable) — audit touches several files.
5. Items 6 & 7 (ADR + release metadata) — docs/CI, no lib impact.
6. Item 9 (worker state assertions) — test-only.
7. Item 8 (VACUUM) — only if desired.

Final gate: `mix check.full`.
