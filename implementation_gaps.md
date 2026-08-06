# V1 Review — Open Gaps & Deviations

## Verdict

An independent re-review pass verified every item from the prior report against the
current code. The behavioral, structural, configuration, and small-deviation sections
(A–F below in earlier revisions) are all **solved** in code and have been removed to
avoid contradicting the implementation.

What remains here is the small set of genuinely open items the latest pass surfaced.
The non-telemetry items have a companion implementation plan under `plans/`:

- [plans/compliance_fixes.md](plans/compliance_fixes.md) — TX-006, API-016, ErrorMapper,
  QUERY-011 boundary, query_compiler crash, MAINT/REF/release metadata.

Telemetry (G2) is specified directly in the master plan
([Implementation_Plan_V1.md §11](Implementation_Plan_V1.md) and
[Architecture.md §20.5](Architecture.md), `OBSV-001` through `OBSV-007`).

---

## Open — correctness (high priority)

### G1. TX-006 stale-parent replay returns success without checking the winner (`REV`/`TX`)

`lib/elixir_db/storage/sqlite/mutations.ex`:

- `candidate_revision/7` (line ~197-203): when the supplied parent is stale **and** the
  identical candidate revision already exists, the branch returns `{:ok, {:replayed, revision}}`.
- `apply_local_tx/2` (line 30-32) consumes that as an unconditional
  `{:ok, %{..., replayed: true}}`.
- The bulk mirror is `prepare_bulk_mutation_operation` (line ~359-369).

TX-006 requires: when the identical revision exists but a later or conflicting revision
has changed the current state, the server MUST return `revision_conflict` with
`operation_already_committed: true`. The stale branch never checks
`doc.winning_revision == candidate.revision_id`, so a retried stale-parent put that was
superseded incorrectly reports success. The non-stale path (`insert_local_revision`,
line 247-263) and the bulk explicit-find path (line 374-395) do the check correctly.

---

## Open — observability (high priority)

### G2. Telemetry is declared but never emitted (Plan §11)

`lib/elixir_db/telemetry.ex` defines the nine mandated event prefixes and a `span/3`
helper, but nothing in `lib/` calls `Telemetry.span/3`, `:telemetry.execute/3`, or
`Telemetry.events/0`. The instrumentation is entirely absent; the events list is dead
data. Addressed by adopting OpenTelemetry from the start — see
[Implementation_Plan_V1.md §11](Implementation_Plan_V1.md) and
[Architecture.md §20.5](Architecture.md) (`OBSV-001` through `OBSV-007`) — rather than
wiring the current placeholder.

---

## Open — API contract (medium priority)

### G3. `internal_error` retryable is hardcoded `true`, not "depends on details" (API-016)

`lib/elixir_db/error.ex:43` — `internal_error: {500, true}`. API-016 specifies
`internal_error` retryable as "**Depends on details**". The registry and `new/4` have no
mechanism to make an `internal_error` non-retryable per-details, so every emitted
`internal_error` reports `retryable: true`.

### G4. `ElixirDB.HTTP.ErrorMapper` is dead code (Plan §5 hygiene)

`lib/elixir_db/http/error_mapper.ex` (11 lines) is never called anywhere in `lib/` or
`test/`. The real code→HTTP mapping is centralized in `error.ex`'s `@registry`, making
this module vestigial. It should be removed.

---

## Open — minor nuances (low priority, reported for completeness)

### G5. QUERY-011 scan-threshold boundary is inclusive (`<=`) vs spec "below"

`lib/elixir_db/storage/sqlite/query_runner.ex:88` uses `count <= scan_threshold`. Spec
wording is "below the configured scan threshold." Strictly, `count == threshold` is
allowed to scan where the spec says "below."

### G6. `query_compiler.ex` uses a bare `{:ok, tokens} = Pointer.parse(path)` match

`lib/elixir_db/storage/sqlite/query_compiler.ex:15` — a hard `MatchError` crash if an
internal caller passes an unvalidated path. Unreachable from HTTP (the Normalizer
pre-validates), so not a live defect, but a crash-instead-of-error for internal misuse.

### G7. Replication worker state-assertion coverage (test nuance, not a behavioral gap)

Of the 12 mandated Plan §7.7 worker states, only `:completed` and `:failed` are asserted
as literal `:gen_statem` states (via `state_notify`); the 8 active phases are asserted
only through `phase_hook` logs, and `:idle`/`:backoff` have no literal assertion in
`test/replication/`. The states exist in code (`worker.ex:19-32`).

---

## Open — soft / scope-acknowledged

### G8. MAINT-002 VACUUM is absent (MAY — non-blocking)

Architecture MAINT-002 ("A maintenance operation **MAY** run SQLite `VACUUM`") is a MAY,
so non-blocking. There is no `VACUUM` code in `lib/` or `test/`, and no route exposes it.
Decision: either implement it or leave descoped (MAY permits both) — the earlier "present
in method matrix" claim was inaccurate and has been removed.

### G9. MAINT-003 retention (expected partial — DEF-008 defers pruning)

Retention config exists; no dedicated retention-pruning test. Expected, because
MAINT-003/DEF-008 explicitly defer automatic pruning. Version 1 MUST retain complete
bodies for all revisions — retained as honest PARTIAL.

### G10. REF-004 upstream-case adaptation not documented (MUST)

REF-004 (MUST): "identify and adapt relevant upstream cases," with "Every excluded
upstream test MUST have a documented reason." REF-005 (SHOULD): differential testing
against pinned PouchDB/CouchDB. This is consistent with DEF-006/007 (independent
implementation), but REF-004's MUST for a documented selected/excluded upstream-case
list is technically unmet. Likely an intentional scope decision — needs a documented
rationale.

### G11. Plan §3.1 release metadata not recorded as release artifacts

Plan §3.1 mandates the release pipeline "MUST record Elixir/OTP/Exqlite/SQLite version /
SQLite compile options / Git commit." `Diagnostics.runtime/0` captures all except git
commit, and the CI workflow only runs `mix check.full` — it does not record/publish these
as release metadata.

---

## Confirmed solved in latest pass (removed from this file)

The following were open in earlier revisions of this report and are now verified solved
in code: A1–A7 (FTS5 contentless-delete + feasibility spike, project matcher as
authoritative post-filter, planner QUERY-009 tiers, UUID-mismatch detection at open,
prepared-statement cache, cooperative cancellation, durable-commit-confirm callback),
B1–B2 (12-state worker + per-transition fault injection), C1–C4 (route split,
storage-neutral planner/projection, owned SQLite modules, real query_compiler), D1–D8
(AdapterCase conformance suite, test/support, six pillars, StreamData properties,
fixtures, requirements-matrix, operations docs, Phase 8 scenario), E1–E4
(shutdown_timeout configurable, JSON nesting host-enforced, interpreted-mode decision,
mise.toml pinning), and F1/F4/F5/F6/F7 (strict-decoder ADR, ChangeNotifier self-stop,
centralized unknown-field rejection, Storage.Results/Commands in use, domain-struct
constructors).
