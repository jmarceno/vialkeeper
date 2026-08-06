# ADR 0001 — Keep hand-rolled `StrictDecoder`

- **Status:** ACCEPTED
- **Date:** 2026-08-06
- **Decides:** Plan §4.4 / gap F1 — migrate to `JSON.decode/3` custom decoder callbacks vs keep the recursive-descent decoder
- **Related IDs:** `JSON-002`, `JSON-003`, `SEC-001`

## Context

`Implementation_Plan_V1.md` §4.4 says `StrictDecoder` SHALL use `JSON.decode/3` custom decoder callbacks for duplicate-key rejection, the binary64 number model, nesting depth, and size bounds. The shipped decoder is a hand-rolled recursive descent that uses `JSON.decode/1` only to unescape string tokens.

Behaviour for Version 1 must remain:

- reject duplicate object keys at every nesting level (`JSON-002`)
- enforce the binary64 safe-integer range and reject non-zero underflow / overflow (`JSON-003`)
- return typed `{:error, %ElixirDB.Error{}}` values (HTTP and storage share the same codes)
- bound nesting depth and payload size (`SEC-001`)

## Evidence reviewed (not smoke-only)

Proof is the adversarial suite plus shared reject fixtures — not the thin smoke cases in `v1_contracts_test` / `json_and_revision_test` alone:

1. **`test/contract/strict_decoder_test.exs`** — adversarial coverage for JSON-002 / JSON-003:
   - `duplicate keys (JSON-002)` — top-level and nested duplicates
   - `binary64 safe integers (JSON-003)` — inclusive bounds and out-of-range rejects
   - `float overflow and underflow (JSON-003)` — `1e-400`, `-1e-400`, `1e309`, explicit zeros
   - `nesting depth and size (JSON-002, SEC-001)` — `max_depth`, host `max_json_nesting_depth`, `max_bytes`
   - `UTF-8 and malformed input (JSON-002)` — invalid UTF-8, trailing garbage, empty input
   - `fixture-driven reject vectors` — loads `priv/fixtures/strict_json/rejects.json`

2. **`priv/fixtures/strict_json/rejects.json`** — shared reject corpus (duplicate keys, unsafe integers, underflow/overflow). Invalid UTF-8 cannot live in JSON fixture text; that case is covered in ExUnit only.

## Spike: `JSON.decode/3` custom decoders

A spike against `JSON.decode/3` callbacks showed that migration is a poor fit for the current contract:

| Requirement | `JSON.decode/3` callback reality | Hand-rolled decoder |
| --- | --- | --- |
| Typed `{:error, %ElixirDB.Error{}}` | Callbacks cannot return `{:error, …}`; they throw/raise, forcing catch-and-remap at the boundary | Direct `{:error, Error.t()}` |
| Nesting depth / size | No first-class depth or byte-limit callbacks | `max_depth` / `max_bytes` checked in-parser |
| Duplicate keys | Parent object is threaded as an accumulator; rejection requires awkward accumulator protocols and still raises on failure | `Map.has_key?/2` → typed error |
| Binary64 + underflow | Number callbacks can rewrite values but combining range, overflow, and underflow with the Error registry is noisy | Dedicated number parser |

Conclusion of the spike: a `JSON.decode/3`-based decoder could approximate behaviour only by sacrificing the typed Error contract and re-implementing depth/size/duplicates outside the callback model. That is not a net win over the existing parser.

## Decision

**ACCEPTED: keep the hand-rolled `ElixirDB.JSON.StrictDecoder`.**

Plan §4.4’s mechanism wording is waived for Version 1. Behavioural requirements `JSON-002` / `JSON-003` / `SEC-001` remain mandatory and are proven by the adversarial suite and fixtures above.

## Consequences

- Document the divergence in `implementation_gaps.md` (F1) as ACCEPTED-with-ADR.
- Do not claim F1 closed from smoke tests alone; regression proof stays in `strict_decoder_test.exs` + `rejects.json`.
- Revisit only if OTP/`JSON` gains typed error returns and first-class depth/duplicate hooks that preserve the Error registry.
