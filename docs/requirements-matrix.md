# Version 1 requirements matrix

`Architecture.md` is authoritative. This local matrix is a proof index: every
row points to an automated test that exists in this repository. It records
implemented seams and does not turn an untested requirement into a claim of
coverage.

## Design and attachment architecture

| ID | Proving test |
| --- | --- |
| ARCH-010 | `test/http/attachments_test.exs` — `"slow upload does not block unrelated owner document put"` and `"slow download does not block unrelated owner document put"` |
| ARCH-011 | `test/http/attachments_test.exs` — `"slow upload does not block unrelated owner document put"` and `"slow download does not block unrelated owner document put"` |
| ARCH-012 | `test/runtime/attachment_coordinator_test.exs` — `"gc waits for existing guards before granting token"` and `"new guards are rejected while gc barrier is active"`; `test/attachments/gc_test.exs` — barrier and overlap tests |

## Attachments

| ID | Proving test |
| --- | --- |
| ATT-001 | `test/http/attachments_test.exs` — `"upload attach get download happy path"`; `test/replication/fault_injection_test.exs` — parameterized `"blob sync fault at #{point} may repeat transfer but never skips revision"` |
| ATT-002 | `test/storage_adapter/attachments_test.exs` — `"mutation inherits, clears, replaces, and rejects missing blob metadata"` |
| ATT-003 | `test/http/attachments_test.exs` — `"slow upload does not block unrelated owner document put"`; `test/attachments/gc_test.exs` — `"expired pending upload with no revision is reclaimed"` |
| ATT-004 | `test/storage_adapter/attachments_test.exs` — `"mutation inherits, clears, replaces, and rejects missing blob metadata"` and `"tombstone persists empty attachment manifest"` |
| ATT-005 | `test/http/attachments_test.exs` — `"upload attach get download happy path"` and `"slow download does not block unrelated owner document put"`; `test/storage_adapter/attachments_test.exs` — `"ticket resolves winner or specific revision attachment"` |
| ATT-006 | `test/attachments/filesystem_store_test.exs` — `"raw read returns exact bytes"` and `"compressed read returns exact original bytes when worthwhile"` |
| ATT-007 | `test/runtime/attachment_coordinator_test.exs` — `"configured read limit is exact"`, `"configured write limit is exact"`, and `"reads and writes use independent counters"`; `test/http/attachments_test.exs` — `"oversize mid-stream returns payload_too_large"` |
| ATT-008 | `test/storage_adapter/attachments_test.exs` — `"list_live_attachment_digests unions retained and pending with paging"`; `test/attachments/gc_test.exs` — last-reference, pending, and post-compact GC tests |

## Revisions and maintenance

| ID | Proving test |
| --- | --- |
| REV-011 | `test/storage_adapter/attachments_test.exs` — `"mutation inherits, clears, replaces, and rejects missing blob metadata"` and `"revision_attachments rows cascade when revision is deleted"` |
| MAINT-010 | `test/attachments/gc_test.exs` — `"compact retention schedules GC that reclaims unreachable blobs"`, `"GC waits for read guard then reader completes"`, and `"crash mid-GC may leave garbage but never dangling retained refs"` |

## Bundle portability

| ID | Proving test |
| --- | --- |
| FILE-001 | `test/end_to_end/offline_copy_test.exs` — `"offline copy, registration, derived index rebuild, and integrity"` and `"copied bundle preserves attachments, encodings, and ignores lease files"` |
| FILE-002 | `test/end_to_end/offline_copy_test.exs` — `"offline copy, registration, derived index rebuild, and integrity"` |
| FILE-003 | `test/storage_adapter/portability_test.exs` — `"closed-file OS copy without lease reopens with integrity"` |
| FILE-004 | `test/storage_adapter/portability_test.exs` — `"closed-file OS copy without lease reopens with integrity"` |
| FILE-005 | `test/end_to_end/offline_copy_test.exs` — `"copied bundle preserves attachments, encodings, and ignores lease files"` |
| FILE-006 | `test/end_to_end/hot_journal_recovery_test.exs` — `"SIGKILL of Adapter-holding child leaves -journal; reopen recovers; then portable"` |
| FILE-007 | `test/end_to_end/offline_copy_test.exs` — `"copied bundle preserves attachments, encodings, and ignores lease files"`; `test/contract/database_bundle_test.exs` — `"prepare_for_open reclaims stale temporary uploads"` |

## Observability

Attachment signals and privacy filtering are verified by:

* `test/observability/attachment_signal_test.exs` — upload, download, GC
  spans/counters and absence of digests and document data.
* `test/observability/privacy_test.exs` — centralized attribute allow-list.

## End-to-end and release gates

Attachment release behaviour is covered across:

* `test/http/attachments_test.exs` — upload/download concurrency and HTTP contracts
* `test/replication/blob_endpoint_test.exs` and fault-injection blob sync points —
  replication blob transfer before import
* `test/attachments/gc_test.exs` — compact-triggered GC and barrier safety
* `test/end_to_end/offline_copy_test.exs` — closed-bundle portability with
  attachments and crash-orphan proofs
* `test/end_to_end/hot_journal_recovery_test.exs` — abnormal-shutdown recovery
  before portability

Full release gates are `mix check.full`, slow-tagged end-to-end suites, and
`MIX_ENV=prod mix release.build`.
