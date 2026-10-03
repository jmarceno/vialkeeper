# VialKeeper performance benchmarks

Opt-in runners live here. They are separate from the normal ExUnit gate:
numbers are useful for trend detection, but they are not stable enough to make
every developer test run fail.

There are three families:

- **Synthetic product controls** — small isolated databases, Tantivy
  generations, and JSON reports under `tmp/bench/vialkeeper/` in the checkout.
  See the sections below.
- **Layer ladder** — native SQLite, ExQLite, and each VialKeeper layer measured
  side by side on the same statements (`mix bench.overhead`, see
  [Layer ladder](#layer-ladder-sqlite-backend-diagnostic)).
- **Dataset-backed suites** — TREC-COVID FTS, Simple Wikipedia stress, and Open
  Images torture. Source data, generated manifests, work databases, caches, and
  reports live only under the repo-local bench root
  (`tmp/bench/vialkeeper` by default). Nothing from those suites is
  committed to Git.

## Dataset-backed suites

These Mix aliases always run with `--no-start` in `MIX_ENV=test`:

| Alias | Measures | Standard scale (bottleneck-finding suite) |
| --- | --- | --- |
| `mix bench.fts` | TREC-COVID / BEIR full-text ingest, index build, nDCG/recall/MAP, first-pass and `all`/`prefix` latency at concurrency 1 and 4 | 20,000 documents, 50 official queries |
| `mix bench.stress` | Simple Wikipedia catalog-path ingest, FTS, attachment reads, mixed load | 20,000 articles, 100 single-doc puts, up to 800 attachments including 16 MiB objects |
| `mix bench.torture` | Open Images attachment ingest, concurrent read/write, dedup, delete/GC, mixed torture | 400 JPEGs, write/read concurrency 1 and 4 |

`--profile smoke` is a tiny correctness path. Open Images `--profile 1k` and
`--profile 10k`, full 171K TREC, and PMC 100K remain optional large runs; they
can take hours on spinning disk and are not the default.

Dataset-backed Mix runners raise host limits for the process, including
`max_search_rebuild_ms` (one hour) so a larger optional `create_index` is not
killed by the interactive query budget. Production operators set
`[limits].max_search_rebuild_ms` in `host.toml` and restart.

Full-text post-filter candidates are bounded by `[limits].max_search_candidates`;
the benchmark reports a resource-limit failure rather than accepting silently
truncated candidates.

### Component diagnostics

`mix bench.diagnostics` measures isolated phases on the same external root
instead of treating one aggregate runtime as a diagnosis. Fixture preparation
stays outside timed regions. Default sections are `documents`, `database`,
`attachments`, and `search`.

```sh
mix bench.diagnostics
mix bench.diagnostics --section documents,database
mix bench.diagnostics --document-mode bulk --counts 100,1000,10000 --batch-sizes 100,500
```

Use this runner to compare:

- database create (schema applied in one durable transaction)
- single `Documents.put` versus `Documents.bulk_write`
- raw durable CAS, `FilesystemStore`, and full attachment upload
- direct Tantivy indexing versus the VialKeeper search wrapper

Single-write latency is the interactive catalog path (one COMMIT per
document). Bulk ingest is the 20,000-article path: batches of 500 documents share one
storage transaction and one search flush. Do not treat put p50 as the ingest
rate.

Direct Tantivy is the native writer control on the same disk. The VialKeeper
wrapper should stay within about 2× of that control for a full-text rebuild;
larger gaps mean extra product work, not Tantivy itself.

### Simple Wikipedia phases

`mix bench.stress` runs a phase-separated catalog workload and writes the
report after every completed phase (`status: running` until `mixed` finishes).
An interrupted run still records completed timings, the current phase,
processed counts, dataset identity, and git revision. Re-running the same
`--output` path continues from a new work database; the JSON is a progress
checkpoint, not a resume of the previous database.

Standard phases, in order:

1. `single_document_ingest` — 100 `Documents.put` samples (interactive latency)
2. `bulk_document_ingest` — remaining articles in batches of 500, with a scaling ladder
3. `attachment_physical_ingest` — up to 800 locally generated blobs, including 16 MiB objects, at bounded batch concurrency (default 16, capped by the database write limit)
4. `attachment_reference_mutation` — one bulk write attaching those blobs
5. `fts_build` — one full-text `create_index` using `max_search_rebuild_ms`
6. `fts_search` — precomputed `queries.json` (`simplewiki-query-v2`) at concurrency 1 and 4
7. `attachment_read`
8. `mixed` — concurrency 4 and 16

The 20,000-article seed does not call `Documents.put` once per article.

### Open Images default

Default torture is 400 JPEGs. Optional `--profile 1k` / `--profile 10k` are
hour-scale ladders, not the default.

```sh
mix bench.data prepare open-images
mix bench.torture
```

Open Images JPEG bytes come from the CVDF train bucket, not Flickr originals.
Prepare takes the first CSV rows (not a full-file rank), over-selects 12×, and
stops once 400 images are on disk. Missing objects (HTTP 404/410) are skipped
without retry.

The torture work database raises attachment write/read limits to the measured
concurrency ladder (writes 1/4, reads 1/4). Ordinary databases still default
to 4 concurrent attachment writes.

`--output` must resolve under `<bench-root>/reports/`.

Torture, stress, FTS, and `mix bench.data prepare` start a progress watchdog
for the whole run. It prints at phase start/end, every 10% (5% on phases
smaller than 20 units), and a heartbeat at least every 30 seconds.
`--stall-timeout-ms` (default 300000) kills a countable phase that has not
completed a unit of work in that window, prints phase/processed/rate/rss
diagnostics, and for torture/stress/FTS closes the work database.
`--stall-timeout-ms 0` disables the kill.

### Why the data root is mandatory

A full Simple Wikipedia or Open Images fixture can be large: Simple Wikipedia
uses one archive plus generated text and attachment objects, while Open Images
has tens of gigabytes of source objects and a second copy inside VialKeeper
bundles (SQLite, CAS blobs, FTS postings).
Budget **source bytes + generated working space + max(10 GiB, 15%)** before
`prepare`. There is no fallback outside `tmp/bench/` in the checkout.

The approved parent is `tmp/bench/` under the repository root. The standard
root is:

```text
tmp/bench/vialkeeper/
  .vialkeeper-bench-root.json
  datasets/     # prepared fixtures (trec-covid/v1, simplewiki/v1, open-images/v7-100k-v1)
  staging/      # incomplete downloads
  work/         # per-run VialKeeper databases
  cache/        # archive and inventory cache
  reports/      # small JSON reports
```

The checkout only stores a gitignored pointer, `.vialkeeper-bench-root`, that
must match the destination marker UUID.

### Configure, status, prepare, run, clean

```sh
mix bench.data configure --root tmp/bench/vialkeeper
# attaching a second checkout to an already-marked root:
mix bench.data configure --root tmp/bench/vialkeeper --reuse-existing

mix bench.data status

mix bench.data prepare trec-covid     # optional: prepare separately
mix bench.data prepare pmc                 # standard 400 articles; first use freezes an inventory snapshot
mix bench.data prepare pmc --profile smoke
mix bench.data prepare simplewiki          # archive + local articles; standard run uses 20,000
mix bench.data prepare simplewiki --profile smoke
mix bench.data prepare open-images         # 400 JPEGs

mix bench.fts                              # 20,000 TREC-COVID documents
mix bench.stress                           # 20,000 Simple Wikipedia articles
mix bench.stress --profile smoke
mix bench.torture                          # 400 Open Images JPEGs
mix bench.diagnostics

mix bench.data clean trec-covid
mix bench.data clean pmc
mix bench.data clean simplewiki
mix bench.data clean open-images
```

`configure` is the only command that accepts `--root`. Status, prepare, clean,
and the runners read the pointer. Cleanup removes one named dataset directory
under `datasets/`; there is no `clean all`, and the tools never `rm -rf` the
benchmark root.

Re-running `prepare` on a READY fixture is a no-op when the recorded profile
and `selection_count` still match. A stale count (for example an older 40-JPEG
or 2,000-article fixture) is deleted and rebuilt.
as `.part` files in `staging/` or `cache/` until that object is completed.

### What Git contains vs what is downloaded

Committed:

- `bench/support/*.ex` — root safety, downloader, registry, BEIR metrics, runners
- `bench/datasets.exs`, `bench/fts_benchmark.exs`, `bench/pmc_stress_benchmark.exs`,
  `bench/open_images_torture_benchmark.exs`
- `test/bench/*_test.exs` — tiny local HTTP fixtures; no live dataset downloads

Not committed (created under the external root on first use):

- TREC-COVID zip, extracted corpus/queries/qrels, generated `manifest.json`
- Simple Wikipedia bzip2 archive, generated article text/attachments, manifest
- PMC inventory snapshot, metadata JSON, article text/PDF/media, generated manifest
- Open Images image-info CSV, selected JPEG bytes, generated manifest
- work databases, CAS blobs, caches, staging, reports

The registry pins source URLs, checksums (TREC MD5
`ce62140cb23feb9becf6270d0d1fe6d1`, 73876720 bytes), and selection algorithms
(`SHA256("vialkeeper-open-images-v7-100k-v1:" <> image_id)` for Open Images).
It does not embed corpus bytes or ID lists.

## Product storage benchmarks

`product_benchmark.exs` measures matched product operations through the
configured storage backend (default: SQLite adapter). Run it in the test
environment so the existing in-memory OpenTelemetry trace and metric exporters
are enabled:

```sh
MIX_ENV=test mix run --no-start bench/product_benchmark.exs -- \
  --mode both \
  --scenario all \
  --iterations 15 \
  --warmup 3 \
  --dataset 500 \
  --batch 100 \
  --reads 100 \
  --root tmp/bench/vialkeeper/work/product-benchmark-baseline \
  --output tmp/bench/vialkeeper/reports/product-baseline.json
```

The command writes JSON under the approved bench root and prints a short
summary. A later run can compare the median latency for every
storage-mode/scenario pair:

```sh
MIX_ENV=test mix run --no-start bench/product_benchmark.exs -- \
  --mode both \
  --root tmp/bench/vialkeeper/work/product-benchmark-baseline \
  --baseline tmp/bench/vialkeeper/reports/product-baseline.json \
  --max-regression 20
```

The comparison exits with status 1 when a matching scenario is more than the
allowed percentage slower than its baseline. Baselines should be regenerated
on the same machine, with the same Elixir/OTP/backend build and representative
dataset. They are trend evidence, not portable hardware-independent promises.

### Scenarios

- `bulk_write`: seed a database, then measure new bulk writes. Each measured
  batch uses new document IDs, so the database grows across the run. This is
  the closest analogue to CouchDB's checked-in bulk-load benchmark.
- `fts_bulk_write`: seed and build a Tantivy full-text index outside the timed
  region, then measure new bulk writes with incremental delete/add refreshes
  and one bounded Tantivy commit per batch. Use this to isolate indexed-ingest
  cost from the storage-only `bulk_write` control.
- `point_read`: measure a batch of individual document reads against a seeded
  working set.
- `changes_read`: measure bounded changes-feed reads.
- `index_build`: measure structured-index creation while deleting the index
  outside the timed region.
- `indexed_query`: measure a query using an existing structured index.
- `fts_query`: measure a full-text query using an existing Tantivy
  index. Setup seeds ~2 KiB ASCII bodies and indexes `/text`; the timed region
  is one `all`-mode search that matches about a quarter of the dataset and
  returns a 50-hit page. This is part of `--scenario all` so a default
  `mix bench` run includes FTS alongside the other sequential metrics.
  Default `--dataset 500` is a smoke size. The design horizon is about 50k
  winning documents; measure that with `--dataset 50000 --scenario fts_query`.
- `fts_rebuild`: measure reconstructing a Tantivy generation from winning
  documents. Setup seeds the same ~2 KiB ASCII bodies and creates the Tantivy
  index outside the timed region; each sample calls `rebuild_index` on that
  index (stream winners into bounded batches and publish after commit). This is part of `--scenario all`. Measure the 50k
  horizon with `--dataset 50000 --scenario fts_rebuild`. The current recorded
  500-document baseline is a 2.34 s Disk median and a 2.11 s Memory median;
  those figures are trend evidence for that machine, not a portable SLO.
- `concurrent_point_read`: **opt-in** catalog-path point reads (not part of
  `--scenario all`). Disk only. Measures 1/2/4/8 concurrent readers, each with
  and without a steady writer, through `DatabaseCatalog` so the snapshot read
  pool is on the timed path. Report rows are named
  `concurrent_point_read.rN` and `concurrent_point_read.rN+writer`. Throughput
  is total gets in the sample; p95 and the existing dirty-scheduler / `msacc`
  fields are included.
- `multi_writer`: **opt-in** catalog-path puts (not part of `--scenario all`).
  Disk only. Measures 1/2/4/8 concurrent writer clients, each issuing
  `--reads` puts per sample. `multi_writer.independent.wN` uses one database
  per client (independent databases stay concurrent). `multi_writer.shared.wN`
  uses N clients on one database (one writer permit serializes them). Product
  still admits one writer at a time per database.

```sh
MIX_ENV=test mix run --no-start bench/product_benchmark.exs -- \
  --mode disk \
  --scenario concurrent_point_read \
  --root tmp/bench/vialkeeper/work/product-benchmark-concurrent \
  --output tmp/bench/vialkeeper/reports/concurrent-point-read.json
```

```sh
MIX_ENV=test mix run --no-start bench/product_benchmark.exs -- \
  --mode disk \
  --scenario multi_writer \
  --root tmp/bench/vialkeeper/work/product-benchmark-multi-writer \
  --output tmp/bench/vialkeeper/reports/multi-writer.json
```

Use `--scenario bulk_write,indexed_query` to select a sequential subset. `--mode disk`
uses a unique durable artifact below the approved benchmark root and cleans up
companion recovery files. `--mode memory` uses a fresh in-memory SQLite
connection plus an ephemeral Tantivy root below the same approved root for
each case. The memory numbers are an I/O-independent lower bound for adapter
work; they do not represent durable writes or reopen/recovery behavior.

Warmups are excluded from the report. Each sample times only the operation,
not database creation, seeding, index setup, or cleanup. The report includes
sample values, median/p95/p99, per-operation latency, throughput, VM memory
before/after, backend pragmas where available, runtime metadata, scheduler
and dirty-scheduler counts, `msacc` samples for the measured region, and
observability signals. With `MIX_ENV=test`, the JSON also includes low-cardinality
OTel span summaries (count, total, mean, and maximum duration) for the bulk
mutation phases, SQLite transaction boundaries, and Tantivy refresh/query
operations.

The product runner requires `--no-start`, then starts the application itself
with an ephemeral listener and an isolated database root below the approved
benchmark root. This prevents registered databases, materializers, and host
configuration from contaminating measurements. The isolated runtime is
removed after the report is written; the small report remains under
`tmp/bench/vialkeeper/reports/`.

## Layer ladder (SQLite backend diagnostic)

`sqlite_exqlite_overhead_benchmark.exs` measures how much latency each
VialKeeper layer adds on top of native SQLite, one layer at a time. It is a
diagnostic for choosing optimisation targets, not a product latency claim.
Run it in the production environment so OpenTelemetry uses its no-op provider:

```sh
scripts/bench_overhead.sh --mode memory --scenario all \
  --output output/benchmarks/layer-ladder-memory.json
scripts/bench_overhead.sh --mode disk --work-dir /dev/shm/vialkeeper-ladder
# equivalent to: MIX_ENV=prod mix run --no-start bench/sqlite_exqlite_overhead_benchmark.exs -- ...
```

Every variant runs the same scenario on its own database. The ladder, from the
floor up:

| Variant | Layer | What it runs |
| --- | --- | --- |
| `native_replay` | L0 native SQLite | A C program replays the exact statements the storage layer executed |
| `exqlite_replay` | L1 ExQLite | The same statements through `Exqlite.Sqlite3` |
| `connection_replay` | L2 Connection | The same statements through `Storage.SQLite.Connection` |
| `vial_keeper_storage` | L3 storage | `Storage.Services` on the SQLite backend: the storage entry point the database owner and read workers call |
| `vial_keeper_service` | L4 service | `Documents` / `Changes` / `Query` through the catalog, admission, owner and read pool (disk mode) |
| `vial_keeper_http` | L5 HTTP router | The `/v1` Plug router in process: request decoding, routing, response encoding; no socket (disk mode) |
| `exqlite_minimal` | reference | Hand-written minimal SQL through ExQLite |

The difference between neighbouring layers is that layer's cost; the report's
`ladder` list gives each step as a paired ratio and a per-operation delta with
95% confidence intervals. `sql_shape` compares `exqlite_minimal` with
`exqlite_replay`: what the SQL the storage layer chooses costs, separately
from the cost of issuing it. Select a subset with `--variants`, for example
`--variants native_replay,exqlite_replay,vial_keeper_storage`.

L3 calls `Storage.Services` rather than the SQLite adapter's own read API
(`Adapter.get_document/2`, `Adapter.execute_query/2`): the adapter API takes
shorter, non-production code paths for point reads and queries, so measuring
it would understate what the runtime actually pays.

### Where each layer's time goes

Every variant records `VialKeeper.Probe` counter deltas around
each timed call (snapshots are taken outside the timer). The benchmark runs
with both probe tiers enabled. Each variant's `probes` map gives, per probe,
`calls_per_operation`, `ns_per_operation` (a mean), and the histogram
percentile bounds; the printed summary lists the six most expensive probes.
Probes are inclusive and nest (`sqlite_step` runs inside
`storage_get_document`, which runs inside `read_worker_job`), so compare a
probe with its enclosing probe, and compare probe totals with the variant's
`mean_ns_per_operation`, not the median.

The probes' own cost is reported, not assumed: each run measures the per-call
cost of an enabled and a disabled probe (`environment.probe_cost_ns`), and each
variant's `probe_overhead_estimate` multiplies it by that variant's probe calls
per operation, both as run here (`profiling_*`, both tiers on) and for the
production default (`default_*`, standard tier only).

### How the replays stay honest

- **Same statements.** A capture worker owns a private database seeded like
  every other variant and runs each sample's storage operation there, untimed,
  under Erlang call tracing of `Connection.query/3`, `Connection.execute/3`, and
  `Connection.exec/2`. The recorded SQL and parameters are what L0–L2 replay
  for that sample. The worker is a separate process so its process-local
  caches never warm the measured storage variant, and trace patterns are removed
  before any timed code runs.
- **Same results.** Each replay must return exactly the rows the captured run
  returned, and every database (including the native one) must end each case
  in the expected state.
- **Same SQLite.** The native control (`bench/native/vk_replay.c`) is compiled
  from ExQLite's vendored `sqlite3.c` with ExQLite's SQLite compile
  definitions, read from its Makefile. The build is cached under
  `_build/<env>/bench/`; it needs only a C compiler (`CC`, default `cc`). Each
  case checks that both sides report the same SQLite version, source ID, and
  compile options, and copies the adapter connection's pragmas to the native
  connection and reads them back. A precompiled ExQLite NIF may come from a
  different compiler version; the report records both compilers, and
  `config :exqlite, force_build: true` builds the NIF locally to match.
- **Same database.** In memory mode the native control restores a serialized
  image of a seeded adapter database into a regular `:memory:` database; in
  disk mode it opens the seeded file after the adapter closes it.
- **Fair floor.** The native control binds with `SQLITE_TRANSIENT`, steps to
  `SQLITE_DONE`, copies every column value out of SQLite (as ExQLite builds
  every row), resets the statement, and times only that loop. It uses the
  system allocator; ExQLite routes SQLite allocations through `enif_alloc`,
  which is part of the measured L0→L1 step.

The service and HTTP layers use catalog bundles seeded through
`Documents.bulk_write`, so their revision IDs differ from the adapter-level
fixture (the catalog generates history IDs); document IDs, bodies, sequences,
and row counts are the same. Their indexed-query plan is checked with
`Query.explain`; the storage layer's is checked by `EXPLAIN QUERY PLAN` on the
captured statements.

The scenarios are `point_read` (`--reads` single-document gets per sample),
`bulk_write` (one `--batch`-document write per sample), `changes_read`, and
`indexed_query` (`--repeat` operations per sample each). Memory mode is the
lower-noise signal for CPU, BEAM, NIF, and SQLite execution; disk mode adds
filesystem and journal behaviour and the L4/L5 layers. Databases and the
isolated runtime root live under `--work-dir` (default
`tmp/bench/vialkeeper/work/overhead`) and are removed after the run; a tmpfs
work directory removes storage-device noise while keeping the file-backed code
path.

### Measurement rules

- **Nothing but the variant call is timed.** Document IDs, write batches,
  revision hashes, canonical JSON, term blobs, captured statements, encoded
  native requests, and HTTP requests for a sample are built before any timer
  starts. The native control times itself around its SQLite calls only.
- **Samples are paired and the order rotates.** Every sample runs each variant
  once on the same input; the order rotates through every position and
  reverses on alternate cycles.
- **Nanosecond monotonic timing.** Raw per-sample durations are reported in
  collection order (`samples_ns`) with the per-sample variant order
  (`sample_order`), so every statistic can be recomputed from the JSON.
- **No forced garbage collection.** A storage owner runs with a warm heap;
  forcing a collection before a sample would charge heap regrowth to the timed
  region. Global GC counts and reductions are recorded per sample instead.
- **Adaptive stopping.** After `--min-iterations` (default 30), collection
  stops when every variant's paired-ratio 95% confidence interval half-width
  is at most `--target-ci-pct` (default 1%), or at `--max-iterations`
  (default 300) or the per-case `--budget-ms` (default 30000). The report
  records which rule stopped each case (`stop_reason`). `--iterations N` fixes
  the count instead.
- **Production VM flags.** ExQLite runs SQLite calls on dirty schedulers.
  Disabling scheduler busy-waiting (`+sbwt none` and friends) makes every
  such hop several times more expensive than in a default release, so the
  wrapper keeps the defaults. `BENCH_CPUS=2-5` optionally pins the VM with
  `taskset`; `BENCH_ERL_OPTIONS` appends VM flags for experiments.

### Reading the report

Each case reports per-variant summaries (`median_ns`, a bootstrap 95% CI of
the median, `p90_ns`, `p99_ns` once there are at least 100 samples, MAD, CV,
per-operation latency, median reductions per operation, and for the native
control the median SQLite VM steps per operation), the captured statement
profile (`captured_statements`), the checks that passed (`verification`), and,
for every
variant, a paired comparison against the lowest selected ladder layer
(`reference_variant`, normally `native_replay`) in `vs_reference`:

- `paired_ratio_median` and `paired_ratio_ci95` — the median of per-sample
  `variant / reference` ratios and its 95% CI. This is the headline number.
- `paired_delta_median_ns` and `paired_delta_ci95_ns` — the same for
  per-sample differences.

Treat two runs as different only when their ratio CIs do not overlap. The
`environment` block records the CPU, clock source, CPU affinity, VM flags,
SQLite version and compile options, and git revision; compare reports only
when those match. Keep dataset shape, batch, read count, and repeat fixed.

## Observability coverage

The product runner uses production instrumentation helpers around the measured
adapter operations and records the span names and metric datapoint signals for
each case. This makes missing instrumentation visible alongside latency
changes. The adapter-level boundary is intentional: an ephemeral in-memory
SQLite database cannot be reopened by the normal file-backed
`DatabaseCatalog`/`DatabaseOwner` lifecycle. The database-command wrapper is
therefore the same service instrumentation used by that lifecycle, while
query, index-build, search-rebuild, and changes spans remain exercised through
their real instrumentation modules. Span counts are reset for each case; metric datapoint
counts are exporter observations and can include multiple aggregation exports.

The ExQLite overhead runner also emits low-cardinality SQLite child spans when
an OTLP endpoint is configured. They are deliberately phase-level backend
diagnostics, not one span per SQL statement or document:

- Reads: `vial_keeper.sqlite.document.lookup` (winning get) and
  `vial_keeper.sqlite.revision.lookup` (historical revision get).
- Bulk writes: `vial_keeper.sqlite.mutation.bulk.prepare` and
  `vial_keeper.sqlite.mutation.bulk.finalize`.
- Changes: `vial_keeper.sqlite.changes.identity`, `.fetch`, `.decode`, and
  `.has_more`.
- Indexed queries: `vial_keeper.sqlite.query.prepare_request`, `.identity`,
  `.index_catalog`, and `.candidates`, plus the product span
  `vial_keeper.query.execute` for shared filter/order/project work.
- Transactions: `vial_keeper.sqlite.transaction.begin`, `.commit`, and
  `.rollback`.

The span attributes are restricted to existing safe fields such as bounded
`entries`, `plan_kind`, and `selected_index_count`; customer IDs, bodies,
search text, and SQL are never attached. Configure `otlp_endpoint` in the
production host configuration, run the overhead benchmark, and inspect these
children under the measured adapter operation in the collector. With no OTLP
endpoint configured, the instrumentation remains a no-op.

Run the HTTP and replication observability suites separately when changing
those paths:

```sh
MIX_ENV=test mix test test/observability --warnings-as-errors
```

## Why this shape

The benchmark matrix follows the useful parts of CouchDB and PouchDB's own
approach:

- CouchDB's [bulk benchmark](https://github.com/apache/couchdb/blob/main/test/bench/benchbulk.sh)
  repeats batches against a growing database, while its
  [performance guide](https://docs.couchdb.org/en/stable/maintenance/performance.html)
  recommends measuring representative data and batch sizes.
- PouchDB's [performance test documentation](https://apache.googlesource.com/pouchdb/+/0e3c3bcfa31e3b6704bf15862134c0c8f984a9b3/TESTING.md)
  selects adapters and iteration counts explicitly. Its
  [testing retrospective](https://pouchdb.com/2014/11/27/testing-pouchdb.html)
  also explains why uncontrolled CI hardware and unbackfilled historical data
  make regression comparisons unreliable.

Consequently, normal CI proves correctness, the product runner produces
comparable local baselines, and the ExQLite runner remains an explicit SQLite
control. The optional threshold is only applied when a prior run is provided
explicitly.
