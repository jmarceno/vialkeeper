# VialKeeper performance benchmarks

Two opt-in runners live here. They are separate from the normal ExUnit gate:
numbers are useful for trend detection, but they are not stable enough to make
every developer test run fail.

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
  --output output/benchmarks/baseline.json
```

The command writes JSON under the ignored `output/` directory and prints a
short summary. A later run can compare the median latency for every
storage-mode/scenario pair:

```sh
MIX_ENV=test mix run --no-start bench/product_benchmark.exs -- \
  --mode both \
  --baseline output/benchmarks/baseline.json \
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
- `point_read`: measure a batch of individual document reads against a seeded
  working set.
- `changes_read`: measure bounded changes-feed reads.
- `index_build`: measure structured-index creation while deleting the index
  outside the timed region.
- `indexed_query`: measure a query using an existing structured index.
- `fts_query`: measure a full-text query using an existing `unicode_words_v1`
  index. Setup seeds ~2 KiB ASCII bodies and indexes `/text`; the timed region
  is one `all`-mode search that matches about a quarter of the dataset and
  returns a 50-hit page. This is part of `--scenario all` so a default
  `mix bench` run includes FTS alongside the other sequential metrics.
  Default `--dataset 500` is a smoke size. The design horizon is about 50k
  winning documents; measure that with `--dataset 50000 --scenario fts_query`.
- `fts_rebuild`: measure reconstructing the Elixir posting-list cache from
  winning documents. Setup seeds the same ~2 KiB ASCII bodies and creates the
  `unicode_words_v1` index outside the timed region; each sample calls
  `rebuild_index` on that index (scan winners, retokenize, persist
  `tmp/search-index.etf`). This is part of `--scenario all`. Measure the 50k
  horizon with `--dataset 50000 --scenario fts_rebuild`. A local three-run
  smoke of `--dataset 500 --iterations 5 --warmup 2` measured disk median
  781 ms and memory median 1.25 s; those figures are trend evidence for that
  machine, not a portable SLO.
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
  --output output/benchmarks/concurrent-point-read.json
```

```sh
MIX_ENV=test mix run --no-start bench/product_benchmark.exs -- \
  --mode disk \
  --scenario multi_writer \
  --output output/benchmarks/multi-writer.json
```

Use `--scenario bulk_write,indexed_query` to select a sequential subset. `--mode disk`
uses a unique temporary durable artifact and cleans up companion recovery
files. `--mode memory` uses a fresh in-memory backend connection for each
case. The memory numbers are an I/O-independent lower bound for adapter work;
they do not represent durable writes or reopen/recovery behavior.

Warmups are excluded from the report. Each sample times only the operation,
not database creation, seeding, index setup, or cleanup. The report includes
sample values, median/p95/p99, per-operation latency, throughput, VM memory
before/after, backend pragmas where available, runtime metadata, scheduler
and dirty-scheduler counts, `msacc` samples for the measured region, and
observability signals.

Both runners require `--no-start` (the Mix aliases include it), then start the
application themselves with an ephemeral listener and an isolated temporary
database root. This prevents registered databases, materializers, and host
configuration from contaminating measurements. The temporary root is removed
after the report is written.

## ExQLite overhead control (SQLite backend diagnostic)

`sqlite_exqlite_overhead_benchmark.exs` is an explicit SQLite/ExQLite control,
not a product latency claim. It answers how much time each VialKeeper layer adds
over direct ExQLite calls. Run it in the production environment so OpenTelemetry
uses its no-op provider and the benchmark process has no test exporter in its
timed path:

```sh
mix bench.overhead --mode memory --scenario all --iterations 30 --warmup 10 \
  --dataset 500 --batch 50 --reads 100 \
  --output output/benchmarks/sqlite-exqlite-overhead-memory.json
```

Use `--mode disk` for durable SQLite I/O, or `--mode both` to produce both
measurements. Memory mode is the lower-noise signal for CPU, BEAM, NIF, and
SQLite execution; disk mode includes filesystem and journal behavior and
should be compared only with other runs on the same machine.

Each case creates three independent databases with the same schema and a
deterministic fixture. It alternates the order of paired samples and warms
prepared statements before recording samples. The JSON report contains raw
samples, median/p95/p99, MAD, coefficient of variation, paired deltas, and
percentage overhead relative to `pure_exqlite`:

- `pure_exqlite` calls prepared statements through `Exqlite.Sqlite3`.
- `vial_keeper_connection` runs the same SQL through the VialKeeper connection
  wrapper and statement cache.
- `vial_keeper_adapter` calls the public SQLite adapter operation.

The scenarios are `point_read`, `bulk_write`, `changes_read`, and
`indexed_query`. Point reads use one winning-document join (document, revision,
and empty attachment columns). Changes reads include the bounded SELECT and
the `has_more` probe. Indexed queries use the same structured expression index
and predicate in the direct and connection controls.

The bulk-write ExQLite control is intentionally a physical baseline: it
inserts the same final document, revision, change-feed, metadata, and
replication-state rows in one prepared transaction. It does not reproduce
VialKeeper validation, revision hashing/lookups, conflict handling, or retention
orchestration. Therefore its adapter delta is the real cost of the current
VialKeeper write path over direct SQLite storage, not merely the cost of a
function call or NIF wrapper. The connection-vs-ExQLite delta isolates the
connection wrapper for every scenario.

Read the adapter's `median_overhead_pct` for the headline number. Use
`paired_median_delta_us` and the MAD/CV fields to judge noise; do not infer a
regression from one p95 sample. Keep Elixir/OTP, SQLite, dataset shape, batch,
read count, warmup, and iteration count fixed when comparing reports.

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
