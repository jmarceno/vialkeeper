# ElixirDB performance benchmarks

`elixirdb_benchmark.exs` is a repeatable, opt-in benchmark runner for the V1
SQLite adapter. It is deliberately separate from the normal ExUnit gate: the
numbers are useful for trend detection, but they are not stable enough to make
every developer test run fail.

Run it in the test environment so the existing in-memory OpenTelemetry trace
and metric exporters are enabled:

```sh
MIX_ENV=test mix run bench/elixirdb_benchmark.exs \
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
MIX_ENV=test mix run bench/elixirdb_benchmark.exs \
  --mode both \
  --baseline output/benchmarks/baseline.json \
  --max-regression 20
```

The comparison exits with status 1 when a matching scenario is more than the
allowed percentage slower than its baseline. Baselines should be regenerated
on the same machine, with the same Elixir/OTP/SQLite build and representative
dataset. They are trend evidence, not portable hardware-independent promises.

## Scenarios

- `bulk_write`: seed a database, then measure new bulk writes. Each measured
  batch uses new document IDs, so the database grows across the run. This is
  the closest analogue to CouchDB's checked-in bulk-load benchmark.
- `point_read`: measure a batch of individual document reads against a seeded
  working set.
- `changes_read`: measure bounded changes-feed reads.
- `index_build`: measure structured-index creation while deleting the index
  outside the timed region.
- `indexed_query`: measure a query using an existing structured index.

Use `--scenario bulk_write,indexed_query` to select a subset. `--mode disk`
uses a unique temporary SQLite file and cleans up its journal sidecars.
`--mode memory` uses a fresh `:memory:` connection for each case. The memory
numbers are an I/O-independent lower bound for adapter/SQLite work; they do
not represent durable writes or reopen/recovery behavior.

Warmups are excluded from the report. Each sample times only the operation,
not database creation, seeding, index setup, or cleanup. The report includes
sample values, median/p95/p99, per-operation latency, throughput, VM memory
before/after, SQLite pragmas, runtime metadata, and observability signals.

## Observability coverage

The runner uses the production instrumentation helpers around the measured
adapter operations and records the span names and metric datapoint signals for
each case. This makes missing instrumentation visible alongside latency
changes. The adapter-level boundary is intentional: an ephemeral in-memory
SQLite database cannot be reopened by the normal file-backed
`DatabaseCatalog`/`DatabaseOwner` lifecycle. The database-command wrapper is
therefore the same service instrumentation used by that lifecycle, while
query, index-build, and changes spans remain exercised through their real
instrumentation modules. Span counts are reset for each case; metric datapoint
counts are exporter observations and can include multiple aggregation exports.

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

Consequently, normal CI proves correctness, this runner produces comparable
local baselines, and the optional threshold is only applied when a prior run is
provided explicitly.
