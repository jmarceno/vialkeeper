# VialKeeper Replication Lab

This is a manual, real-server test harness for the database runtime.

Run it from the repository root:

```sh
./scripts/run-replication-harness.sh
```

Then open <http://127.0.0.1:4180>.

The launcher starts five cooperating processes:

1. A web/database BEAM node on port `4100`, owning Database A and Database B.
2. A separate native-client BEAM node on port `4101`, owning Database C and
   printing its `VialKeeper.Changes.wait/2` feed to the terminal.
3. A managed shadow-worker BEAM node on port `4102`.
4. A second managed shadow-worker BEAM node on port `4103`.
5. A dependency-free Node.js static server on port `4180`. Its small local
   proxy keeps browser requests same-origin while forwarding Client A and
   Client B to the real authenticated `/v1` HTTP server and coordinating
   bounded scenario runs.

The database node enables continuous remote replication in this topology:

```text
Client A / Database A  <──HTTP──>  Client B / Database B
          │
          └──────────────HTTP──────────────>  Native Elixir / Database C
```

The browser panels use different database UUIDs. Write a document in either
panel and watch the other panel’s changes feed update. `Burst writes` sends a
bounded series of real mutations to exercise owner serialization, SQLite
transactions, changes delivery, checkpointing, and replication retries.

The native process owns Database C directly in Elixir; it does not use the web
UI or an HTTP client to observe its changes. Its output is written to the
terminal and to the run directory’s `native-cli.log`.

## Scenario lab

The General feature lab panel drives real public HTTP contracts and records one
JSON result per run under the run directory’s `state/results/` folder. The
browser only receives redacted topology and result data; bearer tokens remain
in the private run-state file used by the local controller.

The recipes cover:

- managed shadow provisioning, eventual versus primary read headers, external
  CAS attachment reads, generation movement between workers, and a controlled
  worker restart;
- A → B and A → C replication, structured indexes, query execution and
  explain, and the NDJSON query subscription lifecycle;
- federation queries, materialized-view generation, and declarative view
  indexing;
- concurrent foreground writes, retention compaction, observability snapshots,
  and integrity checks;
- authenticated replication identities, durable replication jobs, control-plane
  capabilities, ordinary database visibility, and integrity boundaries.

Worker cards also expose Stop and Restart actions. These actions are bounded by
the launcher-owned worker process and are useful for observing route
invalidation and generation recovery without editing production configuration.
The controller endpoints live only in the demo Node server; they do not add
routes to the VialKeeper HTTP application.

The page also reads the same OpenTelemetry metric stream used by the database:
each BEAM node has a local one-second metric reader with no collector network
connection. The telemetry cards show cumulative HTTP requests, database
commands, changes reads, replication batches, latency average/p95, error
counts, memory, scheduler run queue, open databases, and replication workers.
HTTP latency includes the harness’s intentional long-poll requests, so use the
database-command and changes values when judging storage performance. The
snapshot route is disabled unless the harness enables it and is only proxied
through the local demo server.

Optional environment variables:

```sh
VIALKEEPER_DEMO_DB_PORT=4200 \
VIALKEEPER_DEMO_CLI_PORT=4201 \
VIALKEEPER_DEMO_WORKER_A_PORT=4202 \
VIALKEEPER_DEMO_WORKER_B_PORT=4203 \
VIALKEEPER_DEMO_WEB_PORT=4280 \
./scripts/run-replication-harness.sh
```

By default the launcher uses an isolated directory below `tmp/` and removes it
on exit. Set `VIALKEEPER_DEMO_ROOT=/absolute/path` to keep the data and logs for
inspection; each run gets a unique child directory below that root, and an
explicitly supplied root is never removed by the launcher.
