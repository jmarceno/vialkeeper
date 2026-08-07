# ElixirDB Replication Lab

This is a manual, real-server test harness for the database runtime.

Run it from the repository root:

```sh
./scripts/run-replication-harness.sh
```

Then open <http://127.0.0.1:4180>.

The launcher starts three processes:

1. A web/database BEAM node on port `4100`, owning Database A and Database B.
2. A separate native-client BEAM node on port `4101`, owning Database C and
   printing its `ElixirDB.Changes.wait/2` feed to the terminal.
3. A dependency-free Node.js static server on port `4180`. Its small local
   proxy keeps browser requests same-origin while forwarding Client A and
   Client B to the real `/v1` HTTP server.

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

Optional environment variables:

```sh
ELIXIRDB_DEMO_DB_PORT=4200 \
ELIXIRDB_DEMO_CLI_PORT=4201 \
ELIXIRDB_DEMO_WEB_PORT=4280 \
./scripts/run-replication-harness.sh
```

By default the launcher uses an isolated directory below `tmp/` and removes it
on exit. Set `ELIXIRDB_DEMO_ROOT=/absolute/path` to keep the data and logs for
inspection; each run gets a unique child directory below that root, and an
explicitly supplied root is never removed by the launcher.
