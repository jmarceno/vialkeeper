defmodule VialKeeper.Storage.SQLite.Native do
  @moduledoc """
  SQLite driver NIF (`native/vial_sqlite`, Rust over `rusqlite` with bundled
  SQLite).

  Each statement is one call: prepare from the connection's statement cache,
  bind, step to completion and return every row. `query/3` runs on a dirty IO
  scheduler. `query_inline/3` does the same work on the calling scheduler and
  is only for bounded statements whose dirty hop costs more than SQLite's work;
  it returns `:contended` without running anything when another caller holds
  the connection.

  Only `VialKeeper.Storage.SQLite.Connection` calls this module.
  """
  use Rustler, otp_app: :vial_keeper, crate: :vial_sqlite, path: "native/vial_sqlite"

  use VialKeeper.Storage.SQLite.Driver, :nif
end
