defmodule ElixirDB.Storage.SQLite.ConflictsContractTest do
  use ElixirDB.Storage.Contracts.Conflicts, adapter: ElixirDB.Storage.SQLite.Adapter

  @moduletag :sqlite_physical
end
