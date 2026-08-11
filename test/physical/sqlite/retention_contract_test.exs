defmodule ElixirDB.Storage.SQLite.RetentionContractTest do
  use ElixirDB.Storage.Contracts.Retention, adapter: ElixirDB.Storage.SQLite.Adapter

  @moduletag :sqlite_physical
end
