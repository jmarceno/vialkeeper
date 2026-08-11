defmodule ElixirDB.Storage.SQLite.ViewsContractTest do
  use ElixirDB.Storage.Contracts.Views, adapter: ElixirDB.Storage.SQLite.Adapter

  @moduletag :sqlite_physical
end
