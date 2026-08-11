defmodule ElixirDB.Storage.SQLite.DerivedContractTest do
  use ElixirDB.Storage.Contracts.Derived, adapter: ElixirDB.Storage.SQLite.Adapter

  @moduletag :sqlite_physical

  alias ElixirDB.Storage.Contracts.Derived.Support
  alias ElixirDB.Storage.Memory.Adapter, as: MemoryAdapter
  alias ElixirDB.UUID

  test "memory and sqlite produce identical reducer outcomes for shuffled contributions" do
    source_uuid = UUID.v4()
    history_epoch = UUID.v4()

    memory_body = Support.stats_cases(MemoryAdapter, source_uuid, history_epoch)
    sqlite_body = Support.stats_cases(ElixirDB.Storage.SQLite.Adapter, source_uuid, history_epoch)

    assert memory_body == sqlite_body
  end
end
