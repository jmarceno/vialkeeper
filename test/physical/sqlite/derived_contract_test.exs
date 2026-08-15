defmodule VialKeeper.Storage.SQLite.DerivedContractTest do
  use VialKeeper.Storage.Contracts.Derived, adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical

  alias VialKeeper.Storage.Contracts.Derived.Support
  alias VialKeeper.Storage.Memory.Adapter, as: MemoryAdapter
  alias VialKeeper.UUID

  test "memory and sqlite produce identical reducer outcomes for shuffled contributions" do
    source_uuid = UUID.v4()
    history_epoch = UUID.v4()

    memory_body = Support.stats_cases(MemoryAdapter, source_uuid, history_epoch)
    sqlite_body = Support.stats_cases(VialKeeper.Storage.SQLite.Adapter, source_uuid, history_epoch)

    assert memory_body == sqlite_body
  end
end
