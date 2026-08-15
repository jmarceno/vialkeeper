defmodule VialKeeper.Storage.Contract.MemoryRetentionTest do
  use VialKeeper.Storage.Contracts.Retention, adapter: VialKeeper.Storage.Memory.Adapter
end
