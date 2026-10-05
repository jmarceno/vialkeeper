defmodule VialKeeper.Storage.Turso.RetentionContractTest do
  use VialKeeper.Storage.Contracts.Retention, adapter: VialKeeper.Storage.Turso.Adapter

  @moduletag :turso_physical
end
