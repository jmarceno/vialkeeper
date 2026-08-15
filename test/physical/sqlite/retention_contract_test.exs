defmodule VialKeeper.Storage.SQLite.RetentionContractTest do
  use VialKeeper.Storage.Contracts.Retention, adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical
end
