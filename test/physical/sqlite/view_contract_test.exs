defmodule VialKeeper.Storage.SQLite.ViewsContractTest do
  use VialKeeper.Storage.Contracts.Views, adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical
end
