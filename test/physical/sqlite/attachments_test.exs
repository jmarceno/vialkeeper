defmodule VialKeeper.StorageAdapter.AttachmentsTest do
  use VialKeeper.Storage.Contracts.Physical.Attachments, adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical
end
