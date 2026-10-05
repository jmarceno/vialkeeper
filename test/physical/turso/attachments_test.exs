defmodule VialKeeper.Storage.Turso.AttachmentsTest do
  use VialKeeper.Storage.Contracts.Physical.Attachments, adapter: VialKeeper.Storage.Turso.Adapter

  @moduletag :turso_physical
end
