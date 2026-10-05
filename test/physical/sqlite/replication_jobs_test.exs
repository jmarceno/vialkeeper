defmodule VialKeeper.StorageAdapter.ReplicationJobsTest do
  use VialKeeper.Storage.Contracts.Physical.ReplicationJobs,
    adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical
end
