defmodule VialKeeper.Storage.Turso.ReplicationJobsTest do
  use VialKeeper.Storage.Contracts.Physical.ReplicationJobs,
    adapter: VialKeeper.Storage.Turso.Adapter

  @moduletag :turso_physical
end
