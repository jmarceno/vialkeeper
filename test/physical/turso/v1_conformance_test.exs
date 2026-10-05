defmodule VialKeeper.Storage.Turso.V1ConformanceTest do
  use VialKeeper.Storage.Contracts.Physical.V1Conformance, adapter: VialKeeper.Storage.Turso.Adapter

  @moduletag :turso_physical
end
