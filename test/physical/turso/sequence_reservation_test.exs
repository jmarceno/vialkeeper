defmodule VialKeeper.Storage.Turso.SequenceReservationTest do
  use VialKeeper.Storage.Contracts.Physical.SequenceReservation,
    adapter: VialKeeper.Storage.Turso.Adapter

  @moduletag :turso_physical
end
