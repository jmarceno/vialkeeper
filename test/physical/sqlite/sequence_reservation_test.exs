defmodule VialKeeper.StorageAdapter.SequenceReservationTest do
  @moduledoc """
  The SQLite reserved-through value: its initial value, its bound over every
  stored sequence, max-only persistence, and no sequence reuse after the
  process holding a ledger is SIGKILLed.
  """
  use VialKeeper.Storage.Contracts.Physical.SequenceReservation,
    adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical
end
