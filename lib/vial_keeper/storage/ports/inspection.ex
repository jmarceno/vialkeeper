defmodule VialKeeper.Storage.Ports.Inspection do
  @moduledoc """
  Normalized integrity snapshots and capability probes.
  """

  alias VialKeeper.Storage.BackendContext

  @type result(ok) :: {:ok, ok} | {:error, VialKeeper.Error.t()}

  @callback integrity_check(BackendContext.t(), map()) :: result(map())
  @callback load_integrity_snapshot(BackendContext.t(), map()) :: result(map())
  @callback physical_integrity_check(BackendContext.t(), map()) :: result(map())
  @callback capabilities_report() :: map()
  @callback validate_capabilities!() :: term()

  @optional_callbacks load_integrity_snapshot: 2, physical_integrity_check: 2
end
