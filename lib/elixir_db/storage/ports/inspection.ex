defmodule ElixirDB.Storage.Ports.Inspection do
  @moduledoc """
  Normalized integrity snapshots and capability probes.
  """

  alias ElixirDB.Storage.BackendContext

  @type result(ok) :: {:ok, ok} | {:error, ElixirDB.Error.t()}

  @callback integrity_check(BackendContext.t(), map()) :: result(map())
  @callback capabilities_report() :: map()
  @callback validate_capabilities!() :: term()
end
