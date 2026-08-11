defmodule ElixirDB.Storage.Ports.RetentionRecords do
  @moduledoc """
  Retention record port: peer positions, boundary pages, and retention state.

  Compaction policy and safe-report semantics remain in shared retention code.
  """

  alias ElixirDB.Storage.BackendContext

  @type result(ok) :: {:ok, ok} | {:error, ElixirDB.Error.t()}

  @callback retention_state(BackendContext.t()) :: result(map())
  @callback list_peer_positions(BackendContext.t()) :: result([map()])
  @callback put_peer_position_cas(BackendContext.t(), map()) :: result(map())
  @callback read_boundary_pages(BackendContext.t(), map()) :: result(map())
  @callback install_boundary_pages(BackendContext.t(), map()) :: result(map())
end
