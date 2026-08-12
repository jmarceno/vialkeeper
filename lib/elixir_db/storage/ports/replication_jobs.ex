defmodule ElixirDB.Storage.Ports.ReplicationJobs do
  @moduledoc """
  Durable replication-job catalog port.

  Job scheduling and worker lifecycle remain runtime concerns; this port only
  stores and retrieves logical job definitions and their public state.
  """

  alias ElixirDB.Storage.BackendContext

  @type result(ok) :: {:ok, ok} | {:error, ElixirDB.Error.t()}

  @callback list(BackendContext.t()) :: result([map()])
  @callback put(BackendContext.t(), map()) :: result(map())
  @callback delete(BackendContext.t(), binary()) :: :ok | {:error, ElixirDB.Error.t()}
end
