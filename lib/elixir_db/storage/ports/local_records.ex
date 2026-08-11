defmodule ElixirDB.Storage.Ports.LocalRecords do
  @moduledoc """
  Versioned local-record port over typed namespaces.

  Used for checkpoints and other database-local metadata. Compare-and-swap
  semantics are backend-executed; product validation of record payloads stays
  in shared code where appropriate.
  """

  alias ElixirDB.Storage.BackendContext

  @type result(ok) :: {:ok, ok} | {:error, ElixirDB.Error.t()}
  @type local_record :: %{version: non_neg_integer(), value: term()}

  @callback get(BackendContext.t(), binary(), binary()) :: result(local_record() | nil)
  @callback put_cas(BackendContext.t(), map()) ::
              result(%{version: non_neg_integer(), value: term(), replayed: boolean()})
  @callback list(BackendContext.t(), binary()) :: result([%{key: binary(), record: local_record()}])
  @callback delete(BackendContext.t(), binary(), binary()) :: :ok | {:error, ElixirDB.Error.t()}
end
