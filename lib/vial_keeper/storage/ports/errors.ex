defmodule VialKeeper.Storage.Ports.Errors do
  @moduledoc """
  Normalizes backend failures at the storage port edge.

  Callers of storage ports always receive `VialKeeper.Error.t()` values. Engine
  exceptions, driver tuples, and opaque reasons are converted here so shared
  code never inspects physical backend error shapes.
  """

  @doc """
  Returns the error unchanged when already typed; otherwise wraps the failure
  as an internal storage error.
  """
  @spec normalize(term()) :: VialKeeper.Error.t()
  def normalize(%VialKeeper.Error{} = error), do: error

  def normalize(reason),
    do:
      VialKeeper.Error.internal_error("storage backend operation failed", %{cause: inspect(reason)})

  @doc "Maps `{:error, reason}` through `normalize/1`; passes other results through."
  @spec wrap(term()) :: term()
  def wrap({:error, reason}), do: {:error, normalize(reason)}
  def wrap(other), do: other
end
