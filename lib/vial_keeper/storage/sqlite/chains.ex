defmodule VialKeeper.Storage.SQLite.Chains do
  @moduledoc """
  Compatibility wrappers that route SQLite adapter handles into shared chain
  read services. Prefer `VialKeeper.Storage.Services` with a `BackendContext`.
  """

  alias VialKeeper.Storage.Services.Chains
  alias VialKeeper.Storage.SQLite.Adapter

  @doc "Diffs requested leaf revisions against stored leaf sets."
  @spec diff(map(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def diff(adapter, request), do: Chains.diff(Adapter.to_context(adapter), request)

  @doc "Loads parent-ordered revision chains for requested leaves."
  @spec get(map(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def get(adapter, request), do: Chains.get(Adapter.to_context(adapter), request)
end
