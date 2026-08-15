defmodule VialKeeper.Storage.Sentinel.Context do
  @moduledoc """
  Backend-private helpers for resolving an opaque `BackendContext` into the
  sentinel adapter handle.
  """

  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.OpaqueHandle
  alias VialKeeper.Storage.Sentinel.Adapter

  @doc "Returns the sentinel adapter stored in `context` or handle."
  @spec unwrap(BackendContext.t() | OpaqueHandle.t() | Adapter.t()) ::
          {:ok, Adapter.t()} | {:error, VialKeeper.Error.t()}
  def unwrap(%BackendContext{backend_ref: %OpaqueHandle{} = handle}) do
    case OpaqueHandle.backend_unwrap(handle) do
      %Adapter{} = adapter -> {:ok, adapter}
      _ -> {:error, VialKeeper.Error.internal_error("backend context is not a sentinel adapter")}
    end
  end

  def unwrap(%BackendContext{backend_ref: %Adapter{} = adapter}), do: {:ok, adapter}

  def unwrap(%OpaqueHandle{} = handle) do
    case OpaqueHandle.backend_unwrap(handle) do
      %Adapter{} = adapter -> {:ok, adapter}
      _ -> {:error, VialKeeper.Error.internal_error("backend context is not a sentinel adapter")}
    end
  end

  def unwrap(%Adapter{} = adapter), do: {:ok, adapter}

  def unwrap(_),
    do: {:error, VialKeeper.Error.internal_error("backend context is not a sentinel adapter")}
end
