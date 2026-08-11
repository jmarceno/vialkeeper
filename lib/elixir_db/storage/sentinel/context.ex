defmodule ElixirDB.Storage.Sentinel.Context do
  @moduledoc """
  Backend-private helpers for resolving an opaque `BackendContext` into the
  sentinel adapter handle.
  """

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.OpaqueHandle
  alias ElixirDB.Storage.Sentinel.Adapter

  @doc "Returns the sentinel adapter stored in `context` or handle."
  @spec unwrap(BackendContext.t() | OpaqueHandle.t() | Adapter.t()) ::
          {:ok, Adapter.t()} | {:error, ElixirDB.Error.t()}
  def unwrap(%BackendContext{backend_ref: %OpaqueHandle{} = handle}) do
    case OpaqueHandle.unwrap(handle) do
      %Adapter{} = adapter -> {:ok, adapter}
      _ -> {:error, ElixirDB.Error.internal_error("backend context is not a sentinel adapter")}
    end
  end

  def unwrap(%BackendContext{backend_ref: %Adapter{} = adapter}), do: {:ok, adapter}

  def unwrap(%OpaqueHandle{} = handle) do
    case OpaqueHandle.unwrap(handle) do
      %Adapter{} = adapter -> {:ok, adapter}
      _ -> {:error, ElixirDB.Error.internal_error("backend context is not a sentinel adapter")}
    end
  end

  def unwrap(%Adapter{} = adapter), do: {:ok, adapter}

  def unwrap(_),
    do: {:error, ElixirDB.Error.internal_error("backend context is not a sentinel adapter")}
end
