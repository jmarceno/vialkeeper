defmodule VialKeeper.Storage.Memory.Context do
  @moduledoc """
  Backend-private helpers for resolving an opaque `BackendContext` into the
  Memory adapter handle.
  """

  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.ContextRef
  alias VialKeeper.Storage.Memory.Adapter
  alias VialKeeper.Storage.OpaqueHandle

  require ContextRef

  @doc "Returns the Memory adapter stored in `context`."
  @spec unwrap(BackendContext.t()) :: {:ok, Adapter.t()} | {:error, VialKeeper.Error.t()}
  def unwrap(%BackendContext{backend_ref: %OpaqueHandle{} = handle}) do
    case OpaqueHandle.backend_unwrap(handle) do
      %Adapter{} = adapter -> {:ok, adapter}
      _ -> {:error, VialKeeper.Error.internal_error("backend context is not a Memory adapter")}
    end
  end

  def unwrap(%BackendContext{backend_ref: %Adapter{} = adapter}), do: {:ok, adapter}

  def unwrap(_),
    do: {:error, VialKeeper.Error.internal_error("backend context is not a Memory adapter")}

  @doc "Rebuilds a context after mutating adapter fields."
  @spec replace_ref(BackendContext.t(), Adapter.t()) :: BackendContext.t()
  def replace_ref(%BackendContext{} = context, %Adapter{} = adapter) do
    ContextRef.replace_ref(context, adapter)
  end
end
