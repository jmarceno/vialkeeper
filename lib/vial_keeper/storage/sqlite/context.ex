defmodule VialKeeper.Storage.SQLite.Context do
  @moduledoc """
  Backend-private helpers for resolving an opaque `BackendContext` into the
  SQLite adapter handle. Shared/runtime code must not call this module.
  """

  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.ContextRef
  alias VialKeeper.Storage.OpaqueHandle
  alias VialKeeper.Storage.SQLite.Adapter

  require ContextRef

  @doc "Returns the SQLite adapter stored in `context` or handle."
  @spec unwrap(BackendContext.t() | OpaqueHandle.t() | Adapter.t()) ::
          {:ok, Adapter.t()} | {:error, VialKeeper.Error.t()}
  def unwrap(%BackendContext{backend_ref: %OpaqueHandle{} = handle}) do
    case OpaqueHandle.backend_unwrap(handle) do
      %Adapter{} = adapter -> {:ok, adapter}
      _ -> {:error, VialKeeper.Error.internal_error("backend context is not a SQLite adapter")}
    end
  end

  def unwrap(%BackendContext{backend_ref: %Adapter{} = adapter}), do: {:ok, adapter}

  def unwrap(%OpaqueHandle{} = handle) do
    case OpaqueHandle.backend_unwrap(handle) do
      %Adapter{} = adapter -> {:ok, adapter}
      _ -> {:error, VialKeeper.Error.internal_error("backend context is not a SQLite adapter")}
    end
  end

  def unwrap(%Adapter{} = adapter), do: {:ok, adapter}

  def unwrap(_),
    do: {:error, VialKeeper.Error.internal_error("backend context is not a SQLite adapter")}

  @doc "Binds a SQLite adapter into the current process for subsequent unwraps."
  @spec bind(Adapter.t()) :: Adapter.t()
  def bind(%Adapter{} = adapter) do
    handle = OpaqueHandle.wrap(adapter)
    bound = %{adapter | context_ref: handle}
    _ = OpaqueHandle.replace(handle, bound)
    bound
  end

  @doc "Drops the opaque handle bound for this SQLite adapter, if any."
  @spec release(OpaqueHandle.t() | nil) :: :ok
  def release(%OpaqueHandle{} = handle) do
    OpaqueHandle.drop(handle)
    :ok
  end

  def release(nil), do: :ok

  @doc "Raises when `handle` cannot be resolved to a SQLite adapter."
  @spec resolve!(term()) :: Adapter.t()
  def resolve!(handle) do
    case unwrap(handle) do
      {:ok, adapter} -> adapter
      {:error, error} -> raise ArgumentError, error.message
    end
  end

  @doc "Rebuilds a context after mutating adapter fields inside a transaction."
  @spec replace_ref(BackendContext.t(), Adapter.t()) :: BackendContext.t()
  def replace_ref(%BackendContext{} = context, %Adapter{} = adapter) do
    ContextRef.replace_ref(context, adapter)
  end
end
