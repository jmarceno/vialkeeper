defmodule ElixirDB.Storage.SQLite.Context do
  @moduledoc """
  Backend-private helpers for resolving an opaque `BackendContext` into the
  SQLite adapter handle. Shared/runtime code must not call this module.
  """

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.SQLite.Adapter

  @doc "Returns the SQLite adapter stored in `context`."
  @spec unwrap(BackendContext.t() | Adapter.t()) ::
          {:ok, Adapter.t()} | {:error, ElixirDB.Error.t()}
  def unwrap(%BackendContext{backend_ref: %Adapter{} = adapter}), do: {:ok, adapter}
  def unwrap(%Adapter{} = adapter), do: {:ok, adapter}

  def unwrap(_),
    do: {:error, ElixirDB.Error.internal_error("backend context is not a SQLite adapter")}

  @doc "Rebuilds a context after mutating adapter fields inside a transaction."
  @spec replace_ref(BackendContext.t(), Adapter.t()) :: BackendContext.t()
  def replace_ref(%BackendContext{} = context, %Adapter{} = adapter) do
    %{context | backend_ref: adapter, identity: adapter.identity}
  end
end
