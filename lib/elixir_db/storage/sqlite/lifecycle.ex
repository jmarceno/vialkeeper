defmodule ElixirDB.Storage.SQLite.Lifecycle do
  @moduledoc """
  SQLite lifecycle port returning opaque `BackendContext` values.
  """
  @behaviour ElixirDB.Storage.Ports.Lifecycle

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.SQLite.{Adapter, Context}

  use ElixirDB.Storage.Ports.LifecycleHelpers, adapter: Adapter

  @impl true
  def close(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context), do: Adapter.close(adapter)
  end

  @impl true
  def identity(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context), do: Adapter.identity(adapter)
  end

  @impl true
  def update_config(%BackendContext{} = context, config) when is_map(config) do
    with {:ok, adapter} <- Context.unwrap(context), do: Adapter.update_config(adapter, config)
  end

  @impl true
  def capabilities(%BackendContext{} = context) do
    case Context.unwrap(context) do
      {:ok, _adapter} -> Adapter.capabilities_report()
      {:error, _} -> %{}
    end
  end
end
