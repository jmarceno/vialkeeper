defmodule ElixirDB.Storage.Memory.Lifecycle do
  @moduledoc """
  Memory lifecycle port returning opaque `BackendContext` values.

  Implements create/open directly against the Memory adapter rather than the
  shared LifecycleHelpers macro so the Memory and SQLite ports stay distinct.
  """
  @behaviour ElixirDB.Storage.Ports.Lifecycle

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Memory.{Adapter, Context, Store}
  alias ElixirDB.Storage.Ports.Errors

  @impl true
  def create(path, options \\ %{}) when is_binary(path) and is_map(options) do
    case Adapter.create(path, options) do
      {:ok, adapter} -> {:ok, Adapter.to_context(adapter)}
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def open(path, options \\ %{}) when is_binary(path) and is_map(options) do
    case Adapter.open(path, options) do
      {:ok, adapter} -> {:ok, Adapter.to_context(adapter)}
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def close(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Adapter.close(adapter)
    end
  end

  @impl true
  def identity(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      {:ok, Store.identity(adapter.store)}
    end
  end

  @impl true
  def update_config(%BackendContext{} = context, config) when is_map(config) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Adapter.update_config(adapter, config)
    end
  end

  @impl true
  def capabilities(%BackendContext{} = context) do
    case Context.unwrap(context) do
      {:ok, _adapter} -> %{sql: false, engine: "memory"}
      {:error, _} -> %{}
    end
  end
end
