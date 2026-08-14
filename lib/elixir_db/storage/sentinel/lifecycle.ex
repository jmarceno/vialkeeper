defmodule ElixirDB.Storage.Sentinel.Lifecycle do
  @moduledoc """
  Sentinel lifecycle port returning opaque `BackendContext` values without SQL.
  """
  @behaviour ElixirDB.Storage.Ports.Lifecycle

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.Sentinel.{Adapter, Context}

  use ElixirDB.Storage.Ports.LifecycleHelpers, adapter: Adapter

  @impl true
  def close(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(Adapter.close(adapter))
    end
  end

  @impl true
  def identity(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(Adapter.identity(adapter))
    end
  end

  @impl true
  def update_config(%BackendContext{}, _config) do
    {:error, ElixirDB.Error.invalid_request("sentinel backend does not implement update_config")}
  end

  @impl true
  def capabilities(%BackendContext{capabilities: capabilities}) when is_map(capabilities),
    do: capabilities

  def capabilities(_), do: %{engine: "sentinel"}

  @impl true
  def open_reader(%BackendContext{}), do: {:error, :unsupported_readers}

  @impl true
  def close_reader(%BackendContext{} = context), do: close(context)

  @impl true
  def interrupt_reader(%BackendContext{capabilities: _capabilities}), do: :unsupported
end
