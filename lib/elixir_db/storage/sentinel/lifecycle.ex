defmodule ElixirDB.Storage.Sentinel.Lifecycle do
  @moduledoc """
  Sentinel lifecycle port returning opaque `BackendContext` values without SQL.
  """
  @behaviour ElixirDB.Storage.Ports.Lifecycle

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.Sentinel.Adapter

  use ElixirDB.Storage.Ports.LifecycleHelpers, adapter: Adapter

  @impl true
  def close(%BackendContext{backend_ref: %Adapter{} = adapter}) do
    Errors.wrap(Adapter.close(adapter))
  end

  def close(_),
    do: {:error, ElixirDB.Error.internal_error("backend context is not a sentinel adapter")}

  @impl true
  def identity(%BackendContext{backend_ref: %Adapter{} = adapter}) do
    Errors.wrap(Adapter.identity(adapter))
  end

  def identity(_),
    do: {:error, ElixirDB.Error.internal_error("backend context is not a sentinel adapter")}

  @impl true
  def update_config(%BackendContext{}, _config) do
    {:error, ElixirDB.Error.invalid_request("sentinel backend does not implement update_config")}
  end

  @impl true
  def capabilities(%BackendContext{capabilities: capabilities}) when is_map(capabilities),
    do: capabilities

  def capabilities(_), do: %{engine: "sentinel", sql: false}
end
