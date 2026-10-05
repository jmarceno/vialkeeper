defmodule VialKeeper.Storage.Sentinel.Lifecycle do
  @moduledoc """
  Sentinel lifecycle port returning opaque `BackendContext` values without SQL.
  """
  @behaviour VialKeeper.Storage.Ports.Lifecycle

  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.Ports.Errors
  alias VialKeeper.Storage.Sentinel.{Adapter, Context}

  use VialKeeper.Storage.Ports.LifecycleHelpers, adapter: Adapter

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
    {:error, VialKeeper.Error.invalid_request("sentinel backend does not implement update_config")}
  end

  @impl true
  def capabilities(%BackendContext{capabilities: capabilities}) when is_map(capabilities),
    do: Map.merge(capabilities, single_writer())

  def capabilities(_), do: Map.put(single_writer(), :engine, "sentinel")

  @impl true
  def open_reader(%BackendContext{}), do: {:error, :unsupported_readers}

  @impl true
  def close_reader(%BackendContext{} = context), do: close(context)

  @impl true
  def interrupt_reader(%BackendContext{capabilities: _capabilities}), do: :unsupported

  @impl true
  def open_writer(%BackendContext{}), do: {:error, :unsupported_writers}

  # `open_writer/1` never succeeds, so there is no writer context to close.
  @impl true
  def close_writer(%BackendContext{}),
    do: {:error, VialKeeper.Error.invalid_request("sentinel backend opens no writer connections")}

  @impl true
  def reset_writer_caches(%BackendContext{}), do: :ok

  defp single_writer, do: %{max_writers: 1, sequence_persistence: :none}
end
