defmodule ElixirDB.Shadow.WorkerRegistry do
  @moduledoc "Durable-worker process registry for exact shadow generations."
  use GenServer

  @default_name __MODULE__

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, @default_name))
  end

  @spec get(binary(), GenServer.server()) :: {:ok, map()} | :not_found
  def get(shadow_uuid, server \\ @default_name), do: call(server, {:get, shadow_uuid})

  @spec put(binary(), map(), GenServer.server()) :: :ok
  def put(shadow_uuid, entry, server \\ @default_name),
    do: call(server, {:put, shadow_uuid, entry})

  @spec delete(binary(), GenServer.server()) :: :ok
  def delete(shadow_uuid, server \\ @default_name),
    do: call(server, {:delete, shadow_uuid})

  @spec list(GenServer.server()) :: [map()]
  def list(server \\ @default_name), do: call(server, :list)

  defp call(server, message),
    do: GenServer.call(server, message, ElixirDB.Config.request_timeout_ms())

  @impl true
  def init(_opts), do: {:ok, %{entries: %{}}}

  @impl true
  def handle_call({:get, shadow_uuid}, _from, state) do
    case Map.fetch(state.entries, shadow_uuid) do
      {:ok, entry} -> {:reply, {:ok, entry}, state}
      :error -> {:reply, :not_found, state}
    end
  end

  def handle_call({:put, shadow_uuid, entry}, _from, state),
    do: {:reply, :ok, %{state | entries: Map.put(state.entries, shadow_uuid, entry)}}

  def handle_call({:delete, shadow_uuid}, _from, state),
    do: {:reply, :ok, %{state | entries: Map.delete(state.entries, shadow_uuid)}}

  def handle_call(:list, _from, state), do: {:reply, Map.values(state.entries), state}
end
