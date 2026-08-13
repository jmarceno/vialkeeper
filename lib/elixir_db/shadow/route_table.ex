defmodule ElixirDB.Shadow.RouteTable do
  @moduledoc "Immutable ready-route snapshots keyed by source database UUID."
  use GenServer

  @default_name __MODULE__

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec get(binary(), GenServer.server()) :: {:ok, map()} | :not_found
  def get(source_uuid, server \\ @default_name), do: GenServer.call(server, {:get, source_uuid})

  @spec put(binary(), map(), GenServer.server()) :: :ok
  def put(source_uuid, snapshot, server \\ @default_name),
    do: GenServer.call(server, {:put, source_uuid, snapshot})

  @spec delete(binary(), GenServer.server()) :: :ok
  def delete(source_uuid, server \\ @default_name),
    do: GenServer.call(server, {:delete, source_uuid})

  @spec compare_delete(binary(), map(), GenServer.server()) :: :ok | :stale
  def compare_delete(source_uuid, snapshot, server \\ @default_name),
    do: GenServer.call(server, {:compare_delete, source_uuid, snapshot})

  @spec list(GenServer.server()) :: [{binary(), map()}]
  def list(server \\ @default_name), do: GenServer.call(server, :list)

  @impl true
  def init(_opts), do: {:ok, %{routes: %{}}}

  @impl true
  def handle_call({:get, source_uuid}, _from, state) do
    case Map.fetch(state.routes, source_uuid) do
      {:ok, snapshot} -> {:reply, {:ok, snapshot}, state}
      :error -> {:reply, :not_found, state}
    end
  end

  def handle_call({:put, source_uuid, snapshot}, _from, state),
    do: {:reply, :ok, %{state | routes: Map.put(state.routes, source_uuid, snapshot)}}

  def handle_call({:delete, source_uuid}, _from, state),
    do: {:reply, :ok, %{state | routes: Map.delete(state.routes, source_uuid)}}

  def handle_call({:compare_delete, source_uuid, snapshot}, _from, state) do
    case Map.get(state.routes, source_uuid) do
      ^snapshot -> {:reply, :ok, %{state | routes: Map.delete(state.routes, source_uuid)}}
      _ -> {:reply, :stale, state}
    end
  end

  def handle_call(:list, _from, state), do: {:reply, Map.to_list(state.routes), state}
end
