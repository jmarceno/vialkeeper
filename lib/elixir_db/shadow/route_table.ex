defmodule ElixirDB.Shadow.RouteTable do
  @moduledoc "Immutable ready-route snapshots keyed by source database UUID."
  use GenServer

  @default_name __MODULE__
  @table_opts [:named_table, :set, :public, read_concurrency: true, write_concurrency: :auto]

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Looks up a ready snapshot by direct ETS read. This is not a GenServer call."
  @spec get(binary(), atom()) :: {:ok, map()} | :not_found
  def get(source_uuid, table \\ @default_name) when is_binary(source_uuid) and is_atom(table) do
    case :ets.lookup(table, source_uuid) do
      [{^source_uuid, snapshot}] -> {:ok, snapshot}
      _ -> :not_found
    end
  rescue
    ArgumentError -> :not_found
  end

  @spec put(binary(), map(), GenServer.server()) :: :ok | :stale
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
  def init(opts) do
    name = Keyword.get(opts, :name, @default_name)
    table = :ets.new(name, @table_opts)
    {:ok, %{table: table}}
  end

  @impl true
  def handle_call({:put, source_uuid, snapshot}, _from, %{table: table} = state)
      when is_binary(source_uuid) and is_map(snapshot) do
    case :ets.lookup(table, source_uuid) do
      [{^source_uuid, current}] ->
        if replaceable?(current, snapshot) do
          true = :ets.insert(table, {source_uuid, snapshot})
          {:reply, :ok, state}
        else
          {:reply, :stale, state}
        end

      [] ->
        true = :ets.insert(table, {source_uuid, snapshot})
        {:reply, :ok, state}
    end
  end

  def handle_call({:delete, source_uuid}, _from, %{table: table} = state) do
    true = :ets.delete(table, source_uuid)
    {:reply, :ok, state}
  end

  def handle_call({:compare_delete, source_uuid, snapshot}, _from, %{table: table} = state)
      when is_map(snapshot) do
    case :ets.lookup(table, source_uuid) do
      [{^source_uuid, current}] ->
        if same_generation?(current, snapshot) do
          true = :ets.delete(table, source_uuid)
          {:reply, :ok, state}
        else
          {:reply, :stale, state}
        end

      _ ->
        {:reply, :stale, state}
    end
  end

  def handle_call(:list, _from, %{table: table} = state),
    do: {:reply, :ets.tab2list(table), state}

  defp replaceable?(current, incoming) do
    same_generation?(current, incoming) or
      Map.get(incoming, :generation, 0) > Map.get(current, :generation, 0)
  end

  defp same_generation?(left, right) do
    generation_token(left) == generation_token(right)
  end

  defp generation_token(snapshot) when is_map(snapshot) do
    {
      Map.get(snapshot, :source_uuid),
      Map.get(snapshot, :shadow_uuid),
      Map.get(snapshot, :generation),
      Map.get(snapshot, :operation_id)
    }
  end
end
