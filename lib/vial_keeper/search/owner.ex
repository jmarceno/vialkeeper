defmodule VialKeeper.Search.Owner do
  @moduledoc """
  Owns rebuildable ETS posting lists for one database.

  The inverted index is not authoritative document state. It is reconstructed
  from winning documents when the on-disk cache is missing.
  """
  use GenServer

  alias VialKeeper.Search.Engine

  @registry VialKeeper.Search.Registry
  @persist_file "search-index.etf"

  @type args :: {binary(), binary() | nil}

  @spec start_link(args()) :: GenServer.on_start()
  def start_link({uuid, _tmp_path} = arg) when is_binary(uuid) do
    GenServer.start_link(__MODULE__, arg, name: via(uuid))
  end

  def child_spec({uuid, _tmp_path} = arg) when is_binary(uuid) do
    %{
      id: {:search_owner, uuid},
      start: {__MODULE__, :start_link, [arg]},
      restart: :transient,
      type: :worker
    }
  end

  @spec via(binary()) :: {:via, module(), term()}
  def via(uuid) when is_binary(uuid), do: {:via, Registry, {@registry, uuid}}

  @spec whereis(binary()) :: pid() | :undefined
  def whereis(uuid) when is_binary(uuid) do
    case Registry.lookup(@registry, uuid) do
      [{pid, _}] -> pid
      [] -> :undefined
    end
  end

  @spec persist_path(binary() | nil) :: binary() | nil
  def persist_path(tmp_path) when is_binary(tmp_path) and tmp_path not in [":memory:", ""],
    do: Path.join(tmp_path, @persist_file)

  def persist_path(_tmp_path), do: nil

  @impl true
  def init({uuid, tmp_path}) do
    tables = Engine.new_tables()
    path = persist_path(tmp_path)
    :ok = maybe_load(tables, path)
    {:ok, %{uuid: uuid, tmp_path: tmp_path, tables: tables, persist_path: path}}
  end

  @impl true
  def handle_call({:put_index, definition}, _from, state) do
    :ok = Engine.put_index(state.tables, definition)
    {:reply, persist(state), state}
  end

  def handle_call({:drop_index, index_id}, _from, state) do
    :ok = Engine.drop_index(state.tables, index_id)
    {:reply, persist(state), state}
  end

  def handle_call({:rebuild, index_id, definition, documents}, _from, state) do
    :ok = Engine.drop_index(state.tables, index_id)
    :ok = Engine.put_index(state.tables, Map.put(definition, "index_id", index_id))

    Enum.each(documents, fn document ->
      refresh_document(state.tables, document)
    end)

    {:reply, persist(state), state}
  end

  def handle_call({:refresh, document_id, body, deleted}, _from, state) do
    :ok = Engine.refresh(state.tables, document_id, body, deleted)
    {:reply, persist(state), state}
  end

  def handle_call({:refresh_many, updates}, _from, state) do
    Enum.each(updates, fn {document_id, body, deleted} ->
      :ok = Engine.refresh(state.tables, document_id, body, deleted)
    end)

    {:reply, persist(state), state}
  end

  def handle_call({:search, index_id, text, mode}, _from, state) do
    {:reply, Engine.search(state.tables, index_id, text, mode), state}
  end

  def handle_call(:has_indexes, _from, state) do
    {:reply, :ets.info(state.tables.meta, :size) > 0, state}
  end

  @impl true
  def terminate(_reason, state), do: persist(state)

  defp refresh_document(tables, %{id: id, body: body}) when is_binary(id),
    do: Engine.refresh(tables, id, body || %{}, false)

  defp refresh_document(tables, %{document_id: id, body: body, deleted: deleted})
       when is_binary(id) and is_boolean(deleted),
       do: Engine.refresh(tables, id, body, deleted)

  defp refresh_document(_tables, _document), do: :ok

  defp persist(%{persist_path: nil}), do: :ok

  defp persist(%{tables: tables, persist_path: path}) do
    binary = :erlang.term_to_binary({:v1, Engine.dump(tables)})

    with :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- File.write(path, binary, [:binary]) do
      :ok
    else
      {:error, reason} ->
        {:error,
         VialKeeper.Error.internal_error("search index persist failed", %{reason: inspect(reason)})}
    end
  end

  defp maybe_load(_tables, nil), do: :ok

  defp maybe_load(tables, path) do
    case File.read(path) do
      {:ok, binary} ->
        case :erlang.binary_to_term(binary, [:safe]) do
          {:v1, dump} -> Engine.load(tables, dump)
          _ -> :ok
        end

      {:error, :enoent} ->
        :ok

      {:error, _reason} ->
        :ok
    end
  end
end
