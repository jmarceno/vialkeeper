defmodule VialKeeper.Search.Owner do
  @moduledoc """
  Owns Tantivy generations for one database.

  A rebuild writes a new generation while the current committed generation keeps
  serving searches. Only the owner publishes a completed generation or applies
  incremental winner updates, so a failed or interrupted build is invisible to
  readers.
  """
  use GenServer

  alias VialKeeper.AtomicWrite
  alias VialKeeper.Observability.Instrumentation.Search, as: SearchInstrumentation
  alias VialKeeper.Search.Tantivy

  @registry VialKeeper.Search.Registry
  @search_root "search/indexes"
  @manifest "manifest.json"
  @backend "tantivy_ex"
  @open_attempts 8
  @open_retry_ms 25

  @type args :: {binary(), binary() | nil}

  defmodule IndexGeneration do
    @moduledoc "A published Tantivy generation and its logical index definition."

    @enforce_keys [:definition, :generation, :handle]
    defstruct [:definition, :generation, :handle]

    @type t :: %__MODULE__{
            definition: map(),
            generation: non_neg_integer() | nil,
            handle: VialKeeper.Search.Tantivy.handle() | nil
          }
  end

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
    do: Path.join(tmp_path, @search_root)

  def persist_path(_tmp_path), do: nil

  @impl true
  def init({uuid, tmp_path}) do
    state = %{uuid: uuid, tmp_path: tmp_path, indexes: %{}, rebuilds: %{}}
    {:ok, load_existing(state)}
  end

  @impl true
  def handle_call({:put_index, definition}, _from, state) do
    index_id = Map.get(definition, "index_id", Map.get(definition, :index_id))

    if is_binary(index_id) do
      entry =
        Map.get(
          state.indexes,
          index_id,
          %IndexGeneration{definition: definition, generation: nil, handle: nil}
        )

      entry = %{entry | definition: definition}
      new_state = %{state | indexes: Map.put(state.indexes, index_id, entry)}
      {:reply, persist_manifest(new_state, index_id, entry), new_state}
    else
      {:reply, {:error, VialKeeper.Error.invalid_request("full-text index id is required")}, state}
    end
  end

  def handle_call({:drop_index, index_id}, _from, state) do
    state = abort_rebuild_state(state, index_id)

    new_state = %{
      state
      | indexes: Map.delete(state.indexes, index_id),
        rebuilds: Map.delete(state.rebuilds, index_id)
    }

    _ = remove_index_files(state.tmp_path, index_id)
    {:reply, :ok, new_state}
  end

  def handle_call({:begin_rebuild, index_id, definition}, _from, state) do
    state = abort_rebuild_state(state, index_id)

    with {:ok, index_root} <- index_root(state.tmp_path, index_id),
         generation <- next_generation(state, index_id),
         generation_path = Path.join(index_root, "generation-#{generation}"),
         {:ok, handle} <- Tantivy.create(generation_path, definition) do
      rebuild = %{handle: handle, generation: generation, definition: definition, entries: 0}
      {:reply, :ok, %{state | rebuilds: Map.put(state.rebuilds, index_id, rebuild)}}
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  def handle_call({:rebuild_batch, index_id, documents}, _from, state)
      when is_list(documents) do
    case Map.fetch(state.rebuilds, index_id) do
      {:ok, rebuild} ->
        result =
          SearchInstrumentation.rebuild_batch(state.uuid, index_id, length(documents), fn ->
            add_documents(rebuild.handle, documents)
          end)

        case result do
          {:ok, handle, count} ->
            updated = %{rebuild | handle: handle, entries: rebuild.entries + count}
            {:reply, {:ok, count}, put_in(state.rebuilds[index_id], updated)}

          {:error, reason} ->
            {:reply, {:error, reason}, abort_rebuild_state(state, index_id)}
        end

      :error ->
        {:reply, {:error, VialKeeper.Error.index_not_found("full-text rebuild is not active")},
         state}
    end
  end

  def handle_call({:abort_rebuild, index_id}, _from, state) do
    {:reply, :ok, abort_rebuild_state(state, index_id)}
  end

  def handle_call({:finish_rebuild, index_id}, _from, state) do
    case fetch_rebuild(state, index_id) do
      {:ok, rebuild} ->
        finish_rebuild(state, index_id, rebuild)

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:rebuild, index_id, definition, documents}, _from, state)
      when is_list(documents) do
    with {:ok, state} <- begin_rebuild_state(state, index_id, definition),
         {:ok, state, entries} <- rebuild_documents(state, index_id, documents),
         {:ok, state} <- finish_rebuild_state(state, index_id) do
      {:reply, {:ok, entries}, state}
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  def handle_call({:refresh, document_id, body, deleted}, _from, state) do
    {reply, new_state} =
      SearchInstrumentation.refresh(state.uuid, 1, fn ->
        refresh_indexes(state, [{document_id, body, deleted}])
      end)

    {:reply, reply, new_state}
  end

  def handle_call({:refresh_many, updates}, _from, state) when is_list(updates) do
    {reply, new_state} =
      SearchInstrumentation.refresh(state.uuid, length(updates), fn ->
        refresh_indexes(state, updates)
      end)

    {:reply, reply, new_state}
  end

  def handle_call({:search, index_id, text, mode}, _from, state) do
    reply =
      SearchInstrumentation.query(state.uuid, mode, fn ->
        case Map.get(state.indexes, index_id) do
          %{handle: %{searcher: searcher} = handle} when not is_nil(searcher) ->
            Tantivy.search(handle, text, mode)

          %{handle: nil} ->
            {:error, VialKeeper.Error.index_not_found("full-text index is not built")}

          nil ->
            {:error, VialKeeper.Error.index_not_found("full-text index not found")}
        end
      end)

    {:reply, reply, state}
  end

  def handle_call(:has_indexes, _from, state),
    do: {:reply, map_size(state.indexes) > 0, state}

  @impl true
  def terminate(_reason, _state), do: :ok

  defp load_existing(state) do
    case persist_path(state.tmp_path) do
      nil -> state
      root -> load_manifests(state, Path.wildcard(Path.join(root, "*/#{@manifest}")))
    end
  end

  defp load_manifests(state, []), do: state

  defp load_manifests(state, [manifest_path | rest]) do
    state =
      case File.read(manifest_path) do
        {:ok, binary} -> load_manifest(state, manifest_path, binary)
        {:error, _} -> state
      end

    load_manifests(state, rest)
  end

  defp open_published_generation(path, definition),
    do: open_published_generation(path, definition, @open_attempts)

  defp open_published_generation(path, definition, 1), do: Tantivy.open(path, definition)

  defp open_published_generation(path, definition, remaining) do
    case Tantivy.open(path, definition) do
      {:ok, _} = ok ->
        ok

      {:error, _} ->
        Process.sleep(@open_retry_ms)
        open_published_generation(path, definition, remaining - 1)
    end
  end

  defp load_manifest(state, _manifest_path, binary) do
    with {:ok,
          %{
            "backend" => @backend,
            "backend_version" => backend_version,
            "schema_fingerprint" => schema_fingerprint,
            "index_id" => index_id,
            "generation" => generation,
            "definition" => definition
          }} <- Jason.decode(binary),
         true <- backend_version == Tantivy.backend_version(),
         true <- schema_fingerprint == Tantivy.schema_fingerprint(),
         true <- is_binary(index_id) and is_integer(generation) and is_map(definition),
         {:ok, root} <- index_root(state.tmp_path, index_id),
         generation_path = Path.join(root, "generation-#{generation}"),
         {:ok, handle} <- open_published_generation(generation_path, definition) do
      entry = %IndexGeneration{
        definition: definition,
        generation: generation,
        handle: handle
      }

      %{state | indexes: Map.put(state.indexes, index_id, entry)}
    else
      _ -> state
    end
  end

  defp begin_rebuild_state(state, index_id, definition) do
    case handle_call({:begin_rebuild, index_id, definition}, self(), state) do
      {:reply, :ok, new_state} -> {:ok, new_state}
      {:reply, {:error, _} = error, _state} -> error
    end
  end

  defp rebuild_documents(state, index_id, documents) do
    rebuild = Map.fetch!(state.rebuilds, index_id)

    case add_documents(rebuild.handle, documents) do
      {:ok, handle, count} ->
        updated = %{rebuild | handle: handle, entries: rebuild.entries + count}
        {:ok, put_in(state.rebuilds[index_id], updated), count}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp finish_rebuild_state(state, index_id) do
    case handle_call({:finish_rebuild, index_id}, self(), state) do
      {:reply, {:ok, _entries}, new_state} -> {:ok, new_state}
      {:reply, {:error, _} = error, _state} -> error
    end
  end

  defp add_documents(handle, documents) do
    with {:ok, prepared, count} <- prepare_documents(documents),
         :ok <- Tantivy.add_batch(handle, prepared) do
      {:ok, handle, count}
    end
  end

  defp prepare_documents(documents) do
    Enum.reduce(documents, {[], 0}, fn document, {acc, count} ->
      case document_values(document) do
        {:ok, {_id, _body, true}} -> {acc, count + 1}
        {:ok, {id, body, false}} -> {[{id, body} | acc], count + 1}
        :skip -> {acc, count}
      end
    end)
    |> then(fn {prepared, count} -> {:ok, Enum.reverse(prepared), count} end)
  end

  defp document_values(%{id: id, body: body, deleted: deleted})
       when is_binary(id) and is_boolean(deleted),
       do: {:ok, {id, body || %{}, deleted}}

  defp document_values(%{id: id, body: body}) when is_binary(id),
    do: {:ok, {id, body || %{}, false}}

  defp document_values(%{document_id: id, body: body, deleted: deleted})
       when is_binary(id) and is_boolean(deleted), do: {:ok, {id, body || %{}, deleted}}

  defp document_values(%{"id" => id, "body" => body}) when is_binary(id),
    do: {:ok, {id, body || %{}, false}}

  defp document_values(%{"document_id" => id, "body" => body, "deleted" => deleted})
       when is_binary(id) and is_boolean(deleted), do: {:ok, {id, body || %{}, deleted}}

  defp document_values(_), do: :skip

  defp refresh_indexes(state, updates) do
    results =
      state.indexes
      |> Enum.reduce_while({:ok, state}, fn {index_id, entry}, {:ok, acc} ->
        case entry.handle do
          nil -> {:cont, {:ok, acc}}
          handle -> refresh_one(acc, index_id, entry, handle, updates)
        end
      end)

    case results do
      {:ok, new_state} -> {:ok, new_state}
      {:error, _} = error -> {error, state}
    end
  end

  defp refresh_one(state, index_id, entry, handle, updates) do
    case apply_updates(handle, updates) do
      {:ok, updated} ->
        case Tantivy.publish(updated) do
          {:ok, committed} ->
            {:cont, {:ok, put_in(state.indexes[index_id], %{entry | handle: committed})}}

          {:error, reason} ->
            _ = Tantivy.rollback(updated)
            {:halt, {:error, reason}}
        end

      {:error, reason} ->
        _ = Tantivy.rollback(handle)
        {:halt, {:error, reason}}
    end
  end

  defp apply_updates(handle, updates) do
    updates
    |> deduplicate_updates()
    |> Enum.reduce_while({:ok, handle}, fn {document_id, body, deleted}, {:ok, acc} ->
      case Tantivy.replace(acc, document_id, body, deleted) do
        :ok -> {:cont, {:ok, acc}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp deduplicate_updates(updates),
    do: updates |> Enum.reverse() |> Enum.uniq_by(&elem(&1, 0)) |> Enum.reverse()

  defp publish(state, index_id, entry),
    do: %{
      state
      | indexes: Map.put(state.indexes, index_id, entry),
        rebuilds: Map.delete(state.rebuilds, index_id)
    }

  defp abort_rebuild_state(state, index_id) do
    case Map.pop(state.rebuilds, index_id) do
      {nil, _rebuilds} ->
        state

      {rebuild, rebuilds} ->
        _ = Tantivy.rollback(rebuild.handle)
        _ = remove_generation(state.tmp_path, index_id, rebuild.generation)
        %{state | rebuilds: rebuilds}
    end
  end

  defp finish_rebuild(state, index_id, rebuild) do
    case Tantivy.commit(rebuild.handle) do
      {:ok, handle} ->
        entry = %IndexGeneration{
          definition: rebuild.definition,
          generation: rebuild.generation,
          handle: handle
        }

        new_state = publish(state, index_id, entry)

        case persist_manifest(new_state, index_id, entry) do
          :ok ->
            {:reply, {:ok, rebuild.entries},
             cleanup_generations(new_state, index_id, rebuild.generation)}

          {:error, _} = error ->
            _ = remove_generation(state.tmp_path, index_id, rebuild.generation)
            {:reply, error, abort_rebuild_state(state, index_id)}
        end

      {:error, _} = error ->
        {:reply, error, abort_rebuild_state(state, index_id)}
    end
  end

  defp fetch_rebuild(state, index_id) do
    case Map.fetch(state.rebuilds, index_id) do
      {:ok, rebuild} -> {:ok, rebuild}
      :error -> {:error, VialKeeper.Error.index_not_found("full-text rebuild is not active")}
    end
  end

  defp next_generation(state, index_id) do
    current = state.indexes[index_id]
    rebuilding = state.rebuilds[index_id]
    disk = disk_generation(state.tmp_path, index_id)
    max(current_generation(current), max(current_generation(rebuilding), disk)) + 1
  end

  defp current_generation(%{generation: generation}) when is_integer(generation), do: generation
  defp current_generation(_), do: 0

  defp disk_generation(nil, _index_id), do: 0

  defp disk_generation(tmp_path, index_id) do
    with {:ok, root} <- index_root(tmp_path, index_id), {:ok, entries} <- File.ls(root) do
      entries
      |> Enum.map(&generation_number/1)
      |> Enum.max(fn -> 0 end)
    else
      _ -> 0
    end
  end

  defp generation_number("generation-" <> suffix) do
    case Integer.parse(suffix) do
      {generation, ""} -> generation
      _ -> 0
    end
  end

  defp generation_number(_), do: 0

  defp index_root(nil, _index_id),
    do: {:error, VialKeeper.Error.internal_error("persistent search path is unavailable")}

  defp index_root(tmp_path, index_id) when is_binary(tmp_path) and is_binary(index_id) do
    encoded = Base.url_encode64(index_id, padding: false)
    {:ok, Path.join(persist_path(tmp_path), encoded)}
  end

  defp persist_manifest(%{tmp_path: nil}, _index_id, _entry), do: :ok

  defp persist_manifest(state, index_id, %{generation: nil}),
    do: persist_marker(state.tmp_path, index_id)

  defp persist_manifest(state, index_id, entry) do
    with {:ok, root} <- index_root(state.tmp_path, index_id),
         :ok <- File.mkdir_p(root),
         {:ok, binary} <-
           Jason.encode(%{
             "backend" => @backend,
             "backend_version" => Tantivy.backend_version(),
             "schema_fingerprint" => Tantivy.schema_fingerprint(),
             "index_id" => index_id,
             "generation" => entry.generation,
             "definition" => entry.definition
           }),
         :ok <- AtomicWrite.write(Path.join(root, @manifest), binary) do
      :ok
    else
      {:error, reason} ->
        {:error,
         VialKeeper.Error.internal_error("search manifest persist failed", %{cause: inspect(reason)})}
    end
  end

  defp cleanup_generations(state, index_id, current_generation) do
    _ =
      with {:ok, root} <- index_root(state.tmp_path, index_id),
           {:ok, entries} <- File.ls(root) do
        Enum.each(entries, &remove_stale_generation(root, &1, current_generation))
      end

    state
  end

  defp remove_stale_generation(root, entry, current_generation) do
    if generation_number(entry) not in [0, current_generation] do
      _ = File.rm_rf(Path.join(root, entry))
      :ok
    end

    :ok
  end

  defp remove_generation(nil, _index_id, _generation), do: :ok

  defp remove_generation(tmp_path, index_id, generation) do
    {:ok, root} = index_root(tmp_path, index_id)
    File.rm_rf(Path.join(root, "generation-#{generation}"))
  end

  defp persist_marker(tmp_path, index_id) do
    with {:ok, root} <- index_root(tmp_path, index_id) do
      File.mkdir_p(root)
    end
  end

  defp remove_index_files(nil, _index_id), do: :ok

  defp remove_index_files(tmp_path, index_id) do
    {:ok, root} = index_root(tmp_path, index_id)
    File.rm_rf(root)
  end
end
