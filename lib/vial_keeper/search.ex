defmodule VialKeeper.Search do
  @moduledoc """
  Storage-neutral full-text search over rebuildable posting lists.

  Logical index definitions stay in the document store catalog. Tokenization
  and matching use `unicode_words_v1`. Physical posting lists live in ETS and
  a disposable cache under the bundle `tmp/` directory.
  """

  alias VialKeeper.Config
  alias VialKeeper.Domain.Revision
  alias VialKeeper.MapAccess
  alias VialKeeper.Search.{Owner, Supervisor}
  alias VialKeeper.Storage.BackendContext

  @pending_key :vial_keeper_search_pending

  @spec ensure(BackendContext.t()) :: {:ok, pid()} | {:error, VialKeeper.Error.t()}
  def ensure(%BackendContext{} = context) do
    case session(context) do
      {:ok, uuid, tmp_path} -> ensure_owner(uuid, tmp_path)
      {:error, _} = error -> error
    end
  end

  @spec stop(BackendContext.t()) :: :ok
  def stop(%BackendContext{} = context) do
    case session(context) do
      {:ok, uuid, _tmp_path} -> Supervisor.stop_owner(uuid)
      {:error, _} -> :ok
    end
  end

  @spec put_index(BackendContext.t(), map()) :: :ok | {:error, VialKeeper.Error.t()}
  def put_index(%BackendContext{} = context, definition) when is_map(definition) do
    call(context, {:put_index, definition})
  end

  @spec drop_index(BackendContext.t(), binary()) :: :ok | {:error, VialKeeper.Error.t()}
  def drop_index(%BackendContext{} = context, index_id) when is_binary(index_id) do
    with {:ok, uuid, tmp_path} <- session(context) do
      call_existing(uuid, tmp_path, context, {:drop_index, index_id})
    end
  end

  @spec rebuild(BackendContext.t(), binary(), map(), [map()]) ::
          :ok | {:error, VialKeeper.Error.t()}
  def rebuild(%BackendContext{} = context, index_id, definition, documents)
      when is_binary(index_id) and is_map(definition) and is_list(documents) do
    call(context, {:rebuild, index_id, definition, documents})
  end

  @spec search(BackendContext.t(), binary(), binary(), binary()) ::
          {:ok, [map()]} | {:error, VialKeeper.Error.t()}
  def search(%BackendContext{} = context, index_id, text, mode)
      when is_binary(index_id) and is_binary(text) and is_binary(mode) do
    call(context, {:search, index_id, text, mode})
  end

  @spec record_winner(binary(), map()) :: :ok
  def record_winner(document_id, winner) when is_binary(document_id) and is_map(winner) do
    {body, deleted} = winner_payload(winner)
    updates = Process.get(@pending_key, [])
    Process.put(@pending_key, [{document_id, body, deleted} | updates])
    :ok
  end

  @spec clear_pending() :: :ok
  def clear_pending do
    Process.delete(@pending_key)
    :ok
  end

  @spec flush_pending(BackendContext.t()) :: :ok | {:error, VialKeeper.Error.t()}
  def flush_pending(%BackendContext{} = context) do
    updates = Process.get(@pending_key, []) |> Enum.reverse()
    Process.delete(@pending_key)

    if updates == [] do
      :ok
    else
      flush_updates(context, updates)
    end
  end

  @spec with_pending(BackendContext.t(), (-> {:ok, term()} | {:error, VialKeeper.Error.t()})) ::
          {:ok, term()} | {:error, VialKeeper.Error.t()}
  def with_pending(%BackendContext{} = context, fun) when is_function(fun, 0) do
    clear_pending()

    case fun.() do
      {:ok, result} ->
        case flush_pending(context) do
          :ok -> {:ok, result}
          {:error, _} = error -> error
        end

      {:error, _} = error ->
        clear_pending()
        error
    end
  end

  defp flush_updates(%BackendContext{} = context, updates) do
    with {:ok, uuid, tmp_path} <- session(context) do
      call_existing(uuid, tmp_path, context, {:refresh_many, updates})
    end
  end

  defp call_existing(uuid, tmp_path, context, request) do
    case Owner.whereis(uuid) do
      pid when is_pid(pid) -> GenServer.call(pid, request, call_timeout(request))
      :undefined -> call_if_persisted(tmp_path, context, request)
    end
  end

  defp call_if_persisted(tmp_path, context, request) do
    if persist_present?(tmp_path), do: call(context, request), else: :ok
  end

  defp persist_present?(tmp_path) do
    case Owner.persist_path(tmp_path) do
      path when is_binary(path) -> File.exists?(path)
      _ -> false
    end
  end

  defp call(%BackendContext{} = context, request) do
    with {:ok, uuid, tmp_path} <- session(context),
         {:ok, pid} <- ensure_owner(uuid, tmp_path) do
      GenServer.call(pid, request, call_timeout(request))
    end
  end

  defp call_timeout({:rebuild, _index_id, _definition, _documents}),
    do: Config.search_rebuild_timeout_ms()

  defp call_timeout(_request) do
    case Config.host_limits()[:max_query_execution_ms] do
      timeout when is_integer(timeout) and timeout > 0 -> timeout
      _ -> 5_000
    end
  end

  defp ensure_owner(uuid, tmp_path) do
    case Supervisor.start_owner(uuid, tmp_path) do
      {:ok, pid} ->
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        {:ok, pid}

      {:error, reason} ->
        {:error,
         VialKeeper.Error.internal_error("search index owner failed to start", %{
           reason: inspect(reason)
         })}
    end
  end

  defp session(%BackendContext{} = context) do
    uuid = MapAccess.get(context.identity, :database_uuid)

    if is_binary(uuid) do
      {:ok, uuid, tmp_path(context.bundle_root)}
    else
      {:error, VialKeeper.Error.internal_error("search index requires a database UUID")}
    end
  end

  defp tmp_path(":memory:"), do: nil
  defp tmp_path(root) when is_binary(root), do: Path.join(root, "tmp")
  defp tmp_path(_root), do: nil

  defp winner_payload(%Revision{deleted: deleted, body: body}), do: {body || %{}, deleted}

  defp winner_payload(winner) when is_map(winner) do
    deleted = MapAccess.get(winner, :deleted, false) == true
    body = MapAccess.get(winner, :body) || %{}
    {body, deleted}
  end
end
