defmodule VialKeeper.Search do
  @moduledoc """
  Storage-neutral full-text search backed by Tantivy generations.

  Logical index definitions stay in the document store catalog. Tantivy owns
  analysis, matching, ranking, positions, and segment persistence under the
  bundle `tmp/search/indexes/` directory.
  """

  alias VialKeeper.Config
  alias VialKeeper.Deadline
  alias VialKeeper.Domain.Revision
  alias VialKeeper.MapAccess
  alias VialKeeper.Observability.Instrumentation.Mutation
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

  @spec rebuild(BackendContext.t(), binary(), map(), Enumerable.t()) ::
          :ok | {:error, VialKeeper.Error.t()}
  def rebuild(%BackendContext{} = context, index_id, definition, documents)
      when is_binary(index_id) and is_map(definition) do
    case rebuild_stream(context, index_id, definition, documents) do
      {:ok, _entries} -> :ok
      {:error, _} = error -> error
    end
  end

  @spec rebuild_stream(BackendContext.t(), binary(), map(), Enumerable.t()) ::
          {:ok, non_neg_integer()} | {:error, VialKeeper.Error.t()}
  def rebuild_stream(%BackendContext{} = context, index_id, definition, documents)
      when is_binary(index_id) and is_map(definition) do
    deadline = rebuild_deadline()

    result =
      with :ok <- call_with_deadline(context, {:begin_rebuild, index_id, definition}, deadline),
           {:ok, entries} <- stream_batches(context, index_id, documents, deadline),
           :ok <- check_rebuild_deadline(deadline),
           {:ok, final_entries} <-
             call_with_deadline(context, {:finish_rebuild, index_id}, deadline) do
        {:ok, final_entries || entries}
      end

    finish_rebuild_attempt(context, index_id, result)
  end

  @spec rebuild_pages(
          BackendContext.t(),
          binary(),
          map(),
          term(),
          (term() -> {:ok, {[map()], term(), boolean()}} | {:error, VialKeeper.Error.t()})
        ) :: {:ok, non_neg_integer()} | {:error, VialKeeper.Error.t()}
  def rebuild_pages(%BackendContext{} = context, index_id, definition, cursor, page_fun)
      when is_binary(index_id) and is_map(definition) and is_function(page_fun, 1) do
    rebuild_pages(context, index_id, definition, cursor, page_fun, fn -> :ok end)
  end

  @spec rebuild_pages(
          BackendContext.t(),
          binary(),
          map(),
          term(),
          (term() -> {:ok, {[map()], term(), boolean()}} | {:error, VialKeeper.Error.t()}),
          (-> :ok | {:error, VialKeeper.Error.t()})
          | (Deadline.t() -> :ok | {:error, VialKeeper.Error.t()})
        ) :: {:ok, non_neg_integer()} | {:error, VialKeeper.Error.t()}
  def rebuild_pages(
        %BackendContext{} = context,
        index_id,
        definition,
        cursor,
        page_fun,
        before_finish
      )
      when is_binary(index_id) and is_map(definition) and is_function(page_fun, 1) and
             (is_function(before_finish, 0) or is_function(before_finish, 1)) do
    deadline = rebuild_deadline()

    result =
      with :ok <- call_with_deadline(context, {:begin_rebuild, index_id, definition}, deadline),
           {:ok, _count} <- consume_pages(context, index_id, cursor, page_fun, deadline),
           :ok <- check_rebuild_deadline(deadline),
           :ok <- run_before_finish(before_finish, deadline),
           :ok <- check_rebuild_deadline(deadline) do
        call_with_deadline(context, {:finish_rebuild, index_id}, deadline)
      end

    finish_rebuild_attempt(context, index_id, result)
  end

  @spec begin_rebuild(BackendContext.t(), binary(), map()) :: :ok | {:error, VialKeeper.Error.t()}
  def begin_rebuild(%BackendContext{} = context, index_id, definition)
      when is_binary(index_id) and is_map(definition),
      do: call(context, {:begin_rebuild, index_id, definition})

  @spec rebuild_batch(BackendContext.t(), binary(), [map()]) ::
          {:ok, non_neg_integer()} | {:error, VialKeeper.Error.t()}
  def rebuild_batch(%BackendContext{} = context, index_id, documents)
      when is_binary(index_id) and is_list(documents),
      do: call(context, {:rebuild_batch, index_id, documents})

  @spec rebuild_batch(BackendContext.t(), binary(), [map()], Deadline.t()) ::
          {:ok, non_neg_integer()} | {:error, VialKeeper.Error.t()}
  def rebuild_batch(%BackendContext{} = context, index_id, documents, deadline)
      when is_binary(index_id) and is_list(documents) and is_integer(deadline),
      do: call_with_deadline(context, {:rebuild_batch, index_id, documents}, deadline)

  @spec abort_rebuild(BackendContext.t(), binary()) :: :ok | {:error, VialKeeper.Error.t()}
  def abort_rebuild(%BackendContext{} = context, index_id) when is_binary(index_id),
    do: call(context, {:abort_rebuild, index_id})

  @spec finish_rebuild(BackendContext.t(), binary()) ::
          {:ok, non_neg_integer()} | {:error, VialKeeper.Error.t()}
  def finish_rebuild(%BackendContext{} = context, index_id) when is_binary(index_id),
    do: call(context, {:finish_rebuild, index_id})

  @spec search(BackendContext.t(), binary(), binary(), binary()) ::
          {:ok, [map()]} | {:error, VialKeeper.Error.t()}
  def search(%BackendContext{} = context, index_id, text, mode)
      when is_binary(index_id) and is_binary(text) and is_binary(mode) do
    call(context, {:search, index_id, text, mode})
  end

  @doc "Searches one index for a ranked candidate window, expanding score ties when needed."
  @spec search_page(BackendContext.t(), binary(), binary(), binary(), pos_integer()) ::
          {:ok, [map()]} | {:error, VialKeeper.Error.t()}
  def search_page(%BackendContext{} = context, index_id, text, mode, limit)
      when is_binary(index_id) and is_binary(text) and is_binary(mode) and is_integer(limit) and
             limit > 0 do
    call(context, {:search, index_id, text, mode, limit})
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
        flush_search_pending(context, result)

      {:error, _} = error ->
        clear_pending()
        error
    end
  end

  defp flush_search_pending(context, result) do
    case Mutation.phase(:search_flush, fn -> flush_pending(context) end) do
      :ok -> {:ok, result}
      {:error, _} = error -> error
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

  defp call_with_deadline(%BackendContext{} = context, request, deadline) do
    with :ok <- check_rebuild_deadline(deadline),
         {:ok, uuid, tmp_path} <- session(context),
         {:ok, pid} <- ensure_owner(uuid, tmp_path) do
      timeout = min(call_timeout(request), max(1, Deadline.remaining(deadline)))

      try do
        GenServer.call(pid, request, timeout)
      catch
        :exit, reason ->
          if Deadline.exhausted?(deadline) or Deadline.genserver_call_timeout?(reason) do
            {:error, rebuild_timeout_error()}
          else
            exit(reason)
          end
      end
    end
  end

  defp call_timeout({operation, _index_id, _definition})
       when operation in [:begin_rebuild],
       do: Config.search_rebuild_timeout_ms()

  defp call_timeout({operation, _index_id, _documents})
       when operation in [:rebuild_batch],
       do: Config.search_rebuild_timeout_ms()

  defp call_timeout({:finish_rebuild, _index_id}), do: Config.search_rebuild_timeout_ms()

  defp call_timeout(_request) do
    query_timeout()
  end

  defp query_timeout do
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

  defp tmp_path(":memory:"), do: Application.get_env(:vial_keeper, :search_memory_root)
  defp tmp_path(root) when is_binary(root), do: Path.join(root, "tmp")
  defp tmp_path(_root), do: nil

  defp stream_batches(context, index_id, documents, deadline) do
    batch_size = Config.search_rebuild_batch_size()

    stream_batches_from_enum(context, index_id, documents, batch_size, deadline)
  end

  defp stream_batches_from_enum(context, index_id, documents, batch_size, deadline) do
    documents
    |> Stream.chunk_every(batch_size)
    |> Enum.reduce_while({:ok, 0}, fn batch, {:ok, count} ->
      case rebuild_batch(context, index_id, batch, deadline) do
        {:ok, added} -> {:cont, {:ok, count + added}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp consume_pages(context, index_id, cursor, page_fun, deadline) do
    case page_fun.(cursor) do
      {:ok, {[], _next_cursor, _done}} ->
        {:ok, 0}

      {:ok, {page, next_cursor, done}} when is_list(page) and is_boolean(done) ->
        with {:ok, added} <- rebuild_batch(context, index_id, page, deadline),
             {:ok, remaining} <-
               consume_remaining_pages(
                 done,
                 context,
                 index_id,
                 next_cursor,
                 page_fun,
                 deadline
               ) do
          {:ok, added + remaining}
        end

      {:error, _} = error ->
        error

      _ ->
        {:error, VialKeeper.Error.internal_error("full-text rebuild page is invalid")}
    end
  end

  defp consume_remaining_pages(true, _context, _index_id, _cursor, _page_fun, _deadline),
    do: {:ok, 0}

  defp consume_remaining_pages(false, context, index_id, cursor, page_fun, deadline),
    do: consume_pages(context, index_id, cursor, page_fun, deadline)

  defp winner_payload(%Revision{deleted: deleted, body: body}), do: {body || %{}, deleted}

  defp winner_payload(winner) when is_map(winner) do
    deleted = MapAccess.get(winner, :deleted, false) == true
    body = MapAccess.get(winner, :body) || %{}
    {body, deleted}
  end

  defp rebuild_deadline,
    do: Deadline.from_timeout(Config.search_rebuild_timeout_ms())

  defp check_rebuild_deadline(deadline) do
    if Deadline.exhausted?(deadline) do
      {:error, rebuild_timeout_error()}
    else
      :ok
    end
  end

  defp run_before_finish(before_finish, _deadline) when is_function(before_finish, 0),
    do: before_finish.()

  defp run_before_finish(before_finish, deadline) when is_function(before_finish, 1),
    do: before_finish.(deadline)

  defp finish_rebuild_attempt(context, index_id, {:error, _} = error) do
    _ = abort_rebuild(context, index_id)
    error
  end

  defp finish_rebuild_attempt(_context, _index_id, result), do: result

  defp rebuild_timeout_error do
    VialKeeper.Error.resource_limit(
      "full-text rebuild exceeded the configured timeout",
      %{timeout_ms: Config.search_rebuild_timeout_ms()}
    )
  end
end
