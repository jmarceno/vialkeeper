defmodule VialKeeper.Storage.SQLite.SearchIndexes do
  @moduledoc """
  Feeds winning SQLite documents to the Tantivy generation owner.

  Logical index rows stay in the SQLite catalog. Tantivy generations are
  disposable and are reconstructed from winners when missing.
  """

  alias VialKeeper.Deadline
  alias VialKeeper.MapAccess
  alias VialKeeper.Observability.Instrumentation.Search, as: SearchInstrumentation
  alias VialKeeper.Query.Projection
  alias VialKeeper.Search
  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.SQLite.{Changes, Connection, Context, IndexCatalog, TermBlob}

  @query_body_term_cache_limit 256
  @rebuild_page_size 500

  @type rebuild_trigger :: :create | :rebuild | :cache_miss

  @spec rebuild(BackendContext.t(), map(), map()) :: :ok | {:error, VialKeeper.Error.t()}
  def rebuild(%BackendContext{} = context, index, definition)
      when is_map(index) and is_map(definition) do
    rebuild(context, index, definition, :rebuild)
  end

  @spec rebuild(BackendContext.t(), map(), map(), rebuild_trigger()) ::
          :ok | {:error, VialKeeper.Error.t()}
  def rebuild(%BackendContext{} = context, index, definition, trigger)
      when is_map(index) and is_map(definition) and
             trigger in [:create, :rebuild, :cache_miss] do
    if full_text?(index) or full_text?(definition) do
      rebuild_full_text(context, index, definition, trigger)
    else
      :ok
    end
  end

  @spec rebuild_id(BackendContext.t(), binary()) :: :ok | {:error, VialKeeper.Error.t()}
  def rebuild_id(%BackendContext{} = context, index_id) when is_binary(index_id) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, indexes} <- IndexCatalog.list(adapter.conn),
         {:ok, index} <- find_index(indexes, index_id) do
      rebuild(context, index, index)
    end
  end

  @spec drop(BackendContext.t(), binary()) :: :ok
  def drop(%BackendContext{} = context, index_id) when is_binary(index_id) do
    case Search.drop_index(context, index_id) do
      :ok -> :ok
      {:error, _} -> :ok
    end
  end

  @spec winning_documents(Connection.handle()) ::
          {:ok, [map()]} | {:error, VialKeeper.Error.t()}
  def winning_documents(conn) do
    with {:ok, rows} <-
           Connection.query(
             conn,
             "SELECT document_id, winning_revision, winning_body_term FROM documents WHERE winning_deleted = 0"
           ) do
      decode_query_documents(rows)
    end
  end

  @spec decode_query_documents(list()) :: {:ok, [map()]} | {:error, VialKeeper.Error.t()}
  def decode_query_documents(rows) when is_list(rows) do
    max_depth = VialKeeper.Config.host_limits()[:max_json_nesting_depth] || 100
    decode_query_documents(rows, [], max_depth)
  end

  defp decode_query_documents([], acc, _max_depth), do: {:ok, :lists.reverse(acc)}

  defp decode_query_documents([[id, revision, body_term] | rows], acc, max_depth) do
    case TermBlob.decode_trusted_with_cache(
           body_term,
           :query_body_term,
           max_depth,
           @query_body_term_cache_limit
         ) do
      {:ok, body} ->
        decode_query_documents(rows, [Projection.document(id, revision, body) | acc], max_depth)

      {:error, error} ->
        {:error, error}
    end
  end

  defp rebuild_full_text(context, index, definition, trigger) do
    index_id = MapAccess.get(index, :index_id)
    uuid = MapAccess.get(context.identity, :database_uuid)

    case SearchInstrumentation.rebuild(uuid, index_id, trigger, fn ->
           rebuild_posting_lists(context, index, definition, index_id)
         end) do
      {:ok, _entries} -> :ok
      {:error, _} = error -> error
    end
  end

  defp rebuild_posting_lists(context, index, definition, index_id) do
    with {:ok, adapter} <- Context.unwrap(context),
         definition <-
           definition
           |> Map.merge(nested_metadata(index))
           |> Map.put("index_id", index_id),
         {:ok, start_sequence} <- Changes.current_sequence(adapter.conn) do
      Search.rebuild_pages(
        context,
        index_id,
        definition,
        nil,
        fn after_id -> winning_documents_page(adapter.conn, after_id, @rebuild_page_size) end,
        fn deadline -> catch_up(context, adapter.conn, index_id, start_sequence, deadline) end
      )
    end
  end

  @doc "Loads one bounded, document-id ordered rebuild page."
  @spec winning_documents_page(Connection.handle(), binary() | nil, pos_integer()) ::
          {:ok, {[map()], binary() | nil, boolean()}} | {:error, VialKeeper.Error.t()}
  def winning_documents_page(conn, after_id, limit)
      when (is_binary(after_id) or is_nil(after_id)) and is_integer(limit) and limit > 0 do
    {sql, params} =
      case after_id do
        nil ->
          {
            "SELECT document_id, winning_revision, winning_body_term FROM documents WHERE winning_deleted = 0 ORDER BY document_id LIMIT ?",
            [limit]
          }

        _ ->
          {
            "SELECT document_id, winning_revision, winning_body_term FROM documents WHERE winning_deleted = 0 AND document_id > ? ORDER BY document_id LIMIT ?",
            [after_id, limit]
          }
      end

    with {:ok, rows} <- Connection.query(conn, sql, params),
         {:ok, documents} <- decode_query_documents(rows) do
      next_id = documents |> List.last() |> document_id()
      {:ok, {documents, next_id, length(rows) < limit}}
    end
  end

  defp document_id(%{id: id}) when is_binary(id), do: id
  defp document_id(_), do: nil

  defp catch_up(context, conn, index_id, sequence, deadline) do
    with :ok <- check_deadline(deadline),
         {:ok, next_sequence} <- catch_up_page(context, conn, index_id, sequence, deadline),
         {:ok, latest_sequence} <- Changes.current_sequence(conn) do
      if latest_sequence > next_sequence do
        catch_up(context, conn, index_id, next_sequence, deadline)
      else
        :ok
      end
    end
  end

  defp catch_up_page(context, conn, index_id, sequence, deadline) do
    with {:ok, {rows, has_more}} <- Changes.fetch_page(conn, sequence, @rebuild_page_size),
         ids <- rows |> Enum.map(&change_document_id/1) |> Enum.filter(&is_binary/1) |> Enum.uniq(),
         {:ok, documents} <- current_documents(conn, ids),
         {:ok, _count} <- Search.rebuild_batch(context, index_id, documents, deadline) do
      next_sequence = rows |> List.last() |> change_sequence(sequence)

      if has_more do
        catch_up_page(context, conn, index_id, next_sequence, deadline)
      else
        {:ok, next_sequence}
      end
    end
  end

  defp change_document_id([_sequence, document_id | _rest]), do: document_id
  defp change_document_id(_), do: nil

  defp change_sequence([sequence | _rest], _fallback) when is_integer(sequence), do: sequence
  defp change_sequence(_, fallback), do: fallback

  defp current_documents(_conn, []), do: {:ok, []}

  defp current_documents(conn, ids) when is_list(ids) do
    placeholders = Enum.map_join(ids, ",", fn _ -> "?" end)

    with {:ok, rows} <-
           Connection.query(
             conn,
             "SELECT document_id, winning_revision, winning_deleted, winning_body_term FROM documents WHERE document_id IN (#{placeholders})",
             ids
           ) do
      decode_current_documents(rows)
    end
  end

  defp decode_current_documents(rows) do
    max_depth = VialKeeper.Config.host_limits()[:max_json_nesting_depth] || 100

    Enum.reduce_while(rows, {:ok, []}, fn
      [id, _revision, 1, _body_term], {:ok, documents} ->
        {:cont, {:ok, [%{id: id, body: %{}, deleted: true} | documents]}}

      [id, revision, 0, body_term], {:ok, documents} ->
        case TermBlob.decode_trusted_with_cache(
               body_term,
               :query_body_term,
               max_depth,
               @query_body_term_cache_limit
             ) do
          {:ok, body} -> {:cont, {:ok, [%{id: id, revision: revision, body: body} | documents]}}
          {:error, error} -> {:halt, {:error, error}}
        end

      _row, _acc ->
        {:halt, {:error, VialKeeper.Error.integrity_violation("winning document row is invalid")}}
    end)
    |> reverse_documents()
  end

  defp reverse_documents({:ok, documents}), do: {:ok, Enum.reverse(documents)}
  defp reverse_documents(error), do: error

  defp check_deadline(deadline) do
    if Deadline.exhausted?(deadline),
      do:
        {:error,
         VialKeeper.Error.resource_limit("full-text rebuild exceeded the configured timeout")},
      else: :ok
  end

  defp find_index(indexes, index_id) do
    case Enum.find(indexes, fn index ->
           MapAccess.get(index, :index_id) == index_id or MapAccess.get(index, :id) == index_id
         end) do
      nil -> {:error, VialKeeper.Error.index_not_found("index not found")}
      index -> {:ok, Map.merge(index, nested_metadata(index))}
    end
  end

  defp nested_metadata(metadata) when is_map(metadata) do
    case Map.fetch(metadata, "_metadata") do
      {:ok, nested} when is_map(nested) -> nested
      :error -> %{}
    end
  end

  defp full_text?(index) when is_map(index),
    do: MapAccess.get(index, :type) in ["full_text", :full_text]
end
