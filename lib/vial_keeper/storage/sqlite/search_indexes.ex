defmodule VialKeeper.Storage.SQLite.SearchIndexes do
  @moduledoc """
  Rebuilds Elixir full-text posting lists from winning SQLite documents.

  Logical index rows stay in the SQLite catalog. Token posting lists live in
  `VialKeeper.Search` and are reconstructed from winners when missing.
  """

  alias VialKeeper.MapAccess
  alias VialKeeper.Query.Projection
  alias VialKeeper.Search
  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.SQLite.{Connection, Context, IndexCatalog, TermBlob}

  @query_body_term_cache_limit 256

  @spec rebuild(BackendContext.t(), map(), map()) :: :ok | {:error, VialKeeper.Error.t()}
  def rebuild(%BackendContext{} = context, index, definition)
      when is_map(index) and is_map(definition) do
    if full_text?(index) or full_text?(definition) do
      with {:ok, adapter} <- Context.unwrap(context),
           {:ok, documents} <- winning_documents(adapter.conn) do
        index_id = MapAccess.get(index, :index_id)

        definition =
          definition
          |> Map.merge(nested_metadata(index))
          |> Map.put("index_id", index_id)

        Search.rebuild(context, index_id, definition, documents)
      end
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
