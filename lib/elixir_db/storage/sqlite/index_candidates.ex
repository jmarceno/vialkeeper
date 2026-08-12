defmodule ElixirDB.Storage.SQLite.IndexCandidates do
  @moduledoc """
  SQLite index definition and candidate-retrieval port.

  Shared-facing index definitions are logical only: catalog `_metadata`,
  physical names, and other adapter-private fields are stripped before
  `Services.Query` / `Planner` see them. Physical names remain available
  inside this module when compiling SQL from `Adapter.list_indexes/1`.

  Candidate rows are normalized maps. Document `doc_key` values may stay in
  opaque `:backend_meta`; shared query code only sees logical candidates.
  """
  @behaviour ElixirDB.Storage.Ports.IndexCandidates

  alias ElixirDB.JSON.StrictCache
  alias ElixirDB.MapAccess
  alias ElixirDB.Observability.Instrumentation.SQLite
  alias ElixirDB.Query.{Executor, Plan, Projection}
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.SQLite.Adapter

  alias ElixirDB.Storage.SQLite.{
    Connection,
    Context,
    Documents,
    IndexCatalog,
    Indexes,
    QueryCompiler,
    TermBlob
  }

  @candidate_query_cache_limit 16
  @query_body_term_cache_limit 256

  @impl true
  def list_indexes(%BackendContext{} = context) do
    SQLite.trace_sqlite_phase(:query_index_catalog, fn -> list_indexes_untraced(context) end)
  end

  defp list_indexes_untraced(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      case Adapter.list_indexes(adapter) do
        {:ok, indexes} -> {:ok, Enum.map(indexes, &public_index/1)}
        {:error, reason} -> {:error, Errors.normalize(reason)}
      end
    end
  end

  @impl true
  def create_index(%BackendContext{} = context, definition) when is_map(definition) do
    with {:ok, adapter} <- Context.unwrap(context) do
      case Adapter.create_index(adapter, definition) do
        {:ok, index} -> {:ok, public_index(index)}
        {:error, reason} -> {:error, Errors.normalize(reason)}
      end
    end
  end

  @impl true
  def delete_index(%BackendContext{} = context, index_id) when is_binary(index_id) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(Adapter.delete_index(adapter, index_id))
    end
  end

  @impl true
  def rebuild_index(%BackendContext{} = context, index_id) when is_binary(index_id) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(Adapter.rebuild_index(adapter, index_id))
    end
  end

  @impl true
  def winning_document_count(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, [[count]]} <-
           Connection.query(
             adapter.conn,
             "SELECT count(*) FROM documents WHERE winning_deleted = 0"
           ) do
      {:ok, count}
    else
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def lookup_candidates(%BackendContext{} = context, %{kind: :bounded_scan}) do
    SQLite.trace_sqlite_phase(:query_candidates, [plan_kind: :bounded_scan], fn ->
      with {:ok, adapter} <- Context.unwrap(context),
           {:ok, rows} <-
             Connection.query(
               adapter.conn,
               "SELECT document_id, winning_revision, winning_body_term FROM documents WHERE winning_deleted = 0"
             ),
           {:ok, documents} <- decode_query_documents(rows) do
        {:ok, Enum.map(documents, &public_candidate/1)}
      else
        {:error, reason} -> {:error, Errors.normalize(reason)}
      end
    end)
  end

  def lookup_candidates(%BackendContext{} = context, request) when is_map(request) do
    SQLite.trace_sqlite_phase(:query_candidates, candidate_attrs(request), fn ->
      full_text_candidates(context, request)
    end)
  end

  @impl true
  def full_text_candidates(%BackendContext{} = context, request) when is_map(request) do
    text = MapAccess.get(request, :text) || MapAccess.get(request, :query)
    mode = MapAccess.get(request, :mode, "all")
    deadline = MapAccess.get(request, :deadline)

    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, indexes} <- Adapter.list_indexes(adapter),
         {:ok, metadata} <- find_index(indexes, MapAccess.get(request, :index_id)),
         true <- is_binary(text) do
      case Indexes.search(adapter.conn, metadata, text, mode, deadline) do
        {:ok, rows} -> {:ok, Enum.map(rows, &public_candidate/1)}
        {:error, reason} -> {:error, Errors.normalize(reason)}
      end
    else
      false ->
        {:error, ElixirDB.Error.invalid_request("index candidate text is required")}

      {:error, reason} ->
        {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def range_scan_candidates(%BackendContext{} = context, request) when is_map(request) do
    SQLite.trace_sqlite_phase(:query_candidates, candidate_attrs(request), fn ->
      with {:ok, adapter} <- Context.unwrap(context),
           {:ok, indexes} <- Adapter.list_indexes(adapter),
           {:ok, selected} <- resolve_index(indexes, request),
           scan when is_map(scan) <- MapAccess.get(request, :scan) do
        structured_candidates(adapter, selected, scan, request)
      else
        nil ->
          {:error, ElixirDB.Error.invalid_request("range scan requires a scan descriptor")}

        {:error, reason} ->
          {:error, Errors.normalize(reason)}
      end
    end)
  end

  @impl true
  def ready_definitions(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(IndexCatalog.ready_definitions(adapter.conn))
    end
  end

  @impl true
  def refresh_document(%BackendContext{} = context, document_id, winner, ready)
      when is_binary(document_id) and is_map(winner) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, doc} <- Documents.find(adapter.conn, document_id),
         {:ok, doc} <- require_document(doc) do
      case ready do
        :load ->
          Errors.wrap(IndexCatalog.refresh_ready(adapter.conn, doc.doc_key, winner))

        rows when is_list(rows) ->
          Errors.wrap(IndexCatalog.refresh_ready(adapter.conn, doc.doc_key, winner, rows))
      end
    else
      :missing_document -> :ok
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  defp structured_candidates(adapter, selected, scan, request) do
    plan = MapAccess.get(request, :plan)
    query_request = MapAccess.get(request, :request) || %{}
    limit = MapAccess.get(request, :limit)
    deadline = MapAccess.get(request, :deadline)

    if pageable_structured_scan?(plan, query_request, limit) do
      structured_candidate_page(adapter, selected, scan, limit, deadline)
    else
      with {:ok, rows} <- structured_candidate_rows(adapter, selected, scan),
           {:ok, documents} <- decode_query_documents(rows) do
        {:ok, Enum.map(documents, &public_candidate/1)}
      end
    end
  end

  defp pageable_structured_scan?(%Plan{} = plan, request, limit)
       when is_integer(limit) and limit >= 0 do
    fully_pushed_query?(plan, request)
  end

  defp pageable_structured_scan?(_plan, _request, _limit), do: false

  defp fully_pushed_query?(plan, request) do
    is_nil(MapAccess.get(request, :search)) and
      MapAccess.get(request, :sort, []) == [] and
      is_nil(MapAccess.get(request, :after_id)) and
      is_nil(MapAccess.get(request, :after_ordering)) and
      Executor.no_post_filter?(plan, request)
  end

  defp structured_candidate_rows(adapter, selected, scan) do
    with {:ok, {where, params}} <- structured_candidate_query(selected, scan) do
      Connection.query(
        adapter.conn,
        "SELECT document_id, winning_revision, winning_body_term FROM documents WHERE #{where}",
        params
      )
    end
  end

  defp structured_candidate_page(adapter, selected, scan, limit, deadline) do
    with {:ok, {where, params}} <- structured_candidate_query(selected, scan),
         {:ok, rows} <-
           Connection.query(
             adapter.conn,
             "SELECT document_id, winning_revision, winning_body_term, (SELECT count(*) FROM documents AS candidate_count WHERE #{where}) FROM documents WHERE #{where} ORDER BY document_id LIMIT ?",
             params ++ params ++ [limit + 1]
           ),
         :ok <- maybe_check_deadline(deadline),
         {:ok, page_rows, examined} <- page_rows(rows),
         {:ok, documents} <- decode_query_documents(page_rows) do
      {:ok, %{candidates: Enum.map(documents, &public_candidate/1), examined: examined}}
    end
  end

  defp page_rows([]), do: {:ok, [], 0}

  defp page_rows(rows) do
    case List.last(rows) do
      [_id, _revision, _body_term, examined] when is_integer(examined) ->
        {:ok, Enum.map(rows, fn [id, revision, body_term, _] -> [id, revision, body_term] end),
         examined}

      _ ->
        {:error, ElixirDB.Error.integrity_violation("indexed query count is invalid")}
    end
  end

  defp structured_candidate_query(selected, scan) do
    cache_key = {selected["index_id"], selected["definition_digest"], selected["fields"], scan}

    StrictCache.memoize(
      :query_candidate_sql,
      cache_key,
      @candidate_query_cache_limit,
      fn -> structured_candidate_query_for_index(selected, scan) end
    )
  end

  defp structured_candidate_query_for_index(selected, scan) do
    fields = selected["fields"] || []

    with {:ok, conditions} <- QueryCompiler.compile_scan(scan, fields) do
      where = ["winning_deleted = 0" | Enum.map(conditions, &elem(&1, 0))] |> Enum.join(" AND ")
      params = Enum.flat_map(conditions, fn {_sql, value} -> List.wrap(value) end)

      {:ok, {where, params}}
    end
  end

  defp decode_query_documents(rows) do
    max_depth = ElixirDB.Config.host_limits()[:max_json_nesting_depth] || 100
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
        decode_query_documents(
          rows,
          [Projection.document(id, revision, body) | acc],
          max_depth
        )

      {:error, error} ->
        {:error, error}
    end
  end

  defp resolve_index(indexes, request) do
    case MapAccess.get(request, :index) do
      index when is_map(index) -> {:ok, index}
      _ -> find_index(indexes, MapAccess.get(request, :index_id))
    end
  end

  defp find_index(indexes, index_id) when is_binary(index_id) do
    case Enum.find(indexes, &index_id?(&1, index_id)) do
      nil ->
        {:error, ElixirDB.Error.index_not_found("index not found")}

      metadata ->
        {:ok, Map.merge(metadata, nested_metadata(metadata))}
    end
  end

  defp find_index(_, _), do: {:error, ElixirDB.Error.invalid_request("index_id is required")}

  defp candidate_attrs(request) when is_map(request) do
    plan = MapAccess.get(request, :plan)

    plan_kind =
      case plan do
        %Plan{kind: kind} -> kind
        _ -> MapAccess.get(request, :kind) || MapAccess.get(request, :plan_kind) || :single
      end

    selected =
      MapAccess.get(request, :selected_indexes) ||
        case plan do
          %Plan{selected_indexes: bindings} when is_list(bindings) -> bindings
          _ -> []
        end

    [
      plan_kind: plan_kind,
      selected_index_count: length(selected)
    ]
  end

  defp nested_metadata(metadata) when is_map(metadata) do
    case Map.fetch(metadata, "_metadata") do
      {:ok, nested} when is_map(nested) -> nested
      :error -> %{}
    end
  end

  defp index_id?(index, index_id) do
    MapAccess.get(index, :index_id) == index_id or MapAccess.get(index, :id) == index_id
  end

  defp public_index(index) when is_map(index) do
    Map.drop(index, [
      :physical_name,
      "physical_name",
      :_metadata,
      "_metadata",
      :backend_meta,
      "backend_meta"
    ])
  end

  defp public_candidate(row) when is_map(row) do
    doc_key = Map.get(row, :doc_key)

    row
    |> Map.drop([:doc_key])
    |> Map.put(:backend_meta, %{doc_key: doc_key})
  end

  defp require_document(nil), do: :missing_document
  defp require_document(doc), do: {:ok, doc}

  defp maybe_check_deadline(nil), do: :ok
  defp maybe_check_deadline(deadline), do: Executor.check_deadline(deadline)
end
