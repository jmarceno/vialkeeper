defmodule ElixirDB.Storage.SQLite.Views do
  @moduledoc """
  SQLite physical persistence for view definitions, state, and rows.

  Catalog rows, generation metadata, row upserts, and range scans live here.
  Product orchestration — definition CAS, rebuild transitions, query planning,
  bookmarks, and result shaping — lives in `ElixirDB.Storage.Services.Views`.
  """

  alias ElixirDB.JSON.{Canonical, StrictDecoder}
  alias ElixirDB.MapAccess
  alias ElixirDB.Storage.SQLite.{Connection, TermBlob}
  alias ElixirDB.UUID
  alias ElixirDB.View.KeyCodec

  @list_cache_key :elixir_db_sqlite_view_catalog

  @spec list(Connection.handle()) :: {:ok, [map()]} | {:error, ElixirDB.Error.t()}
  def list(conn) do
    case Process.get({@list_cache_key, conn}) do
      {:ok, _views} = cached ->
        cached

      nil ->
        result =
          case Connection.query(
                 conn,
                 """
                 SELECT d.view_id, d.name, d.definition_json, d.definition_digest, d.created_at,
                        s.active_generation, s.building_generation, s.indexed_through, s.status, s.last_error_code
                 FROM view_definitions AS d
                 JOIN view_state AS s ON s.view_id = d.view_id
                 ORDER BY d.name
                 """
               ) do
            {:ok, rows} ->
              {:ok, Enum.map(rows, &view_list_entry/1)}

            {:error, reason} ->
              {:error, normalize_error(reason)}
          end

        if match?({:ok, _}, result), do: Process.put({@list_cache_key, conn}, result)
        result
    end
  end

  @spec clear_cache(Connection.handle()) :: :ok
  def clear_cache(conn) do
    Process.delete({@list_cache_key, conn})
    :ok
  end

  @spec find_by_name(Connection.handle(), binary()) ::
          {:ok, map() | nil} | {:error, ElixirDB.Error.t()}
  def find_by_name(conn, name) when is_binary(name) do
    case Connection.query(
           conn,
           "SELECT view_id, definition_digest FROM view_definitions WHERE name = ?",
           [name]
         ) do
      {:ok, []} -> {:ok, nil}
      {:ok, [[view_id, digest]]} -> {:ok, %{view_id: view_id, definition_digest: digest}}
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @spec count(Connection.handle()) :: {:ok, non_neg_integer()} | {:error, ElixirDB.Error.t()}
  def count(conn) do
    case Connection.query(conn, "SELECT count(*) FROM view_definitions") do
      {:ok, [[count]]} -> {:ok, count}
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @spec get_definition(Connection.handle(), binary()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def get_definition(conn, view_id) when is_binary(view_id) do
    case Connection.query(
           conn,
           "SELECT view_id, name, definition_json, definition_digest, created_at FROM view_definitions WHERE view_id = ?",
           [view_id]
         ) do
      {:ok, [row]} ->
        {:ok, definition_row(row)}

      {:ok, []} ->
        {:error, ElixirDB.Error.view_not_found("view was not found", %{view_id: view_id})}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  @spec get_state(Connection.handle(), binary()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def get_state(conn, view_id) when is_binary(view_id) do
    with {:ok, row} <- fetch_state_row(conn, view_id) do
      {:ok, state_result(row)}
    end
  end

  @spec insert_definition(Connection.handle(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def insert_definition(conn, normalized) when is_map(normalized) do
    clear_cache(conn)

    view_id = UUID.v4()
    created_at = DateTime.utc_now() |> DateTime.to_iso8601()

    with :ok <-
           Connection.execute(
             conn,
             "INSERT INTO view_definitions(view_id, name, definition_json, definition_digest, created_at) VALUES (?, ?, ?, ?, ?)",
             [
               view_id,
               normalized.name,
               normalized.definition_json,
               normalized.definition_digest,
               created_at
             ]
           ),
         :ok <-
           Connection.execute(
             conn,
             "INSERT INTO view_state(view_id, active_generation, building_generation, indexed_through, status, last_error_code) VALUES (?, 1, NULL, 0, 'building', NULL)",
             [view_id]
           ) do
      {:ok,
       %{
         "view_id" => view_id,
         "name" => normalized.name,
         "definition_digest" => normalized.definition_digest,
         "definition_json" => normalized.definition_json,
         "created_at" => created_at,
         "status" => "building",
         "indexed_through" => 0,
         "active_generation" => 1
       }}
    end
  end

  @spec delete(Connection.handle(), binary()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def delete(conn, view_id) when is_binary(view_id) do
    clear_cache(conn)

    with :ok <- Connection.execute(conn, "DELETE FROM view_rows WHERE view_id = ?", [view_id]),
         :ok <- Connection.execute(conn, "DELETE FROM view_state WHERE view_id = ?", [view_id]),
         :ok <-
           Connection.execute(conn, "DELETE FROM view_definitions WHERE view_id = ?", [view_id]) do
      {:ok, %{view_id: view_id, deleted: true}}
    end
  end

  @spec upsert_rows(Connection.handle(), binary(), integer(), [map()]) ::
          :ok | {:error, ElixirDB.Error.t()}
  def upsert_rows(conn, view_id, generation, rows) when is_binary(view_id) and is_list(rows) do
    Enum.reduce_while(rows, :ok, fn row, :ok ->
      with {:ok, encoded} <- encode_row(view_id, generation, row),
           :ok <-
             Connection.execute(
               conn,
               """
               INSERT INTO view_rows(view_id, generation, document_id, revision_id, key_json, key_sort, value_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(view_id, generation, document_id) DO UPDATE SET
                 revision_id = excluded.revision_id,
                 key_json = excluded.key_json,
                 key_sort = excluded.key_sort,
                 value_json = excluded.value_json
               """,
               encoded
             ) do
        {:cont, :ok}
      else
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  @spec delete_removals(Connection.handle(), binary(), integer(), [binary()]) ::
          :ok | {:error, ElixirDB.Error.t()}
  def delete_removals(conn, view_id, generation, removals)
      when is_binary(view_id) and is_list(removals) do
    Enum.reduce_while(removals, :ok, fn document_id, :ok ->
      case Connection.execute(
             conn,
             "DELETE FROM view_rows WHERE view_id = ? AND generation = ? AND document_id = ?",
             [view_id, generation, document_id]
           ) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, normalize_error(reason)}}
      end
    end)
  end

  @spec put_indexed_through(Connection.handle(), binary(), integer()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def put_indexed_through(conn, view_id, through)
      when is_binary(view_id) and is_integer(through) do
    clear_cache(conn)

    Connection.execute(
      conn,
      "UPDATE view_state SET indexed_through = ? WHERE view_id = ?",
      [through, view_id]
    )
  end

  @spec clear_generation_rows(Connection.handle(), binary(), integer()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def clear_generation_rows(conn, view_id, generation)
      when is_binary(view_id) and is_integer(generation) do
    Connection.execute(conn, "DELETE FROM view_rows WHERE view_id = ? AND generation = ?", [
      view_id,
      generation
    ])
  end

  @spec begin_rebuild_effect(Connection.handle(), binary(), integer()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def begin_rebuild_effect(conn, view_id, building_generation)
      when is_binary(view_id) and is_integer(building_generation) do
    clear_cache(conn)

    Connection.execute(
      conn,
      "UPDATE view_state SET building_generation = ?, status = 'building', last_error_code = NULL WHERE view_id = ?",
      [building_generation, view_id]
    )
  end

  @spec finish_rebuild_effect(Connection.handle(), binary(), integer(), integer()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def finish_rebuild_effect(conn, view_id, generation, indexed_through)
      when is_binary(view_id) and is_integer(generation) and is_integer(indexed_through) do
    clear_cache(conn)

    Connection.execute(
      conn,
      """
      UPDATE view_state
      SET active_generation = ?, building_generation = NULL, indexed_through = ?, status = 'ready', last_error_code = NULL
      WHERE view_id = ?
      """,
      [generation, indexed_through, view_id]
    )
  end

  @spec scan_rows(Connection.handle(), binary(), integer(), map(), non_neg_integer() | nil) ::
          {:ok, [map()]} | {:error, ElixirDB.Error.t()}
  def scan_rows(conn, view_id, generation, plan, fetch_limit)
      when is_binary(view_id) and is_integer(generation) and is_map(plan) do
    {sql, params} = query_sql(view_id, generation, plan, fetch_limit)

    case Connection.query(conn, sql, params) do
      {:ok, rows} ->
        {:ok, Enum.map(rows, &decode_row/1)}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  @spec read_winning_documents_page(Connection.handle(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def read_winning_documents_page(conn, request) when is_map(request) do
    after_id = Map.get(request, "after_document_id")
    limit = page_limit(request)

    with {:ok, rows} <- fetch_winning_document_rows(conn, after_id, limit),
         {page_rows, extra_rows} <- Enum.split(rows, limit),
         {:ok, documents} <- decode_winning_document_rows(page_rows) do
      next_after = next_document_id(extra_rows)
      {:ok, %{documents: documents, next_after: next_after}}
    end
  end

  defp fetch_winning_document_rows(conn, after_id, limit) do
    {filter, params} =
      case after_id do
        nil ->
          {"winning_deleted = 0", []}

        id when is_binary(id) ->
          {"winning_deleted = 0 AND document_id > ?", [id]}
      end

    case Connection.query(
           conn,
           "SELECT document_id, winning_revision, winning_body_json, winning_body_term, update_sequence FROM documents WHERE #{filter} ORDER BY document_id LIMIT ?",
           params ++ [limit + 1]
         ) do
      {:ok, rows} -> {:ok, rows}
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp next_document_id([[document_id | _] | _]), do: document_id
  defp next_document_id([]), do: nil

  defp decode_winning_document_rows(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn row, {:ok, acc} ->
      decode_winning_document_row(row, acc)
    end)
    |> then(fn
      {:ok, documents} -> {:ok, Enum.reverse(documents)}
      error -> error
    end)
  end

  defp decode_winning_document_row(
         [id, revision_id, body_json, body_term, sequence],
         acc
       ) do
    case decode_body(body_json, body_term) do
      {:ok, body} ->
        {:cont, {:ok, [winning_document(id, revision_id, body, sequence) | acc]}}

      {:error, _} = error ->
        {:halt, error}
    end
  end

  defp winning_document(id, revision_id, body, sequence) do
    %{
      "document_id" => id,
      "revision_id" => revision_id,
      "body" => body,
      "sequence" => sequence
    }
  end

  defp decode_body(body_json, body_term) do
    case TermBlob.decode(body_term, body_json) do
      {:ok, body} ->
        {:ok, body}

      {:fallback, reason} ->
        {:error, ElixirDB.Error.internal_error("term blob decode failed", %{reason: reason})}
    end
  end

  defp page_limit(request) do
    options = Map.get(request, "options", %{})
    override = Map.get(options, "page_size")
    positive_integer(override || Map.get(request, "limit"), 100)
  end

  defp positive_integer(value, _default) when is_integer(value) and value > 0, do: value
  defp positive_integer(_value, default), do: default

  defp query_sql(view_id, generation, plan, fetch_limit) do
    filters = ["view_id = ?", "generation = ?"]
    params = [view_id, generation]

    {filters, params} =
      case plan.start_sort do
        nil ->
          {filters, params}

        start_sort ->
          if plan.bookmark_after do
            {filters ++ ["(key_sort > ? OR (key_sort = ? AND document_id > ?))"],
             params ++
               [
                 TermBlob.bind(start_sort),
                 TermBlob.bind(start_sort),
                 plan.bookmark_after
               ]}
          else
            {filters ++ ["key_sort >= ?"], params ++ [TermBlob.bind(start_sort)]}
          end
      end

    {filters, params} =
      case plan.end_sort do
        nil ->
          {filters, params}

        end_sort ->
          op = if plan.inclusive_end, do: "<=", else: "<"
          {filters ++ [["key_sort ", op, " ?"]], params ++ [TermBlob.bind(end_sort)]}
      end

    sql =
      IO.iodata_to_binary([
        "SELECT document_id, revision_id, key_json, key_sort, value_json FROM view_rows WHERE ",
        Enum.intersperse(filters, " AND "),
        " ORDER BY key_sort, document_id",
        limit_clause(fetch_limit)
      ])

    params = if fetch_limit, do: params ++ [fetch_limit], else: params
    {sql, params}
  end

  defp limit_clause(nil), do: ""
  defp limit_clause(_fetch_limit), do: " LIMIT ?"

  defp decode_row([document_id, revision_id, key_json, key_sort, value_json]) do
    %{
      id: document_id,
      revision_id: revision_id,
      key: StrictDecoder.decode_or_nil(key_json),
      key_sort: key_sort,
      value: decode_optional_json(value_json)
    }
  end

  defp decode_optional_json(nil), do: nil

  defp decode_optional_json(json) when is_binary(json) do
    StrictDecoder.decode_or_nil(json)
  end

  defp encode_row(view_id, generation, row) do
    document_id = required_string(row, "document_id")
    revision_id = required_string(row, "revision_id")
    key = Map.fetch!(row, "key")

    with {:ok, key_json} <- Canonical.encode(key),
         {:ok, key_sort} <- KeyCodec.encode(key),
         {:ok, value_json} <- encode_optional_value(Map.get(row, "value")) do
      {:ok,
       [
         view_id,
         generation,
         document_id,
         revision_id,
         key_json,
         TermBlob.bind(key_sort),
         value_json
       ]}
    end
  end

  defp encode_optional_value(nil), do: {:ok, nil}

  defp encode_optional_value(value) do
    case Canonical.encode(value) do
      {:ok, encoded} -> {:ok, encoded}
      {:error, _} = error -> error
    end
  end

  defp fetch_state_row(conn, view_id) do
    case Connection.query(
           conn,
           "SELECT view_id, active_generation, building_generation, indexed_through, status, last_error_code FROM view_state WHERE view_id = ?",
           [view_id]
         ) do
      {:ok, [row]} ->
        {:ok, state_row(row)}

      {:ok, []} ->
        {:error, ElixirDB.Error.view_not_found("view was not found", %{view_id: view_id})}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp view_list_entry([
         view_id,
         name,
         definition_json,
         definition_digest,
         created_at,
         active_generation,
         building_generation,
         indexed_through,
         status,
         last_error_code
       ]) do
    definition = StrictDecoder.decode_or_nil(definition_json)

    %{
      "view_id" => view_id,
      "name" => name,
      "definition" => definition,
      "definition_digest" => definition_digest,
      "created_at" => created_at,
      "active_generation" => active_generation,
      "building_generation" => building_generation,
      "indexed_through" => indexed_through,
      "status" => status,
      "last_error_code" => last_error_code,
      "reducer" => get_in(definition, ["reducer"])
    }
  end

  defp definition_row([view_id, name, definition_json, definition_digest, created_at]) do
    %{
      view_id: view_id,
      name: name,
      definition_json: definition_json,
      definition_digest: definition_digest,
      created_at: created_at
    }
  end

  defp state_row([
         view_id,
         active_generation,
         building_generation,
         indexed_through,
         status,
         last_error_code
       ]) do
    %{
      view_id: view_id,
      active_generation: active_generation,
      building_generation: building_generation,
      indexed_through: indexed_through,
      status: status,
      last_error_code: last_error_code
    }
  end

  defp state_result(row) do
    state_row([
      row.view_id,
      row.active_generation,
      row.building_generation,
      row.indexed_through,
      row.status,
      row.last_error_code
    ])
  end

  defp required_string(map, key) when is_binary(key) do
    value = map_get(map, key)

    if is_binary(value) and value != "",
      do: value,
      else: raise(ArgumentError, "missing #{key}")
  end

  defp map_get(map, "document_id"), do: MapAccess.get(map, :document_id)
  defp map_get(map, "revision_id"), do: MapAccess.get(map, :revision_id)
  defp map_get(map, key) when is_binary(key), do: Map.get(map, key)

  defp normalize_error(reason),
    do: ElixirDB.Error.internal_error("SQLite operation failed", %{cause: inspect(reason)})
end
