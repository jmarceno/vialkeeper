defmodule ElixirDB.Storage.SQLite.Views do
  @moduledoc """
  SQLite view definitions, state, rows, and query operations for Version 1.
  """

  alias ElixirDB.JSON.{Canonical, StrictDecoder}
  alias ElixirDB.MapAccess
  alias ElixirDB.Storage.SQLite.{Connection, TermBlob}
  alias ElixirDB.UUID
  alias ElixirDB.View.{BookmarkCodec, Definition, KeyCodec, Reducer}

  @list_cache_key :elixir_db_sqlite_view_catalog
  @default_query_limit 100
  @default_query_max_limit 500
  @query_fields ~w(view_id consistency key start_key end_key inclusive_end group_level limit bookmark)

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

  @doc false
  @spec clear_cache(Connection.handle()) :: :ok
  def clear_cache(conn) do
    Process.delete({@list_cache_key, conn})
    :ok
  end

  @spec create_tx(Connection.handle(), map(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def create_tx(conn, definition, config) do
    clear_cache(conn)

    with {:ok, normalized} <- Definition.normalize(definition),
         :ok <- enforce_definition_limit(conn, config),
         {:ok, existing} <- find_by_name(conn, normalized.name),
         :ok <- ensure_no_conflict(existing, normalized),
         {:ok, result} <- insert_definition(conn, normalized) do
      {:ok, result}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @spec delete_tx(Connection.handle(), binary()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def delete_tx(conn, view_id) do
    clear_cache(conn)

    with {:ok, _row} <- fetch_definition(conn, view_id),
         :ok <- Connection.execute(conn, "DELETE FROM view_rows WHERE view_id = ?", [view_id]),
         :ok <- Connection.execute(conn, "DELETE FROM view_state WHERE view_id = ?", [view_id]),
         :ok <-
           Connection.execute(conn, "DELETE FROM view_definitions WHERE view_id = ?", [view_id]) do
      {:ok, %{view_id: view_id, deleted: true}}
    end
  end

  @spec state(Connection.handle(), binary()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def state(conn, view_id) do
    with {:ok, row} <- fetch_state(conn, view_id) do
      {:ok, state_result(row)}
    end
  end

  @spec apply_batch_tx(Connection.handle(), map(), keyword()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def apply_batch_tx(conn, request, opts \\ []) do
    clear_cache(conn)
    view_id = required_string(request, "view_id")
    expected = required_integer(request, "expected_indexed_through")
    through = required_integer(request, "through_sequence")
    rows = Map.get(request, "rows", [])
    removals = Map.get(request, "removals", [])

    with {:ok, state} <- fetch_state(conn, view_id),
         {:ok, mode} <- validate_batch_cas(state, expected, through),
         generation <- state.active_generation,
         :ok <- maybe_apply_batch(conn, mode, view_id, generation, rows, removals, through, opts) do
      {:ok, %{view_id: view_id, indexed_through: through, applied: mode == :apply}}
    end
  end

  defp maybe_apply_batch(
         _conn,
         :idempotent,
         _view_id,
         _generation,
         _rows,
         _removals,
         _through,
         _opts
       ),
       do: :ok

  defp maybe_apply_batch(conn, :apply, view_id, generation, rows, removals, through, opts) do
    with :ok <- delete_removals(conn, view_id, generation, removals),
         :ok <- upsert_rows(conn, view_id, generation, rows, opts) do
      Connection.execute(
        conn,
        "UPDATE view_state SET indexed_through = ? WHERE view_id = ?",
        [through, view_id]
      )
    end
  end

  @spec begin_rebuild_tx(Connection.handle(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def begin_rebuild_tx(conn, request) do
    clear_cache(conn)
    view_id = required_string(request, "view_id")
    start_sequence = required_integer(request, "start_sequence")

    with {:ok, state} <- fetch_state(conn, view_id),
         building_generation = state.active_generation + 1,
         :ok <-
           Connection.execute(conn, "DELETE FROM view_rows WHERE view_id = ? AND generation = ?", [
             view_id,
             building_generation
           ]),
         :ok <-
           Connection.execute(
             conn,
             "UPDATE view_state SET building_generation = ?, status = 'building', last_error_code = NULL WHERE view_id = ?",
             [building_generation, view_id]
           ) do
      {:ok,
       %{
         view_id: view_id,
         building_generation: building_generation,
         start_sequence: start_sequence,
         active_generation: state.active_generation
       }}
    end
  end

  @spec append_rebuild_page_tx(Connection.handle(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def append_rebuild_page_tx(conn, request) do
    view_id = required_string(request, "view_id")
    generation = required_integer(request, "generation")
    rows = Map.get(request, "rows", [])

    removals = Map.get(request, "removals", [])

    with {:ok, state} <- fetch_state(conn, view_id),
         :ok <- ensure_building_generation(state, generation),
         :ok <- delete_removals(conn, view_id, generation, removals),
         :ok <- upsert_rows(conn, view_id, generation, rows) do
      {:ok, %{view_id: view_id, generation: generation, appended: length(rows)}}
    end
  end

  @spec finish_rebuild_tx(Connection.handle(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def finish_rebuild_tx(conn, request) do
    clear_cache(conn)
    view_id = required_string(request, "view_id")
    generation = required_integer(request, "generation")
    indexed_through = required_integer(request, "indexed_through")

    with {:ok, state} <- fetch_state(conn, view_id),
         :ok <- ensure_building_generation(state, generation),
         :ok <-
           Connection.execute(
             conn,
             """
             UPDATE view_state
             SET active_generation = ?, building_generation = NULL, indexed_through = ?, status = 'ready', last_error_code = NULL
             WHERE view_id = ?
             """,
             [generation, indexed_through, view_id]
           ) do
      {:ok, %{view_id: view_id, active_generation: generation, indexed_through: indexed_through}}
    end
  end

  @spec query_tx(Connection.handle(), map(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def query_tx(conn, request, config \\ %{})

  def query_tx(conn, request, config) when is_map(request) and is_map(config) do
    with :ok <- validate_query_fields(request),
         {:ok, view_id} <- query_view_id(request),
         {:ok, limit} <- normalize_query_limit(request, config),
         :ok <- validate_query_options(request),
         {:ok, definition_row} <- fetch_definition(conn, view_id),
         {:ok, state} <- fetch_state(conn, view_id),
         {:ok, definition} <- decode_definition(definition_row),
         {:ok, query_plan} <- build_query_plan(request, definition, state),
         {:ok, {rows, fetch_has_more}} <-
           fetch_query_rows(conn, view_id, state.active_generation, query_plan, limit, definition) do
      format_query_result(rows, fetch_has_more, definition, state, query_plan, limit)
    end
  end

  def query_tx(_conn, _request, _config),
    do: {:error, ElixirDB.Error.invalid_request("view query must be an object")}

  defp query_view_id(request) do
    case fetch_query_option(request, :view_id) do
      {:ok, view_id} when is_binary(view_id) and view_id != "" -> {:ok, view_id}
      _ -> {:error, ElixirDB.Error.invalid_request("view query requires a view_id")}
    end
  end

  @spec read_winning_documents_page(Connection.handle(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def read_winning_documents_page(conn, request) do
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

  defp build_query_plan(request, definition, state) do
    key_length = length(definition.key)
    group_level = MapAccess.get(request, :group_level)
    bookmark = MapAccess.get(request, :bookmark)
    group_level = normalize_group_level(group_level, key_length, definition.reducer)
    exact_key = MapAccess.get(request, :key)
    start_key = exact_key || MapAccess.get(request, :start_key)
    end_key = exact_key || MapAccess.get(request, :end_key)
    inclusive_end = MapAccess.get(request, :inclusive_end, true) == true

    with {:ok, start_sort} <- encode_bound(start_key),
         {:ok, end_sort} <- encode_bound(end_key),
         {:ok, bookmark_plan} <- decode_bookmark(bookmark, definition, state) do
      {:ok,
       %{
         reducer: definition.reducer,
         key_length: key_length,
         group_level: group_level,
         start_sort: bookmark_plan[:start_sort] || start_sort,
         end_sort: end_sort,
         inclusive_end: inclusive_end,
         bookmark: bookmark,
         bookmark_after: bookmark_plan[:after]
       }}
    end
  end

  defp validate_query_fields(request) do
    allowed = @query_fields ++ Enum.map(@query_fields, &String.to_atom/1)

    if Enum.all?(Map.keys(request), &(&1 in allowed)),
      do: :ok,
      else: {:error, ElixirDB.Error.invalid_request("view query contains an unknown field")}
  end

  defp normalize_query_limit(request, config) do
    maximum = get_in(config, ["queries", "max_limit"]) || @default_query_max_limit

    case fetch_query_option(request, :limit) do
      :missing ->
        {:ok, @default_query_limit}

      {:ok, value} when is_integer(value) and value > 0 and value <= maximum ->
        {:ok, value}

      {:ok, value} when is_integer(value) and value > maximum ->
        {:error, ElixirDB.Error.resource_limit("view query limit exceeds the configured maximum")}

      {:ok, _value} ->
        {:error, ElixirDB.Error.invalid_request("view query limit must be a positive integer")}
    end
  end

  defp validate_query_options(request) do
    with :ok <-
           validate_query_option(
             request,
             :consistency,
             &(&1 in ["stale_ok", "update_after", "consistent"]),
             "view consistency is invalid"
           ),
         :ok <-
           validate_query_option(
             request,
             :key,
             &is_list/1,
             "view query key must be an array"
           ),
         :ok <- validate_exact_key(request),
         :ok <-
           validate_query_option(
             request,
             :start_key,
             &(is_list(&1) or is_nil(&1)),
             "view query start_key must be an array"
           ),
         :ok <-
           validate_query_option(
             request,
             :end_key,
             &(is_list(&1) or is_nil(&1)),
             "view query end_key must be an array"
           ),
         :ok <-
           validate_query_option(
             request,
             :inclusive_end,
             &is_boolean/1,
             "view query inclusive_end must be a boolean"
           ),
         :ok <-
           validate_query_option(
             request,
             :group_level,
             &(is_integer(&1) and &1 >= 0),
             "view query group_level must be a non-negative integer"
           ) do
      validate_query_option(
        request,
        :bookmark,
        &(is_binary(&1) or is_nil(&1)),
        "view query bookmark must be a string"
      )
    end
  end

  defp validate_exact_key(request) do
    case fetch_query_option(request, :key) do
      :missing ->
        :ok

      {:ok, _key} ->
        if query_option_present?(request, :start_key) or
             query_option_present?(request, :end_key) or
             fetch_query_option(request, :inclusive_end) == {:ok, false} do
          {:error,
           ElixirDB.Error.invalid_request("view query key cannot be combined with range options")}
        else
          :ok
        end
    end
  end

  defp query_option_present?(request, key) do
    Map.has_key?(request, key) or Map.has_key?(request, Atom.to_string(key))
  end

  defp validate_query_option(request, key, predicate, message) do
    case fetch_query_option(request, key) do
      :missing ->
        :ok

      {:ok, value} ->
        if predicate.(value), do: :ok, else: {:error, ElixirDB.Error.invalid_request(message)}
    end
  end

  defp fetch_query_option(request, key) do
    case Map.fetch(request, key) do
      {:ok, value} ->
        {:ok, value}

      :error ->
        case Map.fetch(request, Atom.to_string(key)) do
          {:ok, value} -> {:ok, value}
          :error -> :missing
        end
    end
  end

  defp normalize_group_level(nil, key_length, nil), do: key_length
  defp normalize_group_level(level, _key_length, nil) when is_integer(level), do: level

  defp normalize_group_level(level, _key_length, _reducer) when is_integer(level), do: level

  defp normalize_group_level(_level, key_length, _reducer), do: key_length

  defp decode_bookmark(nil, _definition, _state), do: {:ok, %{}}

  defp decode_bookmark(bookmark, definition, state) do
    expected = %{
      "definition_digest" => definition.definition_digest,
      "indexed_through" => state.indexed_through
    }

    with {:ok, decoded} <- BookmarkCodec.decode(bookmark, expected),
         {:ok, key_sort} <- decode_key_sort(decoded["key_sort"]) do
      {:ok, %{start_sort: key_sort, after: decoded["document_id"]}}
    else
      {:error, %ElixirDB.Error{code: :invalid_bookmark}} ->
        {:error, ElixirDB.Error.bookmark_stale("view bookmark is stale")}

      {:error, _} = error ->
        error
    end
  end

  defp encode_bound(nil), do: {:ok, nil}

  defp encode_bound(key) when is_list(key) do
    case KeyCodec.encode(key) do
      {:ok, encoded} -> {:ok, encoded}
      {:error, _} = error -> error
    end
  end

  defp encode_bound(_),
    do: {:error, ElixirDB.Error.invalid_request("view query key bound is invalid")}

  defp fetch_query_rows(conn, view_id, generation, plan, limit, %{reducer: reducer}) do
    if reducer do
      {sql, params} = query_sql(view_id, generation, plan, nil)

      case Connection.query(conn, sql, params) do
        {:ok, rows} ->
          {:ok, {Enum.map(rows, &decode_row/1), nil}}

        {:error, reason} ->
          {:error, normalize_error(reason)}
      end
    else
      fetch_limit = limit + 1
      {sql, params} = query_sql(view_id, generation, plan, fetch_limit)

      case Connection.query(conn, sql, params) do
        {:ok, rows} ->
          decoded = Enum.map(rows, &decode_row/1)
          {:ok, {Enum.take(decoded, limit), length(decoded) > limit}}

        {:error, reason} ->
          {:error, normalize_error(reason)}
      end
    end
  end

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

  defp decode_key_sort(encoded) when is_binary(encoded) do
    case Base.decode64(encoded, padding: false) do
      {:ok, key_sort} -> {:ok, key_sort}
      :error -> {:error, ElixirDB.Error.invalid_bookmark("view bookmark key_sort is invalid")}
    end
  end

  defp format_query_result(rows, fetch_has_more, definition, state, plan, limit) do
    mapped_rows =
      Enum.map(rows, fn row ->
        %{key: row.key, value: row.value, id: row.id, key_sort: row.key_sort}
      end)

    with {:ok, results} <- grouped_results(mapped_rows, definition, plan) do
      {page, group_has_more?} = paginate_results(results, definition.reducer, limit)
      has_more? = group_has_more? || fetch_has_more == true

      {:ok,
       %{
         results: Enum.map(page, &public_result/1),
         bookmark: query_bookmark(page, has_more?, definition, state),
         indexed_through: state.indexed_through,
         definition_digest: definition.definition_digest
       }}
    end
  end

  defp paginate_results(results, reducer, limit) when not is_nil(reducer) do
    page = Enum.take(results, limit)
    {page, length(results) > limit}
  end

  defp paginate_results(results, nil, _limit), do: {results, false}

  defp query_bookmark(_page, false, _definition, _state), do: nil

  defp query_bookmark(page, true, definition, state) do
    encode_query_bookmark(List.last(page), definition, state)
  end

  defp encode_query_bookmark(nil, _definition, _state), do: nil

  defp encode_query_bookmark(last, definition, state) do
    document_id = Map.get(last, :id) || last_document_id(last)

    case BookmarkCodec.encode(%{
           "definition_digest" => definition.definition_digest,
           "indexed_through" => state.indexed_through,
           "key_sort" => Base.encode64(last.key_sort, padding: false),
           "document_id" => document_id || ""
         }) do
      {:ok, encoded} -> encoded
      _ -> nil
    end
  end

  defp last_document_id(%{ids: ids}) when is_list(ids), do: List.last(ids)
  defp last_document_id(_), do: nil

  defp grouped_results(mapped_rows, %{reducer: nil}, _plan),
    do: Reducer.reduce_rows(mapped_rows, nil)

  defp grouped_results(mapped_rows, %{reducer: reducer}, plan),
    do: Reducer.reduce_grouped(mapped_rows, reducer, plan.group_level, plan.key_length)

  defp public_result(%{key: key, value: value} = row) do
    base = %{"key" => key, "value" => value}
    if Map.has_key?(row, :id) and row.id, do: Map.put(base, "id", row.id), else: base
  end

  defp upsert_rows(conn, view_id, generation, rows, opts \\ []) when is_list(rows) do
    view_fault = Keyword.get(opts, :view_fault)

    Enum.reduce_while(rows, :ok, fn row, :ok ->
      with :ok <- view_fault_check(view_fault, :view_upsert_row),
           {:ok, encoded} <- encode_row(view_id, generation, row),
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

  defp delete_removals(conn, view_id, generation, removals) when is_list(removals) do
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

  defp validate_batch_cas(state, expected, through) do
    cond do
      state.indexed_through >= through ->
        {:ok, :idempotent}

      state.indexed_through != expected ->
        {:error, ElixirDB.Error.revision_conflict("view cursor mismatch")}

      through < expected ->
        {:error, ElixirDB.Error.invalid_request("view through_sequence regressed")}

      true ->
        {:ok, :apply}
    end
  end

  defp insert_definition(conn, normalized) do
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

  defp enforce_definition_limit(conn, config) do
    maximum = get_in(config, ["views", "max_definitions"]) || 32

    case Connection.query(conn, "SELECT count(*) FROM view_definitions") do
      {:ok, [[count]]} when count < maximum -> :ok
      {:ok, [[_count]]} -> {:error, ElixirDB.Error.resource_limit("view definition limit reached")}
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp find_by_name(conn, name) do
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

  defp ensure_no_conflict(nil, _normalized), do: :ok

  defp ensure_no_conflict(%{definition_digest: digest}, %{definition_digest: digest}),
    do: :ok

  defp ensure_no_conflict(%{view_id: view_id, definition_digest: _digest}, _normalized),
    do:
      {:error,
       ElixirDB.Error.view_name_conflict("view name is already used by another definition", %{
         view_id: view_id
       })}

  defp fetch_definition(conn, view_id) do
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

  defp fetch_state(conn, view_id) do
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

  defp ensure_building_generation(%{building_generation: generation}, generation), do: :ok

  defp ensure_building_generation(_state, _generation),
    do: {:error, ElixirDB.Error.invalid_request("view rebuild generation mismatch")}

  defp decode_definition(row) do
    case StrictDecoder.decode(row.definition_json) do
      {:ok, decoded} -> Definition.normalize(decoded)
      {:error, _} = error -> error
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

  defp required_integer(map, key) when is_binary(key) do
    value = map_get(map, key)

    if is_integer(value) and value >= 0,
      do: value,
      else: raise(ArgumentError, "missing #{key}")
  end

  defp map_get(map, "view_id"), do: MapAccess.get(map, :view_id)
  defp map_get(map, "document_id"), do: MapAccess.get(map, :document_id)
  defp map_get(map, "revision_id"), do: MapAccess.get(map, :revision_id)
  defp map_get(map, "expected_indexed_through"), do: MapAccess.get(map, :expected_indexed_through)
  defp map_get(map, "through_sequence"), do: MapAccess.get(map, :through_sequence)
  defp map_get(map, "start_sequence"), do: MapAccess.get(map, :start_sequence)
  defp map_get(map, "generation"), do: MapAccess.get(map, :generation)
  defp map_get(map, "indexed_through"), do: MapAccess.get(map, :indexed_through)
  defp map_get(map, key) when is_binary(key), do: Map.get(map, key)

  defp normalize_error(%ElixirDB.Error{} = error), do: error

  defp normalize_error(reason),
    do: ElixirDB.Error.internal_error("SQLite operation failed", %{cause: inspect(reason)})

  defp view_fault_check(nil, _point), do: :ok

  defp view_fault_check(fun, point) when is_function(fun, 1) do
    case fun.(point) do
      :ok -> :ok
      {:error, %ElixirDB.Error{} = error} -> {:error, error}
    end
  end
end
