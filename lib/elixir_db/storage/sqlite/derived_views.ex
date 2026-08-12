defmodule ElixirDB.Storage.SQLite.DerivedViews do
  @moduledoc """
  SQLite physical persistence for derived materialization state.

  Row/range contribution maps, group aggregates, source cursors, and metadata
  updates live here. Product apply/rebuild orchestration lives in
  `ElixirDB.Storage.Services.DerivedViews`.
  """

  alias ElixirDB.DerivedView.{Engine, RebuildState}
  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.Storage.SQLite.{Connection, TermBlob}

  @spec get_metadata(Connection.handle()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def get_metadata(conn) do
    case Connection.query(
           conn,
           "SELECT materialization_id, name, definition_json, definition_digest, enabled, status, options_json, last_error_code FROM derived_view WHERE id = 1"
         ) do
      {:ok,
       [
         [
           materialization_id,
           name,
           definition_json,
           digest,
           enabled,
           status,
           options_json,
           error_code
         ]
       ]} ->
        {:ok,
         %{
           materialization_id: materialization_id,
           name: name,
           definition_json: definition_json,
           definition_digest: digest,
           enabled: enabled == 1,
           status: Engine.status_atom(status),
           options_json: options_json,
           last_error_code: error_code
         }}

      {:ok, []} ->
        {:error, ElixirDB.Error.integrity_violation("derived database metadata is missing")}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  @spec list_sources(Connection.handle()) :: {:ok, [map()]} | {:error, ElixirDB.Error.t()}
  def list_sources(conn) do
    case Connection.query(
           conn,
           "SELECT source_ordinal, source_database_uuid, source_history_epoch, checkpoint_sequence, state, rebuild_generation, rebuild_start_sequence, rebuild_after_document_id, rebuild_catchup_sequence, last_error_code FROM derived_sources ORDER BY source_ordinal"
         ) do
      {:ok, rows} -> {:ok, Enum.map(rows, &source_result/1)}
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @spec get_source(Connection.handle(), binary()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def get_source(conn, source_uuid) when is_binary(source_uuid) do
    case Connection.query(
           conn,
           "SELECT source_ordinal, source_database_uuid, source_history_epoch, checkpoint_sequence, state, rebuild_generation, rebuild_start_sequence, rebuild_after_document_id, rebuild_catchup_sequence, last_error_code FROM derived_sources WHERE source_database_uuid = ?",
           [source_uuid]
         ) do
      {:ok, [row]} ->
        {:ok, source_result(row)}

      {:ok, []} ->
        {:error, ElixirDB.Error.invalid_request("source is not registered with derived database")}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  @spec put_enabled(Connection.handle(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def put_enabled(conn, effect) when is_map(effect) do
    with {:ok, metadata} <- get_metadata(conn),
         :ok <-
           Connection.execute(
             conn,
             "UPDATE derived_view SET enabled = ?, status = ?, last_error_code = NULL WHERE id = 1",
             [bool_to_integer(effect.enabled), effect.status]
           ) do
      {:ok,
       %{
         materialization_id: metadata.materialization_id,
         enabled: effect.enabled,
         status: Engine.status_atom(effect.status)
       }}
    end
  end

  @spec put_source_error(Connection.handle(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def put_source_error(conn, effect) when is_map(effect) do
    status = if effect.enabled, do: "stale", else: "disabled"

    with :ok <-
           Connection.execute(
             conn,
             "UPDATE derived_sources SET last_error_code = ? WHERE source_database_uuid = ?",
             [effect.error_code, effect.source_database_uuid]
           ),
         :ok <-
           Connection.execute(
             conn,
             "UPDATE derived_view SET status = ?, last_error_code = ? WHERE id = 1",
             [status, effect.error_code]
           ) do
      {:ok,
       %{
         materialization_id: effect.materialization_id,
         source_database_uuid: effect.source_database_uuid,
         last_error_code: effect.error_code,
         status: Engine.status_atom(status)
       }}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @spec put_rebuild_begin(Connection.handle(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def put_rebuild_begin(conn, effect) when is_map(effect) do
    with :ok <-
           Connection.execute(
             conn,
             "UPDATE derived_sources SET source_history_epoch = NULL, state = 'rebuilding', rebuild_generation = ?, rebuild_start_sequence = ?, rebuild_after_document_id = NULL, rebuild_catchup_sequence = ?, last_error_code = NULL WHERE source_database_uuid = ?",
             [
               effect.generation,
               effect.start_sequence,
               effect.catchup_sequence,
               effect.source_database_uuid
             ]
           ),
         :ok <-
           Connection.execute(
             conn,
             "UPDATE derived_view SET status = 'rebuilding', last_error_code = NULL WHERE id = 1"
           ) do
      {:ok, RebuildState.new(effect)}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @spec put_rebuild_cursor(Connection.handle(), map()) :: :ok | {:error, ElixirDB.Error.t()}
  def put_rebuild_cursor(conn, effect) when is_map(effect) do
    Connection.execute(
      conn,
      "UPDATE derived_sources SET rebuild_after_document_id = ?, rebuild_catchup_sequence = ?, last_error_code = NULL WHERE source_database_uuid = ?",
      [effect.after_document_id, effect.catchup_sequence, effect.source_database_uuid]
    )
  end

  @spec put_rebuild_finish(Connection.handle(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def put_rebuild_finish(conn, effect) when is_map(effect) do
    case Connection.execute(
           conn,
           "UPDATE derived_sources SET source_history_epoch = ?, checkpoint_sequence = ?, state = 'active', rebuild_start_sequence = NULL, rebuild_after_document_id = NULL, rebuild_catchup_sequence = NULL, last_error_code = NULL WHERE source_database_uuid = ? AND rebuild_generation = ?",
           [
             effect.source_history_epoch,
             effect.checkpoint_sequence,
             effect.source_database_uuid,
             effect.generation
           ]
         ) do
      :ok ->
        {:ok,
         %{
           materialization_id: effect.materialization_id,
           source_database_uuid: effect.source_database_uuid,
           generation: effect.generation,
           checkpoint_sequence: effect.checkpoint_sequence,
           status: :active
         }}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  @spec put_source_checkpoint(Connection.handle(), map()) :: :ok | {:error, ElixirDB.Error.t()}
  def put_source_checkpoint(conn, effect) when is_map(effect) do
    state =
      case Map.get(effect, :state, :active) do
        :rebuilding -> "rebuilding"
        _ -> "active"
      end

    Connection.execute(
      conn,
      "UPDATE derived_sources SET source_history_epoch = ?, checkpoint_sequence = ?, state = ?, last_error_code = NULL WHERE source_database_uuid = ?",
      [effect.history_epoch, effect.checkpoint_sequence, state, effect.source_database_uuid]
    )
  end

  @spec clear_source_error(Connection.handle(), binary()) :: :ok | {:error, ElixirDB.Error.t()}
  def clear_source_error(conn, source_uuid) when is_binary(source_uuid) do
    Connection.execute(
      conn,
      "UPDATE derived_sources SET last_error_code = NULL WHERE source_database_uuid = ?",
      [source_uuid]
    )
  end

  @spec refresh_status(Connection.handle()) :: :ok | {:error, ElixirDB.Error.t()}
  def refresh_status(conn) do
    case Connection.query(
           conn,
           "SELECT count(*) FROM derived_sources WHERE state != 'active' OR last_error_code IS NOT NULL"
         ) do
      {:ok, [[0]]} ->
        Connection.execute(conn, "UPDATE derived_view SET status = 'current' WHERE id = 1")

      {:ok, [[_count]]} ->
        :ok

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  @spec fetch_contributions(Connection.handle(), binary(), [binary()]) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def fetch_contributions(_conn, _source_uuid, []), do: {:ok, %{}}

  def fetch_contributions(conn, source_uuid, document_ids)
      when is_binary(source_uuid) and is_list(document_ids) do
    document_ids = Enum.uniq(document_ids)
    placeholders = Enum.map_join(document_ids, ", ", fn _ -> "?" end)

    sql =
      "SELECT source_document_id, source_revision_id, rebuild_generation, key_json, key_sort, " <>
        "group_key_json, group_key_sort, value_json, value_sort FROM derived_rows " <>
        "WHERE source_database_uuid = ? AND source_document_id IN (" <> placeholders <> ")"

    case Connection.query(conn, sql, [source_uuid | document_ids]) do
      {:ok, rows} -> decode_contributions(rows, source_uuid, %{})
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @spec delete_contributions(Connection.handle(), binary(), [binary()]) ::
          :ok | {:error, ElixirDB.Error.t()}
  def delete_contributions(_conn, _source_uuid, []), do: :ok

  def delete_contributions(conn, source_uuid, document_ids)
      when is_binary(source_uuid) and is_list(document_ids) do
    document_ids = Enum.uniq(document_ids)
    placeholders = Enum.map_join(document_ids, ", ", fn _ -> "?" end)

    sql =
      "DELETE FROM derived_rows WHERE source_database_uuid = ? AND source_document_id IN (" <>
        placeholders <> ")"

    case Connection.execute(conn, sql, [source_uuid | document_ids]) do
      :ok -> :ok
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @spec upsert_contributions(Connection.handle(), binary(), [map()], pos_integer()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def upsert_contributions(_conn, _source_uuid, [], _generation), do: :ok

  def upsert_contributions(conn, source_uuid, rows, generation)
      when is_binary(source_uuid) and is_list(rows) and is_integer(generation) do
    Enum.reduce_while(rows, :ok, fn row, :ok ->
      case Connection.execute(
             conn,
             "INSERT INTO derived_rows (source_database_uuid, source_document_id, source_revision_id, rebuild_generation, key_json, key_sort, group_key_json, group_key_sort, value_json, value_sort) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(source_database_uuid, source_document_id) DO UPDATE SET source_revision_id = excluded.source_revision_id, rebuild_generation = excluded.rebuild_generation, key_json = excluded.key_json, key_sort = excluded.key_sort, group_key_json = excluded.group_key_json, group_key_sort = excluded.group_key_sort, value_json = excluded.value_json, value_sort = excluded.value_sort",
             [
               source_uuid,
               row.source_document_id,
               row.source_revision_id,
               generation,
               row.key_json,
               TermBlob.bind(row.key_sort),
               row.group_key_json,
               nullable_blob(row.group_key_sort),
               row.value_json,
               nullable_blob(row.value_sort)
             ]
           ) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, normalize_error(reason)}}
      end
    end)
  end

  @spec list_stale_contribution_ids(Connection.handle(), map()) ::
          {:ok, [binary()]} | {:error, ElixirDB.Error.t()}
  def list_stale_contribution_ids(conn, request) when is_map(request) do
    source_uuid = request.source_database_uuid
    generation = request.generation
    after_id = request.after_document_id
    limit = request.limit

    {filter, params} =
      case after_id do
        nil ->
          {"source_database_uuid = ? AND rebuild_generation != ?", [source_uuid, generation]}

        value ->
          {"source_database_uuid = ? AND rebuild_generation != ? AND source_document_id > ?",
           [source_uuid, generation, value]}
      end

    case Connection.query(
           conn,
           "SELECT source_document_id FROM derived_rows WHERE #{filter} ORDER BY source_document_id LIMIT ?",
           params ++ [limit]
         ) do
      {:ok, rows} -> {:ok, Enum.map(rows, fn [document_id] -> document_id end)}
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @spec fetch_group(Connection.handle(), binary(), binary()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def fetch_group(conn, group_sort, group_json)
      when is_binary(group_sort) and is_binary(group_json) do
    case Connection.query(
           conn,
           "SELECT group_key_json, count, sum_units, sumsqr_units, min_value_json, min_value_sort, max_value_json, max_value_sort FROM derived_groups WHERE group_key_sort = ?",
           [TermBlob.bind(group_sort)]
         ) do
      {:ok, []} ->
        {:ok, Engine.empty_group(group_json)}

      {:ok, [[stored_json, count, sum_units, sumsqr_units, min_json, min_sort, max_json, max_sort]]} ->
        with :ok <- validate_group_json(stored_json, group_json),
             {:ok, count} <- parse_group_integer(count, "count", &(&1 >= 0)),
             {:ok, sum_units} <- parse_group_integer(sum_units, "sum_units"),
             {:ok, sumsqr_units} <- parse_group_integer(sumsqr_units, "sumsqr_units") do
          {:ok,
           %{
             group_key_json: stored_json,
             count: count,
             sum_units: sum_units,
             sumsqr_units: sumsqr_units,
             min_value_json: min_json,
             min_value_sort: min_sort,
             max_value_json: max_json,
             max_value_sort: max_sort
           }}
        end

      {:ok, _rows} ->
        {:error, ElixirDB.Error.integrity_violation("derived group state is invalid")}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  @spec upsert_group(Connection.handle(), map()) :: :ok | {:error, ElixirDB.Error.t()}
  def upsert_group(conn, effect) when is_map(effect) do
    aggregate = effect.aggregate

    Connection.execute(
      conn,
      "INSERT INTO derived_groups (group_key_sort, group_key_json, count, sum_units, sumsqr_units, min_value_json, min_value_sort, max_value_json, max_value_sort, output_document_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(group_key_sort) DO UPDATE SET group_key_json = excluded.group_key_json, count = excluded.count, sum_units = excluded.sum_units, sumsqr_units = excluded.sumsqr_units, min_value_json = excluded.min_value_json, min_value_sort = excluded.min_value_sort, max_value_json = excluded.max_value_json, max_value_sort = excluded.max_value_sort, output_document_id = excluded.output_document_id",
      [
        TermBlob.bind(effect.group_key_sort),
        effect.group_key_json,
        aggregate.count,
        Integer.to_string(aggregate.sum_units),
        Integer.to_string(aggregate.sumsqr_units),
        aggregate.min_value_json,
        nullable_blob(aggregate.min_value_sort),
        aggregate.max_value_json,
        nullable_blob(aggregate.max_value_sort),
        effect.group_id
      ]
    )
  end

  @spec delete_group(Connection.handle(), binary()) :: :ok | {:error, ElixirDB.Error.t()}
  def delete_group(conn, group_sort) when is_binary(group_sort) do
    Connection.execute(conn, "DELETE FROM derived_groups WHERE group_key_sort = ?", [
      TermBlob.bind(group_sort)
    ])
  end

  @spec list_group_numeric_values(Connection.handle(), binary()) ::
          {:ok, [{term(), binary() | nil, binary(), binary()}]} | {:error, ElixirDB.Error.t()}
  def list_group_numeric_values(conn, group_sort) when is_binary(group_sort) do
    case Connection.query(
           conn,
           "SELECT value_json, value_sort, source_database_uuid, source_document_id FROM derived_rows WHERE group_key_sort = ? AND value_sort IS NOT NULL",
           [TermBlob.bind(group_sort)]
         ) do
      {:ok, rows} -> collect_numeric_values(rows)
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp collect_numeric_values(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn row, {:ok, acc} ->
      case decode_numeric_row(row) do
        {:ok, value} -> {:cont, {:ok, [value | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp decode_numeric_row([json, sort, source, doc]) do
    with {:ok, value} <- decode_numeric_value(json) do
      {:ok, {value, sort, source, doc}}
    end
  end

  defp decode_numeric_value(json) do
    case StrictDecoder.decode(json) do
      {:ok, value} -> {:ok, value}
      _ -> {:error, ElixirDB.Error.integrity_violation("derived contribution value is invalid")}
    end
  end

  defp decode_contributions([], _source_uuid, contributions), do: {:ok, contributions}

  defp decode_contributions([row | rest], source_uuid, contributions) do
    with {:ok, contribution} <- decode_contribution(row, source_uuid) do
      decode_contributions(
        rest,
        source_uuid,
        Map.put(contributions, contribution.source_document_id, contribution)
      )
    end
  end

  defp decode_contribution(
         [
           document_id,
           revision_id,
           generation,
           key_json,
           key_sort,
           group_json,
           group_sort,
           value_json,
           _value_sort
         ],
         source_uuid
       ) do
    with {:ok, key} <- decode_json_array(key_json),
         {:ok, group_key} <- decode_optional_array(group_json),
         {:ok, value} <- decode_optional_value(value_json) do
      {:ok,
       %{
         source_database_uuid: source_uuid,
         source_document_id: document_id,
         source_revision_id: revision_id,
         rebuild_generation: generation,
         key: key,
         key_json: key_json,
         key_sort: key_sort,
         group_key: group_key,
         group_key_json: group_json,
         group_key_sort: group_sort,
         value: value,
         value_json: value_json,
         value_present: not is_nil(value_json)
       }}
    end
  end

  defp decode_json_array(json) when is_binary(json) do
    case StrictDecoder.decode(json) do
      {:ok, value} when is_list(value) -> {:ok, value}
      _ -> {:error, ElixirDB.Error.integrity_violation("derived contribution key is invalid")}
    end
  end

  defp decode_json_array(_),
    do: {:error, ElixirDB.Error.integrity_violation("derived contribution key is missing")}

  defp decode_optional_array(nil), do: {:ok, nil}
  defp decode_optional_array(json), do: decode_json_array(json)

  defp decode_optional_value(nil), do: {:ok, nil}

  defp decode_optional_value(json) do
    case StrictDecoder.decode(json) do
      {:ok, value} -> {:ok, value}
      _ -> {:error, ElixirDB.Error.integrity_violation("derived contribution value is invalid")}
    end
  end

  defp validate_group_json(stored_json, expected_json)
       when is_binary(stored_json) and stored_json == expected_json,
       do: :ok

  defp validate_group_json(_stored_json, _expected_json),
    do: {:error, ElixirDB.Error.integrity_violation("derived group key is inconsistent")}

  defp parse_group_integer(value, field, predicate \\ fn _value -> true end)

  defp parse_group_integer(value, field, predicate)
       when is_binary(value) and is_function(predicate, 1) do
    case Integer.parse(value) do
      {integer, ""} ->
        if predicate.(integer),
          do: {:ok, integer},
          else: {:error, ElixirDB.Error.integrity_violation("derived group #{field} is invalid")}

      _ ->
        {:error, ElixirDB.Error.integrity_violation("derived group #{field} is invalid")}
    end
  end

  defp parse_group_integer(value, _field, predicate)
       when is_integer(value) and is_function(predicate, 1) do
    if predicate.(value),
      do: {:ok, value},
      else: {:error, ElixirDB.Error.integrity_violation("derived group count is invalid")}
  end

  defp parse_group_integer(_value, field, _predicate),
    do: {:error, ElixirDB.Error.integrity_violation("derived group #{field} is invalid")}

  defp source_result([
         ordinal,
         source_uuid,
         history_epoch,
         checkpoint,
         state,
         generation,
         rebuild_start,
         rebuild_after,
         rebuild_catchup,
         error_code
       ]) do
    %{
      source_ordinal: ordinal,
      source_database_uuid: source_uuid,
      history_epoch: history_epoch,
      checkpoint_sequence: checkpoint,
      state: source_state(state),
      rebuild_generation: generation,
      rebuild_start_sequence: rebuild_start,
      rebuild_after_document_id: rebuild_after,
      rebuild_catchup_sequence: rebuild_catchup,
      last_error_code: error_code
    }
  end

  defp source_state("pending"), do: :pending
  defp source_state("rebuilding"), do: :rebuilding
  defp source_state("active"), do: :active
  defp source_state(_), do: :unknown

  defp bool_to_integer(true), do: 1
  defp bool_to_integer(false), do: 0

  defp nullable_blob(nil), do: nil
  defp nullable_blob(value), do: TermBlob.bind(value)

  defp normalize_error(reason),
    do:
      ElixirDB.Error.internal_error("derived sqlite persistence failed", %{cause: inspect(reason)})
end
