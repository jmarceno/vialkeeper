defmodule ElixirDB.Storage.SQLite.DerivedViews do
  @moduledoc """
  SQLite physical persistence for derived materialization state.

  Source-batch normalization, contribution diffs, grouping, reducers, numeric
  extremes, generated outputs, and cursor/history checks live in
  `ElixirDB.DerivedView.Engine`.
  """

  alias ElixirDB.DerivedView.Engine
  alias ElixirDB.JSON.{Canonical, StrictDecoder}
  alias ElixirDB.MapAccess
  alias ElixirDB.Storage.SQLite.{Connection, Documents, Mutations, TermBlob}

  @default_rebuild_page_limit 500
  @default_batch_limit 500

  @spec get_view(Connection.handle()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def get_view(conn) do
    with {:ok, metadata} <- fetch_metadata(conn),
         {:ok, definition} <- Engine.decode_definition(metadata.definition_json) do
      {:ok, Map.put(metadata, :definition, definition)}
    end
  end

  @spec set_enabled_tx(Connection.handle(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def set_enabled_tx(conn, request) when is_map(request) do
    with {:ok, metadata} <- fetch_metadata(conn),
         :ok <- Engine.validate_materialization(metadata, request),
         {:ok, enabled} <- required_boolean(request, :enabled),
         status <- Engine.enabled_status(metadata, enabled),
         :ok <-
           Connection.execute(
             conn,
             "UPDATE derived_view SET enabled = ?, status = ?, last_error_code = NULL WHERE id = 1",
             [bool_to_integer(enabled), status]
           ) do
      {:ok,
       %{
         materialization_id: metadata.materialization_id,
         enabled: enabled,
         status: status_atom(status)
       }}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  def set_enabled_tx(_conn, _request),
    do: {:error, ElixirDB.Error.invalid_request("derived enable request must be an object")}

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

  @spec set_source_error_tx(Connection.handle(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def set_source_error_tx(conn, request) when is_map(request) do
    with {:ok, metadata} <- fetch_metadata(conn),
         :ok <- Engine.validate_materialization(metadata, request),
         {:ok, source_uuid} <- required_uuid(request, :source_database_uuid),
         {:ok, _source} <- fetch_source(conn, source_uuid),
         {:ok, error_code} <- required_string(request, :error_code),
         :ok <-
           Connection.execute(
             conn,
             "UPDATE derived_sources SET last_error_code = ? WHERE source_database_uuid = ?",
             [error_code, source_uuid]
           ),
         :ok <-
           Connection.execute(
             conn,
             "UPDATE derived_view SET status = CASE WHEN enabled = 1 THEN 'stale' ELSE 'disabled' END, last_error_code = ? WHERE id = 1",
             [error_code]
           ) do
      {:ok,
       %{
         materialization_id: metadata.materialization_id,
         source_database_uuid: source_uuid,
         last_error_code: error_code,
         status: if(metadata.enabled, do: :stale, else: :disabled)
       }}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  def set_source_error_tx(_conn, _request),
    do: {:error, ElixirDB.Error.invalid_request("derived source error must be an object")}

  @spec apply_source_batch_tx(map(), map(), keyword()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def apply_source_batch_tx(adapter, request, opts \\ [])

  def apply_source_batch_tx(adapter, request, opts) when is_map(request) do
    with {:ok, context} <- Engine.load_context(adapter.conn, request, &fetch_metadata/1),
         {:ok, source_uuid} <- required_uuid(request, :source_database_uuid),
         {:ok, source} <- fetch_source(adapter.conn, source_uuid),
         {:ok, batch} <- Engine.normalize_batch(request, context.definition, source),
         :ok <- Engine.validate_history_epoch(source, batch.history_epoch),
         {:ok, mode} <- Engine.validate_batch_cursor(source, batch.expected, batch.through) do
      apply_batch_mode(adapter, context, source, source_uuid, batch, mode, opts)
    end
  end

  def apply_source_batch_tx(_adapter, _request, _opts),
    do: {:error, ElixirDB.Error.invalid_request("derived source batch must be an object")}

  @spec begin_source_rebuild_tx(Connection.handle(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def begin_source_rebuild_tx(conn, request) when is_map(request) do
    with {:ok, metadata} <- fetch_metadata(conn),
         :ok <- Engine.validate_materialization(metadata, request),
         {:ok, source_uuid} <- required_uuid(request, :source_database_uuid),
         {:ok, source} <- fetch_source(conn, source_uuid),
         {:ok, start_sequence} <- required_non_negative(request, :start_sequence),
         {:ok, catchup_sequence} <-
           optional_non_negative(request, :catchup_sequence, start_sequence),
         generation <- source.rebuild_generation + 1,
         :ok <-
           Connection.execute(
             conn,
             "UPDATE derived_sources SET source_history_epoch = NULL, state = 'rebuilding', rebuild_generation = ?, rebuild_start_sequence = ?, rebuild_after_document_id = NULL, rebuild_catchup_sequence = ?, last_error_code = NULL WHERE source_database_uuid = ?",
             [generation, start_sequence, catchup_sequence, source_uuid]
           ),
         :ok <-
           Connection.execute(
             conn,
             "UPDATE derived_view SET status = 'rebuilding', last_error_code = NULL WHERE id = 1"
           ) do
      {:ok,
       %{
         materialization_id: metadata.materialization_id,
         source_database_uuid: source_uuid,
         generation: generation,
         start_sequence: start_sequence,
         catchup_sequence: catchup_sequence,
         previous_checkpoint_sequence: source.checkpoint_sequence
       }}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  def begin_source_rebuild_tx(_conn, _request),
    do: {:error, ElixirDB.Error.invalid_request("derived rebuild request must be an object")}

  @spec apply_rebuild_page_tx(map(), map(), keyword()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def apply_rebuild_page_tx(adapter, request, opts \\ [])

  def apply_rebuild_page_tx(adapter, request, opts) when is_map(request) do
    with {:ok, context} <- Engine.load_context(adapter.conn, request, &fetch_metadata/1),
         {:ok, source_uuid} <- required_uuid(request, :source_database_uuid),
         {:ok, source} <- fetch_source(adapter.conn, source_uuid),
         {:ok, generation} <- required_positive(request, :generation),
         :ok <- Engine.validate_rebuild_source(source, generation),
         {:ok, batch} <- Engine.normalize_rebuild_batch(request, context.definition, source),
         {:ok, effect} <-
           apply_contribution_changes(
             adapter,
             context.definition,
             source,
             batch.rows,
             batch.removals,
             generation,
             opts
           ),
         {:ok, cursor} <- optional_string(request, :after_document_id),
         {:ok, catchup_sequence} <-
           optional_non_negative(request, :catchup_sequence, source.rebuild_catchup_sequence || 0),
         :ok <- update_rebuild_cursor(adapter.conn, source_uuid, cursor, catchup_sequence) do
      {:ok,
       %{
         materialization_id: context.metadata.materialization_id,
         source_database_uuid: source_uuid,
         generation: generation,
         after_document_id: cursor,
         catchup_sequence: catchup_sequence,
         last_sequence: effect.last_sequence,
         changed_rows: effect.changed_rows
       }}
    end
  end

  def apply_rebuild_page_tx(_adapter, _request, _opts),
    do: {:error, ElixirDB.Error.invalid_request("derived rebuild page must be an object")}

  @spec prune_rebuild_stale_page_tx(map(), map(), keyword()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def prune_rebuild_stale_page_tx(adapter, request, opts \\ [])

  def prune_rebuild_stale_page_tx(adapter, request, opts) when is_map(request) do
    with {:ok, context} <- Engine.load_context(adapter.conn, request, &fetch_metadata/1),
         {:ok, source_uuid} <- required_uuid(request, :source_database_uuid),
         {:ok, source} <- fetch_source(adapter.conn, source_uuid),
         {:ok, generation} <- required_positive(request, :generation),
         :ok <- Engine.validate_rebuild_source(source, generation),
         {:ok, after_id} <- optional_string(request, :after_document_id),
         {:ok, limit} <- rebuild_limit(request),
         {:ok, stale_ids} <-
           stale_document_ids(adapter.conn, source_uuid, generation, after_id, limit),
         {:ok, effect} <-
           apply_contribution_changes(
             adapter,
             context.definition,
             source,
             [],
             stale_ids,
             generation,
             opts
           ) do
      {:ok,
       %{
         materialization_id: context.metadata.materialization_id,
         source_database_uuid: source_uuid,
         generation: generation,
         removed: length(stale_ids),
         next_after_document_id: List.last(stale_ids),
         has_more: length(stale_ids) == limit,
         last_sequence: effect.last_sequence
       }}
    end
  end

  def prune_rebuild_stale_page_tx(_adapter, _request, _opts),
    do: {:error, ElixirDB.Error.invalid_request("derived stale-prune page must be an object")}

  @spec finish_source_rebuild_tx(Connection.handle(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def finish_source_rebuild_tx(conn, request) when is_map(request) do
    with {:ok, metadata} <- fetch_metadata(conn),
         :ok <- Engine.validate_materialization(metadata, request),
         {:ok, source_uuid} <- required_uuid(request, :source_database_uuid),
         {:ok, source} <- fetch_source(conn, source_uuid),
         {:ok, generation} <- required_positive(request, :generation),
         :ok <- Engine.validate_rebuild_source(source, generation),
         {:ok, catchup_sequence} <-
           required_non_negative(request, :catchup_sequence),
         {:ok, history_epoch} <- required_string(request, :source_history_epoch),
         :ok <- Engine.validate_history_epoch(source, history_epoch),
         :ok <-
           Connection.execute(
             conn,
             "UPDATE derived_sources SET source_history_epoch = ?, checkpoint_sequence = ?, state = 'active', rebuild_start_sequence = NULL, rebuild_after_document_id = NULL, rebuild_catchup_sequence = NULL, last_error_code = NULL WHERE source_database_uuid = ? AND rebuild_generation = ?",
             [
               history_epoch,
               max(source.checkpoint_sequence, catchup_sequence),
               source_uuid,
               generation
             ]
           ),
         :ok <- maybe_mark_current(conn) do
      {:ok,
       %{
         materialization_id: metadata.materialization_id,
         source_database_uuid: source_uuid,
         generation: generation,
         checkpoint_sequence: max(source.checkpoint_sequence, catchup_sequence),
         status: :active
       }}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  def finish_source_rebuild_tx(_conn, _request),
    do: {:error, ElixirDB.Error.invalid_request("derived rebuild finish must be an object")}

  defp apply_batch_mode(adapter, context, source, source_uuid, _batch, :idempotent, _opts) do
    with :ok <- clear_source_error(adapter.conn, source_uuid),
         :ok <- maybe_mark_current(adapter.conn) do
      {:ok,
       %{
         materialization_id: context.metadata.materialization_id,
         source_database_uuid: source_uuid,
         checkpoint_sequence: source.checkpoint_sequence,
         history_epoch: source.history_epoch,
         applied: false,
         last_sequence: 0,
         changed_rows: 0
       }}
    end
  end

  defp apply_batch_mode(adapter, context, source, source_uuid, batch, :apply, opts) do
    with {:ok, effect} <-
           apply_contribution_changes(
             adapter,
             context.definition,
             source,
             batch.rows,
             batch.removals,
             batch.rebuild_generation,
             opts
           ),
         :ok <-
           update_source_checkpoint(
             adapter.conn,
             source,
             source_uuid,
             batch.history_epoch,
             batch.through
           ),
         :ok <- maybe_mark_current(adapter.conn) do
      {:ok,
       %{
         materialization_id: context.metadata.materialization_id,
         source_database_uuid: source_uuid,
         checkpoint_sequence: batch.through,
         history_epoch: batch.history_epoch,
         applied: true,
         last_sequence: effect.last_sequence,
         changed_rows: effect.changed_rows
       }}
    end
  end

  defp apply_contribution_changes(
         adapter,
         definition,
         source,
         rows,
         removals,
         generation,
         opts
       ) do
    conn = adapter.conn
    source_uuid = source.source_database_uuid
    row_map = Map.new(rows, &{&1.source_document_id, &1})

    with {:ok, old_rows} <- fetch_contributions(conn, source_uuid, Map.keys(row_map) ++ removals),
         :ok <- delete_contributions(conn, source_uuid, removals),
         :ok <- upsert_contributions(conn, source_uuid, rows, generation, opts) do
      apply_derived_outputs(
        adapter,
        definition,
        source_uuid,
        source,
        old_rows,
        row_map,
        removals,
        opts
      )
    end
  end

  defp apply_derived_outputs(
         adapter,
         %{reducer: nil},
         source_uuid,
         _source,
         old_rows,
         row_map,
         removals,
         opts
       ) do
    changes = Engine.map_output_changes(source_uuid, old_rows, row_map, removals)

    with {:ok, last_sequence} <- apply_generated_changes(adapter, changes, opts) do
      {:ok, %{last_sequence: last_sequence, changed_rows: length(changes)}}
    end
  end

  defp apply_derived_outputs(
         adapter,
         %{reducer: reducer},
         _source_uuid,
         _source,
         old_rows,
         row_map,
         removals,
         opts
       )
       when reducer in [:_count, :_sum, :_min, :_max, :_stats] do
    affected_groups = Engine.affected_groups(old_rows, row_map, removals)

    Enum.reduce_while(Engine.sorted_groups(affected_groups), {:ok, 0, 0}, fn {group_sort,
                                                                              group_json},
                                                                             {:ok, last_sequence,
                                                                              changed} ->
      case refresh_group(
             adapter,
             reducer,
             group_sort,
             group_json,
             old_rows,
             row_map,
             removals,
             opts
           ) do
        {:ok, sequence, did_change} ->
          {:cont, {:ok, max(last_sequence, sequence), changed + if(did_change, do: 1, else: 0)}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, last_sequence, changed} -> {:ok, %{last_sequence: last_sequence, changed_rows: changed}}
      {:error, _} = error -> error
    end
  end

  defp refresh_group(
         adapter,
         reducer,
         group_sort,
         group_json,
         old_rows,
         row_map,
         removals,
         opts
       ) do
    with {:ok, group_key} <- decode_group_key(group_json),
         {:ok, aggregate} <- fetch_group(adapter.conn, group_sort, group_json),
         {:ok, aggregate} <-
           Engine.update_group(aggregate, group_sort, old_rows, row_map, removals),
         {:ok, group_id} <- Engine.group_document_id(group_key) do
      persist_group_result(
        adapter,
        reducer,
        group_sort,
        group_json,
        group_key,
        group_id,
        group_result(adapter, reducer, group_sort, aggregate),
        opts
      )
    end
  end

  defp group_result(_adapter, _reducer, _group_sort, %{count: 0}), do: {:ok, []}

  defp group_result(adapter, reducer, group_sort, aggregate) do
    with {:ok, aggregate} <- enrich_group(adapter.conn, reducer, group_sort, aggregate),
         {:ok, output} <- Engine.reducer_output(reducer, aggregate) do
      {:ok, Map.put(aggregate, :output, output)}
    end
  end

  defp persist_group_result(
         adapter,
         _reducer,
         group_sort,
         _group_json,
         _group_key,
         group_id,
         {:ok, []},
         opts
       ) do
    with :ok <- delete_group(adapter.conn, group_sort),
         {:ok, sequence} <- apply_generated_change(adapter, {:delete, group_id}, opts) do
      {:ok, sequence, sequence > 0}
    end
  end

  defp persist_group_result(
         adapter,
         _reducer,
         group_sort,
         group_json,
         group_key,
         group_id,
         {:ok, aggregate},
         opts
       ) do
    with :ok <- upsert_group(adapter.conn, group_sort, group_json, group_id, aggregate),
         {:ok, sequence} <-
           apply_generated_change(
             adapter,
             {:put, group_id, Engine.group_body(group_key, aggregate.output)},
             opts
           ) do
      {:ok, sequence, sequence > 0}
    end
  end

  defp persist_group_result(
         _adapter,
         _reducer,
         _sort,
         _json,
         _key,
         _id,
         {:error, _} = error,
         _opts
       ),
       do: error

  defp decode_group_key(json), do: decode_json_array(json)

  defp fetch_group(conn, group_sort, group_json) do
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

  defp enrich_group(conn, reducer, group_sort, aggregate)
       when reducer in [:_min, :_max, :_stats] do
    with {:ok, values} <- group_numeric_values(conn, group_sort),
         {:ok, extrema} <- Engine.extrema_from_values(values) do
      {:ok, Map.merge(aggregate, extrema)}
    end
  end

  defp enrich_group(_conn, reducer, _group_sort, aggregate) when reducer in [:_count, :_sum],
    do: {:ok, aggregate}

  defp group_numeric_values(conn, group_sort) do
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

  defp upsert_group(conn, group_sort, group_json, group_id, aggregate) do
    Connection.execute(
      conn,
      "INSERT INTO derived_groups (group_key_sort, group_key_json, count, sum_units, sumsqr_units, min_value_json, min_value_sort, max_value_json, max_value_sort, output_document_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(group_key_sort) DO UPDATE SET group_key_json = excluded.group_key_json, count = excluded.count, sum_units = excluded.sum_units, sumsqr_units = excluded.sumsqr_units, min_value_json = excluded.min_value_json, min_value_sort = excluded.min_value_sort, max_value_json = excluded.max_value_json, max_value_sort = excluded.max_value_sort, output_document_id = excluded.output_document_id",
      [
        TermBlob.bind(group_sort),
        group_json,
        aggregate.count,
        Integer.to_string(aggregate.sum_units),
        Integer.to_string(aggregate.sumsqr_units),
        aggregate.min_value_json,
        nullable_blob(aggregate.min_value_sort),
        aggregate.max_value_json,
        nullable_blob(aggregate.max_value_sort),
        group_id
      ]
    )
  end

  defp delete_group(conn, group_sort),
    do:
      Connection.execute(conn, "DELETE FROM derived_groups WHERE group_key_sort = ?", [
        TermBlob.bind(group_sort)
      ])

  defp apply_generated_changes(adapter, changes, opts) do
    Enum.reduce_while(changes, {:ok, 0}, fn change, {:ok, last_sequence} ->
      case apply_generated_change(adapter, change, opts) do
        {:ok, sequence} -> {:cont, {:ok, max(last_sequence, sequence)}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp apply_generated_change(adapter, {:delete, document_id}, opts) do
    with {:ok, document} <- Documents.find(adapter.conn, document_id) do
      if is_nil(document) or document.winning_deleted do
        {:ok, 0}
      else
        apply_generated_mutation(adapter, document_id, :delete, nil, document, opts)
      end
    end
  end

  defp apply_generated_change(adapter, {:put, document_id, body}, opts) do
    with {:ok, body_json} <- Canonical.encode(body),
         {:ok, document} <- Documents.find(adapter.conn, document_id) do
      if live_body_matches?(document, body_json),
        do: {:ok, 0},
        else: apply_generated_mutation(adapter, document_id, :put, body, document, opts)
    end
  end

  defp apply_generated_mutation(adapter, document_id, operation, body, document, opts) do
    request = %{
      document_id: document_id,
      operation: operation,
      body: body,
      if_revision: current_revision(document)
    }

    with :ok <- derived_fault_check(opts, :derived_generated_mutation) do
      case Mutations.apply_local_tx(adapter, request) do
        {:ok, %{sequence: sequence}} -> {:ok, sequence}
        {:ok, _result} -> {:ok, 0}
        {:error, _} = error -> error
      end
    end
  end

  defp live_body_matches?(%{winning_deleted: false, winning_body_json: json}, body_json),
    do: json == body_json

  defp live_body_matches?(_document, _body_json), do: false

  defp current_revision(nil), do: nil
  defp current_revision(%{winning_revision: revision}), do: revision

  defp fetch_contributions(_conn, _source_uuid, []), do: {:ok, %{}}

  defp fetch_contributions(conn, source_uuid, document_ids) do
    document_ids = Enum.uniq(document_ids)
    placeholders = Enum.map_join(document_ids, ", ", fn _ -> "?" end)

    sql =
      "SELECT source_document_id, source_revision_id, rebuild_generation, key_json, key_sort, " <>
        "group_key_json, group_key_sort, value_json, value_sort FROM derived_rows " <>
        "WHERE source_database_uuid = ? AND source_document_id IN (" <> placeholders <> ")"

    case Connection.query(
           conn,
           sql,
           [source_uuid | document_ids]
         ) do
      {:ok, rows} -> decode_contributions(rows, source_uuid, %{})
      {:error, reason} -> {:error, normalize_error(reason)}
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

  defp delete_contributions(_conn, _source_uuid, []), do: :ok

  defp delete_contributions(conn, source_uuid, document_ids) do
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

  defp upsert_contributions(_conn, _source_uuid, [], _generation, _opts), do: :ok

  defp upsert_contributions(conn, source_uuid, rows, generation, opts) do
    Enum.reduce_while(rows, :ok, fn row, :ok ->
      with :ok <- derived_fault_check(opts, :derived_upsert_row),
           :ok <-
             Connection.execute(
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
        {:cont, :ok}
      else
        {:error, reason} -> {:halt, {:error, normalize_error(reason)}}
      end
    end)
  end

  defp update_source_checkpoint(conn, source, source_uuid, history_epoch, through) do
    state = if source.state == :rebuilding, do: "rebuilding", else: "active"

    Connection.execute(
      conn,
      "UPDATE derived_sources SET source_history_epoch = ?, checkpoint_sequence = ?, state = ?, last_error_code = NULL WHERE source_database_uuid = ?",
      [history_epoch, through, state, source_uuid]
    )
  end

  defp update_rebuild_cursor(conn, source_uuid, cursor, catchup_sequence) do
    Connection.execute(
      conn,
      "UPDATE derived_sources SET rebuild_after_document_id = ?, rebuild_catchup_sequence = ?, last_error_code = NULL WHERE source_database_uuid = ?",
      [cursor, catchup_sequence, source_uuid]
    )
  end

  defp clear_source_error(conn, source_uuid) do
    Connection.execute(
      conn,
      "UPDATE derived_sources SET last_error_code = NULL WHERE source_database_uuid = ?",
      [source_uuid]
    )
  end

  defp stale_document_ids(conn, source_uuid, generation, after_id, limit) do
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

  defp maybe_mark_current(conn) do
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

  defp fetch_metadata(conn) do
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
           status: status_atom(status),
           options_json: options_json,
           last_error_code: error_code
         }}

      {:ok, []} ->
        {:error, ElixirDB.Error.integrity_violation("derived database metadata is missing")}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp fetch_source(conn, source_uuid) do
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

  defp status_atom("disabled"), do: :disabled
  defp status_atom("rebuilding"), do: :rebuilding
  defp status_atom("current"), do: :current
  defp status_atom("stale"), do: :stale
  defp status_atom("resource_limit"), do: :resource_limit
  defp status_atom(_), do: :unknown

  defp rebuild_limit(request) do
    limit = MapAccess.get(request, :limit, @default_rebuild_page_limit)

    maximum =
      ElixirDB.Config.host_limits()[:max_materialized_view_batch_documents] || @default_batch_limit

    cond do
      not is_integer(limit) or limit <= 0 ->
        {:error, ElixirDB.Error.invalid_request("derived rebuild page limit must be positive")}

      limit > maximum ->
        {:error, ElixirDB.Error.resource_limit("derived rebuild page limit exceeds the host limit")}

      true ->
        {:ok, limit}
    end
  end

  defp required_uuid(map, key) do
    with {:ok, value} <- Engine.required_string(map, key) do
      if uuid?(value), do: {:ok, String.downcase(value)}, else: invalid_field(key)
    end
  end

  defp optional_string(map, key) do
    case MapAccess.get(map, key) do
      nil -> {:ok, nil}
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> invalid_field(key)
    end
  end

  defp required_string(map, key), do: Engine.required_string(map, key)

  defp required_non_negative(map, key), do: Engine.required_non_negative(map, key)

  defp required_positive(map, key) do
    value = MapAccess.get(map, key)

    if is_integer(value) and value > 0, do: {:ok, value}, else: invalid_field(key)
  end

  defp optional_non_negative(map, key, default) do
    case MapAccess.get(map, key) do
      nil -> {:ok, default}
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _ -> invalid_field(key)
    end
  end

  defp required_boolean(map, key) do
    value = MapAccess.get(map, key)
    if is_boolean(value), do: {:ok, value}, else: invalid_field(key)
  end

  defp invalid_field(key) when is_atom(key), do: invalid_field(Atom.to_string(key))

  defp invalid_field(field),
    do: {:error, ElixirDB.Error.invalid_request("derived field #{field} is invalid")}

  defp uuid?(value) do
    Regex.match?(
      ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
      value
    )
  end

  defp bool_to_integer(true), do: 1
  defp bool_to_integer(false), do: 0

  defp nullable_blob(nil), do: nil
  defp nullable_blob(value), do: TermBlob.bind(value)

  defp derived_fault_check(opts, point) do
    case Keyword.get(opts, :derived_fault) do
      nil -> :ok
      fault when is_function(fault, 1) -> fault.(point)
    end
  end

  defp normalize_error(%ElixirDB.Error{} = error), do: error

  defp normalize_error(reason),
    do: ElixirDB.Error.internal_error("SQLite operation failed", %{cause: inspect(reason)})
end
