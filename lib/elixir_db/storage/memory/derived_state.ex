defmodule ElixirDB.Storage.Memory.DerivedState do
  @moduledoc """
  Memory derived-state port.

  Persists metadata, sources, contributions, and groups in the Memory store.
  Shared `ElixirDB.DerivedView.Engine` owns batch normalization, contribution
  diffs, grouping, reducers, numeric extremes, and generated-output mapping.
  Generated documents are written through `ElixirDB.Storage.Services.Mutations`.
  """
  @behaviour ElixirDB.Storage.Ports.DerivedState

  alias ElixirDB.DerivedView.Engine
  alias ElixirDB.JSON.{Canonical, StrictDecoder}
  alias ElixirDB.MapAccess
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Memory.{Context, Store}
  alias ElixirDB.Storage.Services.Mutations

  @default_rebuild_page_limit 500

  @impl true
  def get_derived_view(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, metadata} <- fetch_metadata(Store.get(adapter.store)),
         {:ok, definition} <- Engine.decode_definition(metadata.definition_json) do
      {:ok, Map.put(metadata, :definition, definition)}
    end
  end

  @impl true
  def set_derived_enabled(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Store.update(adapter.store, &set_enabled_in_state(&1, request))
    end
  end

  @impl true
  def list_derived_sources(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      sources =
        Store.get(adapter.store).derived_sources
        |> Map.values()
        |> Enum.sort_by(& &1.source_ordinal)

      {:ok, sources}
    end
  end

  @impl true
  def set_derived_source_error(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Store.update(adapter.store, &set_source_error_in_state(&1, request))
    end
  end

  @impl true
  def apply_derived_source_batch(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context) do
      state = Store.get(adapter.store)

      with {:ok, loaded} <- Engine.load_context(state, request, &fetch_metadata/1),
           {:ok, source_uuid} <- required_uuid(request, :source_database_uuid),
           {:ok, source} <- fetch_source(state, source_uuid),
           {:ok, batch} <- Engine.normalize_batch(request, loaded.definition, source),
           :ok <- Engine.validate_history_epoch(source, batch.history_epoch),
           {:ok, mode} <- Engine.validate_batch_cursor(source, batch.expected, batch.through),
           {:ok, state, result} <-
             apply_batch_mode(adapter, context, state, loaded, source, source_uuid, batch, mode) do
        persist_state(adapter, state, result)
      end
    end
  end

  @impl true
  def begin_derived_source_rebuild(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Store.update(adapter.store, &begin_rebuild_in_state(&1, request))
    end
  end

  @impl true
  def apply_derived_rebuild_page(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context) do
      state = Store.get(adapter.store)

      with {:ok, loaded} <- Engine.load_context(state, request, &fetch_metadata/1),
           {:ok, source_uuid} <- required_uuid(request, :source_database_uuid),
           {:ok, source} <- fetch_source(state, source_uuid),
           {:ok, generation} <- required_positive(request, :generation),
           :ok <- Engine.validate_rebuild_source(source, generation),
           {:ok, batch} <- Engine.normalize_rebuild_batch(request, loaded.definition, source),
           {:ok, state, effect} <-
             apply_contribution_changes(
               adapter,
               context,
               state,
               loaded.definition,
               source,
               batch.rows,
               batch.removals,
               generation
             ),
           {:ok, cursor} <- optional_string(request, :after_document_id),
           {:ok, catchup_sequence} <-
             optional_non_negative(
               request,
               :catchup_sequence,
               source.rebuild_catchup_sequence || 0
             ) do
        source = %{
          source
          | rebuild_after_document_id: cursor,
            rebuild_catchup_sequence: catchup_sequence,
            last_error_code: nil
        }

        state = %{state | derived_sources: Map.put(state.derived_sources, source_uuid, source)}

        persist_state(adapter, state, %{
          materialization_id: loaded.metadata.materialization_id,
          source_database_uuid: source_uuid,
          generation: generation,
          after_document_id: cursor,
          catchup_sequence: catchup_sequence,
          last_sequence: effect.last_sequence,
          changed_rows: effect.changed_rows
        })
      end
    end
  end

  @impl true
  def prune_derived_rebuild_stale_page(%BackendContext{} = context, request)
      when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context) do
      state = Store.get(adapter.store)

      with {:ok, loaded} <- Engine.load_context(state, request, &fetch_metadata/1),
           {:ok, source_uuid} <- required_uuid(request, :source_database_uuid),
           {:ok, source} <- fetch_source(state, source_uuid),
           {:ok, generation} <- required_positive(request, :generation),
           :ok <- Engine.validate_rebuild_source(source, generation),
           {:ok, after_id} <- optional_string(request, :after_document_id),
           {:ok, limit} <- rebuild_limit(request),
           stale_ids <- stale_document_ids(state, source_uuid, generation, after_id, limit),
           {:ok, state, effect} <-
             apply_contribution_changes(
               adapter,
               context,
               state,
               loaded.definition,
               source,
               [],
               stale_ids,
               generation
             ) do
        persist_state(adapter, state, %{
          materialization_id: loaded.metadata.materialization_id,
          source_database_uuid: source_uuid,
          generation: generation,
          removed: length(stale_ids),
          next_after_document_id: List.last(stale_ids),
          has_more: length(stale_ids) == limit,
          last_sequence: effect.last_sequence
        })
      end
    end
  end

  @impl true
  def finish_derived_source_rebuild(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Store.update(adapter.store, &finish_rebuild_in_state(&1, request))
    end
  end

  @doc "Seeds derived metadata into a Memory store during create."
  @spec seed(map(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def seed(state, initial) when is_map(state) and is_map(initial) do
    materialization_id = MapAccess.get(initial, :materialization_id)
    name = MapAccess.get(initial, :name)
    definition_json = MapAccess.get(initial, :definition_json)
    definition_digest = MapAccess.get(initial, :definition_digest)
    sources = MapAccess.get(initial, :sources, [])
    enabled = MapAccess.get(initial, :enabled, false)
    status = MapAccess.get(initial, :status, "disabled")
    options_json = MapAccess.get(initial, :options_json) || "{}"

    with true <- is_binary(materialization_id) and is_binary(name) and is_binary(definition_json),
         true <- is_binary(definition_digest) and is_list(sources),
         {:ok, source_map} <- seed_sources(sources) do
      metadata = %{
        materialization_id: materialization_id,
        name: name,
        definition_json: definition_json,
        definition_digest: definition_digest,
        enabled: enabled == true,
        status: status_atom(status),
        options_json: options_json,
        last_error_code: nil
      }

      {:ok, %{state | derived_view: metadata, derived_sources: source_map}}
    else
      _ -> {:error, ElixirDB.Error.invalid_request("derived metadata is invalid")}
    end
  end

  defp seed_sources(sources) do
    sources
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, %{}}, fn {source, ordinal}, {:ok, acc} ->
      case Engine.source_uuid(source) do
        uuid when is_binary(uuid) ->
          entry = %{
            source_ordinal: ordinal,
            source_database_uuid: uuid,
            history_epoch: nil,
            checkpoint_sequence: 0,
            state: :pending,
            rebuild_generation: 0,
            rebuild_start_sequence: nil,
            rebuild_after_document_id: nil,
            rebuild_catchup_sequence: nil,
            last_error_code: nil
          }

          {:cont, {:ok, Map.put(acc, uuid, entry)}}

        _ ->
          {:halt, {:error, :invalid}}
      end
    end)
  end

  defp set_enabled_in_state(state, request) do
    with {:ok, metadata} <- fetch_metadata(state),
         :ok <- Engine.validate_materialization(metadata, request),
         {:ok, enabled} <- required_boolean(request, :enabled) do
      status = Engine.enabled_status(metadata, enabled)

      metadata = %{
        metadata
        | enabled: enabled,
          status: status_atom(status),
          last_error_code: nil
      }

      {:ok, %{state | derived_view: metadata},
       %{
         materialization_id: metadata.materialization_id,
         enabled: enabled,
         status: status_atom(status)
       }}
    end
  end

  defp set_source_error_in_state(state, request) do
    with {:ok, metadata} <- fetch_metadata(state),
         :ok <- Engine.validate_materialization(metadata, request),
         {:ok, source_uuid} <- required_uuid(request, :source_database_uuid),
         {:ok, source} <- fetch_source(state, source_uuid),
         {:ok, error_code} <- required_string(request, :error_code) do
      source = %{source | last_error_code: error_code}
      status = if metadata.enabled, do: :stale, else: :disabled
      metadata = %{metadata | status: status, last_error_code: error_code}

      state = %{
        state
        | derived_view: metadata,
          derived_sources: Map.put(state.derived_sources, source_uuid, source)
      }

      {:ok, state,
       %{
         materialization_id: metadata.materialization_id,
         source_database_uuid: source_uuid,
         last_error_code: error_code,
         status: status
       }}
    end
  end

  defp begin_rebuild_in_state(state, request) do
    with {:ok, metadata} <- fetch_metadata(state),
         :ok <- Engine.validate_materialization(metadata, request),
         {:ok, source_uuid} <- required_uuid(request, :source_database_uuid),
         {:ok, source} <- fetch_source(state, source_uuid),
         {:ok, start_sequence} <- required_non_negative(request, :start_sequence),
         {:ok, catchup_sequence} <-
           optional_non_negative(request, :catchup_sequence, start_sequence) do
      previous_checkpoint = source.checkpoint_sequence
      generation = source.rebuild_generation + 1

      source = %{
        source
        | history_epoch: nil,
          state: :rebuilding,
          rebuild_generation: generation,
          rebuild_start_sequence: start_sequence,
          rebuild_after_document_id: nil,
          rebuild_catchup_sequence: catchup_sequence,
          last_error_code: nil
      }

      metadata = %{metadata | status: :rebuilding, last_error_code: nil}

      {:ok,
       %{
         state
         | derived_view: metadata,
           derived_sources: Map.put(state.derived_sources, source_uuid, source)
       },
       %{
         materialization_id: metadata.materialization_id,
         source_database_uuid: source_uuid,
         generation: generation,
         start_sequence: start_sequence,
         catchup_sequence: catchup_sequence,
         previous_checkpoint_sequence: previous_checkpoint
       }}
    end
  end

  defp finish_rebuild_in_state(state, request) do
    with {:ok, metadata} <- fetch_metadata(state),
         :ok <- Engine.validate_materialization(metadata, request),
         {:ok, source_uuid} <- required_uuid(request, :source_database_uuid),
         {:ok, source} <- fetch_source(state, source_uuid),
         {:ok, generation} <- required_positive(request, :generation),
         :ok <- Engine.validate_rebuild_source(source, generation),
         {:ok, catchup_sequence} <- required_non_negative(request, :catchup_sequence),
         {:ok, history_epoch} <- required_string(request, :source_history_epoch),
         :ok <- Engine.validate_history_epoch(source, history_epoch) do
      checkpoint = max(source.checkpoint_sequence, catchup_sequence)

      source = %{
        source
        | history_epoch: history_epoch,
          checkpoint_sequence: checkpoint,
          state: :active,
          rebuild_start_sequence: nil,
          rebuild_after_document_id: nil,
          rebuild_catchup_sequence: nil,
          last_error_code: nil
      }

      state = %{state | derived_sources: Map.put(state.derived_sources, source_uuid, source)}
      state = maybe_mark_current(state)

      {:ok, state,
       %{
         materialization_id: metadata.materialization_id,
         source_database_uuid: source_uuid,
         generation: generation,
         checkpoint_sequence: checkpoint,
         status: :active
       }}
    end
  end

  defp persist_state(adapter, state, result) do
    case Store.update(adapter.store, fn _ -> {:ok, state, result} end) do
      {:ok, ^result} -> {:ok, result}
      {:ok, other} -> {:ok, other}
      {:error, _} = error -> error
    end
  end

  defp apply_batch_mode(
         _adapter,
         _context,
         state,
         loaded,
         source,
         source_uuid,
         _batch,
         :idempotent
       ) do
    source = %{source | last_error_code: nil}
    state = %{state | derived_sources: Map.put(state.derived_sources, source_uuid, source)}
    state = maybe_mark_current(state)

    {:ok, state,
     %{
       materialization_id: loaded.metadata.materialization_id,
       source_database_uuid: source_uuid,
       checkpoint_sequence: source.checkpoint_sequence,
       history_epoch: source.history_epoch,
       applied: false,
       last_sequence: 0,
       changed_rows: 0
     }}
  end

  defp apply_batch_mode(adapter, context, state, loaded, source, source_uuid, batch, :apply) do
    with {:ok, state, effect} <-
           apply_contribution_changes(
             adapter,
             context,
             state,
             loaded.definition,
             source,
             batch.rows,
             batch.removals,
             batch.rebuild_generation
           ) do
      source_state = if source.state == :rebuilding, do: :rebuilding, else: :active

      source = %{
        source
        | history_epoch: batch.history_epoch,
          checkpoint_sequence: batch.through,
          state: source_state,
          last_error_code: nil
      }

      state = %{state | derived_sources: Map.put(state.derived_sources, source_uuid, source)}
      state = maybe_mark_current(state)

      {:ok, state,
       %{
         materialization_id: loaded.metadata.materialization_id,
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
         context,
         state,
         definition,
         source,
         rows,
         removals,
         generation
       ) do
    source_uuid = source.source_database_uuid
    row_map = Map.new(rows, &{&1.source_document_id, &1})
    ids = Map.keys(row_map) ++ removals
    old_rows = fetch_contributions(state, source_uuid, ids)

    state = delete_contributions(state, source_uuid, removals)
    state = upsert_contributions(state, source_uuid, rows, generation)

    apply_derived_outputs(
      adapter,
      context,
      state,
      definition,
      source_uuid,
      old_rows,
      row_map,
      removals
    )
  end

  defp apply_derived_outputs(
         adapter,
         context,
         state,
         %{reducer: nil},
         source_uuid,
         old_rows,
         row_map,
         removals
       ) do
    changes = Engine.map_output_changes(source_uuid, old_rows, row_map, removals)

    with {:ok, state, last_sequence} <- apply_generated_changes(adapter, context, state, changes) do
      {:ok, state, %{last_sequence: last_sequence, changed_rows: length(changes)}}
    end
  end

  defp apply_derived_outputs(
         adapter,
         context,
         state,
         %{reducer: reducer},
         _source_uuid,
         old_rows,
         row_map,
         removals
       )
       when reducer in [:_count, :_sum, :_min, :_max, :_stats] do
    affected = Engine.affected_groups(old_rows, row_map, removals)

    Enum.reduce_while(Engine.sorted_groups(affected), {:ok, state, 0, 0}, fn {group_sort,
                                                                              group_json},
                                                                             {:ok, state, last,
                                                                              changed} ->
      case refresh_group(adapter, context, state, %{
             reducer: reducer,
             group_sort: group_sort,
             group_json: group_json,
             old_rows: old_rows,
             row_map: row_map,
             removals: removals
           }) do
        {:ok, state, sequence, did_change} ->
          {:cont, {:ok, state, max(last, sequence), changed + if(did_change, do: 1, else: 0)}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, state, last_sequence, changed} ->
        {:ok, state, %{last_sequence: last_sequence, changed_rows: changed}}

      {:error, _} = error ->
        error
    end
  end

  defp refresh_group(adapter, context, state, args) do
    %{
      reducer: reducer,
      group_sort: group_sort,
      group_json: group_json,
      old_rows: old_rows,
      row_map: row_map,
      removals: removals
    } = args

    with {:ok, group_key} <- decode_group_key(group_json),
         aggregate <- Map.get(state.derived_groups, group_sort) || Engine.empty_group(group_json),
         {:ok, aggregate} <- Engine.update_group(aggregate, group_sort, old_rows, row_map, removals),
         {:ok, group_id} <- Engine.group_document_id(group_key),
         {:ok, result} <- group_result(state, reducer, group_sort, aggregate) do
      persist_group_result(
        adapter,
        context,
        state,
        group_sort,
        group_json,
        group_key,
        group_id,
        result
      )
    end
  end

  defp group_result(_state, _reducer, _group_sort, %{count: 0}), do: {:ok, []}

  defp group_result(state, reducer, group_sort, aggregate) do
    with {:ok, aggregate} <- enrich_group(state, reducer, group_sort, aggregate),
         {:ok, output} <- Engine.reducer_output(reducer, aggregate) do
      {:ok, Map.put(aggregate, :output, output)}
    end
  end

  defp enrich_group(state, reducer, group_sort, aggregate)
       when reducer in [:_min, :_max, :_stats] do
    values =
      state.derived_rows
      |> Map.values()
      |> Enum.filter(&(&1.group_key_sort == group_sort and is_binary(&1.value_sort)))
      |> Enum.map(&{&1.value, &1.value_sort, &1.source_database_uuid, &1.source_document_id})

    with {:ok, extrema} <- Engine.extrema_from_values(values) do
      {:ok, Map.merge(aggregate, extrema)}
    end
  end

  defp enrich_group(_state, reducer, _group_sort, aggregate) when reducer in [:_count, :_sum],
    do: {:ok, aggregate}

  defp persist_group_result(adapter, context, state, group_sort, _json, _key, group_id, []) do
    state = %{state | derived_groups: Map.delete(state.derived_groups, group_sort)}

    with {:ok, state, sequence} <-
           apply_generated_change(adapter, context, state, {:delete, group_id}) do
      {:ok, state, sequence, sequence > 0}
    end
  end

  defp persist_group_result(
         adapter,
         context,
         state,
         group_sort,
         group_json,
         group_key,
         group_id,
         aggregate
       ) do
    group = Map.merge(aggregate, %{group_key_json: group_json, output_document_id: group_id})
    state = %{state | derived_groups: Map.put(state.derived_groups, group_sort, group)}

    with {:ok, state, sequence} <-
           apply_generated_change(
             adapter,
             context,
             state,
             {:put, group_id, Engine.group_body(group_key, aggregate.output)}
           ) do
      {:ok, state, sequence, sequence > 0}
    end
  end

  defp apply_generated_changes(adapter, context, state, changes) do
    Enum.reduce_while(changes, {:ok, state, 0}, fn change, {:ok, state, last} ->
      case apply_generated_change(adapter, context, state, change) do
        {:ok, state, sequence} -> {:cont, {:ok, state, max(last, sequence)}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp apply_generated_change(adapter, context, state, {:delete, document_id}) do
    doc = Store.find_document(state, document_id)

    if is_nil(doc) or doc.winning_deleted do
      {:ok, state, 0}
    else
      mutate(adapter, context, state, document_id, :delete, nil, doc)
    end
  end

  defp apply_generated_change(adapter, context, state, {:put, document_id, body}) do
    with {:ok, body_json} <- Canonical.encode(body) do
      doc = Store.find_document(state, document_id)

      if live_body_matches?(doc, body_json) do
        {:ok, state, 0}
      else
        mutate(adapter, context, state, document_id, :put, body, doc)
      end
    end
  end

  defp mutate(adapter, context, state, document_id, operation, body, document) do
    case persist_state(adapter, state, :sync) do
      {:ok, :sync} ->
        request = %{
          document_id: document_id,
          operation: operation,
          body: body,
          if_revision: current_revision(document)
        }

        case Mutations.apply_local_tx(context, request) do
          {:ok, %{sequence: sequence}} ->
            {:ok, Store.get(adapter.store), sequence}

          {:ok, _} ->
            {:ok, Store.get(adapter.store), 0}

          {:error, _} = error ->
            error
        end

      {:error, _} = error ->
        error
    end
  end

  defp live_body_matches?(%{winning_deleted: false, body: body}, body_json) when not is_nil(body) do
    case Canonical.encode(body) do
      {:ok, ^body_json} -> true
      _ -> false
    end
  end

  defp live_body_matches?(_document, _body_json), do: false

  defp current_revision(nil), do: nil
  defp current_revision(%{winning_revision: revision}), do: revision

  defp fetch_contributions(state, source_uuid, document_ids) do
    document_ids
    |> Enum.uniq()
    |> Enum.reduce(%{}, fn document_id, acc ->
      case Map.get(state.derived_rows, {source_uuid, document_id}) do
        nil -> acc
        row -> Map.put(acc, document_id, row)
      end
    end)
  end

  defp delete_contributions(state, source_uuid, removals) do
    rows =
      Enum.reduce(removals, state.derived_rows, fn document_id, acc ->
        Map.delete(acc, {source_uuid, document_id})
      end)

    %{state | derived_rows: rows}
  end

  defp upsert_contributions(state, source_uuid, rows, generation) do
    rows =
      Enum.reduce(rows, state.derived_rows, fn row, acc ->
        Map.put(acc, {source_uuid, row.source_document_id}, %{
          source_database_uuid: source_uuid,
          source_document_id: row.source_document_id,
          source_revision_id: row.source_revision_id,
          rebuild_generation: generation,
          key: row.key,
          key_json: row.key_json,
          key_sort: row.key_sort,
          group_key: row.group_key,
          group_key_json: row.group_key_json,
          group_key_sort: row.group_key_sort,
          value: row.value,
          value_json: row.value_json,
          value_sort: row.value_sort,
          value_present: row.value_present
        })
      end)

    %{state | derived_rows: rows}
  end

  defp stale_document_ids(state, source_uuid, generation, after_id, limit) do
    state.derived_rows
    |> Enum.flat_map(fn
      {{^source_uuid, document_id}, row} ->
        if row.rebuild_generation != generation and
             (is_nil(after_id) or document_id > after_id) do
          [document_id]
        else
          []
        end

      _ ->
        []
    end)
    |> Enum.sort()
    |> Enum.take(limit)
  end

  defp maybe_mark_current(state) do
    inactive? =
      Enum.any?(state.derived_sources, fn {_id, source} ->
        source.state != :active or not is_nil(source.last_error_code)
      end)

    if inactive? or is_nil(state.derived_view) do
      state
    else
      %{state | derived_view: %{state.derived_view | status: :current}}
    end
  end

  defp fetch_metadata(%{derived_view: nil}),
    do: {:error, ElixirDB.Error.integrity_violation("derived database metadata is missing")}

  defp fetch_metadata(%{derived_view: metadata}), do: {:ok, metadata}

  defp fetch_source(state, source_uuid) do
    case Map.fetch(state.derived_sources, source_uuid) do
      {:ok, source} ->
        {:ok, source}

      :error ->
        {:error, ElixirDB.Error.invalid_request("source is not registered with derived database")}
    end
  end

  defp decode_group_key(json) when is_binary(json) do
    case StrictDecoder.decode(json) do
      {:ok, value} when is_list(value) -> {:ok, value}
      _ -> {:error, ElixirDB.Error.integrity_violation("derived contribution key is invalid")}
    end
  end

  defp rebuild_limit(request) do
    limit = MapAccess.get(request, :limit, @default_rebuild_page_limit)
    maximum = ElixirDB.Config.host_limits()[:max_materialized_view_batch_documents] || 500

    cond do
      not is_integer(limit) or limit <= 0 ->
        {:error, ElixirDB.Error.invalid_request("derived rebuild page limit must be positive")}

      limit > maximum ->
        {:error, ElixirDB.Error.resource_limit("derived rebuild page limit exceeds the host limit")}

      true ->
        {:ok, limit}
    end
  end

  defp status_atom("disabled"), do: :disabled
  defp status_atom("rebuilding"), do: :rebuilding
  defp status_atom("current"), do: :current
  defp status_atom("stale"), do: :stale
  defp status_atom("resource_limit"), do: :resource_limit
  defp status_atom(atom) when is_atom(atom), do: atom
  defp status_atom(_), do: :unknown

  defp required_uuid(map, key) do
    with {:ok, value} <- Engine.required_string(map, key) do
      {:ok, String.downcase(value)}
    end
  end

  defp required_string(map, key), do: Engine.required_string(map, key)

  defp optional_string(map, key) do
    case MapAccess.get(map, key) do
      nil -> {:ok, nil}
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, ElixirDB.Error.invalid_request("#{key} must be a non-empty string")}
    end
  end

  defp required_non_negative(map, key), do: Engine.required_non_negative(map, key)

  defp required_positive(map, key) do
    case MapAccess.get(map, key) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _ -> {:error, ElixirDB.Error.invalid_request("#{key} must be a positive integer")}
    end
  end

  defp optional_non_negative(map, key, default) do
    case MapAccess.get(map, key) do
      nil -> {:ok, default}
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _ -> {:error, ElixirDB.Error.invalid_request("#{key} must be a non-negative integer")}
    end
  end

  defp required_boolean(map, key) do
    case MapAccess.get(map, key) do
      value when is_boolean(value) -> {:ok, value}
      _ -> {:error, ElixirDB.Error.invalid_request("#{key} must be a boolean")}
    end
  end
end
