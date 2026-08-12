defmodule ElixirDB.Storage.Services.DerivedViews do
  @moduledoc """
  Shared derived-materialization orchestration over the derived-state port.

  Owns enable/error transitions, incremental apply, rebuild begin/page/prune/
  finish, contribution diffs, grouping, reducers, numeric extremes, generated
  document outputs, and checkpoint/status transitions. Physical backends only
  load facts and apply atomic effects through `Ports.DerivedState`.
  """

  alias ElixirDB.DerivedView.Engine
  alias ElixirDB.JSON.Canonical
  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Access
  alias ElixirDB.Storage.Services.{Facts, Mutations}
  alias ElixirDB.Storage.Transaction

  @doc "Loads derived view metadata and definition."
  @spec get(BackendContext.t()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def get(%BackendContext{} = context) do
    with {:ok, metadata} <- port(context).get_derived_metadata(context),
         {:ok, definition} <- Engine.decode_definition(metadata.definition_json) do
      {:ok, Map.put(metadata, :definition, definition)}
    end
  end

  @doc "Enables or disables derived materialization."
  @spec set_enabled(BackendContext.t(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def set_enabled(%BackendContext{} = context, request) when is_map(request) do
    Transaction.run(context, fn tx ->
      with {:ok, metadata} <- port(tx).get_derived_metadata(tx),
           :ok <- Engine.validate_materialization(metadata, request),
           {:ok, enabled} <- Engine.required_boolean(request, :enabled) do
        status = Engine.enabled_status(metadata, enabled)

        port(tx).put_derived_enabled(tx, %{
          enabled: enabled,
          status: status
        })
      end
    end)
  end

  def set_enabled(_context, _request),
    do: {:error, ElixirDB.Error.invalid_request("derived enable request must be an object")}

  @doc "Lists derived source checkpoints."
  @spec list_sources(BackendContext.t()) :: {:ok, [map()]} | {:error, ElixirDB.Error.t()}
  def list_sources(%BackendContext{} = context),
    do: port(context).list_derived_sources(context)

  @doc "Records a derived source error."
  @spec set_source_error(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def set_source_error(%BackendContext{} = context, request) when is_map(request) do
    Transaction.run(context, fn tx ->
      with {:ok, metadata} <- port(tx).get_derived_metadata(tx),
           :ok <- Engine.validate_materialization(metadata, request),
           {:ok, source_uuid} <- Engine.required_uuid(request, :source_database_uuid),
           {:ok, _source} <- port(tx).get_derived_source(tx, source_uuid),
           {:ok, error_code} <- Engine.required_string(request, :error_code) do
        port(tx).put_derived_source_error(tx, %{
          materialization_id: metadata.materialization_id,
          source_database_uuid: source_uuid,
          error_code: error_code,
          enabled: metadata.enabled
        })
      end
    end)
  end

  def set_source_error(_context, _request),
    do: {:error, ElixirDB.Error.invalid_request("derived source error must be an object")}

  @doc "Applies one incremental derived source batch."
  @spec apply_source_batch(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def apply_source_batch(%BackendContext{} = context, request) when is_map(request) do
    fault = derived_fault(context)

    Transaction.run(context, fn tx ->
      tx = put_derived_fault(tx, fault)

      with {:ok, loaded} <- load_context(tx, request),
           {:ok, source_uuid} <- Engine.required_uuid(request, :source_database_uuid),
           {:ok, source} <- port(tx).get_derived_source(tx, source_uuid),
           {:ok, batch} <- Engine.normalize_batch(request, loaded.definition, source),
           :ok <- Engine.validate_history_epoch(source, batch.history_epoch),
           {:ok, mode} <- Engine.validate_batch_cursor(source, batch.expected, batch.through) do
        apply_batch_mode(tx, loaded, source, source_uuid, batch, mode)
      end
    end)
  end

  def apply_source_batch(_context, _request),
    do: {:error, ElixirDB.Error.invalid_request("derived source batch must be an object")}

  @doc "Begins a derived source rebuild."
  @spec begin_source_rebuild(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def begin_source_rebuild(%BackendContext{} = context, request) when is_map(request) do
    Transaction.run(context, fn tx ->
      with {:ok, metadata} <- port(tx).get_derived_metadata(tx),
           :ok <- Engine.validate_materialization(metadata, request),
           {:ok, source_uuid} <- Engine.required_uuid(request, :source_database_uuid),
           {:ok, source} <- port(tx).get_derived_source(tx, source_uuid),
           {:ok, start_sequence} <- Engine.required_non_negative(request, :start_sequence),
           {:ok, catchup_sequence} <-
             Engine.optional_non_negative(request, :catchup_sequence, start_sequence) do
        generation = source.rebuild_generation + 1

        port(tx).put_derived_rebuild_begin(tx, %{
          materialization_id: metadata.materialization_id,
          source_database_uuid: source_uuid,
          generation: generation,
          start_sequence: start_sequence,
          catchup_sequence: catchup_sequence,
          previous_checkpoint_sequence: source.checkpoint_sequence
        })
      end
    end)
  end

  def begin_source_rebuild(_context, _request),
    do: {:error, ElixirDB.Error.invalid_request("derived rebuild request must be an object")}

  @doc "Applies one derived rebuild page."
  @spec apply_rebuild_page(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def apply_rebuild_page(%BackendContext{} = context, request) when is_map(request) do
    fault = derived_fault(context)

    Transaction.run(context, fn tx ->
      tx = put_derived_fault(tx, fault)

      with {:ok, loaded} <- load_context(tx, request),
           {:ok, source_uuid} <- Engine.required_uuid(request, :source_database_uuid),
           {:ok, source} <- port(tx).get_derived_source(tx, source_uuid),
           {:ok, generation} <- Engine.required_positive(request, :generation),
           :ok <- Engine.validate_rebuild_source(source, generation),
           {:ok, batch} <- Engine.normalize_rebuild_batch(request, loaded.definition, source),
           {:ok, effect} <-
             apply_contribution_changes(
               tx,
               loaded.definition,
               source,
               batch.rows,
               batch.removals,
               generation
             ),
           {:ok, cursor} <- Engine.optional_string(request, :after_document_id),
           {:ok, catchup_sequence} <-
             Engine.optional_non_negative(
               request,
               :catchup_sequence,
               source.rebuild_catchup_sequence || 0
             ),
           :ok <-
             port(tx).put_derived_rebuild_cursor(tx, %{
               source_database_uuid: source_uuid,
               after_document_id: cursor,
               catchup_sequence: catchup_sequence
             }) do
        {:ok,
         %{
           materialization_id: loaded.metadata.materialization_id,
           source_database_uuid: source_uuid,
           generation: generation,
           after_document_id: cursor,
           catchup_sequence: catchup_sequence,
           last_sequence: effect.last_sequence,
           changed_rows: effect.changed_rows
         }}
      end
    end)
  end

  def apply_rebuild_page(_context, _request),
    do: {:error, ElixirDB.Error.invalid_request("derived rebuild page must be an object")}

  @doc "Prunes one page of stale rebuild contributions."
  @spec prune_rebuild_stale_page(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def prune_rebuild_stale_page(%BackendContext{} = context, request) when is_map(request) do
    fault = derived_fault(context)

    Transaction.run(context, fn tx ->
      tx = put_derived_fault(tx, fault)

      with {:ok, loaded} <- load_context(tx, request),
           {:ok, source_uuid} <- Engine.required_uuid(request, :source_database_uuid),
           {:ok, source} <- port(tx).get_derived_source(tx, source_uuid),
           {:ok, generation} <- Engine.required_positive(request, :generation),
           :ok <- Engine.validate_rebuild_source(source, generation),
           {:ok, after_id} <- Engine.optional_string(request, :after_document_id),
           {:ok, limit} <- Engine.rebuild_page_limit(request),
           {:ok, stale_ids} <-
             port(tx).list_stale_contribution_ids(tx, %{
               source_database_uuid: source_uuid,
               generation: generation,
               after_document_id: after_id,
               limit: limit
             }),
           {:ok, effect} <-
             apply_contribution_changes(tx, loaded.definition, source, [], stale_ids, generation) do
        {:ok,
         %{
           materialization_id: loaded.metadata.materialization_id,
           source_database_uuid: source_uuid,
           generation: generation,
           removed: length(stale_ids),
           next_after_document_id: List.last(stale_ids),
           has_more: length(stale_ids) == limit,
           last_sequence: effect.last_sequence
         }}
      end
    end)
  end

  def prune_rebuild_stale_page(_context, _request),
    do: {:error, ElixirDB.Error.invalid_request("derived stale-prune page must be an object")}

  @doc "Finishes a derived source rebuild."
  @spec finish_source_rebuild(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def finish_source_rebuild(%BackendContext{} = context, request) when is_map(request) do
    Transaction.run(context, fn tx ->
      with {:ok, metadata} <- port(tx).get_derived_metadata(tx),
           :ok <- Engine.validate_materialization(metadata, request),
           {:ok, source_uuid} <- Engine.required_uuid(request, :source_database_uuid),
           {:ok, source} <- port(tx).get_derived_source(tx, source_uuid),
           {:ok, generation} <- Engine.required_positive(request, :generation),
           :ok <- Engine.validate_rebuild_source(source, generation),
           {:ok, catchup_sequence} <- Engine.required_non_negative(request, :catchup_sequence),
           {:ok, history_epoch} <- Engine.required_string(request, :source_history_epoch),
           :ok <- Engine.validate_history_epoch(source, history_epoch),
           {:ok, result} <-
             port(tx).put_derived_rebuild_finish(tx, %{
               materialization_id: metadata.materialization_id,
               source_database_uuid: source_uuid,
               generation: generation,
               catchup_sequence: catchup_sequence,
               source_history_epoch: history_epoch,
               checkpoint_sequence: max(source.checkpoint_sequence, catchup_sequence)
             }),
           :ok <- port(tx).refresh_derived_status(tx) do
        {:ok, result}
      end
    end)
  end

  def finish_source_rebuild(_context, _request),
    do: {:error, ElixirDB.Error.invalid_request("derived rebuild finish must be an object")}

  defp apply_batch_mode(tx, loaded, source, source_uuid, _batch, :idempotent) do
    with :ok <- port(tx).clear_derived_source_error(tx, source_uuid),
         :ok <- port(tx).refresh_derived_status(tx) do
      {:ok,
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
  end

  defp apply_batch_mode(tx, loaded, source, source_uuid, batch, :apply) do
    with {:ok, effect} <-
           apply_contribution_changes(
             tx,
             loaded.definition,
             source,
             batch.rows,
             batch.removals,
             batch.rebuild_generation
           ),
         :ok <-
           port(tx).put_derived_source_checkpoint(tx, %{
             source_database_uuid: source_uuid,
             history_epoch: batch.history_epoch,
             checkpoint_sequence: batch.through,
             previous_history_epoch: source.history_epoch,
             state: if(source.state == :rebuilding, do: :rebuilding, else: :active)
           }),
         :ok <- port(tx).refresh_derived_status(tx) do
      {:ok,
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

  defp apply_contribution_changes(tx, definition, source, rows, removals, generation) do
    source_uuid = source.source_database_uuid
    row_map = Map.new(rows, &{&1.source_document_id, &1})
    document_ids = Map.keys(row_map) ++ removals

    with {:ok, old_rows} <- port(tx).fetch_contributions(tx, source_uuid, document_ids),
         :ok <- port(tx).delete_contributions(tx, source_uuid, removals),
         :ok <-
           maybe_upsert_contributions(tx, source_uuid, rows, generation),
         do: apply_derived_outputs(tx, definition, source_uuid, old_rows, row_map, removals)
  end

  defp maybe_upsert_contributions(_tx, _source_uuid, [], _generation), do: :ok

  defp maybe_upsert_contributions(tx, source_uuid, rows, generation) do
    with :ok <- Engine.derived_fault_check(derived_fault(tx), :derived_upsert_row) do
      port(tx).upsert_contributions(tx, source_uuid, rows, generation)
    end
  end

  defp apply_derived_outputs(tx, %{reducer: nil}, source_uuid, old_rows, row_map, removals) do
    changes = Engine.map_output_changes(source_uuid, old_rows, row_map, removals)

    with {:ok, last_sequence} <- apply_generated_changes(tx, changes) do
      {:ok, %{last_sequence: last_sequence, changed_rows: length(changes)}}
    end
  end

  defp apply_derived_outputs(
         tx,
         %{reducer: reducer},
         _source_uuid,
         old_rows,
         row_map,
         removals
       )
       when reducer in [:_count, :_sum, :_min, :_max, :_stats] do
    affected = Engine.affected_groups(old_rows, row_map, removals)

    Enum.reduce_while(Engine.sorted_groups(affected), {:ok, 0, 0}, fn {group_sort, group_json},
                                                                      {:ok, last_sequence, changed} ->
      case refresh_group(tx, reducer, group_sort, group_json, old_rows, row_map, removals) do
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

  defp refresh_group(tx, reducer, group_sort, group_json, old_rows, row_map, removals) do
    with {:ok, group_key} <- decode_group_key(group_json),
         {:ok, aggregate} <- port(tx).fetch_group(tx, group_sort, group_json),
         {:ok, aggregate} <-
           Engine.update_group(aggregate, group_sort, old_rows, row_map, removals),
         {:ok, group_id} <- Engine.group_document_id(group_key),
         {:ok, result} <- group_result(tx, reducer, group_sort, aggregate) do
      persist_group_result(tx, group_sort, group_json, group_key, group_id, result)
    end
  end

  defp group_result(_tx, _reducer, _group_sort, %{count: 0}), do: {:ok, []}

  defp group_result(tx, reducer, group_sort, aggregate) do
    with {:ok, aggregate} <- enrich_group(tx, reducer, group_sort, aggregate),
         {:ok, output} <- Engine.reducer_output(reducer, aggregate) do
      {:ok, Map.put(aggregate, :output, output)}
    end
  end

  defp enrich_group(tx, reducer, group_sort, aggregate)
       when reducer in [:_min, :_max, :_stats] do
    with {:ok, values} <- port(tx).list_group_numeric_values(tx, group_sort),
         {:ok, extrema} <- Engine.extrema_from_values(values) do
      {:ok, Map.merge(aggregate, extrema)}
    end
  end

  defp enrich_group(_tx, reducer, _group_sort, aggregate) when reducer in [:_count, :_sum],
    do: {:ok, aggregate}

  defp persist_group_result(tx, group_sort, _group_json, _group_key, group_id, []) do
    with :ok <- port(tx).delete_group(tx, group_sort),
         {:ok, sequence} <- apply_generated_change(tx, {:delete, group_id}) do
      {:ok, sequence, sequence > 0}
    end
  end

  defp persist_group_result(tx, group_sort, group_json, group_key, group_id, aggregate)
       when is_map(aggregate) do
    with :ok <-
           port(tx).upsert_group(tx, %{
             group_key_sort: group_sort,
             group_key_json: group_json,
             group_id: group_id,
             aggregate: aggregate
           }),
         {:ok, sequence} <-
           apply_generated_change(
             tx,
             {:put, group_id, Engine.group_body(group_key, aggregate.output)}
           ) do
      {:ok, sequence, sequence > 0}
    end
  end

  defp apply_generated_changes(tx, changes) do
    Enum.reduce_while(changes, {:ok, 0}, fn change, {:ok, last_sequence} ->
      case apply_generated_change(tx, change) do
        {:ok, sequence} -> {:cont, {:ok, max(last_sequence, sequence)}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp apply_generated_change(tx, {:delete, document_id}) do
    with {:ok, document} <- Facts.find_document(tx, document_id) do
      if is_nil(document) or document.winning_deleted do
        {:ok, 0}
      else
        apply_generated_mutation(tx, document_id, :delete, nil, document)
      end
    end
  end

  defp apply_generated_change(tx, {:put, document_id, body}) do
    with {:ok, body_json} <- Canonical.encode(body),
         {:ok, document} <- Facts.find_document(tx, document_id) do
      if live_body_matches?(document, body_json),
        do: {:ok, 0},
        else: apply_generated_mutation(tx, document_id, :put, body, document)
    end
  end

  defp apply_generated_mutation(tx, document_id, operation, body, document) do
    request = %{
      document_id: document_id,
      operation: operation,
      body: body,
      if_revision: current_revision(document)
    }

    with :ok <- Engine.derived_fault_check(derived_fault(tx), :derived_generated_mutation) do
      case Mutations.apply_local_tx(tx, request) do
        {:ok, %{sequence: sequence}} -> {:ok, sequence}
        {:ok, _result} -> {:ok, 0}
        {:error, _} = error -> error
      end
    end
  end

  defp live_body_matches?(%{winning_deleted: false, body: body}, body_json)
       when not is_nil(body) do
    match?({:ok, ^body_json}, Canonical.encode(body))
  end

  defp live_body_matches?(_document, _body_json), do: false

  defp current_revision(nil), do: nil
  defp current_revision(%{winning_revision: revision}), do: revision

  defp decode_group_key(json) when is_binary(json) do
    case StrictDecoder.decode(json) do
      {:ok, value} when is_list(value) -> {:ok, value}
      _ -> {:error, ElixirDB.Error.integrity_violation("derived group key is invalid")}
    end
  end

  defp load_context(tx, request) do
    with {:ok, metadata} <- port(tx).get_derived_metadata(tx),
         :ok <- Engine.validate_materialization(metadata, request),
         {:ok, definition} <- Engine.decode_definition(metadata.definition_json) do
      {:ok, %{metadata: metadata, definition: definition}}
    end
  end

  defp derived_fault(%BackendContext{identity: identity}) when is_map(identity),
    do: Map.get(identity, :derived_fault)

  defp derived_fault(_), do: nil

  defp put_derived_fault(%BackendContext{} = context, fun) when is_function(fun, 1) do
    %{context | identity: Map.put(context.identity || %{}, :derived_fault, fun)}
  end

  defp put_derived_fault(%BackendContext{} = context, _), do: context

  defp port(%BackendContext{} = context), do: Access.port(context, :derived_state)
end
