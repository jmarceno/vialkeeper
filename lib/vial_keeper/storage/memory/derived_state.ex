defmodule VialKeeper.Storage.Memory.DerivedState do
  @moduledoc """
  Memory derived-state fact and effect port.

  Persists metadata, sources, contributions, and groups. Product apply/rebuild
  orchestration lives in `VialKeeper.Storage.Services.DerivedViews`.
  """
  @behaviour VialKeeper.Storage.Ports.DerivedState

  alias VialKeeper.DerivedView.{Engine, RebuildState}
  alias VialKeeper.MapAccess
  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.Memory.{Context, Store}

  @impl true
  def get_derived_metadata(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      fetch_metadata(Store.get(adapter.store))
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
  def get_derived_source(%BackendContext{} = context, source_uuid)
      when is_binary(source_uuid) do
    with {:ok, adapter} <- Context.unwrap(context) do
      fetch_source(Store.get(adapter.store), source_uuid)
    end
  end

  @impl true
  def put_derived_enabled(%BackendContext{} = context, effect) when is_map(effect) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Store.update(adapter.store, fn state ->
        metadata = state.derived_view

        next = %{
          state
          | derived_view: %{
              metadata
              | enabled: effect.enabled,
                status: Engine.status_atom(effect.status),
                last_error_code: nil
            }
        }

        {:ok, next,
         %{
           materialization_id: metadata.materialization_id,
           enabled: effect.enabled,
           status: Engine.status_atom(effect.status)
         }}
      end)
    end
  end

  @impl true
  def put_derived_source_error(%BackendContext{} = context, effect) when is_map(effect) do
    with {:ok, adapter} <- Context.unwrap(context) do
      adapter.store
      |> Store.update(&put_derived_source_error_update(&1, effect))
      |> normalize_ok()
    end
  end

  @impl true
  def put_derived_rebuild_begin(%BackendContext{} = context, effect) when is_map(effect) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Store.update(adapter.store, fn state ->
        source_uuid = effect.source_database_uuid
        source = Map.fetch!(state.derived_sources, source_uuid)

        source = %{
          source
          | history_epoch: nil,
            state: :rebuilding,
            rebuild_generation: effect.generation,
            rebuild_start_sequence: effect.start_sequence,
            rebuild_after_document_id: nil,
            rebuild_catchup_sequence: effect.catchup_sequence,
            last_error_code: nil
        }

        next = %{
          state
          | derived_sources: Map.put(state.derived_sources, source_uuid, source),
            derived_view: %{state.derived_view | status: :rebuilding, last_error_code: nil}
        }

        {:ok, next, RebuildState.new(effect)}
      end)
    end
  end

  @impl true
  def put_derived_rebuild_cursor(%BackendContext{} = context, effect) when is_map(effect) do
    with {:ok, adapter} <- Context.unwrap(context) do
      adapter.store
      |> Store.update(&put_derived_rebuild_cursor_update(&1, effect))
      |> normalize_ok()
    end
  end

  @impl true
  def put_derived_rebuild_finish(%BackendContext{} = context, effect) when is_map(effect) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Store.update(adapter.store, fn state ->
        source_uuid = effect.source_database_uuid
        source = Map.fetch!(state.derived_sources, source_uuid)

        source = %{
          source
          | history_epoch: effect.source_history_epoch,
            checkpoint_sequence: effect.checkpoint_sequence,
            state: :active,
            rebuild_start_sequence: nil,
            rebuild_after_document_id: nil,
            rebuild_catchup_sequence: nil,
            last_error_code: nil
        }

        next = %{state | derived_sources: Map.put(state.derived_sources, source_uuid, source)}

        {:ok, next,
         %{
           materialization_id: effect.materialization_id,
           source_database_uuid: source_uuid,
           generation: effect.generation,
           checkpoint_sequence: effect.checkpoint_sequence,
           status: :active
         }}
      end)
    end
  end

  @impl true
  def put_derived_source_checkpoint(%BackendContext{} = context, effect) when is_map(effect) do
    with {:ok, adapter} <- Context.unwrap(context) do
      adapter.store
      |> Store.update(&put_derived_source_checkpoint_update(&1, effect))
      |> normalize_ok()
    end
  end

  @impl true
  def clear_derived_source_error(%BackendContext{} = context, source_uuid)
      when is_binary(source_uuid) do
    with {:ok, adapter} <- Context.unwrap(context) do
      adapter.store
      |> Store.update(&clear_derived_source_error_update(&1, source_uuid))
      |> normalize_ok()
    end
  end

  @impl true
  def refresh_derived_status(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      adapter.store
      |> Store.update(fn state -> {:ok, maybe_mark_current(state), :ok} end)
      |> normalize_ok()
    end
  end

  @impl true
  def fetch_contributions(%BackendContext{} = context, source_uuid, document_ids)
      when is_binary(source_uuid) and is_list(document_ids) do
    with {:ok, adapter} <- Context.unwrap(context) do
      state = Store.get(adapter.store)

      contributions = fetch_contributions_from_state(state, source_uuid, document_ids)

      {:ok, contributions}
    end
  end

  @impl true
  def delete_contributions(%BackendContext{} = context, source_uuid, document_ids)
      when is_binary(source_uuid) and is_list(document_ids) do
    with {:ok, adapter} <- Context.unwrap(context) do
      adapter.store
      |> Store.update(&delete_contributions_update(&1, source_uuid, document_ids))
      |> normalize_ok()
    end
  end

  @impl true
  def upsert_contributions(%BackendContext{} = context, source_uuid, rows, generation)
      when is_binary(source_uuid) and is_list(rows) and is_integer(generation) do
    with {:ok, adapter} <- Context.unwrap(context) do
      adapter.store
      |> Store.update(&upsert_contributions_update(&1, source_uuid, rows, generation))
      |> normalize_ok()
    end
  end

  @impl true
  def list_stale_contribution_ids(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context) do
      state = Store.get(adapter.store)
      source_uuid = request.source_database_uuid
      generation = request.generation
      after_id = request.after_document_id
      limit = request.limit

      ids =
        state.derived_rows
        |> stale_contribution_ids(source_uuid, generation, after_id)
        |> Enum.sort()
        |> Enum.take(limit)

      {:ok, ids}
    end
  end

  @impl true
  def fetch_group(%BackendContext{} = context, group_sort, group_json)
      when is_binary(group_sort) and is_binary(group_json) do
    with {:ok, adapter} <- Context.unwrap(context) do
      state = Store.get(adapter.store)

      case Map.get(state.derived_groups, group_sort) do
        nil -> {:ok, Engine.empty_group(group_json)}
        aggregate -> {:ok, aggregate}
      end
    end
  end

  @impl true
  def upsert_group(%BackendContext{} = context, effect) when is_map(effect) do
    with {:ok, adapter} <- Context.unwrap(context) do
      adapter.store
      |> Store.update(&upsert_group_update(&1, effect))
      |> normalize_ok()
    end
  end

  @impl true
  def delete_group(%BackendContext{} = context, group_sort) when is_binary(group_sort) do
    with {:ok, adapter} <- Context.unwrap(context) do
      adapter.store
      |> Store.update(&delete_group_update(&1, group_sort))
      |> normalize_ok()
    end
  end

  @impl true
  def list_group_numeric_values(%BackendContext{} = context, group_sort)
      when is_binary(group_sort) do
    with {:ok, adapter} <- Context.unwrap(context) do
      values =
        Store.get(adapter.store).derived_rows
        |> Map.values()
        |> Enum.filter(fn row ->
          row.group_key_sort == group_sort and is_binary(row.value_sort)
        end)
        # Intentionally unordered: product extrema must not depend on map iteration.
        |> Enum.map(fn row ->
          {row.value, row.value_sort, row.source_database_uuid, row.source_document_id}
        end)

      {:ok, values}
    end
  end

  @doc "Seeds derived metadata into a Memory store during create."
  @spec seed(map(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def seed(state, initial) when is_map(state) and is_map(initial) do
    materialization_id = MapAccess.get(initial, :materialization_id)
    name = MapAccess.get(initial, :name)
    definition_json = MapAccess.get(initial, :definition_json)
    definition_digest = MapAccess.get(initial, :definition_digest)
    options_json = MapAccess.get(initial, :options_json)
    enabled = MapAccess.get(initial, :enabled, true)
    status = MapAccess.get(initial, :status, "rebuilding")
    sources = MapAccess.get(initial, :sources, [])

    with true <- is_binary(materialization_id) and materialization_id != "",
         true <- is_binary(name) and name != "",
         true <- is_binary(definition_json) and definition_json != "",
         true <- is_binary(definition_digest) and definition_digest != "",
         {:ok, seeded_sources} <- seed_sources(sources) do
      {:ok,
       %{
         state
         | derived_view: %{
             materialization_id: materialization_id,
             name: name,
             definition_json: definition_json,
             definition_digest: definition_digest,
             options_json: options_json,
             enabled: enabled == true,
             status: Engine.status_atom(status),
             last_error_code: nil
           },
           derived_sources: seeded_sources,
           derived_rows: %{},
           derived_groups: %{}
       }}
    else
      {:error, _} = error ->
        error

      _ ->
        {:error, VialKeeper.Error.invalid_request("derived metadata is invalid")}
    end
  end

  defp seed_sources(sources) when is_list(sources) do
    sources
    |> Enum.with_index(1)
    |> Enum.reduce_while({:ok, %{}}, fn {source, ordinal}, {:ok, acc} ->
      case Engine.source_uuid(source) do
        uuid when is_binary(uuid) and uuid != "" ->
          {:cont,
           {:ok,
            Map.put(acc, uuid, %{
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
            })}}

        _ ->
          {:halt, {:error, VialKeeper.Error.invalid_request("derived source uuid is invalid")}}
      end
    end)
  end

  defp seed_sources(_),
    do: {:error, VialKeeper.Error.invalid_request("derived sources are invalid")}

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
    do: {:error, VialKeeper.Error.integrity_violation("derived database metadata is missing")}

  defp fetch_metadata(%{derived_view: metadata}), do: {:ok, metadata}

  defp fetch_source(state, source_uuid) do
    case Map.get(state.derived_sources, source_uuid) do
      nil ->
        {:error, VialKeeper.Error.invalid_request("source is not registered with derived database")}

      source ->
        {:ok, source}
    end
  end

  defp put_derived_source_error_update(state, effect) do
    source_uuid = effect.source_database_uuid
    source = Map.fetch!(state.derived_sources, source_uuid)
    status = if effect.enabled, do: :stale, else: :disabled

    next = %{
      state
      | derived_sources:
          Map.put(state.derived_sources, source_uuid, %{
            source
            | last_error_code: effect.error_code
          }),
        derived_view: %{
          state.derived_view
          | status: status,
            last_error_code: effect.error_code
        }
    }

    {:ok, next,
     %{
       materialization_id: effect.materialization_id,
       source_database_uuid: source_uuid,
       last_error_code: effect.error_code,
       status: status
     }}
  end

  defp put_derived_rebuild_cursor_update(state, effect) do
    source_uuid = effect.source_database_uuid
    source = Map.fetch!(state.derived_sources, source_uuid)

    source = %{
      source
      | rebuild_after_document_id: effect.after_document_id,
        rebuild_catchup_sequence: effect.catchup_sequence,
        last_error_code: nil
    }

    {:ok, %{state | derived_sources: Map.put(state.derived_sources, source_uuid, source)}, :ok}
  end

  defp put_derived_source_checkpoint_update(state, effect) do
    source_uuid = effect.source_database_uuid
    source = Map.fetch!(state.derived_sources, source_uuid)

    source = %{
      source
      | history_epoch: effect.history_epoch,
        checkpoint_sequence: effect.checkpoint_sequence,
        state: Map.get(effect, :state, :active),
        last_error_code: nil
    }

    {:ok, %{state | derived_sources: Map.put(state.derived_sources, source_uuid, source)}, :ok}
  end

  defp clear_derived_source_error_update(state, source_uuid) do
    source = Map.fetch!(state.derived_sources, source_uuid)
    source = %{source | last_error_code: nil}

    {:ok, %{state | derived_sources: Map.put(state.derived_sources, source_uuid, source)}, :ok}
  end

  defp fetch_contributions_from_state(state, source_uuid, document_ids) do
    rows = state.derived_rows

    document_ids
    |> Enum.uniq()
    |> Enum.reduce(%{}, fn document_id, acc ->
      put_contribution_row(acc, rows, source_uuid, document_id)
    end)
  end

  defp put_contribution_row(acc, rows, source_uuid, document_id) do
    case Map.get(rows, {source_uuid, document_id}) do
      nil -> acc
      row -> Map.put(acc, document_id, row)
    end
  end

  defp delete_contributions_update(state, source_uuid, document_ids) do
    rows =
      Enum.reduce(document_ids, state.derived_rows, fn document_id, acc ->
        Map.delete(acc, {source_uuid, document_id})
      end)

    {:ok, %{state | derived_rows: rows}, :ok}
  end

  defp upsert_contributions_update(state, source_uuid, rows, generation) do
    next_rows =
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

    {:ok, %{state | derived_rows: next_rows}, :ok}
  end

  defp stale_contribution_ids(rows, source_uuid, generation, after_id) do
    Enum.flat_map(rows, fn
      {{^source_uuid, document_id}, row} ->
        if stale_contribution?(row, generation, after_id, document_id),
          do: [document_id],
          else: []

      _ ->
        []
    end)
  end

  defp stale_contribution?(row, generation, after_id, document_id) do
    row.rebuild_generation != generation and
      (is_nil(after_id) or document_id > after_id)
  end

  defp upsert_group_update(state, effect) do
    aggregate =
      effect.aggregate
      |> Map.put(:group_key_json, effect.group_key_json)
      |> Map.put(:output_document_id, effect.group_id)

    {:ok,
     %{
       state
       | derived_groups: Map.put(state.derived_groups, effect.group_key_sort, aggregate)
     }, :ok}
  end

  defp delete_group_update(state, group_sort) do
    {:ok, %{state | derived_groups: Map.delete(state.derived_groups, group_sort)}, :ok}
  end

  defp normalize_ok({:ok, :ok}), do: :ok
  defp normalize_ok({:ok, value}), do: {:ok, value}
  defp normalize_ok({:error, _} = error), do: error
end
