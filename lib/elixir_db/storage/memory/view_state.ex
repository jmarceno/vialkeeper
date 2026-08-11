defmodule ElixirDB.Storage.Memory.ViewState do
  @moduledoc """
  Memory view-state fact port.

  Persists view definitions, generation state, and rows in the Memory store.
  Product CAS, rebuild orchestration, query planning, and result shaping live
  in `ElixirDB.Storage.Services.Views`. Row scans shuffle before filter and sort
  so query order does not depend on map iteration.
  """
  @behaviour ElixirDB.Storage.Ports.ViewState

  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.MapAccess
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Memory.{Context, Store}
  alias ElixirDB.UUID
  alias ElixirDB.View.{Engine, KeyCodec}

  @impl true
  def list_views(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      views =
        Store.get(adapter.store).views
        |> Map.values()
        |> Enum.sort_by(& &1.name)
        |> Enum.map(&list_entry/1)

      {:ok, views}
    end
  end

  @impl true
  def find_view_by_name(%BackendContext{} = context, name) when is_binary(name) do
    with {:ok, adapter} <- Context.unwrap(context) do
      existing = find_view_entry_by_name(Store.get(adapter.store).views, name)
      {:ok, existing}
    end
  end

  @impl true
  def count_views(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      {:ok, map_size(Store.get(adapter.store).views)}
    end
  end

  @impl true
  def get_view_definition(%BackendContext{} = context, view_id) when is_binary(view_id) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, view} <- fetch_view(Store.get(adapter.store), view_id) do
      {:ok,
       %{
         view_id: view.view_id,
         name: view.name,
         definition_json: view.definition_json,
         definition_digest: view.definition_digest,
         created_at: view.created_at
       }}
    end
  end

  @impl true
  def get_view_state(%BackendContext{} = context, view_id) when is_binary(view_id) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, view} <- fetch_view(Store.get(adapter.store), view_id) do
      {:ok, state_result(view)}
    end
  end

  @impl true
  def insert_view(%BackendContext{} = context, normalized) when is_map(normalized) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Store.update(adapter.store, fn state ->
        insert_view_in_state(state, normalized)
      end)
    end
  end

  @impl true
  def delete_view(%BackendContext{} = context, view_id) when is_binary(view_id) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Store.update(adapter.store, &delete_view_in_state(&1, view_id))
    end
  end

  @impl true
  def upsert_view_rows(%BackendContext{} = context, view_id, generation, rows)
      when is_binary(view_id) and is_integer(generation) and is_list(rows) do
    with {:ok, adapter} <- Context.unwrap(context) do
      adapter.store
      |> Store.update(&upsert_view_rows_update(&1, view_id, generation, rows))
      |> normalize_ok()
    end
  end

  @impl true
  def delete_view_rows(%BackendContext{} = context, view_id, generation, removals)
      when is_binary(view_id) and is_integer(generation) and is_list(removals) do
    with {:ok, adapter} <- Context.unwrap(context) do
      adapter.store
      |> Store.update(&delete_view_rows_update(&1, view_id, generation, removals))
      |> normalize_ok()
    end
  end

  @impl true
  def put_view_indexed_through(%BackendContext{} = context, view_id, through)
      when is_binary(view_id) and is_integer(through) do
    with {:ok, adapter} <- Context.unwrap(context) do
      adapter.store
      |> Store.update(&put_view_indexed_through_update(&1, view_id, through))
      |> normalize_ok()
    end
  end

  @impl true
  def clear_view_generation_rows(%BackendContext{} = context, view_id, generation)
      when is_binary(view_id) and is_integer(generation) do
    with {:ok, adapter} <- Context.unwrap(context) do
      adapter.store
      |> Store.update(&clear_view_generation_rows_update(&1, view_id, generation))
      |> normalize_ok()
    end
  end

  @impl true
  def begin_view_rebuild_effect(%BackendContext{} = context, view_id, building_generation)
      when is_binary(view_id) and is_integer(building_generation) do
    with {:ok, adapter} <- Context.unwrap(context) do
      adapter.store
      |> Store.update(&begin_view_rebuild_effect_update(&1, view_id, building_generation))
      |> normalize_ok()
    end
  end

  @impl true
  def finish_view_rebuild_effect(%BackendContext{} = context, view_id, generation, indexed_through)
      when is_binary(view_id) and is_integer(generation) and is_integer(indexed_through) do
    with {:ok, adapter} <- Context.unwrap(context) do
      adapter.store
      |> Store.update(&finish_view_rebuild_effect_update(&1, view_id, generation, indexed_through))
      |> normalize_ok()
    end
  end

  @impl true
  def scan_view_rows(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context) do
      view_id = MapAccess.get(request, :view_id)
      generation = MapAccess.get(request, :generation)
      plan = MapAccess.get(request, :plan)
      fetch_limit = MapAccess.get(request, :fetch_limit)
      state = Store.get(adapter.store)

      rows =
        state.view_rows
        |> Enum.flat_map(fn
          {{^view_id, ^generation, _doc}, row} -> [scan_row(row)]
          _ -> []
        end)
        |> Enum.shuffle()
        |> Enum.filter(&Engine.row_in_range?(&1, plan))
        |> Engine.sort_rows()

      rows = if fetch_limit, do: Enum.take(rows, fetch_limit), else: rows
      {:ok, rows}
    end
  end

  @impl true
  def read_winning_documents_page(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context) do
      state = Store.get(adapter.store)
      after_id = Map.get(request, "after_document_id") || MapAccess.get(request, :after_document_id)
      limit = page_limit(request)

      docs =
        state.documents
        |> Enum.filter(fn {_id, doc} -> not doc.winning_deleted end)
        |> Enum.sort_by(fn {id, _} -> id end)
        |> Enum.drop_while(fn {id, _} -> is_binary(after_id) and id <= after_id end)
        |> Enum.take(limit + 1)

      {page, rest} = Enum.split(docs, limit)

      documents =
        Enum.map(page, fn {id, doc} ->
          %{
            "document_id" => id,
            "revision_id" => doc.winning_revision,
            "body" => doc.body,
            "sequence" => doc.update_sequence
          }
        end)

      next_after =
        case rest do
          [{id, _} | _] -> id
          [] -> nil
        end

      {:ok, %{documents: documents, next_after: next_after}}
    end
  end

  @impl true
  def adapter_query_config(%BackendContext{} = context) do
    case Context.unwrap(context) do
      {:ok, adapter} -> adapter_config(adapter)
      {:error, _} -> ElixirDB.Config.defaults()
    end
  end

  defp insert_view_in_state(state, normalized) do
    view_id = UUID.v4()
    created_at = DateTime.utc_now() |> DateTime.to_iso8601()

    view = %{
      view_id: view_id,
      name: normalized.name,
      definition_json: normalized.definition_json,
      definition_digest: normalized.definition_digest,
      created_at: created_at,
      active_generation: 1,
      building_generation: nil,
      indexed_through: 0,
      status: "building",
      last_error_code: nil
    }

    {:ok, %{state | views: Map.put(state.views, view_id, view)},
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

  defp delete_view_in_state(state, view_id) do
    case Map.fetch(state.views, view_id) do
      :error ->
        {:error, ElixirDB.Error.view_not_found("view was not found", %{view_id: view_id})}

      {:ok, _} ->
        views = Map.delete(state.views, view_id)
        rows = Map.reject(state.view_rows, fn {{id, _gen, _doc}, _} -> id == view_id end)
        {:ok, %{state | views: views, view_rows: rows}, %{view_id: view_id, deleted: true}}
    end
  end

  defp upsert_rows(state, _view_id, _generation, []), do: {:ok, state}

  defp upsert_rows(state, view_id, generation, rows) do
    Enum.reduce_while(rows, {:ok, state}, fn row, {:ok, acc} ->
      case encode_row(row) do
        {:ok, encoded} ->
          key = {view_id, generation, encoded.document_id}

          {:cont, {:ok, %{acc | view_rows: Map.put(acc.view_rows, key, encoded)}}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
  end

  defp encode_row(row) do
    document_id = required_string(row, :document_id)
    revision_id = required_string(row, :revision_id)
    key = MapAccess.get(row, :key)

    with {:ok, key_sort} <- KeyCodec.encode(key) do
      {:ok,
       %{
         document_id: document_id,
         revision_id: revision_id,
         key: key,
         key_sort: key_sort,
         value: MapAccess.get(row, :value)
       }}
    end
  end

  defp scan_row(row) do
    %{
      id: row.document_id,
      revision_id: row.revision_id,
      key: row.key,
      key_sort: row.key_sort,
      value: row.value
    }
  end

  defp fetch_view(state, view_id) do
    case Map.fetch(state.views, view_id) do
      {:ok, view} -> {:ok, view}
      :error -> {:error, ElixirDB.Error.view_not_found("view was not found", %{view_id: view_id})}
    end
  end

  defp list_entry(view) do
    definition =
      case StrictDecoder.decode(view.definition_json) do
        {:ok, decoded} -> decoded
        _ -> nil
      end

    %{
      "view_id" => view.view_id,
      "name" => view.name,
      "definition" => definition,
      "definition_digest" => view.definition_digest,
      "created_at" => view.created_at,
      "active_generation" => view.active_generation,
      "building_generation" => view.building_generation,
      "indexed_through" => view.indexed_through,
      "status" => view.status,
      "last_error_code" => view.last_error_code,
      "reducer" => get_in(definition || %{}, ["reducer"])
    }
  end

  defp state_result(view) do
    %{
      view_id: view.view_id,
      active_generation: view.active_generation,
      building_generation: view.building_generation,
      indexed_through: view.indexed_through,
      status: view.status,
      last_error_code: view.last_error_code
    }
  end

  defp adapter_config(adapter) do
    case Store.get(adapter.store) do
      %{identity: %{config: config}} when is_map(config) -> config
      _ -> ElixirDB.Config.defaults()
    end
  end

  defp page_limit(request) do
    options = Map.get(request, "options", %{})
    override = Map.get(options, "page_size")
    value = override || Map.get(request, "limit") || MapAccess.get(request, :limit) || 100
    if is_integer(value) and value > 0, do: value, else: 100
  end

  defp required_string(map, key) do
    value = MapAccess.get(map, key)
    if is_binary(value) and value != "", do: value, else: raise(ArgumentError, "missing #{key}")
  end

  defp find_view_entry_by_name(views, name) do
    Enum.find_value(views, fn {_id, view} -> view_entry_by_name(view, name) end)
  end

  defp view_entry_by_name(%{name: view_name} = view, name) when view_name == name do
    %{view_id: view.view_id, definition_digest: view.definition_digest}
  end

  defp view_entry_by_name(_view, _name), do: nil

  defp upsert_view_rows_update(state, view_id, generation, rows) do
    with {:ok, _view} <- fetch_view(state, view_id),
         {:ok, next} <- upsert_rows(state, view_id, generation, rows) do
      {:ok, next, :ok}
    else
      {:error, _} = error -> error
    end
  end

  defp delete_view_rows_update(state, view_id, generation, removals) do
    rows =
      Enum.reduce(removals, state.view_rows, fn document_id, acc ->
        Map.delete(acc, {view_id, generation, document_id})
      end)

    {:ok, %{state | view_rows: rows}, :ok}
  end

  defp put_view_indexed_through_update(state, view_id, through) do
    case fetch_view(state, view_id) do
      {:ok, view} ->
        view = %{view | indexed_through: through}
        {:ok, %{state | views: Map.put(state.views, view_id, view)}, :ok}

      {:error, _} = error ->
        error
    end
  end

  defp clear_view_generation_rows_update(state, view_id, generation) do
    rows =
      Map.reject(state.view_rows, fn {{id, gen, _doc}, _} ->
        id == view_id and gen == generation
      end)

    {:ok, %{state | view_rows: rows}, :ok}
  end

  defp begin_view_rebuild_effect_update(state, view_id, building_generation) do
    case fetch_view(state, view_id) do
      {:ok, view} ->
        view = %{
          view
          | building_generation: building_generation,
            status: "building",
            last_error_code: nil
        }

        {:ok, %{state | views: Map.put(state.views, view_id, view)}, :ok}

      {:error, _} = error ->
        error
    end
  end

  defp finish_view_rebuild_effect_update(state, view_id, generation, indexed_through) do
    case fetch_view(state, view_id) do
      {:ok, view} ->
        view = %{
          view
          | active_generation: generation,
            building_generation: nil,
            indexed_through: indexed_through,
            status: "ready",
            last_error_code: nil
        }

        {:ok, %{state | views: Map.put(state.views, view_id, view)}, :ok}

      {:error, _} = error ->
        error
    end
  end

  defp normalize_ok({:ok, :ok}), do: :ok
  defp normalize_ok({:ok, value}), do: {:ok, value}
  defp normalize_ok({:error, _} = error), do: error
end
