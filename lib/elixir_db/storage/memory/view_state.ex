defmodule ElixirDB.Storage.Memory.ViewState do
  @moduledoc """
  Memory view-state port.

  Stores definitions, generations, and rows in the Memory store. Shared
  `ElixirDB.View.Engine` owns CAS, query planning, bookmarks, and grouping.
  Row iteration order is deliberately unsorted until Engine sorts.
  """
  @behaviour ElixirDB.Storage.Ports.ViewState

  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.MapAccess
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Memory.{Context, Store}
  alias ElixirDB.UUID
  alias ElixirDB.View.{Definition, Engine, KeyCodec}

  @impl true
  def list_views(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      state = Store.get(adapter.store)

      views =
        state.views
        |> Map.values()
        |> Enum.sort_by(& &1.name)
        |> Enum.map(&list_entry/1)

      {:ok, views}
    end
  end

  @impl true
  def create_view(%BackendContext{} = context, definition) when is_map(definition) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, normalized} <- Definition.normalize(definition) do
      config = adapter_config(adapter)

      Store.update(adapter.store, fn state ->
        create_in_state(state, normalized, config)
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
  def view_state(%BackendContext{} = context, view_id) when is_binary(view_id) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, view} <- fetch_view(Store.get(adapter.store), view_id) do
      {:ok, state_result(view)}
    end
  end

  @impl true
  def apply_view_batch(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Store.update(adapter.store, fn state ->
        apply_batch_in_state(state, request, nil)
      end)
    end
  end

  @impl true
  def begin_view_rebuild(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Store.update(adapter.store, fn state ->
        begin_rebuild_in_state(state, request)
      end)
    end
  end

  @impl true
  def append_view_rebuild_page(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Store.update(adapter.store, fn state ->
        append_rebuild_in_state(state, request)
      end)
    end
  end

  @impl true
  def finish_view_rebuild(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Store.update(adapter.store, fn state ->
        finish_rebuild_in_state(state, request)
      end)
    end
  end

  @impl true
  def read_view_rows(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         :ok <- Engine.validate_query_fields(request),
         {:ok, view_id} <- Engine.query_view_id(request),
         {:ok, limit} <- Engine.normalize_query_limit(request, adapter_config(adapter)),
         :ok <- Engine.validate_query_options(request),
         state = Store.get(adapter.store),
         {:ok, view} <- fetch_view(state, view_id),
         {:ok, definition} <- Engine.decode_definition(view.definition_json),
         {:ok, plan} <- Engine.build_query_plan(request, definition, view),
         {rows, has_more} <-
           fetch_rows(state, view_id, view.active_generation, plan, limit, definition) do
      Engine.format_query_result(rows, has_more, definition, view, plan, limit)
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

  defp create_in_state(state, normalized, config) do
    maximum = get_in(config, ["views", "max_definitions"]) || 32

    if map_size(state.views) >= maximum do
      {:error, ElixirDB.Error.resource_limit("view definition limit reached")}
    else
      insert_created_view(state, normalized)
    end
  end

  defp insert_created_view(state, normalized) do
    existing =
      Enum.find_value(state.views, fn {_id, view} ->
        if view.name == normalized.name,
          do: %{view_id: view.view_id, definition_digest: view.definition_digest}
      end)

    with :ok <- Engine.ensure_no_conflict(existing, normalized) do
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

  defp apply_batch_in_state(state, request, view_fault) do
    view_id = required_string(request, :view_id)
    expected = required_integer(request, :expected_indexed_through)
    through = required_integer(request, :through_sequence)
    rows = MapAccess.get(request, :rows, [])
    removals = MapAccess.get(request, :removals, [])

    with {:ok, view} <- fetch_view(state, view_id),
         {:ok, mode} <- Engine.validate_batch_cas(view, expected, through) do
      apply_batch_mode(state, view, view_id, through, rows, removals, view_fault, mode)
    end
  end

  defp apply_batch_mode(state, _view, view_id, through, _rows, _removals, _view_fault, :idempotent) do
    {:ok, state, %{view_id: view_id, indexed_through: through, applied: false}}
  end

  defp apply_batch_mode(state, view, view_id, through, rows, removals, view_fault, :apply) do
    with {:ok, state} <-
           apply_row_changes(state, view_id, view.active_generation, rows, removals, view_fault) do
      view = %{view | indexed_through: through}
      state = %{state | views: Map.put(state.views, view_id, view)}
      {:ok, state, %{view_id: view_id, indexed_through: through, applied: true}}
    end
  end

  defp begin_rebuild_in_state(state, request) do
    view_id = required_string(request, :view_id)
    start_sequence = required_integer(request, :start_sequence)

    with {:ok, view} <- fetch_view(state, view_id) do
      building = view.active_generation + 1

      rows =
        Map.reject(state.view_rows, fn {{id, gen, _doc}, _} ->
          id == view_id and gen == building
        end)

      view = %{view | building_generation: building, status: "building", last_error_code: nil}

      {:ok, %{state | views: Map.put(state.views, view_id, view), view_rows: rows},
       %{
         view_id: view_id,
         building_generation: building,
         start_sequence: start_sequence,
         active_generation: view.active_generation
       }}
    end
  end

  defp append_rebuild_in_state(state, request) do
    view_id = required_string(request, :view_id)
    generation = required_integer(request, :generation)
    rows = MapAccess.get(request, :rows, [])
    removals = MapAccess.get(request, :removals, [])

    with {:ok, view} <- fetch_view(state, view_id),
         :ok <- Engine.ensure_building_generation(view, generation),
         {:ok, state} <- apply_row_changes(state, view_id, generation, rows, removals, nil) do
      {:ok, state, %{view_id: view_id, generation: generation, appended: length(rows)}}
    end
  end

  defp finish_rebuild_in_state(state, request) do
    view_id = required_string(request, :view_id)
    generation = required_integer(request, :generation)
    indexed_through = required_integer(request, :indexed_through)

    with {:ok, view} <- fetch_view(state, view_id),
         :ok <- Engine.ensure_building_generation(view, generation) do
      view = %{
        view
        | active_generation: generation,
          building_generation: nil,
          indexed_through: indexed_through,
          status: "ready",
          last_error_code: nil
      }

      {:ok, %{state | views: Map.put(state.views, view_id, view)},
       %{view_id: view_id, active_generation: generation, indexed_through: indexed_through}}
    end
  end

  defp apply_row_changes(state, view_id, generation, rows, removals, view_fault) do
    with {:ok, state} <- delete_removals(state, view_id, generation, removals) do
      upsert_rows(state, view_id, generation, rows, view_fault)
    end
  end

  defp delete_removals(state, view_id, generation, removals) do
    rows =
      Enum.reduce(removals, state.view_rows, fn document_id, acc ->
        Map.delete(acc, {view_id, generation, document_id})
      end)

    {:ok, %{state | view_rows: rows}}
  end

  defp upsert_rows(state, _view_id, _generation, [], _view_fault), do: {:ok, state}

  defp upsert_rows(state, view_id, generation, rows, view_fault) do
    Enum.reduce_while(rows, {:ok, state}, fn row, {:ok, acc} ->
      with :ok <- Engine.view_fault_check(view_fault, :view_upsert_row),
           {:ok, encoded} <- encode_row(row) do
        key = {view_id, generation, encoded.document_id}
        {:cont, {:ok, %{acc | view_rows: Map.put(acc.view_rows, key, encoded)}}}
      else
        {:error, _} = error -> {:halt, error}
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

  defp fetch_rows(state, view_id, generation, plan, limit, definition) do
    fetch_limit = Engine.fetch_limit(definition, limit)

    rows =
      state.view_rows
      |> Enum.flat_map(fn
        {{^view_id, ^generation, _doc}, row} -> [row]
        _ -> []
      end)
      # Deliberately disordered before Engine range filter + sort.
      |> Enum.shuffle()
      |> Enum.filter(&Engine.row_in_range?(&1, plan))
      |> Engine.sort_rows()
      |> Enum.map(fn row ->
        %{
          id: row.document_id,
          revision_id: row.revision_id,
          key: row.key,
          key_sort: row.key_sort,
          value: row.value
        }
      end)

    Engine.split_fetch(
      if(fetch_limit, do: Enum.take(rows, fetch_limit), else: rows),
      fetch_limit,
      limit
    )
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

  defp required_integer(map, key) do
    value = MapAccess.get(map, key)

    if is_integer(value) and value >= 0,
      do: value,
      else: raise(ArgumentError, "missing #{key}")
  end
end
