defmodule ElixirDB.Storage.Services.Views do
  @moduledoc """
  Shared local-view orchestration over the view-state port.

  Owns definition CAS, rebuild generations, batch CAS, query planning,
  bookmarks, grouping, and result shaping via `ElixirDB.View.Engine`. Physical
  backends only persist definitions/rows and execute range scans.
  """

  alias ElixirDB.MapAccess
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Access
  alias ElixirDB.Storage.Transaction
  alias ElixirDB.View.{Definition, Engine}

  @doc "Lists view catalog entries."
  @spec list(BackendContext.t()) :: {:ok, [map()]} | {:error, ElixirDB.Error.t()}
  def list(%BackendContext{} = context), do: port(context).list_views(context)

  @doc "Creates a view definition."
  @spec create(BackendContext.t(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def create(%BackendContext{} = context, definition) when is_map(definition) do
    Transaction.run(context, fn tx ->
      config = port(tx).adapter_query_config(tx)

      with {:ok, normalized} <- Definition.normalize(definition),
           {:ok, count} <- port(tx).count_views(tx),
           :ok <- enforce_definition_limit(count, config),
           {:ok, existing} <- port(tx).find_view_by_name(tx, normalized.name),
           :ok <- Engine.ensure_no_conflict(existing, normalized) do
        port(tx).insert_view(tx, normalized)
      end
    end)
  end

  @doc "Deletes a view definition and its rows."
  @spec delete(BackendContext.t(), binary()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def delete(%BackendContext{} = context, view_id) when is_binary(view_id) do
    Transaction.run(context, fn tx ->
      with {:ok, _definition} <- port(tx).get_view_definition(tx, view_id) do
        port(tx).delete_view(tx, view_id)
      end
    end)
  end

  @doc "Reads view rebuild/index state."
  @spec state(BackendContext.t(), binary()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def state(%BackendContext{} = context, view_id) when is_binary(view_id),
    do: port(context).get_view_state(context, view_id)

  @doc "Applies an incremental view batch with CAS."
  @spec apply_batch(BackendContext.t(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def apply_batch(%BackendContext{} = context, request) when is_map(request) do
    fault = view_fault(context)

    Transaction.run(context, fn tx ->
      tx = put_view_fault(tx, fault)
      view_id = required_string(request, "view_id")
      expected = required_integer(request, "expected_indexed_through")
      through = required_integer(request, "through_sequence")
      rows = Map.get(request, "rows", [])
      removals = Map.get(request, "removals", [])

      with {:ok, state} <- port(tx).get_view_state(tx, view_id),
           {:ok, mode} <- Engine.validate_batch_cas(state, expected, through),
           :ok <-
             apply_batch_mode(
               tx,
               mode,
               view_id,
               state.active_generation,
               rows,
               removals,
               through
             ) do
        {:ok, %{view_id: view_id, indexed_through: through, applied: mode == :apply}}
      end
    end)
  end

  @doc "Begins a view rebuild generation."
  @spec begin_rebuild(BackendContext.t(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def begin_rebuild(%BackendContext{} = context, request) when is_map(request) do
    Transaction.run(context, fn tx ->
      view_id = required_string(request, "view_id")
      start_sequence = required_integer(request, "start_sequence")

      with {:ok, state} <- port(tx).get_view_state(tx, view_id),
           building_generation = state.active_generation + 1,
           :ok <- port(tx).clear_view_generation_rows(tx, view_id, building_generation),
           :ok <- port(tx).begin_view_rebuild_effect(tx, view_id, building_generation) do
        {:ok,
         %{
           view_id: view_id,
           building_generation: building_generation,
           start_sequence: start_sequence,
           active_generation: state.active_generation
         }}
      end
    end)
  end

  @doc "Appends one rebuild page."
  @spec append_rebuild_page(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def append_rebuild_page(%BackendContext{} = context, request) when is_map(request) do
    Transaction.run(context, fn tx ->
      view_id = required_string(request, "view_id")
      generation = required_integer(request, "generation")
      rows = Map.get(request, "rows", [])
      removals = Map.get(request, "removals", [])

      with {:ok, state} <- port(tx).get_view_state(tx, view_id),
           :ok <- Engine.ensure_building_generation(state, generation),
           :ok <- port(tx).delete_view_rows(tx, view_id, generation, removals),
           :ok <- maybe_upsert_rows(tx, view_id, generation, rows) do
        {:ok, %{view_id: view_id, generation: generation, appended: length(rows)}}
      end
    end)
  end

  @doc "Finishes a view rebuild generation."
  @spec finish_rebuild(BackendContext.t(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def finish_rebuild(%BackendContext{} = context, request) when is_map(request) do
    Transaction.run(context, fn tx ->
      view_id = required_string(request, "view_id")
      generation = required_integer(request, "generation")
      indexed_through = required_integer(request, "indexed_through")

      with {:ok, state} <- port(tx).get_view_state(tx, view_id),
           :ok <- Engine.ensure_building_generation(state, generation),
           :ok <-
             port(tx).finish_view_rebuild_effect(tx, view_id, generation, indexed_through) do
        {:ok, %{view_id: view_id, active_generation: generation, indexed_through: indexed_through}}
      end
    end)
  end

  @doc "Queries a view through shared planning and physical row reads."
  @spec query(BackendContext.t(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def query(%BackendContext{} = context, request) when is_map(request) do
    config = port(context).adapter_query_config(context)

    with :ok <- Engine.validate_query_fields(request),
         {:ok, view_id} <- Engine.query_view_id(request),
         {:ok, limit} <- Engine.normalize_query_limit(request, config),
         :ok <- Engine.validate_query_options(request),
         {:ok, definition_row} <- port(context).get_view_definition(context, view_id),
         {:ok, state} <- port(context).get_view_state(context, view_id),
         {:ok, definition} <- Engine.decode_definition(definition_row.definition_json),
         {:ok, query_plan} <- Engine.build_query_plan(request, definition, state) do
      fetch_limit = Engine.fetch_limit(definition, limit)

      with {:ok, rows} <-
             port(context).scan_view_rows(context, %{
               view_id: view_id,
               generation: state.active_generation,
               plan: query_plan,
               fetch_limit: fetch_limit
             }) do
        {page, fetch_has_more} = Engine.split_fetch(rows, fetch_limit, limit)
        Engine.format_query_result(page, fetch_has_more, definition, state, query_plan, limit)
      end
    end
  end

  def query(_context, _request),
    do: {:error, ElixirDB.Error.invalid_request("view query must be an object")}

  @doc "Reads a page of winning documents for rebuild scans."
  @spec read_winning_documents_page(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def read_winning_documents_page(%BackendContext{} = context, request) when is_map(request),
    do: port(context).read_winning_documents_page(context, request)

  defp apply_batch_mode(_tx, :idempotent, _view_id, _generation, _rows, _removals, _through),
    do: :ok

  defp apply_batch_mode(tx, :apply, view_id, generation, rows, removals, through) do
    with :ok <- port(tx).delete_view_rows(tx, view_id, generation, removals),
         :ok <- maybe_upsert_rows(tx, view_id, generation, rows) do
      port(tx).put_view_indexed_through(tx, view_id, through)
    end
  end

  defp maybe_upsert_rows(_tx, _view_id, _generation, []), do: :ok

  defp maybe_upsert_rows(tx, view_id, generation, rows) do
    with :ok <- Engine.view_fault_check(view_fault(tx), :view_upsert_row) do
      port(tx).upsert_view_rows(tx, view_id, generation, rows)
    end
  end

  defp enforce_definition_limit(count, config) do
    maximum = get_in(config, ["views", "max_definitions"]) || 100

    if count < maximum,
      do: :ok,
      else: {:error, ElixirDB.Error.resource_limit("view definition limit exceeded")}
  end

  defp required_string(map, key) do
    case MapAccess.get(map, key) || Map.get(map, key) do
      value when is_binary(value) and value != "" -> value
      _ -> raise ArgumentError, "#{key} must be a non-empty string"
    end
  end

  defp required_integer(map, key) do
    case MapAccess.get(map, key) || Map.get(map, key) do
      value when is_integer(value) -> value
      _ -> raise ArgumentError, "#{key} must be an integer"
    end
  end

  defp view_fault(%BackendContext{identity: identity}) when is_map(identity),
    do: Map.get(identity, :view_fault)

  defp view_fault(_), do: nil

  defp put_view_fault(%BackendContext{} = context, fun) when is_function(fun, 1) do
    %{context | identity: Map.put(context.identity || %{}, :view_fault, fun)}
  end

  defp put_view_fault(%BackendContext{} = context, _), do: context

  defp port(%BackendContext{} = context), do: Access.port(context, :view_state)
end
