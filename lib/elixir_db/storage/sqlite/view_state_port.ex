defmodule ElixirDB.Storage.SQLite.ViewStatePort do
  @moduledoc """
  SQLite view-state fact port.

  Routes opaque contexts to physical `ElixirDB.Storage.SQLite.Views` operations
  without owning product apply/rebuild workflows or query planning.
  """
  @behaviour ElixirDB.Storage.Ports.ViewState

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.SQLite.{Adapter, Context, Views}

  @impl true
  def list_views(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context), do: Errors.wrap(Views.list(adapter.conn))
  end

  @impl true
  def find_view_by_name(%BackendContext{} = context, name) when is_binary(name) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Views.find_by_name(adapter.conn, name))
  end

  @impl true
  def count_views(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context), do: Errors.wrap(Views.count(adapter.conn))
  end

  @impl true
  def get_view_definition(%BackendContext{} = context, view_id) when is_binary(view_id) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Views.get_definition(adapter.conn, view_id))
  end

  @impl true
  def get_view_state(%BackendContext{} = context, view_id) when is_binary(view_id) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Views.get_state(adapter.conn, view_id))
  end

  @impl true
  def insert_view(%BackendContext{} = context, normalized) when is_map(normalized) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Views.insert_definition(adapter.conn, normalized))
  end

  @impl true
  def delete_view(%BackendContext{} = context, view_id) when is_binary(view_id) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Views.delete(adapter.conn, view_id))
  end

  @impl true
  def upsert_view_rows(%BackendContext{} = context, view_id, generation, rows)
      when is_binary(view_id) and is_integer(generation) and is_list(rows) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Views.upsert_rows(adapter.conn, view_id, generation, rows))
  end

  @impl true
  def delete_view_rows(%BackendContext{} = context, view_id, generation, removals)
      when is_binary(view_id) and is_integer(generation) and is_list(removals) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Views.delete_removals(adapter.conn, view_id, generation, removals))
  end

  @impl true
  def put_view_indexed_through(%BackendContext{} = context, view_id, through)
      when is_binary(view_id) and is_integer(through) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Views.put_indexed_through(adapter.conn, view_id, through))
  end

  @impl true
  def clear_view_generation_rows(%BackendContext{} = context, view_id, generation)
      when is_binary(view_id) and is_integer(generation) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Views.clear_generation_rows(adapter.conn, view_id, generation))
  end

  @impl true
  def begin_view_rebuild_effect(%BackendContext{} = context, view_id, building_generation)
      when is_binary(view_id) and is_integer(building_generation) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Views.begin_rebuild_effect(adapter.conn, view_id, building_generation))
  end

  @impl true
  def finish_view_rebuild_effect(%BackendContext{} = context, view_id, generation, indexed_through)
      when is_binary(view_id) and is_integer(generation) and is_integer(indexed_through) do
    with {:ok, adapter} <- Context.unwrap(context),
         do:
           Errors.wrap(
             Views.finish_rebuild_effect(adapter.conn, view_id, generation, indexed_through)
           )
  end

  @impl true
  def scan_view_rows(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(
        Views.scan_rows(
          adapter.conn,
          request.view_id,
          request.generation,
          request.plan,
          request.fetch_limit
        )
      )
    end
  end

  @impl true
  def read_winning_documents_page(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Views.read_winning_documents_page(adapter.conn, request))
  end

  @impl true
  def adapter_query_config(%BackendContext{} = context) do
    case Context.unwrap(context) do
      {:ok, adapter} -> adapter_config(adapter)
      _ -> ElixirDB.Config.defaults()
    end
  end

  defp adapter_config(adapter) do
    case Adapter.identity(adapter) do
      {:ok, %{config: config}} when is_map(config) -> config
      _ -> ElixirDB.Config.defaults()
    end
  end
end
