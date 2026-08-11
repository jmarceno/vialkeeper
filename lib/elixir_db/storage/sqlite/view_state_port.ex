defmodule ElixirDB.Storage.SQLite.ViewStatePort do
  @moduledoc """
  SQLite view-state fact port.
  """
  @behaviour ElixirDB.Storage.Ports.ViewState

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.SQLite.{Adapter, Context}

  @impl true
  def list_views(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context), do: Errors.wrap(Adapter.list_views(adapter))
  end

  @impl true
  def create_view(%BackendContext{} = context, definition) when is_map(definition) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Adapter.create_view(adapter, definition))
  end

  @impl true
  def delete_view(%BackendContext{} = context, view_id) when is_binary(view_id) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Adapter.delete_view(adapter, view_id))
  end

  @impl true
  def view_state(%BackendContext{} = context, view_id) when is_binary(view_id) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Adapter.view_state(adapter, view_id))
  end

  @impl true
  def apply_view_batch(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Adapter.apply_view_batch(adapter, request))
  end

  @impl true
  def begin_view_rebuild(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Adapter.begin_view_rebuild(adapter, request))
  end

  @impl true
  def append_view_rebuild_page(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Adapter.append_view_rebuild_page(adapter, request))
  end

  @impl true
  def finish_view_rebuild(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Adapter.finish_view_rebuild(adapter, request))
  end

  @impl true
  def read_view_rows(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Adapter.query_view(adapter, request))
  end

  @impl true
  def read_winning_documents_page(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Adapter.read_winning_documents_page(adapter, request))
  end
end
