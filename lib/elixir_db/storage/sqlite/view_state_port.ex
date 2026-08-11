defmodule ElixirDB.Storage.SQLite.ViewStatePort do
  @moduledoc """
  SQLite view-state fact port.

  Routes opaque contexts to physical `ElixirDB.Storage.SQLite.Views` operations
  without re-entering adapter feature callbacks.
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
  def create_view(%BackendContext{} = context, definition) when is_map(definition) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(Views.create_tx(adapter.conn, definition, adapter_config(adapter)))
    end
  end

  @impl true
  def delete_view(%BackendContext{} = context, view_id) when is_binary(view_id) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Views.delete_tx(adapter.conn, view_id))
  end

  @impl true
  def view_state(%BackendContext{} = context, view_id) when is_binary(view_id) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Views.state(adapter.conn, view_id))
  end

  @impl true
  def apply_view_batch(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(Views.apply_batch_tx(adapter.conn, request, view_fault: adapter.view_fault))
    end
  end

  @impl true
  def begin_view_rebuild(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Views.begin_rebuild_tx(adapter.conn, request))
  end

  @impl true
  def append_view_rebuild_page(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Views.append_rebuild_page_tx(adapter.conn, request))
  end

  @impl true
  def finish_view_rebuild(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Views.finish_rebuild_tx(adapter.conn, request))
  end

  @impl true
  def read_view_rows(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(Views.query_tx(adapter.conn, request, adapter_config(adapter)))
    end
  end

  @impl true
  def read_winning_documents_page(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Views.read_winning_documents_page(adapter.conn, request))
  end

  defp adapter_config(adapter) do
    case Adapter.identity(adapter) do
      {:ok, %{config: config}} when is_map(config) -> config
      _ -> ElixirDB.Config.defaults()
    end
  end
end
