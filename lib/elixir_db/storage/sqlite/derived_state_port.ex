defmodule ElixirDB.Storage.SQLite.DerivedStatePort do
  @moduledoc """
  SQLite derived-state fact port.

  Routes opaque contexts to physical `ElixirDB.Storage.SQLite.DerivedViews`
  operations without re-entering adapter feature callbacks.
  """
  @behaviour ElixirDB.Storage.Ports.DerivedState

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.SQLite.{Context, DerivedViews}

  @impl true
  def get_derived_view(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(DerivedViews.get_view(adapter.conn))
  end

  @impl true
  def set_derived_enabled(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(DerivedViews.set_enabled_tx(adapter.conn, request))
  end

  @impl true
  def list_derived_sources(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(DerivedViews.list_sources(adapter.conn))
  end

  @impl true
  def set_derived_source_error(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(DerivedViews.set_source_error_tx(adapter.conn, request))
  end

  @impl true
  def apply_derived_source_batch(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(
        DerivedViews.apply_source_batch_tx(adapter, request, derived_fault: adapter.derived_fault)
      )
    end
  end

  @impl true
  def begin_derived_source_rebuild(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(DerivedViews.begin_source_rebuild_tx(adapter.conn, request))
  end

  @impl true
  def apply_derived_rebuild_page(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(
        DerivedViews.apply_rebuild_page_tx(adapter, request, derived_fault: adapter.derived_fault)
      )
    end
  end

  @impl true
  def prune_derived_rebuild_stale_page(%BackendContext{} = context, request)
      when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(
        DerivedViews.prune_rebuild_stale_page_tx(adapter, request,
          derived_fault: adapter.derived_fault
        )
      )
    end
  end

  @impl true
  def finish_derived_source_rebuild(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(DerivedViews.finish_source_rebuild_tx(adapter.conn, request))
  end
end
