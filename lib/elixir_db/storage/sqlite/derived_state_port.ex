defmodule ElixirDB.Storage.SQLite.DerivedStatePort do
  @moduledoc """
  SQLite derived-state fact port.
  """
  @behaviour ElixirDB.Storage.Ports.DerivedState

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.SQLite.{Adapter, Context}

  @impl true
  def get_derived_view(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Adapter.get_derived_view(adapter))
  end

  @impl true
  def set_derived_enabled(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Adapter.set_derived_enabled(adapter, request))
  end

  @impl true
  def list_derived_sources(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Adapter.list_derived_sources(adapter))
  end

  @impl true
  def set_derived_source_error(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Adapter.set_derived_source_error(adapter, request))
  end

  @impl true
  def apply_derived_source_batch(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Adapter.apply_derived_source_batch(adapter, request))
  end

  @impl true
  def begin_derived_source_rebuild(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Adapter.begin_derived_source_rebuild(adapter, request))
  end

  @impl true
  def apply_derived_rebuild_page(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Adapter.apply_derived_rebuild_page(adapter, request))
  end

  @impl true
  def prune_derived_rebuild_stale_page(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Adapter.prune_derived_rebuild_stale_page(adapter, request))
  end

  @impl true
  def finish_derived_source_rebuild(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Adapter.finish_derived_source_rebuild(adapter, request))
  end
end
