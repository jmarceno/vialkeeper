defmodule ElixirDB.Storage.SQLite.DerivedStatePort do
  @moduledoc """
  SQLite derived-state fact port.

  Routes opaque contexts to physical `ElixirDB.Storage.SQLite.DerivedViews`
  row/range/CAS operations without owning product apply/rebuild workflows.
  """
  @behaviour ElixirDB.Storage.Ports.DerivedState

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.SQLite.{Context, DerivedViews}

  @impl true
  def get_derived_metadata(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(DerivedViews.get_metadata(adapter.conn))
  end

  @impl true
  def list_derived_sources(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(DerivedViews.list_sources(adapter.conn))
  end

  @impl true
  def get_derived_source(%BackendContext{} = context, source_uuid) when is_binary(source_uuid) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(DerivedViews.get_source(adapter.conn, source_uuid))
  end

  @impl true
  def put_derived_enabled(%BackendContext{} = context, effect) when is_map(effect) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(DerivedViews.put_enabled(adapter.conn, effect))
  end

  @impl true
  def put_derived_source_error(%BackendContext{} = context, effect) when is_map(effect) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(DerivedViews.put_source_error(adapter.conn, effect))
  end

  @impl true
  def put_derived_rebuild_begin(%BackendContext{} = context, effect) when is_map(effect) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(DerivedViews.put_rebuild_begin(adapter.conn, effect))
  end

  @impl true
  def put_derived_rebuild_cursor(%BackendContext{} = context, effect) when is_map(effect) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(DerivedViews.put_rebuild_cursor(adapter.conn, effect))
  end

  @impl true
  def put_derived_rebuild_finish(%BackendContext{} = context, effect) when is_map(effect) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(DerivedViews.put_rebuild_finish(adapter.conn, effect))
  end

  @impl true
  def put_derived_source_checkpoint(%BackendContext{} = context, effect) when is_map(effect) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(DerivedViews.put_source_checkpoint(adapter.conn, effect))
  end

  @impl true
  def clear_derived_source_error(%BackendContext{} = context, source_uuid)
      when is_binary(source_uuid) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(DerivedViews.clear_source_error(adapter.conn, source_uuid))
  end

  @impl true
  def refresh_derived_status(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(DerivedViews.refresh_status(adapter.conn))
  end

  @impl true
  def fetch_contributions(%BackendContext{} = context, source_uuid, document_ids)
      when is_binary(source_uuid) and is_list(document_ids) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(DerivedViews.fetch_contributions(adapter.conn, source_uuid, document_ids))
  end

  @impl true
  def delete_contributions(%BackendContext{} = context, source_uuid, document_ids)
      when is_binary(source_uuid) and is_list(document_ids) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(DerivedViews.delete_contributions(adapter.conn, source_uuid, document_ids))
  end

  @impl true
  def upsert_contributions(%BackendContext{} = context, source_uuid, rows, generation)
      when is_binary(source_uuid) and is_list(rows) and is_integer(generation) do
    with {:ok, adapter} <- Context.unwrap(context),
         do:
           Errors.wrap(
             DerivedViews.upsert_contributions(adapter.conn, source_uuid, rows, generation)
           )
  end

  @impl true
  def list_stale_contribution_ids(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(DerivedViews.list_stale_contribution_ids(adapter.conn, request))
  end

  @impl true
  def fetch_group(%BackendContext{} = context, group_sort, group_json)
      when is_binary(group_sort) and is_binary(group_json) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(DerivedViews.fetch_group(adapter.conn, group_sort, group_json))
  end

  @impl true
  def upsert_group(%BackendContext{} = context, effect) when is_map(effect) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(DerivedViews.upsert_group(adapter.conn, effect))
  end

  @impl true
  def delete_group(%BackendContext{} = context, group_sort) when is_binary(group_sort) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(DerivedViews.delete_group(adapter.conn, group_sort))
  end

  @impl true
  def list_group_numeric_values(%BackendContext{} = context, group_sort)
      when is_binary(group_sort) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(DerivedViews.list_group_numeric_values(adapter.conn, group_sort))
  end
end
