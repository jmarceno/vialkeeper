defmodule ElixirDB.Storage.Services.Views do
  @moduledoc """
  Shared local-view orchestration over the view-state port.

  Definition CAS, rebuild generations, query planning, bookmarks, and grouping
  live in `ElixirDB.View.Engine`; this module routes opaque backend contexts to
  physical view-state ports.
  """

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Access
  alias ElixirDB.Storage.Transaction

  @doc "Lists view catalog entries."
  @spec list(BackendContext.t()) :: {:ok, [map()]} | {:error, ElixirDB.Error.t()}
  def list(%BackendContext{} = context) do
    Access.port(context, :view_state).list_views(context)
  end

  @doc "Creates a view definition."
  @spec create(BackendContext.t(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def create(%BackendContext{} = context, definition) when is_map(definition) do
    Transaction.run(context, fn tx ->
      Access.port(tx, :view_state).create_view(tx, definition)
    end)
  end

  @doc "Deletes a view definition and its rows."
  @spec delete(BackendContext.t(), binary()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def delete(%BackendContext{} = context, view_id) when is_binary(view_id) do
    Transaction.run(context, fn tx ->
      Access.port(tx, :view_state).delete_view(tx, view_id)
    end)
  end

  @doc "Reads view rebuild/index state."
  @spec state(BackendContext.t(), binary()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def state(%BackendContext{} = context, view_id) when is_binary(view_id) do
    Access.port(context, :view_state).view_state(context, view_id)
  end

  @doc "Applies an incremental view batch with CAS."
  @spec apply_batch(BackendContext.t(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def apply_batch(%BackendContext{} = context, request) when is_map(request) do
    Transaction.run(context, fn tx ->
      Access.port(tx, :view_state).apply_view_batch(tx, request)
    end)
  end

  @doc "Begins a view rebuild generation."
  @spec begin_rebuild(BackendContext.t(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def begin_rebuild(%BackendContext{} = context, request) when is_map(request) do
    Transaction.run(context, fn tx ->
      Access.port(tx, :view_state).begin_view_rebuild(tx, request)
    end)
  end

  @doc "Appends one rebuild page."
  @spec append_rebuild_page(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def append_rebuild_page(%BackendContext{} = context, request) when is_map(request) do
    Transaction.run(context, fn tx ->
      Access.port(tx, :view_state).append_view_rebuild_page(tx, request)
    end)
  end

  @doc "Finishes a view rebuild generation."
  @spec finish_rebuild(BackendContext.t(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def finish_rebuild(%BackendContext{} = context, request) when is_map(request) do
    Transaction.run(context, fn tx ->
      Access.port(tx, :view_state).finish_view_rebuild(tx, request)
    end)
  end

  @doc "Queries a view through shared planning and physical row reads."
  @spec query(BackendContext.t(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def query(%BackendContext{} = context, request) when is_map(request) do
    Access.port(context, :view_state).read_view_rows(context, request)
  end

  @doc "Reads a page of winning documents for rebuild scans."
  @spec read_winning_documents_page(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def read_winning_documents_page(%BackendContext{} = context, request) when is_map(request) do
    Access.port(context, :view_state).read_winning_documents_page(context, request)
  end
end
