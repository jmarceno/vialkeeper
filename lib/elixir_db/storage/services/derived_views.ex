defmodule ElixirDB.Storage.Services.DerivedViews do
  @moduledoc """
  Shared derived-materialization orchestration over the derived-state port.

  Batch normalization, contribution diffs, grouping, reducers, numeric extremes,
  generated outputs, and cursor checks live in `ElixirDB.DerivedView.Engine`.
  """

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Access
  alias ElixirDB.Storage.Transaction

  @doc "Loads derived view metadata and definition."
  @spec get(BackendContext.t()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def get(%BackendContext{} = context) do
    Access.port(context, :derived_state).get_derived_view(context)
  end

  @doc "Enables or disables derived materialization."
  @spec set_enabled(BackendContext.t(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def set_enabled(%BackendContext{} = context, request) when is_map(request) do
    Transaction.run(context, fn tx ->
      Access.port(tx, :derived_state).set_derived_enabled(tx, request)
    end)
  end

  @doc "Lists derived source checkpoints."
  @spec list_sources(BackendContext.t()) :: {:ok, [map()]} | {:error, ElixirDB.Error.t()}
  def list_sources(%BackendContext{} = context) do
    Access.port(context, :derived_state).list_derived_sources(context)
  end

  @doc "Records a derived source error."
  @spec set_source_error(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def set_source_error(%BackendContext{} = context, request) when is_map(request) do
    Transaction.run(context, fn tx ->
      Access.port(tx, :derived_state).set_derived_source_error(tx, request)
    end)
  end

  @doc "Applies one incremental derived source batch."
  @spec apply_source_batch(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def apply_source_batch(%BackendContext{} = context, request) when is_map(request) do
    Transaction.run(context, fn tx ->
      Access.port(tx, :derived_state).apply_derived_source_batch(tx, request)
    end)
  end

  @doc "Begins a derived source rebuild."
  @spec begin_source_rebuild(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def begin_source_rebuild(%BackendContext{} = context, request) when is_map(request) do
    Transaction.run(context, fn tx ->
      Access.port(tx, :derived_state).begin_derived_source_rebuild(tx, request)
    end)
  end

  @doc "Applies one derived rebuild page."
  @spec apply_rebuild_page(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def apply_rebuild_page(%BackendContext{} = context, request) when is_map(request) do
    Transaction.run(context, fn tx ->
      Access.port(tx, :derived_state).apply_derived_rebuild_page(tx, request)
    end)
  end

  @doc "Prunes one page of stale rebuild contributions."
  @spec prune_rebuild_stale_page(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def prune_rebuild_stale_page(%BackendContext{} = context, request) when is_map(request) do
    Transaction.run(context, fn tx ->
      Access.port(tx, :derived_state).prune_derived_rebuild_stale_page(tx, request)
    end)
  end

  @doc "Finishes a derived source rebuild."
  @spec finish_source_rebuild(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def finish_source_rebuild(%BackendContext{} = context, request) when is_map(request) do
    Transaction.run(context, fn tx ->
      Access.port(tx, :derived_state).finish_derived_source_rebuild(tx, request)
    end)
  end
end
