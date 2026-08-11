defmodule ElixirDB.Storage.SQLite.RetentionRecordsPort do
  @moduledoc """
  SQLite retention-records fact port.
  """
  @behaviour ElixirDB.Storage.Ports.RetentionRecords

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.SQLite.{Adapter, Context, Retention, RetentionRecords, Transaction}

  @impl true
  def retention_state(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(Adapter.retention_state(adapter))
    end
  end

  @impl true
  def list_peer_positions(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(Retention.list_peer_positions(adapter.conn))
    end
  end

  @impl true
  def put_peer_position_cas(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Transaction.run_on_adapter(adapter, fn tx_adapter ->
        Errors.wrap(Retention.put_peer_position_cas(tx_adapter, request))
      end)
    end
  end

  @impl true
  def read_boundary_pages(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(Retention.read_boundary_pages(adapter.conn, request))
    end
  end

  @impl true
  def install_boundary_pages(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Transaction.run_on_adapter(adapter, fn tx_adapter ->
        Errors.wrap(Retention.install_boundary_pages(tx_adapter.conn, request))
      end)
    end
  end

  @impl true
  def get_compaction_result(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(RetentionRecords.get_last_result(adapter.conn))
    end
  end

  @impl true
  def put_compaction_result(%BackendContext{} = context, result) when is_map(result) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Transaction.run_on_adapter(adapter, fn tx_adapter ->
        Errors.wrap(RetentionRecords.put_last_result(tx_adapter.conn, result))
      end)
    end
  end

  @impl true
  def list_boundaries(%BackendContext{} = context) do
    list_boundaries(context, [])
  end

  @impl true
  def list_boundaries(%BackendContext{} = context, opts) when is_list(opts) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(RetentionRecords.list_boundaries(adapter.conn, opts))
    end
  end

  @impl true
  def install_imported_boundaries(%BackendContext{} = context, boundaries)
      when is_list(boundaries) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(RetentionRecords.install_imported_boundaries(adapter.conn, boundaries))
    end
  end

  @impl true
  def mark_pending_local_causal(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(RetentionRecords.mark_pending_local_causal(adapter.conn))
    end
  end

  @impl true
  def pending_local_causal?(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(RetentionRecords.pending_local_causal?(adapter.conn))
    end
  end

  @impl true
  def encode_stored_boundary(stored) when is_map(stored) do
    RetentionRecords.encode_stored_boundary(stored)
  end
end
