defmodule ElixirDB.Storage.SQLite.RetentionRecordsPort do
  @moduledoc """
  SQLite retention-records fact port.
  """
  @behaviour ElixirDB.Storage.Ports.RetentionRecords

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.SQLite.{Adapter, Context, Retention, Transaction}

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
end
