defmodule ElixirDB.Storage.SQLite.LocalRecordsPort do
  @moduledoc """
  SQLite local-records fact port.
  """
  @behaviour ElixirDB.Storage.Ports.LocalRecords

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.SQLite.{Connection, Context, LocalRecords, Transaction}

  @impl true
  def get(%BackendContext{} = context, namespace, key)
      when is_binary(namespace) and is_binary(key) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(LocalRecords.fetch(adapter.conn, namespace, key))
    end
  end

  @impl true
  def put_cas(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Transaction.run_on_adapter(adapter, fn tx_adapter ->
        Errors.wrap(LocalRecords.put_cas_tx(tx_adapter, request))
      end)
    end
  end

  @impl true
  def delete(%BackendContext{} = context, namespace, key)
      when is_binary(namespace) and is_binary(key) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, :deleted} <- delete_in_transaction(adapter, namespace, key) do
      :ok
    end
  end

  defp delete_in_transaction(adapter, namespace, key) do
    Transaction.run_on_adapter(adapter, fn tx_adapter ->
      delete_record(tx_adapter.conn, namespace, key)
    end)
  end

  defp delete_record(conn, namespace, key) do
    case Connection.execute(
           conn,
           "DELETE FROM local_records WHERE namespace = ? AND record_key = ?",
           [namespace, key]
         ) do
      :ok -> {:ok, :deleted}
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end
end
