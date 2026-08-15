defmodule VialKeeper.Storage.SQLite.LocalRecordsPort do
  @moduledoc """
  SQLite local-records fact port.
  """
  @behaviour VialKeeper.Storage.Ports.LocalRecords

  alias VialKeeper.JSON.StrictDecoder
  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.Ports.Errors
  alias VialKeeper.Storage.SQLite.{Connection, Context, LocalRecords, Transaction}

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
  def list(%BackendContext{} = context, namespace) when is_binary(namespace) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, rows} <- query_namespace(adapter.conn, namespace) do
      decode_rows(rows)
    end
  end

  defp query_namespace(conn, namespace) do
    case Connection.query(
           conn,
           "SELECT record_key, record_version, value_json FROM local_records WHERE namespace = ? ORDER BY record_key",
           [namespace]
         ) do
      {:ok, rows} -> {:ok, rows}
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  defp decode_rows(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn row, {:ok, acc} ->
      decode_row(row, acc)
    end)
    |> finalize_list()
  end

  defp decode_row([key, version, value_json], acc) do
    case StrictDecoder.decode(value_json) do
      {:ok, value} ->
        {:cont, {:ok, [%{key: key, record: %{version: version, value: value}} | acc]}}

      {:error, reason} ->
        {:halt, {:error, Errors.normalize(reason)}}
    end
  end

  defp finalize_list({:ok, list}), do: {:ok, Enum.reverse(list)}
  defp finalize_list(other), do: other

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
