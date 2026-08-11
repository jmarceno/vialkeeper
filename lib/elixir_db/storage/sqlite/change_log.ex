defmodule ElixirDB.Storage.SQLite.ChangeLog do
  @moduledoc """
  SQLite change-log fact port.
  """
  @behaviour ElixirDB.Storage.Ports.ChangeLog

  alias ElixirDB.MapAccess
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.SQLite.{Adapter, Changes, Connection, Context, Retention}

  @impl true
  def allocate_sequences(%BackendContext{} = context, count)
      when is_integer(count) and count >= 0 do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(Changes.allocate_sequences(adapter.conn, count))
    end
  end

  @impl true
  def append_change(%BackendContext{} = context, entry) when is_map(entry) do
    with {:ok, adapter} <- Context.unwrap(context),
         sequence when is_integer(sequence) <- MapAccess.get(entry, :sequence),
         document_id when is_binary(document_id) <- MapAccess.get(entry, :document_id),
         winner <- MapAccess.get(entry, :winner),
         leaf_json when is_binary(leaf_json) <- MapAccess.get(entry, :leaf_json),
         origin when is_binary(origin) <- MapAccess.get(entry, :origin, "local"),
         doc_key <- MapAccess.get(entry, :backend_meta) |> meta_doc_key() do
      Errors.wrap(
        Changes.insert(adapter.conn, sequence, doc_key, document_id, winner, leaf_json, origin)
      )
    else
      {:error, reason} ->
        {:error, Errors.normalize(reason)}

      _ ->
        {:error, ElixirDB.Error.invalid_request("change log entry fields are invalid")}
    end
  end

  @impl true
  def read_page(%BackendContext{} = context, since, limit)
      when is_integer(since) and since >= 0 and is_integer(limit) and limit > 0 do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(Adapter.read_changes(adapter, %{since: since, limit: limit}))
    end
  end

  @impl true
  def has_local_origin_changes?(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(Changes.has_local_origin_changes?(adapter.conn))
    end
  end

  @impl true
  def has_local_origin_changes?(%BackendContext{} = context, peer_database_uuid) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(Changes.has_local_origin_changes?(adapter.conn, peer_database_uuid))
    end
  end

  @impl true
  def clear_pending_local_causal(%BackendContext{} = context) do
    clear_pending_local_causal(context, nil)
  end

  @impl true
  def clear_pending_local_causal(%BackendContext{} = context, peer_database_uuid) do
    with {:ok, adapter} <- Context.unwrap(context) do
      case Retention.clear_pending_local_causal(adapter.conn, peer_database_uuid) do
        :ok -> {:ok, :cleared}
        {:error, reason} -> {:error, Errors.normalize(reason)}
      end
    end
  end

  @impl true
  def delete_through_boundary(%BackendContext{} = context, through)
      when is_integer(through) and through >= 0 do
    with {:ok, adapter} <- Context.unwrap(context) do
      delete_changes_through(adapter.conn, through)
    end
  end

  defp delete_changes_through(_conn, 0), do: :ok

  defp delete_changes_through(conn, through) do
    case Connection.execute(conn, "DELETE FROM changes WHERE sequence <= ?", [through]) do
      :ok -> :ok
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  defp meta_doc_key(%{doc_key: doc_key}) when not is_nil(doc_key), do: doc_key
  defp meta_doc_key(_), do: nil
end
