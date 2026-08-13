defmodule ElixirDB.Storage.SQLite.ShadowStatePort do
  @moduledoc "SQLite shadow identity and source-origin port implementation."
  @behaviour ElixirDB.Storage.Ports.ShadowState

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.SQLite.{Connection, Context, Transaction}

  @impl true
  def metadata(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, rows} <- query_metadata(adapter.conn) do
      {:ok, decode_metadata(rows)}
    end
  end

  @impl true
  def put_metadata(%BackendContext{} = context, metadata) when is_map(metadata) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Transaction.run_on_adapter(adapter, &put_metadata_tx(&1, metadata))
    end
  end

  @impl true
  def origin(%BackendContext{} = context, document_id) when is_binary(document_id) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, rows} <-
           Connection.query(
             adapter.conn,
             "SELECT source_update_sequence FROM shadow_origins WHERE document_id = ?",
             [document_id]
           ) do
      case rows do
        [] -> {:ok, nil}
        [[sequence]] when is_integer(sequence) -> {:ok, sequence}
        _ -> {:error, ElixirDB.Error.integrity_violation("shadow origin row is invalid")}
      end
    else
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def put_origin(%BackendContext{} = context, document_id, sequence)
      when is_binary(document_id) and is_integer(sequence) and sequence >= 0 do
    with {:ok, adapter} <- Context.unwrap(context) do
      Transaction.run_on_adapter(adapter, &put_origin_tx(&1, context, document_id, sequence))
    end
  end

  def put_origin(_context, _document_id, _sequence),
    do: {:error, ElixirDB.Error.invalid_request("shadow origin is invalid")}

  defp query_metadata(conn) do
    case Connection.query(
           conn,
           "SELECT source_database_uuid, shadow_database_uuid, generation, operation_id, attachment_store_type, attachment_location, specification_digest, created_at FROM shadow_metadata WHERE id = 1"
         ) do
      {:ok, rows} -> {:ok, rows}
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  defp decode_metadata([]), do: nil

  defp decode_metadata([
         [
           source_uuid,
           shadow_uuid,
           generation,
           operation_id,
           store_type,
           location,
           digest,
           created_at
         ]
       ]) do
    %{
      source_database_uuid: source_uuid,
      shadow_database_uuid: shadow_uuid,
      generation: generation,
      operation_id: operation_id,
      attachment_store_type: store_type,
      attachment_location: location,
      specification_digest: digest,
      created_at: created_at
    }
  end

  defp decode_metadata(_), do: nil

  defp insert_metadata(conn, metadata) do
    with {:ok, source_uuid} <- required_binary(metadata, :source_database_uuid),
         {:ok, shadow_uuid} <- required_binary(metadata, :shadow_database_uuid),
         {:ok, generation} <- required_integer(metadata, :generation),
         {:ok, operation_id} <- required_binary(metadata, :operation_id),
         {:ok, store_type} <- required_store_type(metadata),
         {:ok, location} <- required_binary(metadata, :attachment_location),
         {:ok, digest} <- required_binary(metadata, :specification_digest),
         {:ok, created_at} <- required_timestamp(metadata),
         :ok <-
           Connection.execute(
             conn,
             "INSERT INTO shadow_metadata (id, source_database_uuid, shadow_database_uuid, generation, operation_id, attachment_store_type, attachment_location, specification_digest, created_at) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)",
             [
               source_uuid,
               shadow_uuid,
               generation,
               operation_id,
               store_type,
               location,
               digest,
               created_at
             ]
           ) do
      {:ok, metadata}
    else
      {:error, %ElixirDB.Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  defp insert_origin(conn, document_id, sequence) do
    case Connection.execute(
           conn,
           "INSERT INTO shadow_origins (document_id, source_update_sequence) VALUES (?, ?)",
           [document_id, sequence]
         ) do
      :ok -> {:ok, sequence}
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  defp update_origin(conn, document_id, sequence) do
    case Connection.execute(
           conn,
           "UPDATE shadow_origins SET source_update_sequence = ? WHERE document_id = ?",
           [sequence, document_id]
         ) do
      :ok -> {:ok, sequence}
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  defp put_metadata_tx(tx_adapter, metadata) do
    case query_metadata(tx_adapter.conn) do
      {:ok, []} -> insert_metadata(tx_adapter.conn, metadata)
      {:ok, [row]} -> compare_metadata(decode_metadata([row]), metadata)
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  defp compare_metadata(existing, metadata) do
    if existing == metadata,
      do: {:ok, metadata},
      else: {:error, ElixirDB.Error.shadow_identity_conflict("shadow metadata is immutable")}
  end

  defp put_origin_tx(tx_adapter, context, document_id, sequence) do
    case origin(Context.replace_ref(context, tx_adapter), document_id) do
      {:ok, nil} ->
        insert_origin(tx_adapter.conn, document_id, sequence)

      {:ok, current} when sequence >= current ->
        update_origin(tx_adapter.conn, document_id, sequence)

      {:ok, current} ->
        origin_regression(document_id, current, sequence)

      {:error, error} ->
        {:error, error}
    end
  end

  defp origin_regression(document_id, current, sequence) do
    {:error,
     ElixirDB.Error.shadow_generation_conflict(
       "source origin sequence cannot move backwards",
       %{document_id: document_id, current: current, requested: sequence}
     )}
  end

  defp required_binary(map, key) do
    value = Map.get(map, key, Map.get(map, Atom.to_string(key)))

    if is_binary(value) and value != "",
      do: {:ok, value},
      else: {:error, ElixirDB.Error.invalid_request("shadow metadata field is invalid")}
  end

  defp required_integer(map, key) do
    value = Map.get(map, key, Map.get(map, Atom.to_string(key)))

    if is_integer(value) and value > 0,
      do: {:ok, value},
      else: {:error, ElixirDB.Error.invalid_request("shadow metadata field is invalid")}
  end

  defp required_store_type(map) do
    if Map.get(map, :attachment_store_type, Map.get(map, "attachment_store_type")) == "external_cas",
      do: {:ok, "external_cas"},
      else: {:error, ElixirDB.Error.invalid_request("shadow attachment store type is invalid")}
  end

  defp required_timestamp(map) do
    value = Map.get(map, :created_at, Map.get(map, "created_at"))

    case DateTime.from_iso8601(value || "") do
      {:ok, _datetime, 0} -> {:ok, value}
      _ -> {:error, ElixirDB.Error.invalid_request("shadow creation time is invalid")}
    end
  end
end
