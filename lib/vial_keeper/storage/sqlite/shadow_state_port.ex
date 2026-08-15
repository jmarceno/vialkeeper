defmodule VialKeeper.Storage.SQLite.ShadowStatePort do
  @moduledoc "SQLite shadow identity and source-origin port implementation."
  @behaviour VialKeeper.Storage.Ports.ShadowState

  alias VialKeeper.Shadow.Metadata
  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.LocalRecordRequest
  alias VialKeeper.Storage.Ports.Errors
  alias VialKeeper.Storage.SQLite.{Connection, Context, LocalRecords, Transaction}

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
        _ -> {:error, VialKeeper.Error.integrity_violation("shadow origin row is invalid")}
      end
    else
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def put_origin(%BackendContext{} = context, document_id, sequence)
      when is_binary(document_id) and is_integer(sequence) and sequence >= 0 do
    with {:ok, adapter} <- Context.unwrap(context) do
      put_origin_tx(adapter, context, document_id, sequence)
    end
  end

  def put_origin(_context, _document_id, _sequence),
    do: {:error, VialKeeper.Error.invalid_request("shadow origin is invalid")}

  @impl true
  def watermark(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      case LocalRecords.fetch(adapter.conn, "shadow_state", "watermark") do
        {:ok, nil} -> {:ok, 0}
        {:ok, %{value: value}} when is_integer(value) and value >= 0 -> {:ok, value}
        {:ok, _} -> {:error, VialKeeper.Error.integrity_violation("shadow watermark is invalid")}
        {:error, reason} -> {:error, Errors.normalize(reason)}
      end
    end
  end

  @impl true
  def put_watermark(%BackendContext{} = context, sequence)
      when is_integer(sequence) and sequence >= 0 do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, current} <- watermark(context),
         true <- sequence >= current,
         {:ok, record} <- LocalRecords.fetch(adapter.conn, "shadow_state", "watermark"),
         request <- watermark_request(record, sequence),
         {:ok, result} <- LocalRecords.put_cas_tx(adapter, request) do
      {:ok, result.value}
    else
      false ->
        {:error,
         VialKeeper.Error.shadow_generation_conflict("shadow watermark cannot move backwards")}

      {:error, %VialKeeper.Error{} = error} ->
        {:error, error}
    end
  end

  def put_watermark(_context, _sequence),
    do: {:error, VialKeeper.Error.invalid_request("shadow watermark is invalid")}

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
    Metadata.new(
      source_uuid,
      shadow_uuid,
      generation,
      operation_id,
      store_type,
      location,
      digest,
      created_at
    )
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
      {:error, %VialKeeper.Error{} = error} -> {:error, error}
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
      else: {:error, VialKeeper.Error.shadow_identity_conflict("shadow metadata is immutable")}
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
     VialKeeper.Error.shadow_generation_conflict(
       "source origin sequence cannot move backwards",
       %{document_id: document_id, current: current, requested: sequence}
     )}
  end

  defp required_binary(map, key) do
    value = Map.get(map, key, Map.get(map, Atom.to_string(key)))

    if is_binary(value) and value != "",
      do: {:ok, value},
      else: {:error, VialKeeper.Error.invalid_request("shadow metadata field is invalid")}
  end

  defp required_integer(map, key) do
    value = Map.get(map, key, Map.get(map, Atom.to_string(key)))

    if is_integer(value) and value > 0,
      do: {:ok, value},
      else: {:error, VialKeeper.Error.invalid_request("shadow metadata field is invalid")}
  end

  defp required_store_type(map) do
    if Map.get(map, :attachment_store_type, Map.get(map, "attachment_store_type")) == "external_cas",
      do: {:ok, "external_cas"},
      else: {:error, VialKeeper.Error.invalid_request("shadow attachment store type is invalid")}
  end

  defp required_timestamp(map) do
    value = Map.get(map, :created_at, Map.get(map, "created_at"))

    case DateTime.from_iso8601(value || "") do
      {:ok, _datetime, 0} -> {:ok, value}
      _ -> {:error, VialKeeper.Error.invalid_request("shadow creation time is invalid")}
    end
  end

  defp watermark_request(nil, sequence), do: watermark_request(0, sequence)

  defp watermark_request(%{version: version}, sequence),
    do: watermark_request(version, sequence)

  defp watermark_request(expected_version, sequence),
    do: LocalRecordRequest.new("shadow_state", "watermark", expected_version, sequence)
end
