defmodule ElixirDB.Storage.SQLite.RetentionRecords do
  @moduledoc """
  Local-record namespaces for compact-retention metadata.

  ## Namespaces

  * `"peer_ledger"` — key is `peer_database_uuid`, value is a `PeerPosition` wire map.
  * `"retention_boundaries"` — key is `document_id/history_id`. Value wraps `boundary`
    (wire map) and `compaction_epoch`.
  * `"retention_maintenance"` — key `"counter"` holds the maintenance counter;
    key `"last_result"` holds the most recent compaction stats map.
  """

  alias ElixirDB.Domain.{PeerPosition, RetentionBoundary}
  alias ElixirDB.JSON.{Canonical, StrictDecoder}
  alias ElixirDB.Storage.SQLite.Connection

  @peer_ledger "peer_ledger"
  @retention_boundaries "retention_boundaries"
  @retention_maintenance "retention_maintenance"

  @boundary_key_sep "/"

  @spec boundary_key(binary(), binary()) :: binary()
  def boundary_key(document_id, history_id),
    do: document_id <> @boundary_key_sep <> history_id

  @spec parse_boundary_key(binary()) :: {binary(), binary()} | :error
  def parse_boundary_key(key) when is_binary(key) do
    case String.split(key, @boundary_key_sep, parts: 2) do
      [document_id, history_id]
      when document_id != "" and history_id != "" ->
        {document_id, history_id}

      _ ->
        :error
    end
  end

  @spec list_by_namespace(Connection.handle(), binary()) ::
          {:ok, [%{key: binary(), version: non_neg_integer(), value: map()}]}
          | {:error, ElixirDB.Error.t()}
  def list_by_namespace(conn, namespace) when is_binary(namespace) do
    case Connection.query(
           conn,
           "SELECT record_key, record_version, value_json FROM local_records WHERE namespace = ? ORDER BY record_key",
           [namespace]
         ) do
      {:ok, rows} ->
        decode_namespace_rows(rows)

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp decode_namespace_rows(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn [key, version, json], {:ok, acc} ->
      decode_namespace_row(key, version, json, acc)
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      error -> error
    end
  end

  defp decode_namespace_row(key, version, json, acc) do
    case StrictDecoder.decode(json) do
      {:ok, value} -> {:cont, {:ok, [%{key: key, version: version, value: value} | acc]}}
      {:error, error} -> {:halt, {:error, error}}
    end
  end

  @spec list_peers(Connection.handle()) ::
          {:ok, [PeerPosition.t()]} | {:error, ElixirDB.Error.t()}
  def list_peers(conn) do
    with {:ok, rows} <- list_by_namespace(conn, @peer_ledger) do
      decode_peer_rows(rows)
    end
  end

  defp decode_peer_rows(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn %{value: value}, {:ok, acc} ->
      decode_peer_row(value, acc)
    end)
    |> case do
      {:ok, peers} -> {:ok, Enum.reverse(peers)}
      error -> error
    end
  end

  defp decode_peer_row(value, acc) do
    case PeerPosition.from_wire(value) do
      {:ok, peer} -> {:cont, {:ok, [peer | acc]}}
      {:error, error} -> {:halt, {:error, error}}
    end
  end

  @spec list_boundaries(Connection.handle()) ::
          {:ok, [%{boundary: RetentionBoundary.t(), compaction_epoch: non_neg_integer()}]}
          | {:error, ElixirDB.Error.t()}
  def list_boundaries(conn) do
    with {:ok, rows} <- list_by_namespace(conn, @retention_boundaries) do
      decode_boundary_rows(rows)
    end
  end

  defp decode_boundary_rows(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn %{value: value}, {:ok, acc} ->
      decode_boundary_row(value, acc)
    end)
    |> case do
      {:ok, boundaries} -> {:ok, Enum.reverse(boundaries)}
      error -> error
    end
  end

  defp decode_boundary_row(value, acc) do
    with {:ok, boundary} <- RetentionBoundary.from_wire(Map.fetch!(value, "boundary")),
         epoch when is_integer(epoch) <- Map.get(value, "compaction_epoch", 0) do
      {:cont, {:ok, [%{boundary: boundary, compaction_epoch: epoch} | acc]}}
    else
      {:error, error} ->
        {:halt, {:error, error}}

      _ ->
        {:halt,
         {:error, ElixirDB.Error.integrity_violation("retention boundary record is invalid")}}
    end
  end

  @spec encode_boundary(RetentionBoundary.t(), non_neg_integer()) :: map()
  def encode_boundary(%RetentionBoundary{} = boundary, compaction_epoch) do
    %{
      "boundary" => %{
        "document_id" => boundary.document_id,
        "history_id" => boundary.history_id,
        "minimum_retained_generation" => boundary.minimum_retained_generation,
        "retired" => boundary.retired,
        "retired_branch_roots" => boundary.retired_branch_roots
      },
      "compaction_epoch" => compaction_epoch
    }
  end

  @spec maintenance_counter(Connection.handle()) ::
          {:ok, non_neg_integer()} | {:error, ElixirDB.Error.t()}
  def maintenance_counter(conn) do
    case fetch_maintenance(conn, "counter") do
      {:ok, nil} ->
        {:ok, 0}

      {:ok, %{value: %{"value" => value}}} when is_integer(value) and value >= 0 ->
        {:ok, value}

      {:ok, _} ->
        {:error, ElixirDB.Error.integrity_violation("retention maintenance counter is invalid")}

      {:error, error} ->
        {:error, error}
    end
  end

  @spec put_maintenance_counter(Connection.handle(), non_neg_integer()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def put_maintenance_counter(conn, counter) when is_integer(counter) and counter >= 0 do
    put_maintenance(conn, "counter", %{"value" => counter})
  end

  @spec put_last_result(Connection.handle(), map()) :: :ok | {:error, ElixirDB.Error.t()}
  def put_last_result(conn, result) when is_map(result) do
    put_maintenance(conn, "last_result", result)
  end

  @spec peer_ledger_namespace() :: binary()
  def peer_ledger_namespace, do: @peer_ledger

  @spec retention_boundaries_namespace() :: binary()
  def retention_boundaries_namespace, do: @retention_boundaries

  defp fetch_maintenance(conn, key) do
    case Connection.query(
           conn,
           "SELECT record_version, value_json FROM local_records WHERE namespace = ? AND record_key = ?",
           [@retention_maintenance, key]
         ) do
      {:ok, []} -> {:ok, nil}
      {:ok, [[version, json]]} -> decode_record(version, json)
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp put_maintenance(conn, key, value) do
    case Canonical.encode(value) do
      {:ok, json} ->
        Connection.execute(
          conn,
          "INSERT INTO local_records(namespace, record_key, record_version, value_json) VALUES (?, ?, 1, ?) ON CONFLICT(namespace, record_key) DO UPDATE SET record_version = record_version + 1, value_json = excluded.value_json",
          [@retention_maintenance, key, json]
        )

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp decode_record(version, json) do
    case StrictDecoder.decode(json) do
      {:ok, value} -> {:ok, %{version: version, value: value}}
      {:error, error} -> {:error, error}
    end
  end

  defp normalize_error(%ElixirDB.Error{} = error), do: error

  defp normalize_error(reason),
    do: ElixirDB.Error.internal_error("SQLite operation failed", %{cause: inspect(reason)})
end
