defmodule ElixirDB.Storage.SQLite.LocalRecords do
  @moduledoc """
  Local-record (checkpoint / job metadata) SQL helpers for the SQLite adapter.

  Owns fetch and compare-and-swap write helpers. Transaction boundaries remain
  in the adapter; documents/revisions mutation paths are still centralized.
  """

  alias ElixirDB.JSON.{Canonical, StrictDecoder}
  alias ElixirDB.MapAccess
  alias ElixirDB.Storage.SQLite.Adapter
  alias ElixirDB.Storage.SQLite.Connection
  @doc false
  def get(adapter, namespace, key),
    do: Adapter.get_local_record(adapter, namespace, key)

  @doc false
  def put(adapter, request),
    do: Adapter.put_local_record_cas(adapter, request)

  @doc """
  Loads one local record by namespace and key, or `nil` when absent.
  """
  @spec fetch(Connection.handle(), binary(), binary()) ::
          {:ok, nil | map()} | {:error, ElixirDB.Error.t()}
  def fetch(conn, namespace, key) do
    case Connection.query(
           conn,
           "SELECT record_version, value_json FROM local_records WHERE namespace = ? AND record_key = ?",
           [namespace, key]
         ) do
      {:ok, []} ->
        {:ok, nil}

      {:ok, [[version, value_json]]} ->
        with {:ok, value} <- StrictDecoder.decode(value_json),
             do: {:ok, %{version: version, value: value}}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  @doc """
  Compare-and-swap write for a local record inside an open transaction.
  """
  @spec put_cas_tx(map(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def put_cas_tx(%{conn: conn}, request) when is_map(request) do
    namespace = MapAccess.get(request, :namespace)
    key = MapAccess.get(request, :key)
    expected = MapAccess.get(request, :expected_version, 0)
    value = MapAccess.get(request, :value)

    with {:ok, current} <- fetch(conn, namespace, key),
         observed <- if(is_nil(current), do: 0, else: current.version),
         {:ok, json} <- Canonical.encode(value),
         :ok <- validate_request(namespace, key, expected, observed, current, json),
         {:ok, next_version, replayed} <- next_version(expected, observed, current, json),
         :ok <-
           Connection.execute(
             conn,
             "INSERT INTO local_records(namespace, record_key, record_version, value_json) VALUES (?, ?, ?, ?) ON CONFLICT(namespace, record_key) DO UPDATE SET record_version=excluded.record_version, value_json=excluded.value_json",
             [namespace, key, next_version, json]
           ) do
      {:ok, %{version: next_version, value: value, replayed: replayed}}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp validate_request(namespace, key, expected, _observed, _current, _json)
       when not is_binary(namespace) or not is_binary(key) or not is_integer(expected) or
              expected < 0,
       do: {:error, ElixirDB.Error.invalid_request("local record CAS fields are invalid")}

  defp validate_request(_namespace, _key, expected, observed, current, json)
       when observed != expected and not is_nil(current) do
    current_json = Canonical.encode!(current.value)

    if current_json == json do
      :ok
    else
      {:error,
       ElixirDB.Error.checkpoint_conflict("local record version is stale", %{
         expected_version: expected,
         observed_version: observed
       })}
    end
  end

  defp validate_request(_namespace, _key, expected, observed, nil, _json)
       when observed != expected,
       do:
         {:error,
          ElixirDB.Error.checkpoint_conflict("local record version is stale", %{
            expected_version: expected,
            observed_version: observed
          })}

  defp validate_request(_namespace, _key, _expected, _observed, _current, _json), do: :ok

  defp next_version(expected, observed, current, json)
       when observed != expected and not is_nil(current) do
    if Canonical.encode!(current.value) == json,
      do: {:ok, observed, true},
      else: {:error, ElixirDB.Error.checkpoint_conflict("local record version is stale")}
  end

  defp next_version(expected, observed, _current, _json) when expected == observed,
    do: {:ok, expected + 1, false}

  defp next_version(_expected, _observed, _current, _json),
    do: {:error, ElixirDB.Error.checkpoint_conflict("local record version is stale")}

  defp normalize_error(%ElixirDB.Error{} = error), do: error

  defp normalize_error(reason),
    do: ElixirDB.Error.internal_error("SQLite operation failed", %{cause: inspect(reason)})
end
