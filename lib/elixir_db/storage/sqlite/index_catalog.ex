defmodule ElixirDB.Storage.SQLite.IndexCatalog do
  @moduledoc """
  Index-definition catalog SQL for the Version 1 SQLite adapter.

  Owns create/delete/rebuild/list against `index_definitions` and ready-index
  document refresh. Transaction boundaries remain in the adapter.
  """

  alias ElixirDB.JSON.{Canonical, StrictDecoder}
  alias ElixirDB.Storage.SQLite.{Connection, FullTextIndexes, StructuredIndexes}

  @doc """
  Lists all logical index definitions with adapter metadata attached.
  """
  @spec list(Connection.handle()) :: {:ok, [map()]} | {:error, ElixirDB.Error.t()}
  def list(conn) do
    with {:ok, rows} <-
           Connection.query(
             conn,
             "SELECT index_id, definition_json, definition_digest, lifecycle_state, adapter_metadata_json FROM index_definitions ORDER BY index_id"
           ) do
      {:ok,
       Enum.map(rows, fn [id, json, digest_value, state, metadata_json] ->
         definition = decode_json!(json)
         metadata = decode_json!(metadata_json)

         definition
         |> Map.merge(%{
           "index_id" => id,
           "definition_digest" => digest_value,
           "lifecycle_state" => state,
           "_metadata" => metadata
         })
       end)}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @doc """
  Creates or returns an existing identical index definition inside an open transaction.
  """
  @spec create_tx(Connection.handle(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def create_tx(conn, definition) do
    definition = Map.delete(definition, "definition_digest") |> Map.delete(:definition_digest)

    with {:ok, definition_json} <- Canonical.encode(definition),
         digest <- :crypto.hash(:sha256, definition_json) |> Base.encode16(case: :lower),
         id <- "idx_" <> binary_part(digest, 0, 24),
         {:ok, existing} <- find_by_name(conn, index_name(definition), definition_json, digest),
         {:ok, result} <-
           (case existing do
              nil ->
                with {:ok, metadata} <- create_physical(conn, id, definition),
                     {:ok, metadata_json} <- Canonical.encode(metadata),
                     :ok <-
                       Connection.execute(
                         conn,
                         "INSERT INTO index_definitions(index_id, name, index_type, definition_digest, definition_json, lifecycle_state, adapter_metadata_json) VALUES (?, ?, ?, ?, ?, 'ready', ?)",
                         [
                           id,
                           index_name(definition),
                           index_type(definition),
                           digest,
                           definition_json,
                           metadata_json
                         ]
                       ) do
                  {:ok, index_result(definition, id, digest)}
                end

              existing ->
                {:ok, existing}
            end) do
      {:ok, result}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @doc """
  Drops physical storage and deletes the catalog row inside an open transaction.
  """
  @spec delete_tx(Connection.handle(), binary()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def delete_tx(conn, index_id) do
    with {:ok, row} <- find(conn, index_id),
         {:ok, metadata} <- decode_metadata(row),
         :ok <- drop_physical(conn, metadata),
         :ok <-
           Connection.execute(conn, "DELETE FROM index_definitions WHERE index_id = ?", [index_id]) do
      {:ok, %{index_id: index_id, deleted: true}}
    end
  end

  @doc """
  Rebuilds physical storage for one index inside an open transaction.
  """
  @spec rebuild_tx(Connection.handle(), binary()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def rebuild_tx(conn, index_id) do
    with {:ok, row} <- find(conn, index_id),
         {:ok, definition} <- decode_json(row.definition_json),
         {:ok, old_metadata} <- decode_metadata(row),
         :ok <- drop_physical(conn, old_metadata),
         {:ok, new_metadata} <- create_physical(conn, index_id, definition),
         {:ok, metadata_json} <- Canonical.encode(new_metadata),
         :ok <-
           Connection.execute(
             conn,
             "UPDATE index_definitions SET lifecycle_state = 'ready', adapter_metadata_json = ? WHERE index_id = ?",
             [metadata_json, index_id]
           ) do
      {:ok, %{index_id: index_id, rebuilt: true}}
    end
  end

  @doc """
  Refreshes all ready indexes for one document after winner materialization.
  """
  @spec refresh_ready(Connection.handle(), integer(), map()) :: :ok | {:error, ElixirDB.Error.t()}
  def refresh_ready(conn, doc_key, winner) do
    with {:ok, rows} <-
           Connection.query(
             conn,
             "SELECT index_id, definition_json, adapter_metadata_json FROM index_definitions WHERE lifecycle_state = 'ready' ORDER BY index_id"
           ) do
      Enum.reduce_while(rows, :ok, fn [index_id, definition_json, metadata_json], :ok ->
        with {:ok, definition} <- decode_json(definition_json),
             {:ok, metadata} <- decode_json(metadata_json),
             :ok <-
               FullTextIndexes.refresh_document(
                 conn,
                 Map.merge(Map.put(metadata, "index_id", index_id), definition),
                 doc_key,
                 winner.body,
                 winner.deleted
               ) do
          {:cont, :ok}
        else
          {:error, error} -> {:halt, {:error, error}}
        end
      end)
    end
  end

  defp create_physical(conn, index_id, definition) do
    case index_type(definition) do
      "full_text" -> FullTextIndexes.create_physical(conn, index_id, definition)
      _ -> StructuredIndexes.create_physical(conn, index_id, definition)
    end
  end

  defp drop_physical(conn, metadata) do
    case metadata["index_type"] || metadata[:index_type] || metadata["type"] || metadata[:type] do
      "full_text" -> FullTextIndexes.drop(conn, metadata)
      :full_text -> FullTextIndexes.drop(conn, metadata)
      _ -> StructuredIndexes.drop(conn, metadata)
    end
  end

  defp index_name(definition), do: definition["name"] || definition[:name]

  defp index_type(definition) do
    case definition["type"] || definition[:type] do
      :structured -> "structured"
      :full_text -> "full_text"
      value -> value
    end
  end

  defp index_result(definition, id, digest_value),
    do:
      definition
      |> stringify_definition()
      |> Map.merge(%{
        "index_id" => id,
        "definition_digest" => digest_value,
        "lifecycle_state" => "ready"
      })

  defp stringify_definition(value) when is_map(value),
    do: Map.new(value, fn {key, child} -> {to_string(key), stringify_definition(child)} end)

  defp stringify_definition(value) when is_list(value), do: Enum.map(value, &stringify_definition/1)
  defp stringify_definition(value), do: value

  defp find_by_name(conn, name, definition_json, digest_value) do
    case Connection.query(
           conn,
           "SELECT index_id, definition_digest, definition_json, lifecycle_state FROM index_definitions WHERE name = ?",
           [name]
         ) do
      {:ok, []} ->
        {:ok, nil}

      {:ok, [[id, existing_digest, existing_json, state]]} when existing_digest == digest_value ->
        with {:ok, definition} <- decode_json(existing_json) do
          {:ok,
           Map.merge(definition, %{
             "index_id" => id,
             "definition_digest" => digest_value,
             "lifecycle_state" => state
           })}
        end

      {:ok, [[_id, _existing_digest, _existing_json, _state]]} ->
        {:error,
         ElixirDB.Error.index_name_conflict("index name is already used by another definition", %{
           name: name,
           definition: definition_json
         })}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp find(conn, index_id) do
    case Connection.query(
           conn,
           "SELECT index_id, name, index_type, definition_digest, definition_json, lifecycle_state, adapter_metadata_json FROM index_definitions WHERE index_id = ?",
           [index_id]
         ) do
      {:ok, [[id, name, type, digest_value, definition_json, state, metadata_json]]} ->
        {:ok,
         %{
           index_id: id,
           name: name,
           index_type: type,
           definition_digest: digest_value,
           definition_json: definition_json,
           lifecycle_state: state,
           adapter_metadata_json: metadata_json
         }}

      {:ok, []} ->
        {:error, ElixirDB.Error.index_not_found("index not found", %{index: index_id})}

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp decode_metadata(row) do
    with {:ok, metadata} <- decode_json(row.adapter_metadata_json) do
      {:ok, Map.put(metadata, "index_id", row.index_id)}
    end
  end

  defp decode_json(json), do: StrictDecoder.decode(json)

  defp decode_json!(json) do
    case decode_json(json) do
      {:ok, value} -> value
      _ -> nil
    end
  end

  # Pass already-typed domain errors (e.g. index_name_conflict, index_not_found) through
  # unchanged so they keep their HTTP status and code. Only wrap raw SQLite/driver reasons
  # as a generic internal_error. Without this, a legitimate 409 index_name_conflict was
  # being degraded to a 500 internal_error by the `with`...`else` clause in create_tx.
  defp normalize_error(%ElixirDB.Error{} = error), do: error

  defp normalize_error(reason),
    do: ElixirDB.Error.internal_error("SQLite operation failed", %{cause: inspect(reason)})
end
