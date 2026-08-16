defmodule VialKeeper.Storage.SQLite.Indexes do
  @moduledoc "SQLite-derived structured index management and full-text catalog metadata."

  alias VialKeeper.MapAccess
  alias VialKeeper.Storage.SQLite.{Connection, QueryCompiler}

  @physical_version 1

  @spec create(Connection.handle(), binary(), map()) ::
          {:ok, map()} | {:error, VialKeeper.Error.t()}
  def create(conn, index_id, definition) do
    case type(definition) do
      :structured -> create_structured(conn, index_id, definition)
      :full_text -> create_full_text(index_id, definition)
      _ -> {:error, VialKeeper.Error.invalid_request("unknown logical index type")}
    end
  end

  @spec drop(Connection.handle(), map()) :: :ok | {:error, VialKeeper.Error.t()}
  def drop(conn, metadata) do
    if type(metadata) == :full_text do
      :ok
    else
      name = MapAccess.get(metadata, :physical_name)

      if valid_identifier?(name) do
        Connection.execute(conn, "DROP INDEX #{quote_identifier(name)}")
        |> normalize_result()
      else
        {:error, VialKeeper.Error.integrity_violation("index physical metadata is invalid")}
      end
    end
  end

  @spec rebuild(Connection.handle(), binary(), map()) ::
          {:ok, map()} | {:error, VialKeeper.Error.t()}
  def rebuild(conn, index_id, definition) do
    create(conn, index_id, definition)
  end

  @spec integrity(Connection.handle(), map()) ::
          {:ok, map()} | {:error, VialKeeper.Error.t()}
  def integrity(conn, metadata) do
    if type(metadata) == :full_text do
      {:ok, %{index_id: MapAccess.get(metadata, :index_id), physical: :external}}
    else
      structured_integrity(conn, metadata)
    end
  end

  def physical_name(index_id, :structured), do: "exdb_s_" <> suffix(index_id)

  defp structured_integrity(conn, metadata) do
    name = MapAccess.get(metadata, :physical_name)
    index_id = MapAccess.get(metadata, :index_id)
    expected_name = physical_name(index_id, :structured)

    with true <- valid_identifier?(name) and name == expected_name,
         {:ok, [[kind, sql]]} <-
           Connection.query(
             conn,
             "SELECT type, sql FROM sqlite_master WHERE name = ?",
             [name]
           ),
         true <- kind == "index" and is_binary(sql) do
      {:ok, %{index_id: index_id, physical: :present}}
    else
      false ->
        {:error,
         VialKeeper.Error.integrity_violation("derived index is missing", %{physical_name: name})}

      {:ok, []} ->
        {:error,
         VialKeeper.Error.integrity_violation("derived index is missing", %{physical_name: name})}

      {:error, reason} ->
        normalize_result({:error, reason})
    end
  end

  defp create_structured(conn, index_id, definition) do
    fields = MapAccess.get(definition, :fields, [])
    name = physical_name(index_id, :structured)

    with {:ok, expressions} <- reduce_compiled_expressions(fields) do
      sql =
        IO.iodata_to_binary([
          "CREATE INDEX ",
          quote_identifier(name),
          " ON documents (winning_deleted, ",
          Enum.join(expressions, ", "),
          ", document_id)"
        ])

      with :ok <- Connection.execute(conn, sql),
           {:ok, metadata} <- metadata(index_id, definition, name) do
        {:ok, metadata}
      else
        {:error, reason} -> normalize_result({:error, reason})
      end
    end
  end

  defp reduce_compiled_expressions(fields) do
    Enum.reduce_while(fields, {:ok, []}, fn field, {:ok, acc} ->
      case QueryCompiler.structured_expression(field) do
        {:ok, expression} -> {:cont, {:ok, [expression | acc]}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, expressions} -> {:ok, Enum.reverse(expressions)}
      error -> error
    end
  end

  defp create_full_text(index_id, definition) do
    metadata(index_id, definition, nil)
  end

  defp metadata(index_id, definition, name) do
    base = %{
      "index_id" => index_id,
      "physical_version" => @physical_version,
      "type" => type_string(definition),
      "fields" => MapAccess.get(definition, :fields, []),
      "analyzer" => "tantivy_default"
    }

    base = if is_binary(name), do: Map.put(base, "physical_name", name), else: base
    base = if type(definition) == :full_text, do: Map.put(base, "engine", "tantivy"), else: base
    {:ok, base}
  end

  defp type(%{"type" => "structured"}), do: :structured
  defp type(%{"type" => "full_text"}), do: :full_text
  defp type(%{type: :structured}), do: :structured
  defp type(%{type: "structured"}), do: :structured
  defp type(%{type: :full_text}), do: :full_text
  defp type(%{type: "full_text"}), do: :full_text
  defp type(_), do: nil

  defp type_string(definition),
    do: if(type(definition) == :full_text, do: "full_text", else: "structured")

  defp suffix(index_id) do
    index_id
    |> to_string()
    |> String.replace(~r/[^a-zA-Z0-9_]/, "_")
  end

  defp valid_identifier?(value) do
    is_binary(value) and Regex.match?(~r/^exdb_s_[a-zA-Z0-9_]+$/, value)
  end

  defp quote_identifier(value), do: "\"" <> String.replace(value, "\"", "\"\"") <> "\""

  defp normalize_result(:ok), do: :ok

  defp normalize_result({:error, reason}),
    do:
      {:error,
       VialKeeper.Error.internal_error("SQLite index operation failed", %{cause: inspect(reason)})}
end
