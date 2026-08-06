defmodule ElixirDB.Storage.SQLite.Indexes do
  @moduledoc "SQLite-derived structured and FTS5 index management."

  alias ElixirDB.JSON.{Pointer, StrictDecoder}
  alias ElixirDB.Query.FullText
  alias ElixirDB.Storage.SQLite.Connection

  @physical_version 1

  @spec create(Connection.handle(), binary(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def create(conn, index_id, definition) do
    case type(definition) do
      :structured -> create_structured(conn, index_id, definition)
      :full_text -> create_full_text(conn, index_id, definition)
      _ -> {:error, ElixirDB.Error.invalid_request("unknown logical index type")}
    end
  end

  @spec drop(Connection.handle(), map()) :: :ok | {:error, ElixirDB.Error.t()}
  def drop(conn, metadata) do
    name = metadata["physical_name"] || metadata[:physical_name]

    if valid_identifier?(name) do
      Connection.execute(conn, "DROP #{drop_kind(metadata)} #{quote_identifier(name)}")
      |> normalize_result()
    else
      {:error, ElixirDB.Error.integrity_violation("index physical metadata is invalid")}
    end
  end

  @spec refresh_document(Connection.handle(), map(), integer(), map() | nil, boolean()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def refresh_document(conn, metadata, doc_key, body, deleted) do
    if type(metadata) == :full_text do
      name = metadata["physical_name"] || metadata[:physical_name]

      with true <- valid_identifier?(name),
           :ok <-
             Connection.execute(
               conn,
               "DELETE FROM #{quote_identifier(name)} WHERE rowid = ?",
               [doc_key]
             ),
           :ok <- insert_text_if_present(conn, name, metadata, doc_key, body, deleted) do
        :ok
      else
        false -> {:error, ElixirDB.Error.integrity_violation("index physical metadata is invalid")}
        {:error, reason} -> normalize_result({:error, reason})
      end
    else
      :ok
    end
  end

  @spec search(Connection.handle(), map(), binary(), binary()) ::
          {:ok, list()} | {:error, ElixirDB.Error.t()}
  def search(conn, metadata, text, mode) do
    name = metadata["physical_name"] || metadata[:physical_name]

    with true <- valid_identifier?(name),
         {:ok, query} <- compile_search(text, metadata, mode),
         {:ok, rows} <-
           Connection.query(
             conn,
             "SELECT d.doc_key, d.document_id, d.winning_revision, d.winning_body_json, bm25(#{quote_identifier(name)}) FROM #{quote_identifier(name)} AS f JOIN documents AS d ON d.doc_key = f.rowid WHERE #{quote_identifier(name)} MATCH ? AND d.winning_deleted = 0",
             [query]
           ),
         {:ok, result} <- decode_search_rows(rows) do
      {:ok, Enum.sort_by(result, fn row -> {row.rank, row.id} end)}
    else
      false -> {:error, ElixirDB.Error.integrity_violation("index physical metadata is invalid")}
      {:error, %ElixirDB.Error{} = error} -> {:error, error}
      {:error, reason} -> normalize_result({:error, reason})
    end
  end

  @spec rebuild(Connection.handle(), binary(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def rebuild(conn, index_id, definition) do
    with {:ok, metadata} <- create(conn, index_id, definition) do
      {:ok, metadata}
    end
  end

  @spec integrity(Connection.handle(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def integrity(conn, metadata) do
    name = metadata["physical_name"] || metadata[:physical_name]
    index_id = metadata["index_id"] || metadata[:index_id]

    expected_name =
      physical_name(index_id, if(type(metadata) == :full_text, do: :full_text, else: :structured))

    with true <- valid_identifier?(name) and name == expected_name,
         {:ok, [[kind, sql]]} <-
           Connection.query(
             conn,
             "SELECT type, sql FROM sqlite_master WHERE name = ?",
             [name]
           ),
         true <-
           (type(metadata) == :full_text and kind == "table") or
             (type(metadata) == :structured and kind == "index"),
         true <- is_binary(sql) do
      if type(metadata) == :full_text do
        check_full_text_integrity(conn, metadata, name)
      else
        {:ok, %{index_id: metadata["index_id"] || metadata[:index_id], physical: :present}}
      end
    else
      false ->
        {:error,
         ElixirDB.Error.integrity_violation("derived index is missing", %{physical_name: name})}

      {:ok, []} ->
        {:error,
         ElixirDB.Error.integrity_violation("derived index is missing", %{physical_name: name})}

      {:error, reason} ->
        normalize_result({:error, reason})
    end
  end

  def physical_name(index_id, :structured), do: "exdb_s_" <> suffix(index_id)
  def physical_name(index_id, :full_text), do: "exdb_f_" <> suffix(index_id)

  defp create_structured(conn, index_id, definition) do
    fields = definition["fields"] || definition[:fields] || []
    name = physical_name(index_id, :structured)
    expressions = Enum.map(fields, &structured_expression/1)

    sql =
      "CREATE INDEX #{quote_identifier(name)} ON documents (winning_deleted, " <>
        Enum.join(expressions, ", ") <> ", document_id)"

    with :ok <- Connection.execute(conn, sql),
         {:ok, metadata} <- metadata(index_id, definition, name) do
      {:ok, metadata}
    else
      {:error, reason} -> normalize_result({:error, reason})
    end
  end

  defp create_full_text(conn, index_id, definition) do
    name = physical_name(index_id, :full_text)
    diacritics = get_in(definition, ["tokenization", "diacritics"]) || "preserve"
    remove = if diacritics == "remove", do: "2", else: "0"

    sql =
      "CREATE VIRTUAL TABLE #{quote_identifier(name)} USING fts5(document_key UNINDEXED, content, tokenize = 'unicode61 remove_diacritics #{remove}')"

    with :ok <- Connection.execute(conn, sql),
         :ok <- rebuild_full_text_rows(conn, name, definition),
         {:ok, metadata} <- metadata(index_id, definition, name) do
      {:ok, metadata}
    else
      {:error, reason} -> normalize_result({:error, reason})
    end
  end

  defp metadata(index_id, definition, name) do
    {:ok,
     %{
       "index_id" => index_id,
       "physical_name" => name,
       "physical_version" => @physical_version,
       "type" => type_string(definition),
       "fields" => definition["fields"] || definition[:fields] || [],
       "tokenization" => definition["tokenization"] || definition[:tokenization] || %{}
     }}
  end

  defp rebuild_full_text_rows(conn, name, definition) do
    with {:ok, rows} <-
           Connection.query(
             conn,
             "SELECT doc_key, winning_body_json FROM documents WHERE winning_deleted = 0"
           ) do
      Enum.reduce_while(rows, :ok, fn [doc_key, body_json], :ok ->
        case StrictDecoder.decode(body_json) do
          {:ok, body} ->
            case insert_text_if_present(conn, name, definition, doc_key, body, false) do
              :ok -> {:cont, :ok}
              {:error, reason} -> {:halt, {:error, reason}}
            end

          {:error, error} ->
            {:halt, {:error, error}}
        end
      end)
    end
  end

  defp insert_text_if_present(_conn, _name, _definition, _doc_key, _body, true), do: :ok

  defp insert_text_if_present(conn, name, definition, doc_key, body, false) do
    content = extract_text(body, definition)

    if content == "" do
      :ok
    else
      Connection.execute(
        conn,
        "INSERT INTO #{quote_identifier(name)} (rowid, document_key, content) VALUES (?, ?, ?)",
        [doc_key, doc_key, content]
      )
    end
  end

  defp extract_text(body, definition) do
    fields = definition["fields"] || definition[:fields] || []

    fields
    |> Enum.map(fn
      field when is_binary(field) -> field
      field -> field["path"] || field[:path]
    end)
    |> Enum.flat_map(fn path ->
      case Pointer.get(body, path) do
        {:ok, value} when is_binary(value) -> [value]
        _ -> []
      end
    end)
    |> Enum.join("\n")
  end

  defp compile_search(text, metadata, mode) do
    fields = metadata["fields"] || metadata[:fields] || []
    diacritics = get_in(metadata, ["tokenization", "diacritics"]) || "preserve"
    terms = FullText.tokens(text, if(diacritics == "remove", do: :remove, else: :preserve))

    if terms == [] do
      {:error, ElixirDB.Error.invalid_request("full-text search requires at least one term")}
    else
      escaped = Enum.map(terms, &quote_term/1)

      query =
        case mode do
          "any" ->
            Enum.join(escaped, " OR ")

          "phrase" ->
            "\"" <> Enum.join(terms |> Enum.map(&String.replace(&1, "\"", "\"\"")), " ") <> "\""

          _ ->
            Enum.join(escaped, " AND ")
        end

      _ = fields
      {:ok, query}
    end
  end

  defp decode_search_rows(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn [doc_key, id, revision, body_json, rank], {:ok, acc} ->
      case StrictDecoder.decode(body_json) do
        {:ok, body} ->
          {:cont,
           {:ok, [%{doc_key: doc_key, id: id, revision: revision, body: body, rank: rank} | acc]}}

        {:error, error} ->
          {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp check_full_text_integrity(conn, metadata, name) do
    expected =
      case Connection.query(
             conn,
             "SELECT doc_key, winning_body_json FROM documents WHERE winning_deleted = 0"
           ) do
        {:ok, rows} ->
          rows
          |> Enum.filter(fn [_key, json] -> expected_text?(json, metadata) end)
          |> Enum.map(&List.first/1)
          |> MapSet.new()

        {:error, reason} ->
          throw({:error, reason})
      end

    with {:ok, rows} <- Connection.query(conn, "SELECT rowid FROM #{quote_identifier(name)}"),
         actual <- rows |> Enum.map(&List.first/1) |> MapSet.new(),
         true <- actual == expected do
      {:ok, %{index_id: metadata["index_id"] || metadata[:index_id], physical: :consistent}}
    else
      false ->
        {:error,
         ElixirDB.Error.integrity_violation("full-text index contents are stale", %{index: name})}

      {:error, reason} ->
        normalize_result({:error, reason})
    end
  catch
    {:error, reason} -> normalize_result({:error, reason})
  end

  defp expected_text?(json, definition) do
    with {:ok, body} <- StrictDecoder.decode(json),
         text when text != "" <- extract_text(body, definition) do
      true
    else
      _ -> false
    end
  end

  defp structured_expression(field) do
    path = field["path"] || field[:path] || field
    sqlite_path = sqlite_path(path)

    "json_type(winning_body_json, #{quote_literal(sqlite_path)}), json_extract(winning_body_json, #{quote_literal(sqlite_path)})"
  end

  defp sqlite_path(path) do
    {:ok, tokens} = Pointer.parse(path)

    Enum.reduce(tokens, "$", fn token, acc ->
      acc <> ".\"" <> String.replace(token, "\"", "\"\"") <> "\""
    end)
  end

  defp drop_kind(metadata), do: if(type(metadata) == :full_text, do: "TABLE", else: "INDEX")
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

  defp valid_identifier?(value),
    do: is_binary(value) and Regex.match?(~r/^exdb_[sf]_[a-zA-Z0-9_]+$/, value)

  defp quote_identifier(value), do: "\"" <> String.replace(value, "\"", "\"\"") <> "\""
  defp quote_literal(value), do: "'" <> String.replace(value, "'", "''") <> "'"
  defp quote_term(value), do: "\"" <> String.replace(value, "\"", "\"\"") <> "\""

  defp normalize_result(:ok), do: :ok
  defp normalize_result({:error, %ElixirDB.Error{} = error}), do: {:error, error}

  defp normalize_result({:error, reason}),
    do:
      {:error,
       ElixirDB.Error.internal_error("SQLite index operation failed", %{cause: inspect(reason)})}
end
