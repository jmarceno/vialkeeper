defmodule ElixirDB.Storage.SQLite.Indexes do
  @moduledoc "SQLite-derived structured and FTS5 index management."

  alias ElixirDB.Diagnostics
  alias ElixirDB.JSON.{Pointer, StrictDecoder}
  alias ElixirDB.MapAccess
  alias ElixirDB.Query.FullText
  alias ElixirDB.Storage.SQLite.{Connection, QueryCompiler}

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
    name = MapAccess.get(metadata, :physical_name)

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
      name = MapAccess.get(metadata, :physical_name)

      with true <- valid_identifier?(name),
           :ok <- delete_fts_row(conn, name, metadata, doc_key),
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
    search(conn, metadata, text, mode, nil)
  end

  @spec search(Connection.handle(), map(), binary(), binary(), term()) ::
          {:ok, list()} | {:error, ElixirDB.Error.t()}
  def search(conn, metadata, text, mode, deadline) do
    name = MapAccess.get(metadata, :physical_name)
    match_definition = Map.put(metadata, "mode", mode || "all")

    with true <- valid_identifier?(name),
         {:ok, query} <- compile_search(text, metadata, mode),
         :ok <- check_deadline(deadline),
         {:ok, rows} <-
           Connection.query(
             conn,
             "SELECT d.doc_key, d.document_id, d.winning_revision, d.winning_body_json, bm25(#{quote_identifier(name)}) FROM #{quote_identifier(name)} AS f JOIN documents AS d ON d.doc_key = f.rowid WHERE #{quote_identifier(name)} MATCH ? AND d.winning_deleted = 0",
             [query]
           ),
         :ok <- check_deadline(deadline),
         {:ok, candidates} <- decode_search_rows(rows, deadline),
         {:ok, filtered} <- filter_search_candidates(candidates, match_definition, text, deadline),
         {:ok, sorted} <- sort_search_candidates(filtered, deadline) do
      # FTS5 retrieves candidates; unicode_words_v1 via FullText.matches?/3 is authoritative.
      {:ok, sorted}
    else
      false -> {:error, ElixirDB.Error.integrity_violation("index physical metadata is invalid")}
      {:error, %ElixirDB.Error{} = error} -> {:error, error}
      {:error, reason} -> normalize_result({:error, reason})
    end
  end

  @spec rebuild(Connection.handle(), binary(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def rebuild(conn, index_id, definition) do
    create(conn, index_id, definition)
  end

  @spec integrity(Connection.handle(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def integrity(conn, metadata) do
    name = MapAccess.get(metadata, :physical_name)
    index_id = MapAccess.get(metadata, :index_id)

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
         true <- is_binary(sql),
         true <- full_text_sql_ok?(metadata, sql) do
      if type(metadata) == :full_text do
        check_full_text_integrity(conn, metadata, name)
      else
        {:ok, %{index_id: index_id, physical: :present}}
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

  def physical_name(index_id, :full_text), do: "fts_" <> digest24(index_id)

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

  defp create_full_text(conn, index_id, definition) do
    name = physical_name(index_id, :full_text)
    diacritics = get_in(definition, ["tokenization", "diacritics"]) || "preserve"
    remove = if diacritics == "remove", do: "2", else: "0"
    kind = fts_table_kind()

    sql = full_text_create_sql(name, remove, kind)

    with :ok <- Connection.execute(conn, sql),
         :ok <- rebuild_full_text_rows(conn, name, definition),
         {:ok, metadata} <- metadata(index_id, definition, name, kind) do
      {:ok, metadata}
    else
      {:error, reason} -> normalize_result({:error, reason})
    end
  end

  defp fts_table_kind do
    if Diagnostics.fts5_contentless_delete_supported?() do
      "contentless_delete"
    else
      "contentless"
    end
  end

  defp full_text_create_sql(name, remove, "contentless_delete") do
    "CREATE VIRTUAL TABLE #{quote_identifier(name)} USING fts5(content, tokenize = 'unicode61 remove_diacritics #{remove}', content='', contentless_delete=1)"
  end

  defp full_text_create_sql(name, remove, "contentless") do
    "CREATE VIRTUAL TABLE #{quote_identifier(name)} USING fts5(content, tokenize = 'unicode61 remove_diacritics #{remove}', content='')"
  end

  defp metadata(index_id, definition, name, fts_kind \\ nil) do
    base = %{
      "index_id" => index_id,
      "physical_name" => name,
      "physical_version" => @physical_version,
      "type" => type_string(definition),
      "fields" => MapAccess.get(definition, :fields, []),
      "tokenization" => MapAccess.get(definition, :tokenization, %{})
    }

    {:ok, if(fts_kind, do: Map.put(base, "fts_table_kind", fts_kind), else: base)}
  end

  defp rebuild_full_text_rows(conn, name, definition) do
    with {:ok, rows} <-
           Connection.query(
             conn,
             "SELECT doc_key, winning_body_json FROM documents WHERE winning_deleted = 0"
           ) do
      Enum.reduce_while(rows, :ok, fn [doc_key, body_json], :ok ->
        rebuild_full_text_row(conn, name, definition, doc_key, body_json)
      end)
    end
  end

  defp rebuild_full_text_row(conn, name, definition, doc_key, body_json) do
    case StrictDecoder.decode(body_json) do
      {:ok, body} ->
        case insert_text_if_present(conn, name, definition, doc_key, body, false) do
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, reason}}
        end

      {:error, error} ->
        {:halt, {:error, error}}
    end
  end

  defp delete_fts_row(conn, name, metadata, doc_key) do
    case MapAccess.get(metadata, :fts_table_kind, fts_table_kind()) do
      "contentless_delete" ->
        Connection.execute(conn, "DELETE FROM #{quote_identifier(name)} WHERE rowid = ?", [doc_key])

      "contentless" ->
        # Ordinary contentless tables reject DELETE; use the FTS5 delete command.
        # Without prior content we cannot remove tokens precisely — prefer contentless-delete.
        Connection.execute(
          conn,
          "INSERT INTO #{quote_identifier(name)}(#{quote_identifier(name)}, rowid, content) VALUES ('delete', ?, ?)",
          [doc_key, ""]
        )
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
        "INSERT INTO #{quote_identifier(name)} (rowid, content) VALUES (?, ?)",
        [doc_key, content]
      )
    end
  end

  defp extract_text(body, definition) do
    fields = MapAccess.get(definition, :fields, [])

    fields
    |> Enum.map(fn
      field when is_binary(field) -> field
      field -> MapAccess.get(field, :path)
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
    fields = MapAccess.get(metadata, :fields, [])
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
            "\"" <> Enum.map_join(terms, " ", &String.replace(&1, "\"", "\"\"")) <> "\""

          "prefix" ->
            Enum.map_join(terms, " AND ", &prefix_term/1)

          _ ->
            Enum.join(escaped, " AND ")
        end

      _ = fields
      {:ok, query}
    end
  end

  defp decode_search_rows(rows, deadline) do
    rows
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {row, index}, {:ok, acc} ->
      case periodic_deadline_check(deadline, index) do
        :ok -> decode_search_row(row, acc)
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp decode_search_row([doc_key, id, revision, body_json, rank], acc) do
    case StrictDecoder.decode(body_json) do
      {:ok, body} ->
        {:cont,
         {:ok, [%{doc_key: doc_key, id: id, revision: revision, body: body, rank: rank} | acc]}}

      {:error, error} ->
        {:halt, {:error, error}}
    end
  end

  defp filter_search_candidates(candidates, definition, text, deadline) do
    candidates
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, &filter_search_candidate(&1, &2, definition, text, deadline))
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp filter_search_candidate({row, index}, {:ok, acc}, definition, text, deadline) do
    case periodic_deadline_check(deadline, index) do
      :ok ->
        if FullText.matches?(row.body, definition, text),
          do: {:cont, {:ok, [row | acc]}},
          else: {:cont, {:ok, acc}}

      {:error, _} = error ->
        {:halt, error}
    end
  end

  defp sort_search_candidates(candidates, deadline) do
    with :ok <- check_deadline(deadline),
         sorted <- Enum.sort_by(candidates, fn row -> {row.rank, row.id} end),
         :ok <- check_deadline(deadline) do
      {:ok, sorted}
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
      {:ok, %{index_id: MapAccess.get(metadata, :index_id), physical: :consistent}}
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

  defp full_text_sql_ok?(metadata, sql) do
    if type(metadata) != :full_text do
      true
    else
      kind = MapAccess.get(metadata, :fts_table_kind, fts_table_kind())
      lowered = String.downcase(sql)

      case kind do
        "contentless_delete" ->
          String.contains?(lowered, "content=''") and
            String.contains?(lowered, "contentless_delete=1")

        "contentless" ->
          String.contains?(lowered, "content=''") and
            not String.contains?(lowered, "contentless_delete")

        _ ->
          false
      end
    end
  end

  defp expected_text?(json, definition) do
    with {:ok, body} <- StrictDecoder.decode(json),
         text when text != "" <- extract_text(body, definition) do
      true
    else
      _ -> false
    end
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

  defp digest24(index_id) do
    case to_string(index_id) do
      <<"idx_", digest::binary-size(24)>> ->
        digest

      other ->
        other
        |> then(&:crypto.hash(:sha256, &1))
        |> Base.encode16(case: :lower)
        |> binary_part(0, 24)
    end
  end

  defp valid_identifier?(value) do
    is_binary(value) and
      (Regex.match?(~r/^exdb_s_[a-zA-Z0-9_]+$/, value) or
         Regex.match?(~r/^fts_[0-9a-f]{24}$/, value))
  end

  defp check_deadline(nil), do: :ok

  defp check_deadline({deadline, maximum_ms}) do
    if System.monotonic_time() < deadline do
      :ok
    else
      {:error,
       ElixirDB.Error.resource_limit("query execution exceeded the configured limit", %{
         maximum_ms: maximum_ms
       })}
    end
  end

  defp periodic_deadline_check(nil, _index), do: :ok

  defp periodic_deadline_check(deadline, index) do
    if rem(index, 32) == 0, do: check_deadline(deadline), else: :ok
  end

  defp quote_identifier(value), do: "\"" <> String.replace(value, "\"", "\"\"") <> "\""
  defp quote_term(value), do: "\"" <> String.replace(value, "\"", "\"\"") <> "\""
  defp prefix_term(value), do: quote_term(value) <> "*"

  defp normalize_result(:ok), do: :ok
  defp normalize_result({:error, %ElixirDB.Error{} = error}), do: {:error, error}

  defp normalize_result({:error, reason}),
    do:
      {:error,
       ElixirDB.Error.internal_error("SQLite index operation failed", %{cause: inspect(reason)})}
end
