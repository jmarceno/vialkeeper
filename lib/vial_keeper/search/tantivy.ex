defmodule VialKeeper.Search.Tantivy do
  @moduledoc """
  The TantivyEx boundary for VialKeeper full-text indexes.

  Tantivy owns text analysis, term dictionaries, postings, phrase positions,
  prefix expansion, scoring, and on-disk segment persistence. VialKeeper only
  supplies the stable document id and the configured JSON-pointer field values.
  """

  alias VialKeeper.JSON.Pointer
  alias VialKeeper.MapAccess

  @content_field "content"
  @id_field "id"
  @backend_version "0.4.1"
  @schema_fingerprint "tantivy-ex-v1:content=fast_stored/default-positional:id=text_stored/raw"

  @type handle :: %{
          index: reference(),
          schema: reference(),
          writer: reference(),
          searcher: reference() | nil,
          path: binary(),
          definition: map()
        }

  @spec create(binary(), map()) :: {:ok, handle()} | {:error, term()}
  def create(path, definition) when is_binary(path) and is_map(definition) do
    with :ok <- File.mkdir_p(path),
         schema <- schema(),
         {:ok, index} <- TantivyEx.Index.create_in_dir(path, schema),
         {:ok, writer} <- TantivyEx.IndexWriter.new(index, writer_memory_bytes()) do
      {:ok,
       %{
         index: index,
         schema: schema,
         writer: writer,
         searcher: nil,
         path: path,
         definition: definition
       }}
    else
      {:error, reason} -> {:error, error("Tantivy index creation failed", reason)}
    end
  rescue
    error -> {:error, error("Tantivy index creation failed", error)}
  end

  @spec open(binary(), map()) :: {:ok, handle()} | {:error, term()}
  def open(path, definition) when is_binary(path) and is_map(definition) do
    with schema <- schema(),
         {:ok, index} <- TantivyEx.Index.open(path),
         {:ok, writer} <- TantivyEx.IndexWriter.new(index, writer_memory_bytes()),
         {:ok, searcher} <- TantivyEx.Searcher.new(index) do
      {:ok,
       %{
         index: index,
         schema: schema,
         writer: writer,
         searcher: searcher,
         path: path,
         definition: definition
       }}
    else
      {:error, reason} -> {:error, error("Tantivy index open failed", reason)}
    end
  rescue
    error -> {:error, error("Tantivy index open failed", error)}
  end

  @spec commit(handle()) :: {:ok, handle()} | {:error, term()}
  def commit(%{index: index, writer: writer} = handle) do
    with :ok <- checked(:commit, fn -> TantivyEx.IndexWriter.commit(writer) end),
         {:ok, searcher} <- TantivyEx.Searcher.new(index) do
      {:ok, %{handle | searcher: searcher}}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @spec rollback(handle()) :: :ok | {:error, term()}
  def rollback(%{writer: writer}) do
    checked(:rollback, fn -> TantivyEx.IndexWriter.rollback(writer) end)
  end

  @spec add(handle(), binary(), map() | nil) :: :ok | {:error, term()}
  def add(%{writer: writer} = handle, document_id, body)
      when is_binary(document_id) and (is_map(body) or is_nil(body)) do
    with {:ok, content} <- content(body || %{}, handle.definition),
         :ok <-
           checked(:add_document, fn ->
             TantivyEx.IndexWriter.add_document(writer, %{
               @id_field => document_id,
               @content_field => content
             })
           end) do
      :ok
    end
  end

  @spec delete(handle(), binary()) :: :ok | {:error, term()}
  def delete(%{schema: schema, writer: writer}, document_id) when is_binary(document_id) do
    with {:ok, query} <- TantivyEx.Query.term(schema, @id_field, document_id),
         :ok <-
           checked(:delete_documents, fn ->
             TantivyEx.IndexWriter.delete_documents(writer, query)
           end) do
      :ok
    end
  end

  @spec replace(handle(), binary(), map() | nil, boolean()) :: :ok | {:error, term()}
  def replace(handle, document_id, body, deleted)
      when is_binary(document_id) and (is_map(body) or is_nil(body)) and is_boolean(deleted) do
    with :ok <- delete(handle, document_id),
         :ok <- if(deleted, do: :ok, else: add(handle, document_id, body)) do
      :ok
    end
  end

  @spec search(handle(), binary(), binary()) :: {:ok, [map()]} | {:error, term()}
  def search(%{searcher: nil}, _text, _mode),
    do: {:error, VialKeeper.Error.index_not_found("full-text index is not committed")}

  def search(%{schema: schema, searcher: searcher}, text, mode)
      when is_binary(text) and is_binary(mode) do
    with {:ok, query} <- query(schema, text, mode),
         {:ok, results} <-
           TantivyEx.Searcher.search_documents(
             searcher,
             query,
             VialKeeper.Config.search_candidate_limit() + 1
           ) do
      hits = results |> Enum.map(&hit/1) |> Enum.sort_by(&hit_sort_key/1)
      {:ok, hits}
    else
      {:error, reason} -> {:error, normalize_query_error(reason)}
    end
  rescue
    error ->
      {:error, VialKeeper.Error.internal_error("full-text search failed", %{cause: inspect(error)})}
  end

  @spec schema() :: reference()
  def schema do
    TantivyEx.Tokenizer.register_default_tokenizers()

    TantivyEx.Schema.new()
    |> TantivyEx.Schema.add_text_field_with_tokenizer(@id_field, :text_stored, "raw")
    |> TantivyEx.Schema.add_text_field(@content_field, :fast_stored)
  end

  @spec backend_version() :: binary()
  def backend_version, do: @backend_version

  @spec schema_fingerprint() :: binary()
  def schema_fingerprint, do: @schema_fingerprint

  @spec content(map(), map()) :: {:ok, binary()} | {:error, VialKeeper.Error.t()}
  def content(body, definition) when is_map(body) and is_map(definition) do
    paths =
      definition
      |> MapAccess.get(:fields, [])
      |> Enum.map(&field_path/1)
      |> Enum.filter(&is_binary/1)

    values =
      Enum.flat_map(paths, fn path ->
        case Pointer.get(body, path) do
          {:ok, value} when is_binary(value) -> [value]
          _ -> []
        end
      end)

    content = Enum.join(values, "\n")

    if String.valid?(content) do
      {:ok, content}
    else
      {:error, VialKeeper.Error.invalid_request("full-text fields must contain valid UTF-8")}
    end
  end

  defp query(schema, text, mode) do
    terms =
      TantivyEx.Tokenizer.tokenize_text("default", text)
      |> Enum.map(&String.downcase/1)
      |> Enum.reject(&(&1 == ""))

    if terms == [] do
      {:error, VialKeeper.Error.invalid_request("full-text query must contain searchable text")}
    else
      build_query(schema, terms, mode)
    end
  end

  defp build_query(schema, [term], "phrase"), do: TantivyEx.Query.term(schema, @content_field, term)

  defp build_query(schema, terms, "phrase"),
    do: TantivyEx.Query.phrase(schema, @content_field, terms)

  defp build_query(schema, terms, "prefix") do
    with {:ok, queries} <-
           map_queries(terms, fn term ->
             TantivyEx.Query.wildcard(schema, @content_field, term <> "*")
           end),
         {:ok, query} <- combine(queries, :all) do
      {:ok, query}
    end
  end

  defp build_query(schema, terms, "any"), do: combine_terms(schema, terms, :any)
  defp build_query(schema, terms, "all"), do: combine_terms(schema, terms, :all)
  defp build_query(schema, terms, _mode), do: combine_terms(schema, terms, :all)

  defp combine_terms(schema, terms, mode) do
    with {:ok, queries} <- map_queries(terms, &TantivyEx.Query.term(schema, @content_field, &1)),
         {:ok, query} <- combine(queries, mode) do
      {:ok, query}
    end
  end

  defp combine([query], _mode), do: {:ok, query}
  defp combine(queries, :any), do: TantivyEx.Query.boolean([], queries, [])
  defp combine(queries, :all), do: TantivyEx.Query.boolean(queries, [], [])

  defp map_queries(terms, fun) do
    Enum.reduce_while(terms, {:ok, []}, fn term, {:ok, acc} ->
      case fun.(term) do
        {:ok, query} -> {:cont, {:ok, [query | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> reverse_queries()
  end

  defp reverse_queries({:ok, queries}), do: {:ok, Enum.reverse(queries)}
  defp reverse_queries(error), do: error

  defp hit(result) when is_map(result) do
    %{
      id: Map.get(result, @id_field),
      rank: -1.0 * (Map.get(result, "score") || 0.0)
    }
  end

  defp hit_sort_key(%{rank: rank, id: id}), do: {rank, id || ""}

  defp field_path(field) when is_binary(field), do: field
  defp field_path(field) when is_map(field), do: MapAccess.get(field, :path)
  defp field_path(_field), do: nil

  defp checked(operation, fun) do
    case fun.() do
      :ok -> :ok
      {:ok, _} -> :ok
      {:error, reason} -> {:error, error("Tantivy #{operation} failed", reason)}
      other -> {:error, error("Tantivy #{operation} returned an unexpected result", other)}
    end
  rescue
    error -> {:error, error("Tantivy #{operation} failed", error)}
  end

  defp normalize_query_error(%VialKeeper.Error{} = error), do: error

  defp normalize_query_error(reason),
    do: VialKeeper.Error.internal_error("Tantivy query failed", %{cause: inspect(reason)})

  defp error(message, reason),
    do: VialKeeper.Error.internal_error(message, %{cause: inspect(reason)})

  defp writer_memory_bytes do
    VialKeeper.Config.search_writer_memory_bytes()
  end
end
