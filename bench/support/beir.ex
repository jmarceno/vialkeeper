defmodule VialKeeper.Bench.Beir do
  @moduledoc "BEIR/TREC-COVID corpus, query, and qrel parsers."

  alias VialKeeper.JSON.StrictDecoder

  @spec find_file(Path.t(), binary()) :: {:ok, Path.t()} | {:error, binary()}
  def find_file(root, name) when is_binary(root) and is_binary(name) do
    matches = Path.wildcard(Path.join(root, "**/" <> name))

    case Enum.find(matches, &File.regular?/1) do
      path when is_binary(path) -> {:ok, path}
      nil -> {:error, "BEIR fixture is missing #{name} under #{root}"}
    end
  end

  @spec stream_jsonl(Path.t()) :: Enumerable.t()
  def stream_jsonl(path) when is_binary(path) do
    path
    |> File.stream!()
    |> Stream.map(&String.trim/1)
    |> Stream.reject(&(&1 == ""))
    |> Stream.map(&decode_jsonl_line/1)
  end

  @spec load_queries(Path.t()) :: {:ok, [map()]} | {:error, binary()}
  def load_queries(path) do
    queries =
      path
      |> stream_jsonl()
      |> Enum.to_list()

    if Enum.any?(queries, &match?({:error, _}, &1)) do
      {:error, "queries.jsonl contains invalid JSON"}
    else
      {:ok, queries}
    end
  end

  @spec load_qrels(Path.t()) :: {:ok, %{binary() => %{binary() => integer()}}} | {:error, binary()}
  def load_qrels(path) do
    path
    |> File.stream!()
    |> Stream.map(&String.trim/1)
    |> Stream.reject(&(&1 == "" or String.starts_with?(&1, "#")))
    |> Enum.reduce_while({:ok, %{}}, fn line, {:ok, acc} ->
      case parse_qrel_line(line) do
        {:ok, qid, doc_id, grade} ->
          qrels = Map.update(acc, qid, %{doc_id => grade}, &Map.put(&1, doc_id, grade))
          {:cont, {:ok, qrels}}

        :skip ->
          {:cont, {:ok, acc}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
  end

  @spec document_body(map()) :: {:ok, binary(), map()} | {:error, binary()}
  def document_body(row) when is_map(row) do
    id = row["_id"] || row["id"]
    title = row["title"] || ""
    text = row["text"] || ""
    metadata = row["metadata"] || %{}

    if is_binary(id) and id != "" do
      {:ok, id,
       %{
         "_id" => id,
         "title" => title,
         "text" => text,
         "metadata" => metadata
       }}
    else
      {:error, "BEIR document is missing _id"}
    end
  end

  @spec query_text(map()) :: {:ok, binary(), binary()} | {:error, binary()}
  def query_text(row) when is_map(row) do
    id = row["_id"] || row["id"]
    text = row["text"] || row["query"] || ""

    if is_binary(id) and id != "" and is_binary(text) and text != "" do
      {:ok, id, text}
    else
      {:error, "BEIR query is missing _id or text"}
    end
  end

  @spec validate_fixture(Path.t(), map()) :: :ok | {:error, binary()}
  def validate_fixture(root, spec) when is_map(spec) do
    with {:ok, corpus} <- find_file(root, "corpus.jsonl"),
         {:ok, queries} <- find_file(root, "queries.jsonl"),
         {:ok, qrels} <- find_qrels(root),
         :ok <- nonempty_file(corpus, "corpus.jsonl"),
         :ok <- nonempty_file(queries, "queries.jsonl"),
         :ok <- nonempty_file(qrels, "qrels"),
         {:ok, query_rows} <- load_queries(queries) do
      expected = spec["expected_queries"]

      cond do
        query_rows == [] ->
          {:error, "TREC-COVID queries.jsonl has no queries"}

        is_integer(expected) and Enum.count_until(query_rows, 10) >= 10 and
            length(query_rows) != expected ->
          {:error,
           "TREC-COVID query count looks wrong: #{length(query_rows)} (expected #{expected})"}

        true ->
          :ok
      end
    end
  end

  defp find_qrels(root) do
    case find_file(root, "test.tsv") do
      {:ok, _} = ok -> ok
      {:error, _} -> find_file(root, "qrels.tsv")
    end
  end

  defp nonempty_file(path, label) do
    case File.stat(path) do
      {:ok, %{size: size}} when size > 0 -> :ok
      {:ok, _} -> {:error, "#{label} is empty"}
      {:error, reason} -> {:error, "cannot stat #{label}: #{inspect(reason)}"}
    end
  end

  defp decode_jsonl_line(line) do
    case StrictDecoder.decode(line) do
      {:ok, row} when is_map(row) -> row
      _ -> {:error, :invalid_jsonl}
    end
  end

  defp parse_qrel_line(line) do
    case String.split(line, ~r/\s+/, trim: true) do
      ["query-id", "corpus-id", "score"] ->
        :skip

      ["qid", "Q0", "docid", "rel"] ->
        :skip

      [qid, _iter, doc_id, grade] ->
        parse_grade(qid, doc_id, grade)

      [qid, doc_id, grade] ->
        parse_grade(qid, doc_id, grade)

      _ ->
        {:error, "invalid qrel line: #{line}"}
    end
  end

  defp parse_grade(qid, doc_id, grade) do
    case Integer.parse(grade) do
      {value, ""} -> {:ok, qid, doc_id, value}
      _ -> {:error, "invalid qrel grade #{inspect(grade)}"}
    end
  end
end
