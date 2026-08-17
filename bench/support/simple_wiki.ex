defmodule VialKeeper.Bench.SimpleWiki do
  @moduledoc """
  Simple English Wikipedia fixture generation for the catalog stress benchmark.

  The source is one Wikimedia bzip2 dump. Articles are streamed out of the
  decompressor and attachments are generated locally, so preparation does not
  fan out into one network request per document or attachment.
  """

  alias VialKeeper.AtomicWrite
  alias VialKeeper.Bench.{Checksums, Root}

  @page_start "<page>"
  @page_end "</page>"
  @standard_count 2_000
  @smoke_count 3
  @standard_attachment_count 800
  @smoke_attachment_stride 5
  @small_attachment_count 640
  @medium_attachment_count 120
  @standard_attachment_total 800
  @small_attachment_size 64 * 1024
  @medium_attachment_size 1024 * 1024
  @large_attachment_size 16 * 1024 * 1024
  @query_workload_version "simplewiki-query-v2"

  @type article :: %{
          required(:id) => binary(),
          required(:version) => pos_integer(),
          required(:title) => binary(),
          required(:text) => binary(),
          required(:attachments) => [map()]
        }

  @spec generate_fixture(Path.t(), map(), atom(), Path.t(), keyword()) ::
          {:ok, map()} | {:error, binary()}
  def generate_fixture(archive, spec, profile, staging, opts \\ [])
      when is_binary(archive) and is_map(spec) and profile in [:standard, :smoke] and
             is_binary(staging) and is_list(opts) do
    count = Keyword.get(opts, :article_count, count_for(profile))

    with {:ok, archive_size} <- archive_size(archive),
         {:ok, archive_md5} <- Checksums.md5_file(archive),
         {:ok, state} <- generate_articles(archive, staging, profile, count),
         :ok <- write_query_workload(staging, query_workload(state.token_counts)) do
      articles = Enum.reverse(state.articles)

      {:ok,
       %{
         "dataset" => "simplewiki",
         "version" => spec["version"],
         "profile" => Atom.to_string(profile),
         "selection_algorithm" => "first-current-main-v1",
         "query_workload_version" => @query_workload_version,
         "query_workload_path" => "queries.json",
         "selection_count" => count,
         "source_url" => spec["source_url"],
         "source_archive_size_bytes" => archive_size,
         "source_archive_md5" => archive_md5,
         "articles" => articles,
         "expected_fixture_bytes" => archive_size + state.text_bytes + state.attachment_bytes,
         "text_bytes" => state.text_bytes,
         "attachment_bytes" => state.attachment_bytes,
         "attachment_count" => state.attachment_count
       }}
    end
  end

  @spec count_for(atom()) :: pos_integer()
  def count_for(:standard), do: @standard_count
  def count_for(:smoke), do: @smoke_count

  @doc "Returns the deterministic query workload algorithm version."
  @spec query_workload_version() :: binary()
  def query_workload_version, do: @query_workload_version

  @doc "Loads and validates the prepared query workload without reading article text."
  @spec load_query_workload(Path.t()) :: {:ok, map()} | {:error, binary()}
  def load_query_workload(dataset) when is_binary(dataset) do
    path = Path.join(dataset, "queries.json")

    with {:ok, body} <- File.read(path),
         {:ok, workload} <- JSON.decode(body),
         true <- valid_query_workload?(workload) do
      {:ok, workload}
    else
      {:error, reason} -> {:error, "cannot read SimpleWiki query workload: #{inspect(reason)}"}
      _ -> {:error, "SimpleWiki query workload is invalid or stale"}
    end
  end

  @doc "Creates a missing v2 query workload for an already prepared fixture."
  @spec ensure_query_workload(Root.t(), Path.t()) :: :ok | {:error, binary()}
  def ensure_query_workload(%Root{} = context, dataset) when is_binary(dataset) do
    if Root.descendant?(dataset, context.root) do
      case load_query_workload(dataset) do
        {:ok, _workload} ->
          ensure_manifest_query_version(dataset)

        {:error, _reason} ->
          build_query_workload(dataset)
      end
    else
      {:error, "SimpleWiki fixture path escapes the benchmark root"}
    end
  end

  defp build_query_workload(dataset) do
    with {:ok, manifest} <- read_manifest(dataset) do
      counts = count_fixture_tokens(dataset, manifest["articles"] || [])

      case write_query_workload(dataset, query_workload(counts)) do
        :ok -> write_manifest_query_version(dataset, manifest)
        error -> error
      end
    end
  end

  @spec document_body(map(), binary()) :: map()
  def document_body(article, text) when is_map(article) and is_binary(text) do
    %{
      "title" => article["title"],
      "text" => text,
      "wiki_page_id" => article["page_id"]
    }
  end

  @spec classify_name(binary()) :: binary()
  def classify_name(name) when is_binary(name),
    do: if(String.ends_with?(name, ".bin"), do: "binary", else: "text")

  @spec content_type(binary()) :: binary()
  def content_type(name) when is_binary(name) do
    if String.ends_with?(name, ".txt"), do: "text/plain", else: "application/octet-stream"
  end

  @spec parse_page(binary()) :: {:ok, map()} | :skip
  def parse_page(page) when is_binary(page) do
    with {:ok, "0"} <- capture_tag(page, "<ns>", "</ns>"),
         {:ok, page_id} <- capture_tag(page, "<id>", "</id>"),
         {:ok, title} <- capture_tag(page, "<title>", "</title>"),
         {:ok, text} <- capture_text(page),
         true <- :binary.match(page, "<redirect") == :nomatch,
         true <- String.trim(text) != "" do
      {:ok,
       %{
         "page_id" => String.trim(page_id),
         "title" => xml_unescape(title),
         "text" => xml_unescape(text)
       }}
    else
      _ -> :skip
    end
  end

  defp generate_articles(archive, staging, profile, count) do
    state = %{
      articles: [],
      selected: 0,
      attachment_stride: attachment_stride(profile, count),
      text_bytes: 0,
      attachment_bytes: 0,
      attachment_count: 0,
      token_counts: %{}
    }

    stream_pages(archive, state, fn page, state ->
      generate_article(staging, profile, page, state, count)
    end)
    |> case do
      {:ok, %{selected: selected} = state} when selected == count ->
        {:ok, state}

      {:ok, %{selected: selected}} ->
        {:error, "Simple Wikipedia archive ended after #{selected} articles; expected #{count}"}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp generate_article(_staging, _profile, _page, %{selected: selected} = state, count)
       when selected >= count,
       do: {:halt, state}

  defp generate_article(staging, profile, page, state, count) do
    case write_article(staging, profile, page, state) do
      {:ok, %{selected: selected} = next} when selected >= count -> {:halt, next}
      {:ok, next} -> {:cont, next}
      {:error, reason} -> {:error, reason}
    end
  end

  defp write_article(staging, profile, page, state) do
    id = "wiki-" <> page["page_id"]
    prefix = id <> ".1"
    text_name = prefix <> ".txt"
    text_path = Path.join([staging, "objects", prefix, text_name])

    with :ok <- File.mkdir_p(Path.dirname(text_path)),
         :ok <- File.write(text_path, page["text"]),
         {:ok, attachment} <- maybe_attachment(staging, profile, id, prefix, state) do
      item = %{
        "id" => id,
        "page_id" => page["page_id"],
        "version" => 1,
        "title" => page["title"],
        "text" => %{"name" => text_name},
        "attachments" => List.wrap(attachment)
      }

      attachment_bytes = if is_map(attachment), do: attachment["expected_size"], else: 0

      {:ok,
       %{
         state
         | articles: [item | state.articles],
           selected: state.selected + 1,
           text_bytes: state.text_bytes + byte_size(page["text"]),
           attachment_bytes: state.attachment_bytes + attachment_bytes,
           attachment_count: state.attachment_count + if(is_map(attachment), do: 1, else: 0),
           token_counts: count_tokens(page["text"], state.token_counts)
       }}
    else
      {:error, reason} ->
        {:error, "failed to write Simple Wikipedia article #{id}: #{inspect(reason)}"}
    end
  end

  defp maybe_attachment(staging, profile, id, prefix, state) do
    if rem(state.selected, state.attachment_stride) != 0 do
      {:ok, nil}
    else
      ordinal = div(state.selected, state.attachment_stride)

      case attachment_size(profile, ordinal) do
        nil -> {:ok, nil}
        size -> write_attachment(staging, id, prefix, ordinal, size)
      end
    end
  end

  defp attachment_size(:smoke, 0), do: 4096
  defp attachment_size(:smoke, _), do: nil

  defp attachment_size(:standard, ordinal) when ordinal < @small_attachment_count,
    do: @small_attachment_size

  defp attachment_size(:standard, ordinal)
       when ordinal < @small_attachment_count + @medium_attachment_count,
       do: @medium_attachment_size

  defp attachment_size(:standard, ordinal) when ordinal < @standard_attachment_total,
    do: @large_attachment_size

  defp attachment_size(:standard, _), do: nil

  defp attachment_stride(:standard, count),
    do: max(div(count, @standard_attachment_count), 1)

  defp attachment_stride(:smoke, _count), do: @smoke_attachment_stride

  defp write_attachment(staging, id, prefix, ordinal, size) do
    name = "attachment-#{ordinal}.bin"
    path = Path.join([staging, "objects", prefix, name])
    seed = id <> ":" <> name
    key = :crypto.hash(:sha256, seed)
    iv = binary_part(:crypto.hash(:sha256, seed <> ":iv"), 0, 16)
    body = :crypto.crypto_one_time(:aes_256_ctr, key, iv, :binary.copy(<<0>>, size), true)

    case File.write(path, body) do
      :ok ->
        {:ok,
         %{
           "name" => name,
           "category" => "binary",
           "expected_size" => size,
           "size_estimated?" => false,
           "content_type" => "application/octet-stream"
         }}

      {:error, reason} ->
        {:error, "failed to write attachment #{path}: #{inspect(reason)}"}
    end
  end

  defp stream_pages(archive, state, callback) do
    case System.find_executable("bzip2") do
      nil ->
        {:error, "bzip2 executable is required to prepare Simple Wikipedia"}

      executable ->
        port =
          Port.open({:spawn_executable, executable}, [
            :binary,
            :exit_status,
            {:args, ["-dc", archive]}
          ])

        consume_port(port, <<>>, state, callback)
    end
  end

  defp consume_port(port, buffer, state, callback) do
    receive do
      {^port, {:data, data}} ->
        case consume_pages(buffer <> data, state, callback) do
          {:cont, next_buffer, next_state} ->
            consume_port(port, next_buffer, next_state, callback)

          {:halt, next_state} ->
            close_port(port)
            {:ok, next_state}

          {:error, reason} ->
            close_port(port)
            {:error, reason}
        end

      {^port, {:exit_status, 0}} ->
        case consume_pages(buffer, state, callback) do
          {:cont, _buffer, next_state} -> {:ok, next_state}
          {:halt, next_state} -> {:ok, next_state}
          {:error, reason} -> {:error, reason}
        end

      {^port, {:exit_status, status}} ->
        {:error, "bzip2 exited with status #{status}"}
    after
      300_000 ->
        close_port(port)
        {:error, "timed out while reading Simple Wikipedia archive"}
    end
  end

  defp close_port(port) do
    if Port.info(port), do: Port.close(port), else: :ok
  end

  defp consume_pages(buffer, state, callback) do
    case :binary.match(buffer, @page_start) do
      :nomatch ->
        {:cont, suffix(buffer, byte_size(@page_start) - 1), state}

      {start, _} ->
        page_buffer = binary_part(buffer, start, byte_size(buffer) - start)
        consume_page_buffer(page_buffer, state, callback)
    end
  end

  defp consume_page_buffer(page_buffer, state, callback) do
    case :binary.match(page_buffer, @page_end) do
      :nomatch ->
        {:cont, page_buffer, state}

      {finish, _} ->
        split_and_consume_page(page_buffer, finish, state, callback)
    end
  end

  defp split_and_consume_page(page_buffer, finish, state, callback) do
    page = binary_part(page_buffer, 0, finish + byte_size(@page_end))
    rest = binary_part(page_buffer, byte_size(page), byte_size(page_buffer) - byte_size(page))

    case parse_page(page) do
      :skip -> consume_pages(rest, state, callback)
      {:ok, article} -> continue_page(callback.(article, state), rest, callback)
    end
  end

  defp continue_page({:cont, state}, rest, callback), do: consume_pages(rest, state, callback)
  defp continue_page({:halt, state}, _rest, _callback), do: {:halt, state}
  defp continue_page({:error, reason}, _rest, _callback), do: {:error, reason}

  defp capture_text(page) do
    case :binary.match(page, "<text") do
      :nomatch ->
        :error

      {start, _} ->
        header = binary_part(page, start, byte_size(page) - start)
        capture_text_body(header)
    end
  end

  defp capture_text_body(header) do
    case :binary.match(header, ">") do
      :nomatch ->
        :error

      {open_end, _} ->
        body = binary_part(header, open_end + 1, byte_size(header) - open_end - 1)
        capture_until(body, "</text>")
    end
  end

  defp capture_until(body, close) do
    case :binary.match(body, close) do
      :nomatch -> :error
      {finish, _} -> {:ok, binary_part(body, 0, finish)}
    end
  end

  defp capture_tag(page, open, close) do
    case :binary.match(page, open) do
      :nomatch ->
        :error

      {start, _} ->
        body = binary_part(page, start + byte_size(open), byte_size(page) - start - byte_size(open))
        capture_until(body, close)
    end
  end

  defp xml_unescape(value) do
    value
    |> String.replace("&lt;", "<")
    |> String.replace("&gt;", ">")
    |> String.replace("&quot;", "\"")
    |> String.replace("&apos;", "'")
    |> String.replace("&amp;", "&")
  end

  defp suffix(binary, count) when byte_size(binary) <= count, do: binary
  defp suffix(binary, count), do: binary_part(binary, byte_size(binary) - count, count)

  defp archive_size(path) do
    case File.stat(path) do
      {:ok, %{size: size}} when size > 0 ->
        {:ok, size}

      {:ok, _} ->
        {:error, "Simple Wikipedia archive is empty: #{path}"}

      {:error, reason} ->
        {:error, "cannot stat Simple Wikipedia archive #{path}: #{inspect(reason)}"}
    end
  end

  defp query_workload(counts) do
    tokens = Enum.sort_by(counts, fn {token, count} -> {-count, token} end)
    common = tokens |> Enum.take(5) |> Enum.map(&elem(&1, 0))

    medium =
      tokens
      |> Enum.slice(div(length(tokens), 3), 5)
      |> Enum.map(&elem(&1, 0))

    rare = tokens |> Enum.reverse() |> Enum.take(5) |> Enum.map(&elem(&1, 0))

    multi_term =
      [Enum.take(common, 2), [List.first(medium), List.first(rare)]]
      |> Enum.map(fn terms -> terms |> Enum.reject(&is_nil/1) |> Enum.join(" ") end)
      |> Enum.reject(&(&1 == ""))
      |> Enum.uniq()

    categories = %{
      "common" => common,
      "medium" => medium,
      "rare" => rare,
      "multi_term" => multi_term,
      "zero_match" => ["zzzzzxxyy-no-such-term"]
    }

    %{
      "schema_version" => 1,
      "query_workload_version" => @query_workload_version,
      "categories" => categories,
      "queries" => categories |> Map.values() |> List.flatten() |> Enum.uniq()
    }
  end

  defp write_query_workload(directory, workload) do
    case AtomicWrite.write(Path.join(directory, "queries.json"), JSON.encode!(workload) <> "\n") do
      :ok -> :ok
      {:error, reason} -> {:error, "failed to write SimpleWiki queries: #{inspect(reason)}"}
    end
  end

  defp valid_query_workload?(%{
         "query_workload_version" => @query_workload_version,
         "categories" => categories,
         "queries" => queries
       }) do
    is_map(categories) and is_list(queries) and queries != [] and
      Enum.all?(queries, &is_binary/1)
  end

  defp valid_query_workload?(_workload), do: false

  defp ensure_manifest_query_version(dataset) do
    with {:ok, manifest} <- read_manifest(dataset) do
      if manifest["query_workload_version"] == @query_workload_version do
        :ok
      else
        write_manifest_query_version(dataset, manifest)
      end
    end
  end

  defp write_manifest_query_version(dataset, manifest) do
    updated =
      manifest
      |> Map.put("query_workload_version", @query_workload_version)
      |> Map.put("query_workload_path", "queries.json")

    case AtomicWrite.write(Path.join(dataset, "manifest.json"), JSON.encode!(updated) <> "\n") do
      :ok -> :ok
      {:error, reason} -> {:error, "failed to update SimpleWiki manifest: #{inspect(reason)}"}
    end
  end

  defp read_manifest(dataset) do
    case File.read(Path.join(dataset, "manifest.json")) do
      {:ok, body} ->
        case JSON.decode(body) do
          {:ok, manifest} when is_map(manifest) -> {:ok, manifest}
          _ -> {:error, "SimpleWiki manifest is invalid"}
        end

      {:error, reason} ->
        {:error, "cannot read SimpleWiki manifest: #{inspect(reason)}"}
    end
  end

  defp count_fixture_tokens(dataset, articles) do
    Enum.reduce(articles, %{}, fn article, counts ->
      case File.read(fixture_text_path(dataset, article)) do
        {:ok, text} -> count_tokens(text, counts)
        {:error, _reason} -> counts
      end
    end)
  end

  defp fixture_text_path(dataset, article) do
    prefix = IO.iodata_to_binary([to_string(article["id"]), ".", to_string(article["version"])])
    text_name = get_in(article, ["text", "name"]) || IO.iodata_to_binary([prefix, ".txt"])
    Path.join([dataset, "objects", prefix, text_name])
  end

  defp count_tokens(text, counts) do
    text
    |> String.downcase()
    |> String.split(~r/[^a-z0-9]+/u, trim: true)
    |> Enum.filter(&(String.length(&1) > 3))
    |> Enum.reduce(counts, fn token, acc -> Map.update(acc, token, 1, &(&1 + 1)) end)
  end
end
