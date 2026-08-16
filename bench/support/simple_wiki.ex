defmodule VialKeeper.Bench.SimpleWiki do
  @moduledoc """
  Simple English Wikipedia fixture generation for the catalog stress benchmark.

  The source is one Wikimedia bzip2 dump. Articles are streamed out of the
  decompressor and attachments are generated locally, so preparation does not
  fan out into one network request per document or attachment.
  """

  alias VialKeeper.Bench.Checksums

  @page_start "<page>"
  @page_end "</page>"
  @standard_count 100_000
  @smoke_count 3
  @standard_attachment_count 800
  @smoke_attachment_stride 5
  @small_attachment_count 640
  @medium_attachment_count 120
  @standard_attachment_total 800
  @small_attachment_size 64 * 1024
  @medium_attachment_size 1024 * 1024
  @large_attachment_size 16 * 1024 * 1024

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
         {:ok, state} <- generate_articles(archive, staging, profile, count) do
      articles = Enum.reverse(state.articles)

      {:ok,
       %{
         "dataset" => "simplewiki",
         "version" => spec["version"],
         "profile" => Atom.to_string(profile),
         "selection_algorithm" => "first-current-main-v1",
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
      attachment_count: 0
    }

    stream_pages(archive, state, fn page, state ->
      if state.selected >= count do
        {:halt, state}
      else
        case write_article(staging, profile, page, state) do
          {:ok, next} when next.selected >= count -> {:halt, next}
          {:ok, next} -> {:cont, next}
          {:error, reason} -> {:error, reason}
        end
      end
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
           attachment_count: state.attachment_count + if(is_map(attachment), do: 1, else: 0)
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

        case :binary.match(page_buffer, @page_end) do
          :nomatch ->
            {:cont, page_buffer, state}

          {finish, _} ->
            page = binary_part(page_buffer, 0, finish + byte_size(@page_end))

            rest =
              binary_part(page_buffer, byte_size(page), byte_size(page_buffer) - byte_size(page))

            case parse_page(page) do
              :skip ->
                consume_pages(rest, state, callback)

              {:ok, article} ->
                case callback.(article, state) do
                  {:cont, next_state} -> consume_pages(rest, next_state, callback)
                  {:halt, next_state} -> {:halt, next_state}
                  {:error, reason} -> {:error, reason}
                end
            end
        end
    end
  end

  defp capture_text(page) do
    case :binary.match(page, "<text") do
      :nomatch ->
        :error

      {start, _} ->
        header = binary_part(page, start, byte_size(page) - start)

        case :binary.match(header, ">") do
          :nomatch ->
            :error

          {open_end, _} ->
            body = binary_part(header, open_end + 1, byte_size(header) - open_end - 1)

            case :binary.match(body, "</text>") do
              :nomatch -> :error
              {finish, _} -> {:ok, binary_part(body, 0, finish)}
            end
        end
    end
  end

  defp capture_tag(page, open, close) do
    case :binary.match(page, open) do
      :nomatch ->
        :error

      {start, _} ->
        body = binary_part(page, start + byte_size(open), byte_size(page) - start - byte_size(open))

        case :binary.match(body, close) do
          :nomatch -> :error
          {finish, _} -> {:ok, binary_part(body, 0, finish)}
        end
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
end
