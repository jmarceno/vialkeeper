defmodule VialKeeper.Bench.Downloader do
  @moduledoc """
  Req/Finch downloader that writes only into verified staging paths.

  Destinations come from the dataset registry through `Root.resolve/2`. A 206
  resume is accepted only after validating `Content-Range`; a 200 response
  restarts that object instead of appending.
  """

  alias VialKeeper.Bench.{Checksums, Root}
  alias VialKeeper.DurableFS

  @max_retries 3
  @receive_timeout 300_000
  @pool_size 16

  @type object :: %{
          required(:url) => binary(),
          required(:dest) => binary(),
          optional(:md5) => binary(),
          optional(:expected_size) => non_neg_integer(),
          optional(:headers) => [{binary(), binary()}]
        }

  @spec download(Root.t(), object(), keyword()) :: :ok | {:error, binary()}
  def download(%Root{} = context, object, opts \\ []) when is_map(object) do
    with :ok <- assert_under_root(context, object.dest),
         :ok <- File.mkdir_p(Path.dirname(object.dest)),
         :ok <- ensure_http() do
      part = object.dest <> ".part"
      retry_download(object, part, Keyword.get(opts, :max_retries, @max_retries), 0)
    end
  end

  @spec get_body(binary(), keyword()) :: {:ok, binary()} | {:error, binary()}
  def get_body(url, opts \\ []) when is_binary(url) do
    with :ok <- ensure_http() do
      extra = Enum.map(opts[:headers] || [], fn {k, v} -> {to_string(k), to_string(v)} end)
      request = Finch.build(:get, url, extra)

      case Finch.request(request, finch_name(),
             receive_timeout: Keyword.get(opts, :receive_timeout, 60_000)
           ) do
        {:ok, %{status: 200, body: body}} when is_binary(body) ->
          {:ok, body}

        {:ok, %{status: status}} ->
          {:error, "GET #{url} returned HTTP #{status}"}

        {:error, reason} ->
          {:error, "GET #{url} failed: #{inspect(reason)}"}
      end
    end
  end

  @spec download_many(Root.t(), [object()], keyword()) :: :ok | {:error, binary()}
  def download_many(%Root{} = context, objects, opts \\ []) when is_list(objects) do
    concurrency = Keyword.get(opts, :max_concurrency, 4)

    if concurrency < 1 or concurrency > 16 do
      {:error, "download concurrency must be between 1 and 16"}
    else
      objects
      |> Task.async_stream(
        fn object -> download(context, object, opts) end,
        max_concurrency: concurrency,
        timeout: :infinity,
        ordered: false
      )
      |> Enum.reduce_while(:ok, fn
        {:ok, :ok}, :ok -> {:cont, :ok}
        {:ok, {:error, reason}}, :ok -> {:halt, {:error, reason}}
        {:exit, reason}, :ok -> {:halt, {:error, "download task crashed: #{inspect(reason)}"}}
      end)
    end
  end

  @spec promote_dir(Root.t(), binary(), binary()) :: :ok | {:error, binary()}
  def promote_dir(%Root{} = context, staging, dest) do
    with :ok <- assert_under_root(context, staging),
         :ok <- assert_under_root(context, dest),
         :ok <- File.mkdir_p(Path.dirname(dest)),
         :ok <- maybe_remove_incomplete_dest(dest),
         :ok <- rename(staging, dest) do
      DurableFS.sync_directory(Path.dirname(dest))
    end
  end

  @spec ensure_started() :: :ok | {:error, binary()}
  def ensure_started, do: ensure_http()

  defp retry_download(object, part, max_retries, attempt) do
    case download_once(object, part) do
      {:ok, headers} ->
        finalize(object, part, headers)

      {:error, _reason} when attempt + 1 < max_retries ->
        _ = File.rm(part)
        retry_download(object, part, max_retries, attempt + 1)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp download_once(object, part) do
    offset = existing_size(part)
    request = Finch.build(:get, object.url, range_headers(object, offset))

    acc = %{
      status: nil,
      headers: [],
      offset: offset,
      part: part,
      io: nil,
      error: nil,
      allow_html: is_binary(object[:md5]) or is_binary(object[:sha256])
    }

    result =
      Finch.stream_while(request, finch_name(), acc, &handle_stream/2,
        receive_timeout: @receive_timeout,
        request_timeout: :infinity
      )

    acc = close_stream_io(result)

    cond do
      match?({:error, _, _}, result) ->
        {:error, elem(result, 1) |> format()}

      is_binary(acc.error) ->
        {:error, acc.error}

      acc.status in [200, 206] ->
        {:ok, acc.headers}

      is_integer(acc.status) ->
        {:error, "download of #{object.url} returned HTTP #{acc.status}"}

      true ->
        {:error, "download of #{object.url} returned no HTTP status"}
    end
  end

  defp handle_stream({:status, status}, acc), do: {:cont, %{acc | status: status}}

  defp handle_stream({:headers, headers}, %{io: nil} = acc) do
    headers = normalize_headers(headers) ++ acc.headers

    case begin_body(acc, headers) do
      {:ok, acc} -> {:cont, %{acc | headers: headers}}
      {:error, reason} -> {:halt, %{acc | headers: headers, error: reason}}
    end
  end

  defp handle_stream({:headers, headers}, acc) do
    {:cont, %{acc | headers: normalize_headers(headers) ++ acc.headers}}
  end

  defp handle_stream({:data, _data}, acc) when is_binary(acc.error), do: {:halt, acc}

  defp handle_stream({:data, data}, %{io: io} = acc) when not is_nil(io) do
    :ok = IO.binwrite(io, data)
    {:cont, acc}
  end

  defp handle_stream({:data, _data}, acc),
    do: {:halt, %{acc | error: "received body before headers"}}

  defp handle_stream({:trailers, _}, acc), do: {:cont, acc}

  defp begin_body(%{status: 206, offset: offset} = acc, headers) when offset > 0 do
    with :ok <- validate_content_range(headers, offset),
         {:ok, io} <- File.open(acc.part, [:append, :binary, :raw]) do
      {:ok, %{acc | io: io}}
    end
  end

  defp begin_body(%{status: 206, offset: 0}, _headers) do
    {:error, "HTTP 206 received without a local partial file"}
  end

  defp begin_body(%{status: 200} = acc, headers) do
    with :ok <- reject_html_headers(headers, acc.allow_html),
         :ok <- reset_part(acc.part),
         {:ok, io} <- File.open(acc.part, [:write, :binary, :raw]) do
      {:ok, %{acc | io: io}}
    end
  end

  defp begin_body(%{status: status}, _headers) do
    {:error, "download returned HTTP #{status}"}
  end

  defp close_stream_io({:ok, acc}), do: close_io(acc)
  defp close_stream_io({:error, _reason, acc}), do: close_io(acc)

  defp close_io(%{io: io} = acc) when not is_nil(io) do
    File.close(io)
    %{acc | io: nil}
  end

  defp close_io(acc), do: acc

  defp finalize(object, part, headers) do
    with :ok <-
           Checksums.verify_file(part,
             md5: object[:md5],
             expected_size: object[:expected_size],
             etag: header(headers, "etag")
           ),
         :ok <- sync_file(part) do
      rename(part, object.dest)
    end
  end

  defp range_headers(object, offset) do
    extra = object[:headers] || []
    extra = Enum.map(extra, fn {k, v} -> {to_string(k), to_string(v)} end)

    if offset > 0 do
      [{"range", "bytes=#{offset}-"} | extra]
    else
      extra
    end
  end

  defp validate_content_range(headers, offset) do
    case header(headers, "content-range") do
      nil -> {:error, "HTTP 206 missing Content-Range"}
      value -> match_content_range(value, offset)
    end
  end

  defp match_content_range(value, offset) do
    case Regex.run(~r/^bytes (\d+)-(\d+)\/(\d+|\*)$/, value) do
      [_, start_s, _end_s, _total] -> match_range_start(start_s, offset, value)
      _ -> {:error, "invalid Content-Range #{value}"}
    end
  end

  defp match_range_start(start_s, offset, value) do
    case Integer.parse(start_s) do
      {^offset, ""} -> :ok
      {other, ""} -> {:error, "Content-Range start #{other} does not match local offset #{offset}"}
      _ -> {:error, "invalid Content-Range #{value}"}
    end
  end

  defp reject_html_headers(_headers, true), do: :ok

  defp reject_html_headers(headers, false) do
    case header(headers, "content-type") do
      value when is_binary(value) ->
        if String.contains?(String.downcase(value), "text/html") do
          {:error, "refusing HTML payload"}
        else
          :ok
        end

      _ ->
        :ok
    end
  end

  defp existing_size(path) do
    case File.stat(path) do
      {:ok, %{size: size}} -> size
      {:error, _} -> 0
    end
  end

  defp reset_part(path) do
    case File.rm(path) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      {:error, reason} -> {:error, "cannot reset #{path}: #{inspect(reason)}"}
    end
  end

  defp rename(from, to) do
    case File.rename(from, to) do
      :ok -> :ok
      {:error, reason} -> {:error, "rename #{from} -> #{to} failed: #{inspect(reason)}"}
    end
  end

  defp maybe_remove_incomplete_dest(dest) do
    marker = Path.join(dest, "READY.json")

    cond do
      not File.exists?(dest) ->
        :ok

      File.exists?(marker) ->
        {:error, "refusing to replace ready dataset at #{dest}"}

      File.dir?(dest) ->
        case File.rm_rf(dest) do
          {:ok, _} -> :ok
          {:error, reason, _} -> {:error, "cannot replace #{dest}: #{inspect(reason)}"}
        end

      true ->
        {:error, "destination #{dest} is not a directory"}
    end
  end

  defp assert_under_root(context, path) do
    expanded = Path.expand(path)

    if Root.descendant?(expanded, context.root) do
      :ok
    else
      {:error, "download destination #{expanded} is outside #{context.root}"}
    end
  end

  defp sync_file(path) do
    case File.open(path, [:read, :write], fn io -> :file.sync(io) end) do
      {:ok, :ok} -> :ok
      {:error, reason} -> {:error, "fsync #{path} failed: #{inspect(reason)}"}
    end
  end

  defp ensure_http do
    case Application.ensure_all_started(:req) do
      {:ok, _} ->
        ensure_finch()

      {:error, reason} ->
        {:error, "failed to start HTTP client: #{inspect(reason)}"}
    end
  end

  defp ensure_finch do
    case start_finch() do
      :ok -> wait_pool_supervisor(40)
      error -> error
    end
  end

  defp start_finch do
    case Finch.start_link(
           name: finch_name(),
           pools: %{default: [protocols: [:http1], size: @pool_size, count: 1]}
         ) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
      {:error, reason} -> {:error, "failed to start Finch: #{inspect(reason)}"}
    end
  end

  defp wait_pool_supervisor(0), do: {:error, "HTTP client pool failed to start"}

  defp wait_pool_supervisor(remaining) do
    case Process.whereis(Module.concat(finch_name(), PoolSupervisor)) do
      pid when is_pid(pid) ->
        :ok

      nil ->
        Process.sleep(25)
        wait_pool_supervisor(remaining - 1)
    end
  end

  defp finch_name, do: VialKeeper.Bench.Finch

  defp normalize_headers(headers) do
    Enum.map(headers, fn {k, v} -> {String.downcase(to_string(k)), to_string(v)} end)
  end

  defp header(headers, name) do
    name = String.downcase(name)

    Enum.find_value(headers, fn
      {^name, value} -> value
      _ -> nil
    end)
  end

  defp format(reason) when is_binary(reason), do: reason
  defp format(reason), do: inspect(reason)
end
