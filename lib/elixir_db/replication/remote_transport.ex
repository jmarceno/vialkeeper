defmodule ElixirDB.Replication.RemoteTransport do
  @moduledoc "Executes authenticated HTTP requests and lazy response streams for remote replication."

  alias ElixirDB.Error
  alias ElixirDB.Observability.Tracer
  alias ElixirDB.Replication.BlobRepresentationStream
  alias ElixirDB.Replication.WireCompression

  def request(base_url, method, path, body \\ nil, auth_token \\ nil) do
    with {:ok, options} <- request_options(base_url, method, path, body, auth_token) do
      timeout = ElixirDB.Config.host_limits()[:max_request_timeout_ms] || 30_000
      otel_ctx = OpenTelemetry.Ctx.get_current()
      task = request_task(options, otel_ctx)
      handle_task_result(Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill))
    end
  end

  @doc """
  Opens a lazy blob-representation GET response.

  Must run in the calling process (`into: :self`). The returned enumerable
  cancels the HTTP response when abandoned early. Does not decode JSON bodies.
  """
  @spec open_stream(binary(), binary(), binary(), binary() | nil) ::
          {:ok, map(), Enumerable.t()} | {:error, ElixirDB.Error.t()}
  def open_stream(base_url, path, digest, auth_token \\ nil) do
    options =
      stream_base_options(base_url, :get, path)
      |> Keyword.merge(
        into: :self,
        decode_body: false,
        compressed: false,
        headers: [
          {"accept", BlobRepresentationStream.media_type()},
          {"accept-encoding", "zstd"}
          | auth_headers(auth_token) ++ trace_headers()
        ]
      )

    case Req.request(options) do
      {:ok, %{status: status, body: body} = result} when status in 200..299 ->
        open_stream_success(result, body, digest)

      {:ok, %{status: status, body: body} = result} ->
        open_stream_error(status, body, result)

      {:error, reason} ->
        transport_error(reason)
    end
  end

  @doc """
  PUTs a blob representation as `application/vnd.elixirdb.blob-representation`.

  Runs in the calling process so a body sourced from `open_stream/3` remains
  valid. Expects a compressed JSON success envelope. Does not JSON-encode the
  blob body and does not set Content-Encoding on the blob payload.
  """
  @spec put_stream(
          binary(),
          binary(),
          BlobRepresentationStream.t(),
          binary() | nil
        ) :: :ok | {:error, ElixirDB.Error.t()}
  def put_stream(base_url, path, %BlobRepresentationStream{} = stream, auth_token \\ nil) do
    auth = auth_headers(auth_token)
    trace = trace_headers()

    options =
      stream_base_options(base_url, :put, path)
      |> Keyword.merge(
        body: stream.body,
        decode_body: false,
        compressed: false,
        headers:
          [
            {"accept", "application/json"},
            {"accept-encoding", "zstd"}
            | BlobRepresentationStream.response_headers(stream)
          ] ++ auth ++ trace
      )

    case Req.request(options) do
      {:ok, %{status: status} = result} when status in 200..299 ->
        case decode_wire_json(result) do
          {:ok, _body} -> :ok
          {:error, _} = error -> error
        end

      {:ok, %{status: status} = result} ->
        {:error, error_response(status, result)}

      {:error, reason} ->
        transport_error(reason)
    end
  end

  defp open_stream_success(result, body, digest) do
    with {:ok, descriptor} <- BlobRepresentationStream.parse_http_headers(result.headers, digest) do
      {:ok, descriptor, body}
    end
  end

  defp open_stream_error(status, body, result) do
    binary = consume_error_body(body)
    {:error, error_response(status, %{result | body: binary})}
  end

  # Non-2xx bodies are not guaranteed to use the compressed wire encoding:
  # intermediaries (load balancers, proxies) emit plain 5xx pages. When the
  # body cannot be decoded as wire JSON, classify by HTTP status so 5xx stays
  # retryable instead of surfacing as a permanent incompatibility.
  defp error_response(status, result) do
    case decode_wire_json(result) do
      {:ok, response} -> decode_error(status, response)
      {:error, _} -> decode_error(status, nil)
    end
  end

  defp consume_error_body(body) do
    if enumerable_body?(body) do
      body
      |> Enum.to_list()
      |> IO.iodata_to_binary()
    else
      IO.iodata_to_binary(body)
    end
  rescue
    exception in [ArgumentError, ErlangError, Protocol.UndefinedError, RuntimeError] ->
      _ = exception
      ""
  end

  defp enumerable_body?(%Req.Response.Async{}), do: true
  defp enumerable_body?(body) when is_struct(body), do: true
  defp enumerable_body?(_), do: false

  defp stream_base_options(base_url, method, path) do
    timeout = ElixirDB.Config.host_limits()[:max_request_timeout_ms] || 30_000

    [
      method: method,
      url: String.trim_trailing(base_url, "/") <> path,
      retry: false,
      receive_timeout: timeout,
      connect_options: [timeout: 5_000]
    ]
  end

  defp request_options(base_url, method, path, body, auth_token) do
    trace_headers = trace_headers()
    auth_headers = auth_headers(auth_token)

    options = [
      method: method,
      url: String.trim_trailing(base_url, "/") <> path,
      retry: false,
      receive_timeout: 30_000,
      connect_options: [timeout: 5_000],
      decode_body: false,
      compressed: false,
      headers: json_request_headers(auth_headers, trace_headers)
    ]

    add_request_body(options, body, auth_headers, trace_headers)
  end

  defp json_request_headers(auth_headers, trace_headers) do
    [
      {"accept", "application/json"},
      {"accept-encoding", "zstd"}
      | auth_headers ++ trace_headers
    ]
  end

  defp trace_headers do
    Tracer.inject([])
    |> Enum.map(fn {key, value} -> {to_string(key), to_string(value)} end)
  end

  defp add_request_body(options, nil, _auth_headers, _trace_headers), do: {:ok, options}

  defp add_request_body(options, body, auth_headers, trace_headers) do
    case WireCompression.encode_json(body, decoded_limit()) do
      {:ok, encoded} ->
        {:ok,
         Keyword.merge(options,
           body: encoded.body,
           headers: json_body_headers(encoded, auth_headers, trace_headers)
         )}

      {:error, _} = error ->
        error
    end
  end

  defp json_body_headers(encoded, auth_headers, trace_headers) do
    [
      {"accept", "application/json"},
      {"accept-encoding", "zstd"},
      {"content-type", "application/json"},
      {"content-encoding", "zstd"},
      {"x-elixirdb-uncompressed-length", Integer.to_string(encoded.uncompressed_length)},
      {"content-length", Integer.to_string(encoded.compressed_length)}
      | auth_headers ++ trace_headers
    ]
  end

  defp request_task(options, otel_ctx) do
    Task.Supervisor.async_nolink(ElixirDB.TaskSupervisor, fn ->
      token = OpenTelemetry.Ctx.attach(otel_ctx)

      try do
        Req.request(options)
      after
        OpenTelemetry.Ctx.detach(token)
      end
    end)
  end

  defp handle_task_result({:ok, {:ok, %{status: status} = result}}) when status in 200..299 do
    decode_wire_json(result)
  end

  defp handle_task_result({:ok, {:ok, %{status: status} = result}}) do
    {:error, error_response(status, result)}
  end

  defp handle_task_result({:ok, {:error, reason}}), do: transport_error(reason)

  # SAFETY: Req.request/1 (or OpenTelemetry context handling) may exit rather
  # than return an error. Convert that exit into a typed retryable failure.
  defp handle_task_result({:exit, reason}),
    do:
      {:error,
       Error.internal_error("replication transport request failed", %{
         cause: inspect(reason)
       })}

  defp handle_task_result(nil),
    do: {:error, Error.database_unavailable("remote endpoint request timed out")}

  defp decode_wire_json(result) do
    body = result.body
    headers = result.headers
    decoded_limit = decoded_limit()
    encoded_limit = WireCompression.encoded_limit(decoded_limit)

    cond do
      not is_binary(body) ->
        {:error, incompatible_response()}

      byte_size(body) > encoded_limit ->
        {:error, Error.payload_too_large("remote endpoint response is too large")}

      true ->
        case WireCompression.decode_json(body,
               decoded_limit: decoded_limit,
               headers: headers,
               expect: :map
             ) do
          {:ok, value} ->
            {:ok, value}

          {:error, %Error{code: :payload_too_large} = error} ->
            {:error, error}

          {:error, _error} ->
            {:error, incompatible_response()}
        end
    end
  end

  defp incompatible_response do
    Error.replication_incompatible("remote endpoint returned an incompatible replication response")
  end

  defp transport_error(reason) do
    {:error,
     Error.database_unavailable("remote endpoint request failed", %{
       cause: inspect(reason)
     })}
  end

  defp decode_error(_status, %{"error" => error}) when is_map(error) do
    code =
      Error.registry()
      |> Map.keys()
      |> Enum.find(:internal_error, &(Atom.to_string(&1) == error["code"]))

    Error.new(code, error["message"] || "remote request failed", error["details"] || %{},
      retryable: error["retryable"] || false
    )
  rescue
    _error in [
      ArgumentError,
      BadMapError,
      ErlangError,
      FunctionClauseError,
      KeyError,
      MatchError,
      Protocol.UndefinedError,
      RuntimeError,
      UndefinedFunctionError
    ] ->
      Error.database_unavailable("remote endpoint returned an invalid error")
  end

  defp decode_error(status, _),
    do:
      Error.new(:internal_error, "remote endpoint returned HTTP #{status}", %{},
        retryable: status >= 500
      )

  defp decoded_limit,
    do: ElixirDB.Config.host_limits()[:max_replication_batch_bytes] || 16_777_216

  defp auth_headers(nil), do: []
  defp auth_headers(token) when is_binary(token), do: [{"authorization", "Bearer " <> token}]
end
