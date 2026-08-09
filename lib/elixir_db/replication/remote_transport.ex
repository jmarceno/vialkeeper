defmodule ElixirDB.Replication.RemoteTransport do
  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.Observability.Tracer
  @moduledoc false

  def request(base_url, method, path, body \\ nil, auth_token \\ nil) do
    # Inject the current trace context into outgoing replication requests so a
    # push job's trace spans both the local worker and the remote server
    # (plan §6.2). The noop propagator (no SDK) returns the headers unchanged.
    options = request_options(base_url, method, path, body, auth_token)

    timeout = ElixirDB.Config.host_limits()[:max_request_timeout_ms] || 30_000

    # Carry the trace context into the timeout-enforcement task so the Finch
    # telemetry-bridge span for this request parents under the replication
    # trace (plan §6.2); without it the span would be a parentless root.
    otel_ctx = OpenTelemetry.Ctx.get_current()

    task = request_task(options, otel_ctx)
    handle_task_result(Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill))
  end

  @doc """
  Opens a lazy octet-stream GET response.

  Must run in the calling process (`into: :self`). The returned enumerable
  cancels the HTTP response when abandoned early.
  """
  @spec open_stream(binary(), binary(), binary() | nil) ::
          {:ok, non_neg_integer(), Enumerable.t()} | {:error, ElixirDB.Error.t()}
  def open_stream(base_url, path, auth_token \\ nil) do
    options =
      stream_base_options(base_url, :get, path)
      |> Keyword.merge(
        into: :self,
        decode_body: false,
        headers: [
          {"accept", "application/octet-stream"} | auth_headers(auth_token) ++ trace_headers()
        ]
      )

    case Req.request(options) do
      {:ok, %{status: status, body: body} = result} when status in 200..299 ->
        open_stream_success(result, body)

      {:ok, %{status: status, body: body} = result} ->
        open_stream_error(status, body, result)

      {:error, reason} ->
        {:error,
         ElixirDB.Error.database_unavailable("remote endpoint request failed", %{
           cause: inspect(reason)
         })}
    end
  end

  @doc """
  PUTs an enumerable request body as `application/octet-stream`.

  Runs in the calling process so a body sourced from `open_stream/3` remains
  valid. Expects a JSON success envelope.
  """
  @spec put_stream(binary(), binary(), Enumerable.t(), non_neg_integer(), binary() | nil) ::
          :ok | {:error, ElixirDB.Error.t()}
  def put_stream(base_url, path, body, expected_length, auth_token \\ nil)
      when is_integer(expected_length) and expected_length >= 0 do
    auth = auth_headers(auth_token)
    trace = trace_headers()

    options =
      stream_base_options(base_url, :put, path)
      |> Keyword.merge(
        body: body,
        decode_body: true,
        headers: [
          {"accept", "application/json"},
          {"content-type", "application/octet-stream"},
          {"content-length", Integer.to_string(expected_length)}
          | auth ++ trace
        ]
      )

    case Req.request(options) do
      {:ok, %{status: status} = result} when status in 200..299 ->
        if valid_success_response?(result),
          do: :ok,
          else: invalid_response_error()

      {:ok, %{status: status, body: response} = result} ->
        if response_size(result) <= max_response_bytes(),
          do: {:error, decode_error(status, response)},
          else: {:error, ElixirDB.Error.payload_too_large("remote endpoint response is too large")}

      {:error, reason} ->
        {:error,
         ElixirDB.Error.database_unavailable("remote endpoint request failed", %{
           cause: inspect(reason)
         })}
    end
  end

  defp open_stream_success(result, body) do
    with :ok <- validate_octet_stream_response(result),
         {:ok, length} <- response_content_length(result) do
      {:ok, length, body}
    end
  end

  defp open_stream_error(status, body, result) do
    decoded =
      cond do
        enumerable_body?(body) ->
          consume_error_body(body)

        is_map(body) ->
          body

        is_binary(body) ->
          case StrictDecoder.decode(body) do
            {:ok, map} when is_map(map) -> map
            _ -> body
          end

        true ->
          body
      end

    sized = %{result | body: decoded_size_body(decoded, body)}

    if response_size(sized) <= max_response_bytes() do
      {:error, decode_error(status, decoded)}
    else
      {:error, ElixirDB.Error.payload_too_large("remote endpoint response is too large")}
    end
  end

  defp consume_error_body(body) do
    binary =
      body
      |> Enum.to_list()
      |> IO.iodata_to_binary()

    case StrictDecoder.decode(binary) do
      {:ok, map} when is_map(map) -> map
      _ -> binary
    end
  rescue
    exception in [ArgumentError, ErlangError, Protocol.UndefinedError, RuntimeError] ->
      _ = exception
      ""
  end

  defp decoded_size_body(decoded, _original) when is_binary(decoded), do: decoded
  defp decoded_size_body(decoded, _original) when is_map(decoded), do: decoded
  defp decoded_size_body(_decoded, original) when is_binary(original), do: original
  defp decoded_size_body(_, _), do: ""

  defp enumerable_body?(%Req.Response.Async{}), do: true
  defp enumerable_body?(body) when is_struct(body), do: true
  defp enumerable_body?(_), do: false

  defp validate_octet_stream_response(result) do
    content_type =
      result
      |> Map.get(:headers, [])
      |> Enum.find_value(fn
        {key, value} when key in ["content-type", "Content-Type"] -> value
        _ -> nil
      end)
      |> content_type_header()

    if is_nil(content_type) or String.starts_with?(content_type, "application/octet-stream") do
      :ok
    else
      invalid_response_error()
    end
  end

  defp response_content_length(result) do
    length_header =
      result
      |> Map.get(:headers, [])
      |> Enum.find_value(fn
        {key, value} when key in ["content-length", "Content-Length"] -> value
        _ -> nil
      end)
      |> content_type_header()

    case length_header do
      nil ->
        {:error, ElixirDB.Error.database_unavailable("remote blob response missing content-length")}

      value ->
        case Integer.parse(value) do
          {length, ""} when length >= 0 ->
            {:ok, length}

          _ ->
            {:error,
             ElixirDB.Error.database_unavailable("remote blob response has invalid content-length")}
        end
    end
  end

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
      headers: [{"accept", "application/json"} | auth_headers ++ trace_headers]
    ]

    add_request_body(options, body, auth_headers, trace_headers)
  end

  defp trace_headers do
    Tracer.inject([])
    |> Enum.map(fn {key, value} -> {to_string(key), to_string(value)} end)
  end

  defp add_request_body(options, nil, _auth_headers, _trace_headers), do: options

  defp add_request_body(options, body, auth_headers, trace_headers) do
    Keyword.merge(options,
      json: body,
      headers: [
        {"accept", "application/json"},
        {"content-type", "application/json"} | auth_headers ++ trace_headers
      ]
    )
  end

  defp request_task(options, otel_ctx) do
    Task.async(fn ->
      token = OpenTelemetry.Ctx.attach(otel_ctx)

      try do
        Req.request(options)
      after
        OpenTelemetry.Ctx.detach(token)
      end
    end)
  end

  defp handle_task_result({:ok, {:ok, %{status: status, body: response} = result}})
       when status in 200..299 do
    if valid_success_response?(result),
      do: {:ok, response},
      else: invalid_response_error()
  end

  defp handle_task_result({:ok, {:ok, %{status: status, body: response} = result}}) do
    if response_size(result) <= max_response_bytes(),
      do: {:error, decode_error(status, response)},
      else: {:error, ElixirDB.Error.payload_too_large("remote endpoint response is too large")}
  end

  defp handle_task_result({:ok, {:error, reason}}),
    do:
      {:error,
       ElixirDB.Error.database_unavailable("remote endpoint request failed", %{
         cause: inspect(reason)
       })}

  # SAFETY: Req.request/1 (or OpenTelemetry context handling) may exit rather
  # than return an error. Convert that exit into a typed retryable failure.
  defp handle_task_result({:exit, reason}),
    do:
      {:error,
       ElixirDB.Error.internal_error("replication transport request failed", %{
         cause: inspect(reason)
       })}

  defp handle_task_result(nil),
    do: {:error, ElixirDB.Error.database_unavailable("remote endpoint request timed out")}

  defp valid_success_response?(result),
    do: response_size(result) <= max_response_bytes() and accepted_content_type?(result)

  defp invalid_response_error do
    {:error, ElixirDB.Error.database_unavailable("remote endpoint returned an invalid response")}
  end

  defp decode_error(_status, %{"error" => error}) when is_map(error) do
    code =
      ElixirDB.Error.registry()
      |> Map.keys()
      |> Enum.find(:internal_error, &(Atom.to_string(&1) == error["code"]))

    ElixirDB.Error.new(code, error["message"] || "remote request failed", error["details"] || %{},
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
      ElixirDB.Error.database_unavailable("remote endpoint returned an invalid error")
  end

  defp decode_error(status, _),
    do:
      ElixirDB.Error.new(:internal_error, "remote endpoint returned HTTP #{status}", %{},
        retryable: status >= 500
      )

  defp max_response_bytes,
    do: ElixirDB.Config.host_limits()[:max_replication_batch_bytes] || 16_777_216

  defp response_size(%{body: body}) when is_binary(body), do: byte_size(body)

  defp response_size(%{body: body}),
    do: byte_size(IO.iodata_to_binary(JSON.encode_to_iodata!(body)))

  defp accepted_content_type?(%{headers: headers}) do
    headers
    |> Enum.find_value(fn
      {key, value} when key in ["content-type", "Content-Type"] -> value
      _ -> nil
    end)
    |> content_type_header()
    |> case do
      nil -> true
      value -> String.starts_with?(value, "application/json")
    end
  end

  defp accepted_content_type?(_), do: true

  defp content_type_header(nil), do: nil
  defp content_type_header(value) when is_binary(value), do: value
  defp content_type_header([value | _]) when is_binary(value), do: value
  defp content_type_header(_), do: nil

  defp auth_headers(nil), do: []
  defp auth_headers(token) when is_binary(token), do: [{"authorization", "Bearer " <> token}]
end
