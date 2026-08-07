defmodule ElixirDB.Replication.RemoteTransport do
  @moduledoc false

  def request(base_url, method, path, body \\ nil, auth_token \\ nil) do
    # Inject the current trace context into outgoing replication requests so a
    # push job's trace spans both the local worker and the remote server
    # (plan §6.2). The noop propagator (no SDK) returns the headers unchanged.
    trace_headers =
      ElixirDB.Observability.Tracer.inject([])
      |> Enum.map(fn {k, v} -> {to_string(k), to_string(v)} end)

    # When the endpoint declares an auth_token (AUTH-003), present it as the
    # bearer credential so the target's AuthPlug accepts the replication call.
    auth_headers = auth_headers(auth_token)

    options = [
      method: method,
      url: String.trim_trailing(base_url, "/") <> path,
      retry: false,
      receive_timeout: 30_000,
      connect_options: [timeout: 5_000],
      headers: [{"accept", "application/json"} | auth_headers ++ trace_headers]
    ]

    options =
      if is_nil(body),
        do: options,
        else:
          Keyword.merge(options,
            json: body,
            headers: [
              {"accept", "application/json"},
              {"content-type", "application/json"} | auth_headers ++ trace_headers
            ]
          )

    timeout = ElixirDB.Config.host_limits()[:max_request_timeout_ms] || 30_000

    # Carry the trace context into the timeout-enforcement task so the Finch
    # telemetry-bridge span for this request parents under the replication
    # trace (plan §6.2); without it the span would be a parentless root.
    otel_ctx = OpenTelemetry.Ctx.get_current()

    task =
      Task.async(fn ->
        token = OpenTelemetry.Ctx.attach(otel_ctx)

        try do
          Req.request(options)
        after
          OpenTelemetry.Ctx.detach(token)
        end
      end)

    case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, {:ok, %{status: status, body: response} = result}} when status in 200..299 ->
        if response_size(result) <= max_response_bytes() and accepted_content_type?(result),
          do: {:ok, response},
          else:
            {:error,
             ElixirDB.Error.database_unavailable("remote endpoint returned an invalid response")}

      {:ok, {:ok, %{status: status, body: response} = result}} ->
        if response_size(result) <= max_response_bytes(),
          do: {:error, decode_error(status, response)},
          else: {:error, ElixirDB.Error.payload_too_large("remote endpoint response is too large")}

      {:ok, {:error, reason}} ->
        {:error,
         ElixirDB.Error.database_unavailable("remote endpoint request failed", %{
           cause: inspect(reason)
         })}

      # SAFETY: Req.request/1 (or the OpenTelemetry context handling) may raise rather
      # than return {:error, _} on certain malformed inputs or transport faults. The task
      # then exits with {:exit, reason}. Without these arms the surrounding `case` would
      # raise CaseClauseError in the worker. Funnel the crash into a typed error so the
      # replication worker retries instead of aborting.
      {:exit, reason} ->
        {:error,
         ElixirDB.Error.internal_error("replication transport request failed", %{
           cause: inspect(reason)
         })}

      nil ->
        {:error, ElixirDB.Error.database_unavailable("remote endpoint request timed out")}
    end
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
    _ -> ElixirDB.Error.database_unavailable("remote endpoint returned an invalid error")
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
