defmodule ElixirDB.Observability.Instrumentation.HTTP do
  @moduledoc """
  Emitter for the HTTP request event:

    * `elixir_db.http.request` — span + histogram

  `ElixirDB.HTTP.Router` overrides `call/2` to wrap the whole routing pipeline
  with `wrap/2`, so every request gets exactly one server span.

  The span is ended in a `register_before_send` callback: `before_send` runs
  inside `send_resp`, before the response bytes reach the client, so a client
  that observed the response can immediately observe the (synchronously
  exported) span. Requests that raise before any response is sent never fire
  `before_send`; `wrap/2` ends that span itself (with `http.status_code: 500`
  and status ERROR) and reraises, so no span is leaked either way.

  Sets `http.method`, `http.route` (a route template, never the raw path),
  `http.status_code`, and duration. Extracts inbound W3C
  `traceparent`/`tracestate` headers so a caller's trace continues into the
  request span and restores the prior context afterwards (Bandit
  reuses the connection process across keep-alive requests).
  """

  require OpenTelemetry.Tracer

  alias ElixirDB.Observability.{Attributes, Meters, Tracer}

  @request_span "elixir_db.http.request"

  @doc """
  Runs `fun.(conn)` (the routing pipeline) inside an `elixir_db.http.request`
  server span. The span ends when the response is sent (or with status 500 if
  the pipeline raises first); the prior trace context is restored in both
  cases.
  """
  @spec wrap(Plug.Conn.t(), (Plug.Conn.t() -> Plug.Conn.t())) :: Plug.Conn.t()
  def wrap(conn, fun) when is_function(fun, 1) do
    # Extract inbound trace context (W3C traceparent/tracestate) so an external
    # caller's trace continues into this request span. extract/1 attaches the
    # extracted context and returns a detach token used to restore the prior
    # context after the response.
    extract_token = Tracer.extract(header_list(conn.req_headers))

    started = System.monotonic_time()
    # Capture the route template and db uuid before routing consumes path_info.
    route = route_template(conn)
    db_uuid = database_uuid(conn)

    start_attrs =
      Attributes.build(
        [http_method: conn.method, http_route: route] ++
          if(db_uuid, do: [db_uuid: db_uuid], else: [])
      )

    span_ctx =
      OpenTelemetry.Tracer.start_span(@request_span, %{kind: :server, attributes: start_attrs})

    OpenTelemetry.Tracer.set_current_span(span_ctx)

    conn =
      Plug.Conn.register_before_send(conn, fn conn ->
        # Runs inside send_resp, before the bytes reach the client.
        finish(span_ctx, conn, route, db_uuid, started)
        conn
      end)

    try do
      fun.(conn)
    rescue
      error in [
        ArgumentError,
        ArithmeticError,
        BadMapError,
        CaseClauseError,
        ErlangError,
        FunctionClauseError,
        KeyError,
        MatchError,
        Protocol.UndefinedError,
        RuntimeError,
        UndefinedFunctionError,
        WithClauseError
      ] ->
        # No response was (or will be) sent: before_send never fires, so end
        # the span here with the effective 500.
        finish_raised(span_ctx, conn, route, db_uuid, started)

        # SAFETY net: an unanticipated raise inside a route handler must never escape as a
        # bare process crash that drops the HTTP connection without a JSON body. If the
        # response has not yet started (conn.state still unsent), render a typed
        # internal_error envelope so the client observes a stable 500. If the response was
        # already started (e.g. mid-chunk on a streaming endpoint), the body can no longer
        # be replaced, so reraise and let the server close the connection.
        if conn.state in [:unset, :set, :set_chunked, :set_file] do
          Plug.Conn.put_resp_content_type(conn, "application/json")
          |> Plug.Conn.send_resp(
            500,
            JSON.encode_to_iodata!(%{
              "request_id" => ElixirDB.UUID.v4(),
              "error" =>
                ElixirDB.Error.public(
                  ElixirDB.Error.internal_error("request failed", %{
                    cause: inspect(error)
                  })
                )
            })
          )
        else
          reraise error, __STACKTRACE__
        end
    after
      # Restore the prior context so the extracted parent doesn't leak into
      # the next keep-alive request on this connection process.
      if extract_token, do: OpenTelemetry.Ctx.detach(extract_token)
      OpenTelemetry.Tracer.set_current_span(:undefined)
    end
  end

  defp finish(span_ctx, conn, route, db_uuid, started) do
    duration = System.monotonic_time() - started

    metric_attrs =
      [http_method: conn.method, http_route: route] ++
        if(conn.status, do: [http_status_code: conn.status], else: []) ++
        if(db_uuid, do: [db_uuid: db_uuid], else: [])

    # Metrics remain useful when tracing is deliberately disabled (the normal
    # no-collector configuration). Span mutation/end calls still need the
    # recording guard because the span may be non-recording or already ended.
    Meters.record(:"elixir_db.http.request.duration", duration, metric_attrs)

    # Guards the raise-after-send case: the span may already be ended.
    if OpenTelemetry.Span.is_recording(span_ctx) do
      record_http_status(span_ctx, conn.status)
      OpenTelemetry.Span.end_span(span_ctx)
    end

    :ok
  end

  defp record_http_status(_span_ctx, nil), do: :ok

  defp record_http_status(span_ctx, status) do
    _ = OpenTelemetry.Span.set_attributes(span_ctx, Attributes.build(http_status_code: status))

    if status >= 500 do
      OpenTelemetry.Span.set_status(span_ctx, :opentelemetry.status(:error))
    end
  end

  # The pipeline raised before a response: the request failed with a 500 by
  # Bandit semantics. End the span here (no response callback will run).
  defp finish_raised(span_ctx, conn, route, db_uuid, started) do
    duration = System.monotonic_time() - started

    metric_attrs =
      [http_method: conn.method, http_route: route, http_status_code: 500] ++
        if(db_uuid, do: [db_uuid: db_uuid], else: [])

    Meters.record(:"elixir_db.http.request.duration", duration, metric_attrs)

    if OpenTelemetry.Span.is_recording(span_ctx) do
      _ =
        OpenTelemetry.Span.set_attributes(span_ctx, Attributes.build(http_status_code: 500))

      OpenTelemetry.Span.set_status(span_ctx, :opentelemetry.status(:error))
      OpenTelemetry.Span.end_span(span_ctx)
    end

    :ok
  end

  # Normalizes Plug header keys (atoms or binaries) to the list of
  # {binary, binary} pairs the propagator's default carrier expects.
  defp header_list(headers) do
    Enum.map(headers, fn
      {key, value} when is_atom(key) -> {Atom.to_string(key), value}
      {key, value} -> {key, value}
    end)
  end

  # Builds the route template from conn.path_info. This is a CLOSED allow-list
  # of every route the router and its forwards declare (path-based; method is
  # recorded separately). Anything else maps to the constant "unknown" — NEVER
  # the raw path, so ids cannot leak into telemetry and metric cardinality
  # stays bounded (§3.1).
  defp route_template(conn) do
    template_for(conn.path_info || [])
  end

  defp template_for(["v1", "federation", "query"]), do: "/v1/federation/query"

  defp template_for(["v1", "federation", "saved-queries"]),
    do: "/v1/federation/saved-queries"

  defp template_for(["v1", "federation", "saved-queries", "execute"]),
    do: "/v1/federation/saved-queries/execute"

  defp template_for(["v1", "materialized-views"]), do: "/v1/materialized-views"

  defp template_for(["v1", "materialized-views", _uuid]),
    do: "/v1/materialized-views/:derived_uuid"

  defp template_for(["v1", "materialized-views", _uuid, action])
       when action in ~w(refresh rebuild enable disable),
       do: "/v1/materialized-views/:derived_uuid/" <> action

  defp template_for(["v1", "databases"]), do: "/v1/databases"
  defp template_for(["v1", "databases", _uuid]), do: "/v1/databases/:uuid"
  defp template_for(["v1", "databases", _uuid, "config"]), do: "/v1/databases/:uuid/config"
  defp template_for(["v1", "databases", _uuid, "close"]), do: "/v1/databases/:uuid/close"

  defp template_for(["v1", "databases", _uuid, "integrity-check"]),
    do: "/v1/databases/:uuid/integrity-check"

  defp template_for(["v1", "databases", _uuid, "query"]), do: "/v1/databases/:uuid/query"

  defp template_for(["v1", "databases", _uuid, "query", "explain"]),
    do: "/v1/databases/:uuid/query/explain"

  defp template_for(["v1", "databases", _uuid, "documents", action])
       when action in ~w(get put delete resolve bulk-get bulk-write),
       do: "/v1/databases/:uuid/documents/" <> action

  defp template_for(["v1", "databases", _uuid, "changes"]), do: "/v1/databases/:uuid/changes"

  defp template_for(["v1", "databases", _uuid, "changes", "stream"]),
    do: "/v1/databases/:uuid/changes/stream"

  defp template_for(["v1", "databases", _uuid, "indexes"]), do: "/v1/databases/:uuid/indexes"

  defp template_for(["v1", "databases", _uuid, "indexes", _index_id]),
    do: "/v1/databases/:uuid/indexes/:index_id"

  defp template_for(["v1", "databases", _uuid, "indexes", _index_id, "rebuild"]),
    do: "/v1/databases/:uuid/indexes/:index_id/rebuild"

  defp template_for(["v1", "databases", _uuid, "replications"]),
    do: "/v1/databases/:uuid/replications"

  defp template_for(["v1", "databases", _uuid, "replications", _job_id]),
    do: "/v1/databases/:uuid/replications/:job_id"

  defp template_for(["v1", "databases", _uuid, "replications", _job_id, action])
       when action in ~w(start cancel enable disable),
       do: "/v1/databases/:uuid/replications/:job_id/" <> action

  defp template_for(["v1", "databases", _uuid, "replication", "identity"]),
    do: "/v1/databases/:uuid/replication/identity"

  defp template_for(["v1", "databases", _uuid, "replication", "changes"]),
    do: "/v1/databases/:uuid/replication/changes"

  defp template_for(["v1", "databases", _uuid, "replication", "revisions", action])
       when action in ~w(diff get put),
       do: "/v1/databases/:uuid/replication/revisions/" <> action

  defp template_for(["v1", "databases", _uuid, "replication", "checkpoints", _replication_id]),
    do: "/v1/databases/:uuid/replication/checkpoints/:replication_id"

  defp template_for(["v1", "registrations"]), do: "/v1/registrations"
  defp template_for(["v1", "registrations", _uuid]), do: "/v1/registrations/:uuid"
  defp template_for(["v1", "observability", "snapshot"]), do: "/v1/observability/snapshot"

  # Unknown route: constant fallback, never the raw path (§3.1 privacy,
  # bounded cardinality).
  defp template_for(_), do: "unknown"

  # The :uuid path param. At pipeline entry (before routing) conn.path_params
  # isn't populated, so extract the uuid from path_info for database-scoped
  # routes.
  defp database_uuid(conn) do
    case conn.path_info do
      ["v1", "databases", uuid | _] -> uuid
      ["v1", "materialized-views", uuid | _] -> uuid
      _ -> nil
    end
  end
end
