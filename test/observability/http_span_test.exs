defmodule ElixirDB.Observability.HTTPSpanTest do
  @moduledoc """
  Assert the `elixir_db.http.request` span is emitted with method,
  route template, and status code; that an inbound `traceparent` makes the
  caller span the parent; and that the prior context never leaks into the next
  keep-alive request.
  """

  use ElixirDB.Observability.OtelCase, async: false

  @moduletag :integration

  alias ElixirDB.Observability.TestExporter
  alias ElixirDB.TestServer

  @trace_id_hex "0af7651916cd43dd8448eb211c80319c"
  @parent_span_hex "b7ad6b7169203331"

  setup do
    server = TestServer.start_supervised!()
    root = ElixirDB.Config.database_root()
    path = "http-span-#{System.unique_integer([:positive])}.elixirdb"

    # Leftover files from a previous VM run would fail creation with
    # "database file already exists"; remove ours on exit.
    on_exit(fn ->
      ElixirDB.TempDatabase.cleanup(Path.join(root, path))
    end)

    [server: server, path: path]
  end

  test "emits an http.request span with method, route template, and status", %{
    server: server,
    path: path
  } do
    assert {:ok, %{status: 201}} =
             Req.post(server.base_url <> "/v1/databases", json: %{"path" => path})

    spans = TestExporter.spans_named("elixir_db.http.request")
    assert [_ | _] = spans, "expected at least one http.request span"

    span =
      Enum.find(spans, fn s ->
        TestExporter.span_attr(s, :"http.method") == "POST" and
          TestExporter.span_attr(s, :"http.route") == "/v1/databases"
      end)

    assert span != nil,
           "no POST /v1/databases span; got: #{inspect(Enum.map(spans, &{TestExporter.span_attr(&1, :"http.method"), TestExporter.span_attr(&1, :"http.route")}))}"

    assert TestExporter.span_attr(span, :"http.status_code") == 201
    assert TestExporter.status_code(span) in [:unset, :ok]
  end

  test "unknown routes record the constant 'unknown' route, never the raw path", %{
    server: server
  } do
    secret = "sentinel-#{System.unique_integer([:positive])}"

    assert {:ok, %{status: 400}} = Req.get(server.base_url <> "/totally/#{secret}/seg2")

    spans = TestExporter.spans_named("elixir_db.http.request")

    span =
      Enum.find(spans, fn s -> TestExporter.span_attr(s, :"http.method") == "GET" end)

    assert span != nil, "no GET span recorded for the unknown route"
    assert TestExporter.span_attr(span, :"http.route") == "unknown"

    # §3.1: the raw path (and its unbounded segments) must not leak into
    # telemetry.
    refute String.contains?(inspect(span), secret)
  end

  test "inbound traceparent makes the client span the parent", %{server: server} do
    assert {:ok, %{status: 200}} =
             Req.get(server.base_url <> "/v1/databases",
               headers: [{"traceparent", "00-#{@trace_id_hex}-#{@parent_span_hex}-01"}]
             )

    spans = TestExporter.spans_named("elixir_db.http.request")

    span =
      Enum.find(spans, fn s -> s[:trace_id] == String.to_integer(@trace_id_hex, 16) end)

    assert span != nil,
           "no http.request span continued the inbound trace; got trace_ids: " <>
             "#{inspect(Enum.map(spans, & &1[:trace_id]))}"

    assert span[:parent_span_id] == String.to_integer(@parent_span_hex, 16)
  end

  test "a follow-up request without traceparent starts a fresh trace", %{server: server} do
    # First request carries a caller trace; the second carries nothing. Bandit
    # reuses the keep-alive connection process, so the second span proves the
    # extracted context was detached after the first response.
    assert {:ok, _} =
             Req.get(server.base_url <> "/v1/databases",
               headers: [{"traceparent", "00-#{@trace_id_hex}-#{@parent_span_hex}-01"}]
             )

    assert {:ok, _} = Req.get(server.base_url <> "/v1/databases")

    spans =
      TestExporter.spans_named("elixir_db.http.request")
      |> Enum.filter(fn s -> TestExporter.span_attr(s, :"http.method") == "GET" end)

    caller_trace = String.to_integer(@trace_id_hex, 16)
    {continued, fresh} = Enum.split_with(spans, &(&1[:trace_id] == caller_trace))

    assert [_] = continued, "exactly one span may continue the inbound trace"
    assert [_ | _] = fresh, "expected a follow-up span in a fresh trace"

    for span <- fresh do
      refute span[:trace_id] == caller_trace
      assert span[:parent_span_id] == :undefined
    end
  end
end
