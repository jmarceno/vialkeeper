defmodule ElixirDB.Observability.FederationSignalTest do
  @moduledoc "Covers federation tracing and metric signals without secrets."

  use ElixirDB.Observability.OtelCase, async: false

  @moduletag :integration

  alias ElixirDB.Error
  alias ElixirDB.Federation
  alias ElixirDB.TestServer

  @source "123e4567-e89b-12d3-a456-426614174000"
  @duration_metric "elixir_db.federation.query.duration"

  test "records bounded source count, outcome, and stable error code" do
    assert {:error, %Error{code: :database_not_registered}} =
             Federation.query(%{databases: [@source], query: %{limit: 1}})

    # FLAKE: `TestExporter.spans_named(...)` is a single non-retried snapshot of the
    # async exporter. The matching span may not be exported yet under full-suite load, so
    # `assert [span] = ...` fails even though the query ran (passes in isolation). The metric
    # assertion below is properly `eventually`-guarded; this span read is not. Rewrite to poll
    # for the span with `Eventual.eventually` or flush the exporter before asserting.
    assert [span] = TestExporter.spans_named("elixir_db.federation.query")
    assert TestExporter.span_attr(span, :"federation.source_count") == 1
    assert TestExporter.span_attr(span, :outcome) == :rejected
    assert TestExporter.span_attr(span, :"error.code") == :database_not_registered
    assert TestExporter.status_code(span) == :unset
    refute String.contains?(inspect(span), @source)

    ElixirDB.Eventual.eventually(
      fn ->
        TestMetricExporter.datapoints_matching(@duration_metric, %{
          :"federation.source_count" => 1
        }) != []
      end,
      timeout: 2_000,
      message: "expected a federation duration datapoint"
    )
  end

  test "uses route templates for federation HTTP spans" do
    server = TestServer.start_supervised!()
    secret = "123e4567-e89b-12d3-a456-426614174099"

    assert {:ok, %{status: 404}} =
             Req.post(server.base_url <> "/v1/federation/query",
               json: %{"databases" => [secret], "query" => %{"limit" => 1}}
             )

    span =
      TestExporter.spans_named("elixir_db.http.request")
      |> Enum.find(fn candidate ->
        TestExporter.span_attr(candidate, :"http.route") == "/v1/federation/query"
      end)

    # FLAKE: this HTTP span is read once from the async exporter with no retry; under
    # full-suite load it may not be exported yet (`span == nil`), the same exporter race as
    # the test above. `assert span != nil` then fails spuriously. Poll with
    # `Eventual.eventually` (or flush) so the request's span is definitely exported.
    assert span != nil
    refute String.contains?(inspect(span), secret)
  end
end
