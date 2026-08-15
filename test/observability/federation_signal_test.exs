defmodule VialKeeper.Observability.FederationSignalTest do
  @moduledoc "Covers federation tracing and metric signals without secrets."

  use VialKeeper.Observability.OtelCase, async: false

  @moduletag :integration

  alias VialKeeper.Error
  alias VialKeeper.Eventual
  alias VialKeeper.Federation
  alias VialKeeper.TestServer

  @source "123e4567-e89b-12d3-a456-426614174000"
  @duration_metric "vial_keeper.federation.query.duration"

  test "records bounded source count, outcome, and stable error code" do
    assert {:error, %Error{code: :database_not_registered}} =
             Federation.query(%{databases: [@source], query: %{limit: 1}})

    span =
      Eventual.eventually(
        fn ->
          TestExporter.spans_named("vial_keeper.federation.query")
          |> Enum.find(fn candidate ->
            TestExporter.span_attr(candidate, :"federation.source_count") == 1 and
              TestExporter.span_attr(candidate, :outcome) == :rejected
          end)
        end,
        message: "federation query span missing"
      )

    assert TestExporter.span_attr(span, :"federation.source_count") == 1
    assert TestExporter.span_attr(span, :outcome) == :rejected
    assert TestExporter.span_attr(span, :"error.code") == :database_not_registered
    assert TestExporter.status_code(span) == :unset
    refute String.contains?(inspect(span), @source)

    VialKeeper.Eventual.eventually(
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
      Eventual.eventually(
        fn ->
          TestExporter.spans_named("vial_keeper.http.request")
          |> Enum.find(fn candidate ->
            TestExporter.span_attr(candidate, :"http.route") == "/v1/federation/query"
          end)
        end,
        message: "federation HTTP span missing"
      )

    refute String.contains?(inspect(span), secret)
  end
end
