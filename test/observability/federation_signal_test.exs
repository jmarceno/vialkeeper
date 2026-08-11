defmodule ElixirDB.Observability.FederationSignalTest do
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

    assert span != nil
    refute String.contains?(inspect(span), secret)
  end
end
