defmodule ElixirDB.Observability.PrivacyTest do
  @moduledoc """
  Plan §7.2 privacy gate (security): put a document with a distinctive body,
  document id, and search text, run a query and HTTP requests, then assert NO
  recorded span attribute contains the body, the search text, or the document
  id. Enforces the Plan §11 allow-list (document bodies, search text, and IDs
  are forbidden).
  """

  use ElixirDB.Observability.OtelCase, async: false

  alias ElixirDB.Documents
  alias ElixirDB.Observability.TestExporter
  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.TestServer

  @body_secret "PRIVACY_BODY_SENTINEL_a1b2c3"
  @doc_id "priv-doc-id-sentinel-9999"
  @search_secret "PRIVACY_SEARCH_SENTINEL_z9y8x7"

  setup do
    # The catalog creates the file under database_root; clean up THERE (a
    # leftover from a previous run would fail create with "database file
    # already exists").
    rel = "obs-privacy-#{System.unique_integer([:positive])}.elixirdb"
    {:ok, %{database_uuid: uuid}} = DatabaseCatalog.create(rel)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      root = ElixirDB.Config.database_root()
      ElixirDB.TempDatabase.cleanup(Path.join(root, rel))
    end)

    [uuid: uuid]
  end

  test "no span attribute leaks the body, document id, or search text", %{uuid: uuid} do
    assert {:ok, _} =
             Documents.put(uuid, %{id: @doc_id, body: %{"value" => @body_secret}})

    # A get on the same doc exercises the command span path with the doc id in
    # scope at the owner; the allow-list must keep it out of attributes.
    {:ok, _} = Documents.get(uuid, %{id: @doc_id})

    # A query exercises the query.execute span path with the SEARCH TEXT in
    # scope (the riskiest §3.1 item). The query may match nothing — only the
    # span emission matters here.
    _ =
      ElixirDB.Query.execute(uuid, %{
        "selector" => %{"/value" => @search_secret},
        "limit" => 10
      })

    all_spans = TestExporter.spans()
    assert [_ | _] = all_spans, "expected some spans to be recorded"

    assert Enum.any?(all_spans, &(&1[:name] == "elixir_db.query.execute")),
           "expected a query.execute span to exercise the search-text path"

    assert_no_leaks(all_spans, [@body_secret, @doc_id, @search_secret])
  end

  test "unknown HTTP routes never leak the raw path into spans", %{uuid: uuid} do
    server = TestServer.start_supervised!()
    path_secret = "priv-path-sentinel-#{@doc_id}"

    # A database-scoped unknown route and a fully unknown route. Both must map
    # to constant route templates, never the raw path.
    assert {:ok, %{status: status_a}} =
             Req.get(server.base_url <> "/v1/databases/#{uuid}/#{path_secret}")

    assert status_a in [400, 404]

    assert {:ok, %{status: status_b}} =
             Req.get(server.base_url <> "/nowhere/#{path_secret}/deep")

    assert status_b in [400, 404]

    all_spans = TestExporter.spans()
    http_spans = Enum.filter(all_spans, &(&1[:name] == "elixir_db.http.request"))
    assert [_, _ | _] = http_spans

    assert_no_leaks(http_spans, [path_secret, @doc_id])

    routes = Enum.map(http_spans, &TestExporter.span_attr(&1, :"http.route"))
    refute Enum.any?(routes, &is_nil/1), "every http span must carry a route template"
    refute Enum.any?(routes, fn r -> String.contains?(to_string(r), path_secret) end)
  end

  defp assert_no_leaks(spans, forbidden) do
    leaks =
      for span <- spans,
          value <- span_attr_values(span[:attributes]),
          secret <- forbidden,
          String.contains?(inspect(value), secret) do
        {span[:name], secret}
      end

    assert leaks == [],
           "forbidden values leaked into span attributes: #{inspect(leaks)}"
  end

  # Extracts all attribute values from an otel_attributes record or plain map.
  defp span_attr_values({:attributes, _, _, _, map}) when is_map(map), do: Map.values(map)
  defp span_attr_values(map) when is_map(map), do: Map.values(map)
  defp span_attr_values(_), do: []
end
