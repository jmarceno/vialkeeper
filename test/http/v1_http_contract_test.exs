defmodule ElixirDB.HTTP.V1HTTPContractTest do
  @moduledoc "Covers stable HTTP request and error contracts."

  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.HTTP.Router
  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.Query.BookmarkCodec
  alias ElixirDB.Runtime.DatabaseCatalog
  alias Plug.Conn

  test "HTTP rejects unknown fields and returns the stable error envelope" do
    response =
      request(:post, "/v1/databases", %{"path" => "contract-invalid.elixirdb", "unexpected" => true})

    assert response.status == 400

    assert response.resp_headers
           |> Enum.any?(fn {key, value} -> key == "x-request-id" and value != "" end)

    assert {:ok, %{"error" => %{"code" => "invalid_request", "retryable" => false}}} =
             StrictDecoder.decode(response.resp_body)
  end

  test "database, query, integrity, and NDJSON changes routes follow V1 envelopes" do
    path = "contract-#{System.unique_integer([:positive])}.elixirdb"
    created = request(:post, "/v1/databases", %{"path" => path})
    assert created.status == 201

    {:ok, %{"data" => %{"database_uuid" => uuid}}} =
      StrictDecoder.decode(created.resp_body)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path))
    end)

    put =
      request(:post, "/v1/databases/#{uuid}/documents/put", %{
        "id" => "doc",
        "body" => %{"kind" => "task"}
      })

    assert put.status == 201

    bad_put =
      request(:post, "/v1/databases/#{uuid}/documents/put", %{
        "id" => "bad",
        "body" => %{},
        "unexpected" => true
      })

    assert bad_put.status == 400

    index =
      request(:post, "/v1/databases/#{uuid}/indexes", %{
        "name" => "by-kind",
        "type" => "structured",
        "fields" => [%{"path" => "/kind", "type" => "string", "direction" => "asc"}]
      })

    assert index.status == 201

    query = request(:post, "/v1/databases/#{uuid}/query", %{"selector" => %{"/kind" => "task"}})
    assert query.status == 200

    {:ok, %{"data" => %{"documents" => [%{"id" => "doc"}]}}} =
      StrictDecoder.decode(query.resp_body)

    integrity = request(:post, "/v1/databases/#{uuid}/integrity-check", %{})
    assert integrity.status == 200

    stream =
      request(:post, "/v1/databases/#{uuid}/changes/stream", %{
        "since" => 0,
        "limit" => 10,
        "heartbeat_ms" => 0
      })

    assert stream.status == 200
    assert Conn.get_resp_header(stream, "content-type") |> List.first() =~ "application/x-ndjson"
    assert stream.resp_body =~ "\"type\":\"change\""
    assert stream.resp_body =~ "\"type\":\"caught_up\""
  end

  test "unicode and slash-bearing document ids round-trip through put, get, and changes" do
    path = "contract-ids-#{System.unique_integer([:positive])}.elixirdb"
    created = request(:post, "/v1/databases", %{"path" => path})
    assert created.status == 201

    {:ok, %{"data" => %{"database_uuid" => uuid}}} =
      StrictDecoder.decode(created.resp_body)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path))
    end)

    ids = ["café", "日本語", "a/b", "foo%2Fbar", "_foo", "plain-id-1"]

    for id <- ids do
      put =
        request(:post, "/v1/databases/#{uuid}/documents/put", %{
          "id" => id,
          "body" => %{"id_echo" => id}
        })

      assert put.status == 201, "expected 201 for id #{inspect(id)}, got #{put.status}"

      {:ok, %{"data" => %{"revision" => revision}}} = StrictDecoder.decode(put.resp_body)

      get = request(:post, "/v1/databases/#{uuid}/documents/get", %{"id" => id})
      assert get.status == 200

      {:ok, %{"data" => data}} = StrictDecoder.decode(get.resp_body)
      assert data["id"] == id
      assert data["revision"] == revision
      assert data["body"]["id_echo"] == id
    end

    changes =
      request(:post, "/v1/databases/#{uuid}/changes", %{"since" => 0, "limit" => 100})

    assert changes.status == 200
    {:ok, %{"data" => %{"results" => results}}} = StrictDecoder.decode(changes.resp_body)

    changed_ids = MapSet.new(Enum.map(results, & &1["document_id"]))
    assert MapSet.subset?(MapSet.new(ids), changed_ids)
  end

  test "query contracts expose extended predicates, plan metadata, and explain fields" do
    path = "contract-query-wave4-#{System.unique_integer([:positive])}.elixirdb"
    created = request(:post, "/v1/databases", %{"path" => path})
    assert created.status == 201

    {:ok, %{"data" => %{"database_uuid" => uuid}}} =
      StrictDecoder.decode(created.resp_body)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path))
    end)

    for {id, body} <- [
          {"open",
           %{
             "status" => "open",
             "priority" => 1,
             "kind" => "task",
             "title" => "replication checkpoint"
           }},
          {"high",
           %{
             "status" => "closed",
             "priority" => 5,
             "kind" => "task",
             "title" => "replication guide"
           }},
          {"missing", %{"priority" => 1, "kind" => "task", "title" => "unrelated"}}
        ] do
      response =
        request(:post, "/v1/databases/#{uuid}/documents/put", %{"id" => id, "body" => body})

      assert response.status == 201
    end

    index_ids =
      for {name, path, type} <- [
            {"by-status", "/status", "string"},
            {"by-priority", "/priority", "number"}
          ],
          into: %{} do
        response =
          request(:post, "/v1/databases/#{uuid}/indexes", %{
            "name" => name,
            "type" => "structured",
            "fields" => [%{"path" => path, "type" => type, "direction" => "asc"}]
          })

        assert response.status == 201
        {:ok, %{"data" => %{"index_id" => index_id}}} = StrictDecoder.decode(response.resp_body)
        {name, index_id}
      end

    full_text =
      request(:post, "/v1/databases/#{uuid}/indexes", %{
        "name" => "titles",
        "type" => "full_text",
        "fields" => ["/title"],
        "tokenization" => %{"strategy" => "unicode_words_v1", "diacritics" => "preserve"}
      })

    assert full_text.status == 201

    query =
      request(:post, "/v1/databases/#{uuid}/query", %{
        "selector" => %{
          "$or" => [%{"/status" => "open"}, %{"/priority" => %{"$gte" => 5}}],
          "/title" => %{"$beginsWith" => "replication"}
        },
        "limit" => 10
      })

    assert query.status == 200
    {:ok, %{"data" => data}} = StrictDecoder.decode(query.resp_body)
    assert data["plan_kind"] == "union"
    assert is_binary(data["plan_digest"])
    assert [_, _] = data["selected_indexes"]
    assert Enum.map(data["documents"], & &1["id"]) == ["high", "open"]

    for selector <- [
          %{"/status" => %{"$ne" => "closed"}},
          %{"/priority" => %{"$gt" => 0, "$lte" => 5}},
          %{"/status" => %{"$in" => ["open", "queued"]}},
          %{"/status" => %{"$nin" => ["deleted"]}},
          %{"/status" => %{"$exists" => true}},
          %{"/priority" => %{"$type" => "number"}},
          %{"/title" => %{"$beginsWith" => "rep"}},
          %{"/title" => %{"$regex" => "^rep"}},
          %{"/tags" => %{"$all" => ["one"]}},
          %{"/items" => %{"$elemMatch" => %{"/state" => "open"}}},
          %{"/tags" => %{"$size" => 1}},
          %{"/priority" => %{"$mod" => [2, 1]}},
          %{"$not" => %{"/status" => "closed"}},
          %{"$nor" => [%{"/status" => "deleted"}, %{"/priority" => 99}]}
        ] do
      response = request(:post, "/v1/databases/#{uuid}/query", %{"selector" => selector})
      assert response.status == 200, "operator contract failed for #{inspect(selector)}"
    end

    prefix_query =
      request(:post, "/v1/databases/#{uuid}/query", %{
        "search" => %{"index" => "titles", "text" => "replic checkp", "mode" => "prefix"},
        "limit" => 10
      })

    assert prefix_query.status == 200

    assert {:ok, %{"data" => %{"documents" => [%{"id" => "open"}]}}} =
             StrictDecoder.decode(prefix_query.resp_body)

    explain =
      request(:post, "/v1/databases/#{uuid}/query/explain", %{
        "selector" => %{
          "$or" => [%{"/status" => "open"}, %{"/priority" => %{"$gte" => 5}}],
          "/title" => %{"$beginsWith" => "replication"}
        }
      })

    assert explain.status == 200
    {:ok, %{"data" => explanation}} = StrictDecoder.decode(explain.resp_body)
    assert explanation["plan_kind"] == "union"
    assert is_binary(explanation["plan_digest"])
    assert [_, _] = explanation["selected_indexes"]
    assert is_list(explanation["union_branches"])
    assert is_list(explanation["pushdown_predicates"])
    assert explanation["candidate_count"] == 2
    assert Enum.all?(explanation["pushdown_predicates"], &(&1["operator"] in ["$eq", "$gte"]))

    assert explanation["post_filter_predicates"] == [
             %{
               "path" => "/title",
               "predicate" => %{"op" => "$beginsWith", "value" => "replication"}
             }
           ]

    refute query.resp_body =~ "physical_name"
    refute explain.resp_body =~ "physical_name"
    refute explain.resp_body =~ "SELECT"

    invalid_hint =
      request(:post, "/v1/databases/#{uuid}/query", %{
        "index" => "by-priority",
        "selector" => %{"/status" => "open"}
      })

    assert invalid_hint.status == 422

    assert {:ok, %{"error" => %{"code" => "invalid_index_hint"}}} =
             StrictDecoder.decode(invalid_hint.resp_body)

    bookmark_query = %{
      "selector" => %{
        "$or" => [%{"/status" => "open"}, %{"/priority" => %{"$gte" => 5}}]
      },
      "limit" => 1
    }

    first_page = request(:post, "/v1/databases/#{uuid}/query", bookmark_query)
    assert first_page.status == 200

    {:ok, %{"data" => %{"bookmark" => bookmark, "documents" => [_]}}} =
      StrictDecoder.decode(first_page.resp_body)

    assert is_binary(bookmark)

    continued =
      request(
        :post,
        "/v1/databases/#{uuid}/query",
        Map.put(bookmark_query, "bookmark", bookmark) |> Map.put("limit", 10)
      )

    assert continued.status == 200

    assert {:ok, %{"data" => %{"documents" => [%{"id" => "open"}]}}} =
             StrictDecoder.decode(continued.resp_body)

    bounded_query = %{"selector" => %{"/title" => %{"$beginsWith" => "replication"}}, "limit" => 1}
    bounded_page = request(:post, "/v1/databases/#{uuid}/query", bounded_query)
    assert bounded_page.status == 200

    {:ok, %{"data" => %{"bookmark" => bounded_bookmark, "documents" => [_]}}} =
      StrictDecoder.decode(bounded_page.resp_body)

    bounded_continued =
      request(
        :post,
        "/v1/databases/#{uuid}/query",
        Map.put(bounded_query, "bookmark", bounded_bookmark) |> Map.put("limit", 10)
      )

    assert bounded_continued.status == 200

    assert {:ok, %{"data" => %{"documents" => [%{"id" => "open"}]}}} =
             StrictDecoder.decode(bounded_continued.resp_body)

    full_text_bookmark_query = %{
      "search" => %{"index" => "titles", "text" => "replication", "mode" => "prefix"},
      "limit" => 1
    }

    full_text_page = request(:post, "/v1/databases/#{uuid}/query", full_text_bookmark_query)
    assert full_text_page.status == 200

    {:ok, %{"data" => %{"bookmark" => full_text_bookmark, "documents" => [%{"id" => first_id}]}}} =
      StrictDecoder.decode(full_text_page.resp_body)

    full_text_continued =
      request(
        :post,
        "/v1/databases/#{uuid}/query",
        Map.put(full_text_bookmark_query, "bookmark", full_text_bookmark) |> Map.put("limit", 10)
      )

    assert full_text_continued.status == 200

    assert {:ok, %{"data" => %{"documents" => [%{"id" => second_id}]}}} =
             StrictDecoder.decode(full_text_continued.resp_body)

    assert first_id != second_id

    rebuild_query = %{"selector" => %{"/priority" => %{"$gte" => 1}}, "limit" => 1}
    rebuild_page = request(:post, "/v1/databases/#{uuid}/query", rebuild_query)
    assert rebuild_page.status == 200

    {:ok, %{"data" => %{"bookmark" => rebuild_bookmark, "documents" => [_]}}} =
      StrictDecoder.decode(rebuild_page.resp_body)

    assert request(:post, "/v1/databases/#{uuid}/indexes/#{index_ids["by-priority"]}/rebuild", nil).status ==
             200

    rebuild_continued =
      request(
        :post,
        "/v1/databases/#{uuid}/query",
        Map.put(rebuild_query, "bookmark", rebuild_bookmark) |> Map.put("limit", 10)
      )

    assert rebuild_continued.status == 200

    competing_query = %{
      "selector" => %{
        "$and" => [%{"/kind" => "task"}, %{"/priority" => %{"$gte" => 1}}]
      },
      "limit" => 1
    }

    competing_page = request(:post, "/v1/databases/#{uuid}/query", competing_query)
    assert competing_page.status == 200

    {:ok, %{"data" => %{"bookmark" => competing_bookmark}}} =
      StrictDecoder.decode(competing_page.resp_body)

    competing_index =
      request(:post, "/v1/databases/#{uuid}/indexes", %{
        "name" => "by-kind-priority",
        "type" => "structured",
        "fields" => [
          %{"path" => "/kind", "type" => "string", "direction" => "asc"},
          %{"path" => "/priority", "type" => "number", "direction" => "asc"}
        ]
      })

    assert competing_index.status == 201

    competing_invalidated =
      request(
        :post,
        "/v1/databases/#{uuid}/query",
        Map.put(competing_query, "bookmark", competing_bookmark) |> Map.put("limit", 10)
      )

    assert competing_invalidated.status == 400

    assert {:ok, %{"error" => %{"code" => "invalid_bookmark"}}} =
             StrictDecoder.decode(competing_invalidated.resp_body)

    {:ok, decoded_bookmark} = BookmarkCodec.decode(bookmark)

    {:ok, tampered_bookmark} =
      BookmarkCodec.encode(%{
        "query_fingerprint" => decoded_bookmark.query_fingerprint,
        "plan_digest" => decoded_bookmark.plan_digest,
        "index_bindings" => Enum.reverse(decoded_bookmark.index_bindings),
        "sequence" => decoded_bookmark.sequence,
        "sort_direction" => decoded_bookmark.sort_direction,
        "ordering_key" => decoded_bookmark.ordering_key,
        "last_id" => decoded_bookmark.last_id
      })

    tampered =
      request(
        :post,
        "/v1/databases/#{uuid}/query",
        Map.put(bookmark_query, "bookmark", tampered_bookmark) |> Map.put("limit", 10)
      )

    assert tampered.status == 400

    assert {:ok, %{"error" => %{"code" => "invalid_bookmark"}}} =
             StrictDecoder.decode(tampered.resp_body)

    assert request(:delete, "/v1/databases/#{uuid}/indexes/#{index_ids["by-status"]}", nil).status ==
             200

    invalidated =
      request(
        :post,
        "/v1/databases/#{uuid}/query",
        Map.put(bookmark_query, "bookmark", bookmark) |> Map.put("limit", 10)
      )

    assert invalidated.status == 400

    assert {:ok, %{"error" => %{"code" => "invalid_bookmark"}}} =
             StrictDecoder.decode(invalidated.resp_body)

    malformed =
      request(:post, "/v1/databases/#{uuid}/query", %{
        "selector" => %{"/status" => %{"$mod" => [0, 0]}}
      })

    assert malformed.status == 400

    assert {:ok, %{"error" => %{"code" => "invalid_request"}}} =
             StrictDecoder.decode(malformed.resp_body)
  end

  test "document id grammar table rejects forbidden ids and accepts valid ones" do
    path = "contract-id-grammar-#{System.unique_integer([:positive])}.elixirdb"
    created = request(:post, "/v1/databases", %{"path" => path})
    assert created.status == 201

    {:ok, %{"data" => %{"database_uuid" => uuid}}} =
      StrictDecoder.decode(created.resp_body)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path))
    end)

    # Contract from Documents.validate_id/1: empty, NUL, control chars, `_system/`
    # prefix, non-string, and oversized ids are rejected. Underscore prefixes other
    # than `_system/` are allowed. Oversized ids may surface as HTTP 400 or 422
    # depending on whether the typed error is invalid_request or resource_limit.
    cases = [
      {123, [400], "invalid_request"},
      {"", [400], "invalid_request"},
      {"a\0b", [400], "invalid_request"},
      {"a\nb", [400], "invalid_request"},
      {"_system/secret", [400], "invalid_request"},
      {String.duplicate("x", 513), [400, 422], "resource_limit"},
      {"alphanumeric-ok", [201], nil},
      {"café", [201], nil},
      {"a/b", [201], nil},
      {"_foo", [201], nil},
      {"_local", [201], nil}
    ]

    for {id, expected_statuses, expected_code} <- cases do
      response =
        request(:post, "/v1/databases/#{uuid}/documents/put", %{
          "id" => id,
          "body" => %{"ok" => true}
        })

      assert response.status in expected_statuses,
             "id=#{inspect(id)} expected status in #{inspect(expected_statuses)}, got #{response.status}"

      if expected_code do
        assert {:ok, %{"error" => %{"code" => ^expected_code}}} =
                 StrictDecoder.decode(response.resp_body)
      else
        assert {:ok, %{"data" => %{"revision" => _}}} =
                 StrictDecoder.decode(response.resp_body)
      end
    end
  end

  defp request(method, path, body) do
    payload = IO.iodata_to_binary(JSON.encode_to_iodata!(body))

    Plug.Test.conn(method, path, payload)
    |> Conn.put_req_header("content-type", "application/json")
    |> Router.call([])
  end
end
