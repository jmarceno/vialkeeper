defmodule ElixirDB.EndToEnd.Wave5QueryScenarioTest do
  @moduledoc """
  Plan §33 Wave 5 integration scenario.

  The test uses a real `.elixirdb` bundle and Bandit HTTP for the public
  document, index, query, explain, attachment, integrity, and lifecycle paths.
  Replication is exercised through the local replication coordinator so the
  same attachment-bearing revisions are transferred to a second bundle.

  Section §33 step 25 is the release-gate command sequence and is intentionally
  run by the integrator, not asserted as one ExUnit test.
  """

  use ExUnit.Case, async: false

  alias ElixirDB.Query.BookmarkCodec
  alias ElixirDB.Replication
  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.TestServer

  @tag :slow
  test "Plan §33 extended query integration scenario" do
    server = TestServer.start_supervised!()
    root = ElixirDB.Config.database_root()
    prefix = "wave5-query-#{System.unique_integer([:positive])}"
    source_path = prefix <> "-source.elixirdb"
    target_path = prefix <> "-target.elixirdb"

    source_uuid = create_database!(server, source_path)
    target_uuid = create_database!(server, target_path)

    on_exit(fn ->
      for uuid <- [source_uuid, target_uuid] do
        _ = DatabaseCatalog.close(uuid)
        _ = DatabaseCatalog.unregister(uuid)
      end

      for path <- [source_path, target_path] do
        ElixirDB.TempDatabase.cleanup(Path.join(root, path))
      end
    end)

    # §33.1–2 — a real bundle and a heterogeneous body fixture.
    fixture = fixture_documents()

    for {id, body} <- fixture do
      assert %{status: 201} = put_document!(server, source_uuid, id, body)
    end

    attachment_bytes = "wave-5-attachment-#{System.unique_integer([:positive])}"
    blob = upload_attachment!(server, source_uuid, attachment_bytes)

    assert %{status: 201} =
             put_document!(
               server,
               source_uuid,
               "attached",
               %{
                 "type" => "task",
                 "status" => "open",
                 "priority" => 9,
                 "title" => "attachment replication",
                 "summary" => "replication checkpoint",
                 "scan_title" => "attachment replication",
                 "attachment_meta" => %{"kind" => "report"},
                 "tags" => ["one", "attachment"],
                 "items" => [%{"status" => "open", "priority" => 9}],
                 "mod_value" => -10
               },
               %{"note.txt" => %{"blob" => blob, "content_type" => "text/plain"}}
             )

    # §33.3–4 — structured indexes and one two-field full-text index.
    index_ids = create_structured_indexes!(server, source_uuid)
    full_text_id = create_full_text_index!(server, source_uuid)
    index_bindings = index_bindings!(server, source_uuid)

    # §33.5 — equality, exact complex values, and condition disambiguation.
    assert_ids(
      query!(server, source_uuid, %{"selector" => %{"/metadata" => %{"name" => "alpha"}}}),
      ["missing-status", "open"]
    )

    assert_ids(
      query!(server, source_uuid, %{"selector" => %{"/tags" => ["one", "urgent"]}}),
      ["open"]
    )

    assert_ids(
      query!(server, source_uuid, %{
        "selector" => %{"/operator_meta" => %{"$eq" => %{"$kind" => "A"}}}
      }),
      ["open"]
    )

    # §33.6 — negative predicates distinguish missing from explicit null.
    non_closed = %{
      "selector" => %{"/status" => %{"$ne" => "closed"}},
      "limit" => 20
    }

    assert_ids(query!(server, source_uuid, non_closed), [
      "attached",
      "bool",
      "note-null",
      "open",
      "task-both",
      "unicode"
    ])

    assert_ids(
      query!(server, source_uuid, %{
        "selector" => %{"/status" => %{"$nin" => ["closed", "deleted"]}}
      }),
      ["attached", "bool", "note-null", "open", "task-both", "unicode"]
    )

    assert_ids(
      query!(server, source_uuid, %{"selector" => %{"/status" => %{"$exists" => false}}}),
      ["missing-status"]
    )

    assert_ids(
      query!(server, source_uuid, %{"selector" => %{"/status" => %{"$type" => "null"}}}),
      ["note-null"]
    )

    # §33.7 — nested AND/OR/NOT/NOR truth behavior.
    assert_ids(
      query!(server, source_uuid, %{
        "selector" => %{
          "$and" => [
            %{"$or" => [%{"/status" => "open"}, %{"/priority" => %{"$gte" => 7}}]},
            %{"$not" => %{"/status" => "closed"}}
          ]
        }
      }),
      ["attached", "open", "task-both", "unicode"]
    )

    assert_ids(
      query!(server, source_uuid, %{
        "selector" => %{"$nor" => [%{"/status" => "closed"}, %{"/priority" => 99}]}
      }),
      [
        "attached",
        "bool",
        "missing-status",
        "note-null",
        "open",
        "task-both",
        "unicode"
      ]
    )

    # §33.8 — indexed OR union uses both logical indexes and deduplicates `task-both`.
    union_request = %{
      "selector" => %{
        "$or" => [%{"/status" => "open"}, %{"/priority" => %{"$gte" => 5}}]
      },
      "limit" => 20
    }

    union_response = query!(server, source_uuid, union_request)
    assert union_response["data"]["plan_kind"] == "union"

    assert union_response["data"]["selected_indexes"] == [
             index_ids["by-status"],
             index_ids["by-priority"]
           ]

    assert union_response["data"]["index_bindings"] == [
             index_bindings[index_ids["by-status"]],
             index_bindings[index_ids["by-priority"]]
           ]

    assert_ids(union_response, ["attached", "open", "task-both", "task-high", "unicode"])

    union_explain = explain!(server, source_uuid, union_request)
    assert union_explain["data"]["plan_kind"] == "union"

    assert union_explain["data"]["selected_indexes"] ==
             union_response["data"]["selected_indexes"]

    assert [_, _] = union_explain["data"]["union_branches"]

    # §33.9 — use a separate real bundle to prove both sides of the 1000-row
    # full-scan boundary without polluting the scenario's later bounded scans.
    threshold_uuid = create_database!(server, prefix <> "-threshold.elixirdb")

    on_exit(fn ->
      _ = DatabaseCatalog.close(threshold_uuid)
      _ = DatabaseCatalog.unregister(threshold_uuid)
      ElixirDB.TempDatabase.cleanup(Path.join(root, prefix <> "-threshold.elixirdb"))
    end)

    assert %{status: 201} =
             request!(
               server,
               :post,
               "/v1/databases/#{threshold_uuid}/indexes",
               json: %{
                 "name" => "threshold-status",
                 "type" => "structured",
                 "fields" => [%{"path" => "/status", "type" => "string", "direction" => "asc"}]
               }
             )

    for number <- 1..999 do
      assert {:ok, _} =
               ElixirDB.Documents.put(threshold_uuid, %{
                 id: "threshold-#{number}",
                 body: %{"unindexed" => number}
               })
    end

    mixed_threshold_selector = %{
      "selector" => %{
        "$or" => [%{"/status" => "open"}, %{"/unindexed" => 999}]
      }
    }

    assert query!(server, threshold_uuid, mixed_threshold_selector)["data"]["plan_kind"] ==
             "bounded_scan"

    assert {:ok, _} =
             ElixirDB.Documents.put(threshold_uuid, %{
               id: "threshold-1000",
               body: %{"unindexed" => 1000}
             })

    mixed_threshold_at_limit = %{
      "selector" => %{
        "$or" => [%{"/status" => "open"}, %{"/unindexed" => 1000}]
      }
    }

    threshold_error = query_error!(server, threshold_uuid, mixed_threshold_at_limit)

    assert threshold_error["error"]["code"] == "index_required"

    # §33.10 — AND chooses one candidate source, never an index intersection.
    and_request = %{
      "selector" => %{
        "$and" => [%{"/status" => "open"}, %{"/priority" => %{"$gte" => 5}}]
      },
      "limit" => 20
    }

    and_explain = explain!(server, source_uuid, and_request)
    assert and_explain["data"]["plan_kind"] == "single"
    assert [_] = and_explain["data"]["selected_indexes"]
    assert_ids(query!(server, source_uuid, and_request), ["attached", "task-both", "unicode"])

    # §33.11 — compare indexed Unicode prefix candidates with a permitted
    # bounded-scan query over the same values.
    indexed_prefix =
      query!(server, source_uuid, %{"selector" => %{"/title" => %{"$beginsWith" => "café"}}})

    bounded_prefix =
      query!(server, source_uuid, %{
        "selector" => %{"/scan_title" => %{"$beginsWith" => "café"}}
      })

    assert_ids(indexed_prefix, ["unicode"])
    assert_ids(bounded_prefix, ["unicode"])

    # §33.12 — normal regex matching and a bounded pathological match.
    assert_ids(
      query!(server, source_uuid, %{"selector" => %{"/title" => %{"$regex" => "^replication"}}}),
      ["open", "task-high"]
    )

    regex_error =
      query_error!(server, source_uuid, %{
        "selector" => %{"/text" => %{"$regex" => "(a+)+$"}}
      })

    assert regex_error["error"]["code"] == "resource_limit"

    # §33.13 — array predicates stay post-filter-only.
    array_explain =
      explain!(server, source_uuid, %{
        "selector" => %{"/items" => %{"$elemMatch" => %{"/status" => "open"}}}
      })

    assert array_explain["data"]["plan_kind"] == "bounded_scan"
    assert array_explain["data"]["selected_indexes"] == []
    assert array_explain["data"]["post_filter_predicates"] != []

    assert_ids(
      query!(server, source_uuid, %{"selector" => %{"/tags" => %{"$all" => ["one", "urgent"]}}}),
      ["open"]
    )

    assert_ids(
      query!(server, source_uuid, %{
        "selector" => %{"$or" => [%{"/items" => %{"$elemMatch" => %{"/status" => "open"}}}]}
      }),
      ["attached", "open", "task-both", "unicode"]
    )

    assert_ids(
      query!(server, source_uuid, %{"selector" => %{"/tags" => %{"$size" => 0}}}),
      ["note-null"]
    )

    # §33.14 — modulo uses exact integral arithmetic, including a negative value.
    assert_ids(
      query!(server, source_uuid, %{"selector" => %{"/mod_value" => %{"$mod" => [3, -1]}}}),
      ["attached", "unicode"]
    )

    assert_ids(
      query!(server, source_uuid, %{"selector" => %{"/mod_value" => %{"$mod" => [2, 0]}}}),
      ["attached", "note-null", "open"]
    )

    # §33.15 — all, any, phrase, prefix, and structured post-filtering on FTS.
    assert_ids(
      query!(server, source_uuid, %{
        "search" => %{"index" => "body-text", "text" => "replication checkpoint", "mode" => "all"}
      }),
      ["attached", "open", "task-both", "task-high"]
    )

    assert_ids(
      query!(server, source_uuid, %{
        "search" => %{"index" => "body-text", "text" => "replication absent", "mode" => "any"}
      }),
      ["attached", "bool", "open", "task-both", "task-high"]
    )

    assert_ids(
      query!(server, source_uuid, %{
        "search" => %{
          "index" => "body-text",
          "text" => "replication checkpoint",
          "mode" => "phrase"
        }
      }),
      ["attached", "open", "task-both"]
    )

    assert_ids(
      query!(server, source_uuid, %{
        "search" => %{"index" => "body-text", "text" => "replic checkp", "mode" => "prefix"}
      }),
      ["attached", "open", "task-both", "task-high"]
    )

    assert_ids(
      query!(server, source_uuid, %{
        "search" => %{"index" => "body-text", "text" => "replic checkp", "mode" => "prefix"},
        "selector" => %{"/status" => "open"}
      }),
      ["attached", "open", "task-both"]
    )

    # §33.16–17 — page an indexed union and assert its complete plan binding.
    first_page = query!(server, source_uuid, Map.put(union_request, "limit", 1))
    first_data = first_page["data"]
    bookmark = first_data["bookmark"]
    assert is_binary(bookmark)
    assert first_data["selected_indexes"] == union_response["data"]["selected_indexes"]
    assert first_data["plan_digest"] == union_response["data"]["plan_digest"]
    assert {:ok, decoded} = BookmarkCodec.decode(bookmark)
    assert decoded.plan_digest == first_data["plan_digest"]
    assert decoded.index_bindings == first_data["index_bindings"]

    assert decoded.index_bindings == [
             index_bindings[index_ids["by-status"]],
             index_bindings[index_ids["by-priority"]]
           ]

    {paged_ids, _last_data} =
      collect_pages(server, source_uuid, union_request, document_ids(first_page), bookmark)

    assert Enum.uniq(paged_ids) == paged_ids
    assert MapSet.new(paged_ids) == MapSet.new(union_ids(union_response))

    # §33.18 — any document mutation makes the old continuation stale.
    assert %{status: 201} =
             put_document!(server, source_uuid, "bookmark-mutation", %{
               "type" => "note",
               "title" => "bookmark mutation"
             })

    stale_error = query_error!(server, source_uuid, Map.put(union_request, "bookmark", bookmark))
    assert stale_error["error"]["code"] == "bookmark_stale"

    # §33.19 — index deletion changes the plan without changing document
    # sequence and therefore invalidates a new union continuation.
    second_page = query!(server, source_uuid, Map.put(union_request, "limit", 1))
    second_bookmark = second_page["data"]["bookmark"]
    assert is_binary(second_bookmark)

    assert %{status: 200} =
             request!(
               server,
               :delete,
               "/v1/databases/#{source_uuid}/indexes/#{index_ids["by-status"]}"
             )

    deleted_index_error =
      query_error!(server, source_uuid, Map.put(union_request, "bookmark", second_bookmark))

    assert deleted_index_error["error"]["code"] == "invalid_bookmark"

    # §33.20 — rebuilding an unchanged logical definition preserves its plan
    # binding and continuation behavior.
    rebuild_request = %{"selector" => %{"/priority" => %{"$gte" => 5}}, "limit" => 1}
    rebuild_page = query!(server, source_uuid, rebuild_request)
    rebuild_bookmark = rebuild_page["data"]["bookmark"]
    assert is_binary(rebuild_bookmark)

    assert %{status: 200} =
             request!(
               server,
               :post,
               "/v1/databases/#{source_uuid}/indexes/#{index_ids["by-priority"]}/rebuild",
               json: %{}
             )

    rebuilt_continuation =
      query!(server, source_uuid, Map.put(rebuild_request, "bookmark", rebuild_bookmark))

    assert rebuilt_continuation["data"]["documents"] != []

    # §33.21 — explain remains storage-neutral.
    explain_data = explain!(server, source_uuid, and_request)["data"]
    explain_json = JSON.encode!(explain_data)
    refute explain_json =~ "SELECT"
    refute explain_json =~ "physical_name"
    refute explain_json =~ "sqlite_private"
    refute explain_json =~ "compiled"

    # §33.22 — replicate revisions and attachment bytes, then create only
    # target-local equivalent indexes.
    assert {:ok, %{status: :completed}} = Replication.one_shot(source_uuid, target_uuid)
    assert {:ok, target_attachment} = ElixirDB.Documents.get(target_uuid, %{id: "attached"})
    assert target_attachment.attachments["note.txt"].digest == blob

    assert %{status: 200, body: %{"data" => []}} =
             request!(server, :get, "/v1/databases/#{target_uuid}/indexes")

    assert %{status: 200, body: target_attachment_bytes} =
             request!(
               server,
               :post,
               "/v1/databases/#{target_uuid}/attachments/get",
               json: %{"id" => "attached", "revision" => nil, "name" => "note.txt"},
               decode_body: false
             )

    assert target_attachment_bytes == attachment_bytes

    create_structured_indexes!(server, target_uuid)
    create_full_text_index!(server, target_uuid)

    source_ids =
      query!(server, source_uuid, %{"selector" => %{"/type" => "task"}, "limit" => 20})
      |> document_ids()

    target_ids =
      query!(server, target_uuid, %{"selector" => %{"/type" => "task"}, "limit" => 20})
      |> document_ids()

    assert MapSet.new(source_ids) == MapSet.new(target_ids)

    # §33.23 — close, unregister, re-register, and verify stable query planning.
    assert :ok = DatabaseCatalog.close(source_uuid)
    assert :ok = DatabaseCatalog.unregister(source_uuid)
    assert {:ok, restored} = DatabaseCatalog.register(source_path)
    assert restored.database_uuid == source_uuid

    restored_explain = explain!(server, source_uuid, rebuild_request)["data"]
    assert restored_explain["plan_kind"] == "single"
    assert restored_explain["selected_indexes"] == [index_ids["by-priority"]]

    restored_query = query!(server, source_uuid, rebuild_request)
    assert document_ids(restored_query) == ["attached"]

    restored_continuation =
      query!(server, source_uuid, Map.put(rebuild_request, "bookmark", rebuild_bookmark))

    assert document_ids(restored_continuation) == document_ids(rebuilt_continuation)

    # §33.24 — integrity, attachment, retention, and query checks coexist.
    assert %{status: 200, body: %{"data" => %{"ok" => true}}} =
             request!(server, :post, "/v1/databases/#{source_uuid}/integrity-check", json: %{})

    assert %{status: 200, body: downloaded} =
             request!(
               server,
               :post,
               "/v1/databases/#{source_uuid}/attachments/get",
               json: %{"id" => "attached", "revision" => nil, "name" => "note.txt"},
               decode_body: false
             )

    assert downloaded == attachment_bytes

    assert {:ok, retention} =
             DatabaseCatalog.command(source_uuid, {:command, :retention_status, %{}})

    assert %{floor_sequence: floor_sequence} = retention
    assert is_integer(floor_sequence)
    assert %{status: 200} = query!(server, source_uuid, rebuild_request) |> response_status()

    # §33.25 — final gates are run as shell commands after this scenario.
    assert is_binary(full_text_id)
  end

  defp fixture_documents do
    [
      {"open",
       %{
         "type" => "task",
         "status" => "open",
         "priority" => 3,
         "title" => "replication checkpoint",
         "summary" => "queue recovery",
         "scan_title" => "replication checkpoint",
         "metadata" => %{"name" => "alpha"},
         "operator_meta" => %{"$kind" => "A"},
         "tags" => ["one", "urgent"],
         "items" => [%{"status" => "open", "priority" => 3}],
         "mod_value" => 4
       }},
      {"task-high",
       %{
         "type" => "task",
         "status" => "closed",
         "priority" => 5,
         "title" => "replication guide",
         "summary" => "checkpoint queue",
         "scan_title" => "replication guide",
         "metadata" => %{"name" => "gamma"},
         "tags" => ["two"],
         "items" => [%{"status" => "closed", "priority" => 5}],
         "mod_value" => 5
       }},
      {"task-both",
       %{
         "type" => "task",
         "status" => "open",
         "priority" => 5,
         "title" => "unicode café 🧪",
         "summary" => "replication checkpoint",
         "scan_title" => "unicode café 🧪",
         "metadata" => %{"name" => "delta"},
         "tags" => ["one", "two"],
         "items" => [%{"status" => "open", "priority" => 5}],
         "mod_value" => 7
       }},
      {"note-null",
       %{
         "type" => "note",
         "status" => nil,
         "priority" => 0,
         "title" => "null record",
         "summary" => "plain note",
         "scan_title" => "null record",
         "metadata" => %{"name" => "beta"},
         "tags" => [],
         "items" => [],
         "mod_value" => 4
       }},
      {"missing-status",
       %{
         "type" => "task",
         "priority" => 1,
         "title" => "missing field",
         "summary" => "unrelated",
         "scan_title" => "missing field",
         "metadata" => %{"name" => "alpha"},
         "tags" => ["one"],
         "items" => [%{"status" => "closed"}],
         "mod_value" => 1
       }},
      {"bool",
       %{
         "type" => "flag",
         "status" => "queued",
         "enabled" => true,
         "priority" => 2,
         "title" => "queue switch",
         "summary" => "replication notes",
         "scan_title" => "queue switch",
         "metadata" => %{"name" => "epsilon"},
         "tags" => ["three"],
         "items" => ["open", %{"status" => "closed"}],
         "mod_value" => 1
       }},
      {"unicode",
       %{
         "type" => "task",
         "status" => "open",
         "priority" => 7,
         "title" => "café 猫 🧪",
         "summary" => "checkpoint analysis",
         "scan_title" => "café 猫 🧪",
         "metadata" => %{"name" => "zeta"},
         "tags" => ["one"],
         "items" => [%{"status" => "open", "priority" => 7}],
         "mod_value" => -7,
         "text" => String.duplicate("a", 1_000) <> "!"
       }}
    ]
  end

  defp create_database!(server, path) do
    response = request!(server, :post, "/v1/databases", json: %{"path" => path})
    assert response.status == 201
    response.body["data"]["database_uuid"]
  end

  defp put_document!(server, uuid, id, body, attachments \\ nil) do
    payload = %{"id" => id, "body" => body}
    payload = if attachments, do: Map.put(payload, "attachments", attachments), else: payload
    request!(server, :post, "/v1/databases/#{uuid}/documents/put", json: payload)
  end

  defp upload_attachment!(server, uuid, bytes) do
    response =
      request!(
        server,
        :post,
        "/v1/databases/#{uuid}/attachments/upload",
        body: bytes,
        headers: [{"content-type", "application/octet-stream"}]
      )

    assert response.status == 201
    response.body["data"]["blob"]
  end

  defp create_structured_indexes!(server, uuid) do
    definitions = [
      {"by-type", [%{"path" => "/type", "type" => "string", "direction" => "asc"}]},
      {"by-status", [%{"path" => "/status", "type" => "string", "direction" => "asc"}]},
      {"by-priority", [%{"path" => "/priority", "type" => "number", "direction" => "asc"}]},
      {"by-title", [%{"path" => "/title", "type" => "string", "direction" => "asc"}]},
      {"by-type-status-priority",
       [
         %{"path" => "/type", "type" => "string", "direction" => "asc"},
         %{"path" => "/status", "type" => "string", "direction" => "asc"},
         %{"path" => "/priority", "type" => "number", "direction" => "asc"}
       ]}
    ]

    for {name, fields} <- definitions, into: %{} do
      response =
        request!(
          server,
          :post,
          "/v1/databases/#{uuid}/indexes",
          json: %{"name" => name, "type" => "structured", "fields" => fields}
        )

      assert response.status == 201
      {name, response.body["data"]["index_id"]}
    end
  end

  defp create_full_text_index!(server, uuid) do
    response =
      request!(
        server,
        :post,
        "/v1/databases/#{uuid}/indexes",
        json: %{
          "name" => "body-text",
          "type" => "full_text",
          "fields" => ["/title", "/summary"],
          "tokenization" => %{"strategy" => "unicode_words_v1", "diacritics" => "preserve"}
        }
      )

    assert response.status == 201
    response.body["data"]["index_id"]
  end

  defp index_bindings!(server, uuid) do
    response = request!(server, :get, "/v1/databases/#{uuid}/indexes")
    assert response.status == 200

    Map.new(response.body["data"], fn index ->
      binding = Map.take(index, ["index_id", "definition_digest"])
      {index["index_id"], binding}
    end)
  end

  defp query!(server, uuid, request) do
    response = request!(server, :post, "/v1/databases/#{uuid}/query", json: request)
    assert response.status == 200
    response.body
  end

  defp query_error!(server, uuid, request) do
    response = request!(server, :post, "/v1/databases/#{uuid}/query", json: request)
    assert response.status in [400, 409, 422, 429]
    response.body
  end

  defp explain!(server, uuid, request) do
    response = request!(server, :post, "/v1/databases/#{uuid}/query/explain", json: request)
    assert response.status == 200
    response.body
  end

  defp assert_ids(response, expected) do
    assert document_ids(response) == Enum.sort(expected)
  end

  defp document_ids(%{"data" => %{"documents" => documents}}),
    do: documents |> Enum.map(& &1["id"]) |> Enum.sort()

  defp union_ids(response), do: document_ids(response)

  defp collect_pages(server, uuid, request, ids, bookmark) do
    next_request = request |> Map.put("bookmark", bookmark) |> Map.put("limit", 1)
    response = query!(server, uuid, next_request)
    page_ids = ids ++ document_ids(response)

    case response["data"]["bookmark"] do
      next when is_binary(next) -> collect_pages(server, uuid, request, page_ids, next)
      _ -> {page_ids, response["data"]}
    end
  end

  defp request!(server, method, path, options \\ []) do
    {:ok, response} =
      Req.request(
        Keyword.merge([method: method, url: server.base_url <> path, retry: false], options)
      )

    response
  end

  defp response_status(%{"data" => _}), do: %{status: 200}
end
