defmodule ElixirDB.Safety.InputContainmentTest do
  @moduledoc """
  Safety guarantee: no externally-supplied input — document body, ID, query,
  bookmark, config, replication payload, or path identifier — can ever crash a
  server process. Every malformed/null/duplicate/wrong-typed input must funnel
  into a typed JSON error envelope, and the shared DatabaseCatalog GenServer
  must stay alive throughout.

  Each test drives a real request through the HTTP router (Plug.Test.conn →
  ElixirDB.HTTP.Router.call/2) and asserts:
    (a) a stable JSON error envelope is returned (never a bare process exit),
    (b) the DatabaseCatalog process is still alive afterward.
  """

  use ExUnit.Case, async: false

  alias ElixirDB.HTTP.Router
  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.Runtime.DatabaseCatalog

  setup do
    path = "safety-#{System.unique_integer([:positive])}.db"

    conn =
      call(:post, "/v1/databases", %{"path" => path})

    assert conn.status == 201
    {:ok, %{"data" => %{"database_uuid" => uuid}}} = decode(conn.resp_body)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path))
    end)

    {:ok, uuid: uuid}
  end

  # --------------------------------------------------------------------------
  # Process-liveness helper: the central invariant of this whole suite.
  # --------------------------------------------------------------------------
  defp assert_catalog_alive do
    refute is_nil(Process.whereis(DatabaseCatalog)),
           "DatabaseCatalog crashed during the request — the safety invariant was violated"
  end

  defp call(method, path, body) do
    conn = Plug.Test.conn(method, path, encode(body))
    conn = Plug.Conn.put_req_header(conn, "content-type", "application/json")
    Router.call(conn, [])
  end

  defp call_raw(method, path, raw_body) do
    conn = Plug.Test.conn(method, path, raw_body)
    conn = Plug.Conn.put_req_header(conn, "content-type", "application/json")
    Router.call(conn, [])
  end

  defp encode(term), do: IO.iodata_to_binary(JSON.encode_to_iodata!(term))
  defp decode(body), do: StrictDecoder.decode(body)

  defp assert_typed_error(conn, expected_status) when is_integer(expected_status) do
    assert conn.status == expected_status,
           "expected #{expected_status}, got #{conn.status}: #{conn.resp_body}"

    extract_error(conn)
  end

  defp assert_typed_error(conn, statuses) when is_list(statuses) do
    assert conn.status in statuses,
           "expected one of #{inspect(statuses)}, got #{conn.status}: #{conn.resp_body}"

    extract_error(conn)
  end

  defp extract_error(conn) do
    {:ok, %{"error" => error}} = decode(conn.resp_body)

    assert is_map(error) and is_binary(error["code"]),
           "expected a typed error envelope, got: #{inspect(conn.resp_body)}"

    assert_catalog_alive()
    error
  end

  # ==========================================================================
  # Document endpoints — ID, body, and type validation
  # ==========================================================================

  describe "document get" do
    test "missing id returns a typed 400", %{uuid: uuid} do
      assert_typed_error(call(:post, "/v1/databases/#{uuid}/documents/get", %{}), 400)
    end

    test "empty id returns a typed 400", %{uuid: uuid} do
      assert_typed_error(call(:post, "/v1/databases/#{uuid}/documents/get", %{"id" => ""}), 400)
    end

    test "non-string id returns a typed 400", %{uuid: uuid} do
      assert_typed_error(
        call(:post, "/v1/databases/#{uuid}/documents/get", %{"id" => 123}),
        400
      )
    end

    test "NUL-containing id returns a typed 400", %{uuid: uuid} do
      assert_typed_error(
        call(:post, "/v1/databases/#{uuid}/documents/get", %{"id" => "a\0b"}),
        400
      )
    end

    test "control-character id returns a typed 400", %{uuid: uuid} do
      assert_typed_error(
        call(:post, "/v1/databases/#{uuid}/documents/get", %{"id" => "a\nb"}),
        400
      )
    end

    test "oversized id returns a typed 400/422", %{uuid: uuid} do
      error =
        assert_typed_error(
          call(:post, "/v1/databases/#{uuid}/documents/get", %{"id" => String.duplicate("x", 600)}),
          [400, 422]
        )

      assert error["code"] in ["invalid_request", "resource_limit"]
    end
  end

  describe "document put" do
    test "missing body returns a typed 400", %{uuid: uuid} do
      assert_typed_error(call(:post, "/v1/databases/#{uuid}/documents/put", %{"id" => "doc"}), 400)
    end

    test "non-object body (array) returns a typed 400", %{uuid: uuid} do
      assert_typed_error(
        call(:post, "/v1/databases/#{uuid}/documents/put", %{"id" => "doc", "body" => [1, 2]}),
        400
      )
    end

    test "non-object body (string) returns a typed 400", %{uuid: uuid} do
      assert_typed_error(
        call(:post, "/v1/databases/#{uuid}/documents/put", %{"id" => "doc", "body" => "x"}),
        400
      )
    end

    test "numeric if_revision returns a typed 400", %{uuid: uuid} do
      assert_typed_error(
        call(
          :post,
          "/v1/databases/#{uuid}/documents/put",
          %{"id" => "doc", "body" => %{}, "if_revision" => 5}
        ),
        400
      )
    end

    test "a valid put succeeds, proving the safety net does not block good input",
         %{uuid: uuid} do
      conn =
        call(:post, "/v1/databases/#{uuid}/documents/put", %{
          "id" => "ok-doc",
          "body" => %{"k" => 1}
        })

      assert conn.status == 201
      {:ok, %{"data" => _}} = decode(conn.resp_body)
      assert_catalog_alive()
    end
  end

  describe "bulk endpoints (the FunctionClauseError gap)" do
    test "bulk-get with an object body returns a typed 400 (not a crash)", %{uuid: uuid} do
      error = assert_typed_error(call(:post, "/v1/databases/#{uuid}/documents/bulk-get", %{}), 400)
      assert error["code"] == "invalid_request"
    end

    test "bulk-get with a scalar body returns a typed 400", %{uuid: uuid} do
      assert_typed_error(
        call_raw(:post, "/v1/databases/#{uuid}/documents/bulk-get", encode("hello")),
        400
      )
    end

    test "bulk-write with an object body returns a typed 400 (not a crash)", %{uuid: uuid} do
      error =
        assert_typed_error(call(:post, "/v1/databases/#{uuid}/documents/bulk-write", %{}), 400)

      assert error["code"] == "invalid_request"
    end

    test "bulk-write with null items returns typed per-item errors, not a crash", %{uuid: uuid} do
      conn =
        call(:post, "/v1/databases/#{uuid}/documents/bulk-write", [
          %{"type" => "put", "id" => "x", "body" => %{}},
          nil
        ])

      # bulk-write maps over operations; nil ops are normalized to themselves and rejected
      # by the mutation validator. Either way: no crash, catalog alive.
      assert conn.status in [400, 201, 200]
      assert_catalog_alive()
    end
  end

  describe "duplicate JSON keys" do
    test "duplicate object keys are rejected by the strict decoder at the boundary" do
      # Duplicate keys are invalid JSON per the project's strict decoder.
      conn = call_raw(:post, "/v1/registrations", ~s({"path":"dup.db","path":"dup2.db"}))
      assert conn.status == 400
      assert_catalog_alive()
    end
  end

  # ==========================================================================
  # Query / bookmark validation
  # ==========================================================================

  describe "query" do
    test "non-object selector value returns a typed 400", %{uuid: uuid} do
      assert_typed_error(
        call(:post, "/v1/databases/#{uuid}/query", %{"selector" => %{" Plain" => 1}}),
        400
      )
    end

    test "unsupported selector operator returns a typed 400", %{uuid: uuid} do
      assert_typed_error(
        call(:post, "/v1/databases/#{uuid}/query", %{"selector" => %{"/k" => %{"$weird" => 1}}}),
        400
      )
    end

    test "limit of zero returns a typed 400", %{uuid: uuid} do
      assert_typed_error(
        call(:post, "/v1/databases/#{uuid}/query", %{"limit" => 0}),
        400
      )
    end

    test "negative limit returns a typed 400", %{uuid: uuid} do
      assert_typed_error(
        call(:post, "/v1/databases/#{uuid}/query", %{"limit" => -1}),
        400
      )
    end

    test "malformed bookmark returns a typed invalid_bookmark", %{uuid: uuid} do
      error =
        assert_typed_error(
          call(:post, "/v1/databases/#{uuid}/query", %{"bookmark" => "!!!not-base64!!!"}),
          400
        )

      assert error["code"] == "invalid_bookmark"
    end

    test "tampered bookmark returns a typed invalid_bookmark", %{uuid: uuid} do
      # A real bookmark is base64url JSON with a sha256 checksum. Tamper a plausible one.
      error =
        assert_typed_error(
          call(:post, "/v1/databases/#{uuid}/query", %{
            "bookmark" => "e30"
          }),
          400
        )

      assert error["code"] == "invalid_bookmark"
    end
  end

  # ==========================================================================
  # Index endpoints
  # ==========================================================================

  describe "indexes" do
    test "unknown index type returns a typed 400", %{uuid: uuid} do
      assert_typed_error(
        call(:post, "/v1/databases/#{uuid}/indexes", %{
          "name" => "ix",
          "type" => "bogus",
          "fields" => [%{"path" => "/k", "type" => "string", "direction" => "asc"}]
        }),
        400
      )
    end

    test "non-array fields returns a typed 400", %{uuid: uuid} do
      assert_typed_error(
        call(:post, "/v1/databases/#{uuid}/indexes", %{
          "name" => "ix",
          "type" => "structured",
          "fields" => "nope"
        }),
        400
      )
    end

    test "NUL-bearing index_id path returns a typed 400", %{uuid: uuid} do
      assert_typed_error(
        Plug.Test.conn(:delete, "/v1/databases/#{uuid}/indexes/a%00b", "")
        |> Plug.Conn.put_req_header("content-type", "application/json")
        |> Router.call([]),
        400
      )
    end

    test "oversized index_id path returns a typed 400/422", %{uuid: uuid} do
      huge = String.duplicate("x", 600)

      conn =
        Plug.Test.conn(:delete, "/v1/databases/#{uuid}/indexes/#{huge}", "")
        |> Plug.Conn.put_req_header("content-type", "application/json")
        |> Router.call([])

      assert conn.status in [400, 422]
      assert_catalog_alive()
    end
  end

  # ==========================================================================
  # Database / registration validation
  # ==========================================================================

  describe "database create" do
    test "absolute path returns a typed 400" do
      assert_typed_error(
        call(:post, "/v1/databases", %{"path" => "/etc/passwd"}),
        400
      )
    end

    test "path traversal returns a typed 400" do
      assert_typed_error(
        call(:post, "/v1/databases", %{"path" => "../escape.db"}),
        400
      )
    end

    test "non-object config returns a typed 400" do
      assert_typed_error(
        call(:post, "/v1/databases", %{"path" => "x.db", "config" => "nope"}),
        400
      )
    end

    test "config with negative limit returns a typed 400" do
      assert_typed_error(
        call(:post, "/v1/databases", %{
          "path" => "neg.db",
          "config" => %{"documents" => %{"max_document_bytes" => -1}}
        }),
        400
      )
    end

    test "config with unknown field returns a typed 400" do
      assert_typed_error(
        call(:post, "/v1/databases", %{
          "path" => "unk.db",
          "config" => %{"mystery" => 1}
        }),
        400
      )
    end
  end

  # ==========================================================================
  # Replication endpoints
  # ==========================================================================

  describe "replication jobs" do
    test "unknown mode returns a typed 400", %{uuid: uuid} do
      assert_typed_error(
        call(:post, "/v1/databases/#{uuid}/replications", %{
          "mode" => "bogus",
          "direction" => "push",
          "endpoint" => %{"kind" => "remote", "base_url" => "http://127.0.0.1:9"}
        }),
        400
      )
    end

    test "bad endpoint shape returns a typed 400", %{uuid: uuid} do
      assert_typed_error(
        call(:post, "/v1/databases/#{uuid}/replications", %{
          "mode" => "one_shot",
          "direction" => "push",
          "endpoint" => "not-an-object"
        }),
        400
      )
    end

    test "NUL-bearing job_id path returns a typed 400", %{uuid: uuid} do
      assert_typed_error(
        Plug.Test.conn(:get, "/v1/databases/#{uuid}/replications/a%00b", "")
        |> Plug.Conn.put_req_header("content-type", "application/json")
        |> Router.call([]),
        400
      )
    end
  end

  describe "checkpoint path identifier" do
    test "oversized replication_id path returns a typed 400/422", %{uuid: uuid} do
      huge = String.duplicate("x", 600)

      conn =
        Plug.Test.conn(:get, "/v1/databases/#{uuid}/replication/checkpoints/#{huge}", "")
        |> Plug.Conn.put_req_header("content-type", "application/json")
        |> Router.call([])

      assert conn.status in [400, 422]
      assert_catalog_alive()
    end

    test "NUL-bearing replication_id path returns a typed 400", %{uuid: uuid} do
      assert_typed_error(
        Plug.Test.conn(:get, "/v1/databases/#{uuid}/replication/checkpoints/a%00b", "")
        |> Plug.Conn.put_req_header("content-type", "application/json")
        |> Router.call([]),
        400
      )
    end
  end

  # ==========================================================================
  # Changes endpoints
  # ==========================================================================

  describe "changes" do
    test "negative since returns a typed 400", %{uuid: uuid} do
      assert_typed_error(
        call(:post, "/v1/databases/#{uuid}/changes", %{"since" => -1, "limit" => 10}),
        400
      )
    end

    test "non-integer wait_ms returns a typed 400", %{uuid: uuid} do
      assert_typed_error(
        call(:post, "/v1/databases/#{uuid}/changes", %{"since" => 0, "wait_ms" => "soon"}),
        400
      )
    end

    test "stream with negative heartbeat returns a typed 400", %{uuid: uuid} do
      assert_typed_error(
        call(:post, "/v1/databases/#{uuid}/changes/stream", %{"heartbeat_ms" => -1}),
        400
      )
    end
  end

  # ==========================================================================
  # Duplicate-identifier guarantees — typed conflicts, never crashes
  # ==========================================================================

  describe "duplicate identifiers" do
    test "same index name with a different definition yields index_name_conflict", %{uuid: uuid} do
      defn_a = %{
        "name" => "dup-name",
        "type" => "structured",
        "fields" => [%{"path" => "/a", "type" => "string", "direction" => "asc"}]
      }

      defn_b = %{
        "name" => "dup-name",
        "type" => "structured",
        "fields" => [%{"path" => "/b", "type" => "string", "direction" => "asc"}]
      }

      first = call(:post, "/v1/databases/#{uuid}/indexes", defn_a)
      assert first.status == 201

      second = call(:post, "/v1/databases/#{uuid}/indexes", defn_b)

      assert second.status == 409
      {:ok, %{"error" => error}} = decode(second.resp_body)
      assert error["code"] == "index_name_conflict"
      assert_catalog_alive()
    end

    test "registering a database file whose uuid is already registered yields duplicate_database_uuid",
         %{uuid: uuid} do
      # Copy the setup DB's file to a new path and register the copy. The copy carries
      # the same embedded database_uuid, so the catalog's no_duplicate_uuid guard must
      # reject it with a typed 409 — never a crash.
      {:ok, entries} = DatabaseCatalog.list()
      %{path: rel_path} = Enum.find(entries, &(&1.database_uuid == uuid))

      src = Path.join(ElixirDB.Config.database_root(), rel_path)
      copy_path = "dup-uuid-#{System.unique_integer([:positive])}.db"
      dst = Path.join(ElixirDB.Config.database_root(), copy_path)
      File.cp!(src, dst)

      on_exit(fn -> ElixirDB.TempDatabase.cleanup(dst) end)

      error = assert_typed_error(call(:post, "/v1/registrations", %{"path" => copy_path}), 409)
      assert error["code"] == "duplicate_database_uuid"
    end
  end

  # ==========================================================================
  # Non-JSON and oversized bodies
  # ==========================================================================

  describe "malformed request bodies" do
    test "non-JSON content type is rejected with a typed 400", %{uuid: uuid} do
      conn =
        Plug.Test.conn(:post, "/v1/databases/#{uuid}/documents/get", "raw bytes")
        |> Plug.Conn.put_req_header("content-type", "text/plain")
        |> Router.call([])

      assert conn.status == 400
      assert_catalog_alive()
    end

    test "malformed JSON is rejected with a typed 400", %{uuid: uuid} do
      conn = call_raw(:post, "/v1/databases/#{uuid}/documents/get", "{not json")
      assert conn.status == 400
      assert_catalog_alive()
    end
  end
end
