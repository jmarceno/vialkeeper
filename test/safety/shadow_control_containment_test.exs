defmodule ElixirDB.Safety.ShadowControlContainmentTest do
  @moduledoc """
  Exercises malformed shadow control-plane requests through the real HTTP router.

  Every rejected request must produce a typed JSON error without restarting the
  shared database catalog or shadow supervision boundary.
  """

  use ExUnit.Case, async: false

  @moduletag :integration

  @control_token "shadow-control-containment-token"
  alias ElixirDB.HTTP.Router
  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.Runtime.DatabaseCatalog

  @identity_fields ~w(source_uuid shadow_uuid generation operation_id)
  @read_paths ~w(reads/document reads/documents/bulk reads/attachment)

  setup do
    previous_shadow_worker = Application.get_env(:elixir_db, :shadow_worker)
    configure_control_auth(previous_shadow_worker)

    path = "shadow-safety-#{System.unique_integer([:positive])}.elixirdb"
    conn = public_json_request(:post, "/v1/databases", %{"path" => path})

    assert conn.status == 201
    {:ok, %{"data" => %{"database_uuid" => source_uuid}}} = decode(conn.resp_body)

    process_pids = %{
      catalog: Process.whereis(DatabaseCatalog),
      shadow_supervisor: Process.whereis(ElixirDB.Shadow.Supervisor)
    }

    assert Enum.all?(process_pids, fn {_name, pid} -> is_pid(pid) and Process.alive?(pid) end)

    on_exit(fn ->
      restore_shadow_worker(previous_shadow_worker)
      _ = DatabaseCatalog.close(source_uuid)
      _ = DatabaseCatalog.unregister(source_uuid)
      ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path))
    end)

    {:ok, process_pids: process_pids, source_uuid: source_uuid}
  end

  test "generation routes reject non-UUID source path segments", context do
    for method <- [:put, :get, :delete] do
      path = generation_path("not-a-uuid", 1)
      conn = generation_request(method, path, provision_body("not-a-uuid", 1))

      assert_typed_4xx(conn, context.process_pids)
    end
  end

  test "generation routes reject malformed, negative, and oversized generations", context do
    oversized = Integer.pow(2, 70)

    for generation <- ["abc", -1, oversized],
        method <- [:put, :get, :delete] do
      path = generation_path(context.source_uuid, generation)
      conn = generation_request(method, path, provision_body(context.source_uuid, generation))

      error = assert_typed_4xx(conn, context.process_pids)
      assert error["code"] == "invalid_request"
    end
  end

  test "provision rejects malformed JSON", context do
    conn =
      raw_request(
        :put,
        generation_path(context.source_uuid, 1),
        "{not json",
        "application/json"
      )

    assert_typed_4xx(conn, context.process_pids)
  end

  test "provision rejects unknown fields", context do
    body =
      context.source_uuid
      |> provision_body(1)
      |> Map.put("unexpected", true)

    conn = json_request(:put, generation_path(context.source_uuid, 1), body)
    error = assert_typed_4xx(conn, context.process_pids)

    assert error["code"] == "invalid_request"
  end

  test "provision rejects nil required fields", context do
    required_fields =
      @identity_fields ++
        ~w(attachment_store_type attachment_location specification_digest)

    for field <- required_fields do
      body =
        context.source_uuid
        |> provision_body(1)
        |> Map.put(field, nil)

      conn = json_request(:put, generation_path(context.source_uuid, 1), body)
      assert_typed_4xx(conn, context.process_pids)
    end
  end

  test "provision rejects the wrong content type", context do
    conn =
      raw_request(
        :put,
        generation_path(context.source_uuid, 1),
        encode(provision_body(context.source_uuid, 1)),
        "text/plain"
      )

    error = assert_typed_4xx(conn, context.process_pids)
    assert error["code"] == "invalid_request"
  end

  test "read routes reject non-object JSON bodies", context do
    for suffix <- @read_paths do
      conn =
        raw_request(
          :post,
          read_path(context.source_uuid, 1, suffix),
          encode("not-an-object"),
          "application/json"
        )

      assert_typed_4xx(conn, context.process_pids)
    end
  end

  test "read routes reject nil identity fields", context do
    for suffix <- @read_paths,
        field <- @identity_fields do
      body =
        context.source_uuid
        |> read_body(1)
        |> Map.put(field, nil)

      conn = json_request(:post, read_path(context.source_uuid, 1, suffix), body)
      assert_typed_4xx(conn, context.process_pids)
    end
  end

  test "bulk read rejects oversized nested request arrays", context do
    oversized_array =
      List.duplicate(%{"id" => "document", "revision" => nil}, max_bulk_operations() + 1)

    body =
      context.source_uuid
      |> read_body(1)
      |> Map.put("requests", oversized_array)

    conn =
      json_request(
        :post,
        read_path(context.source_uuid, 1, "reads/documents/bulk"),
        body
      )

    error = assert_typed_4xx(conn, context.process_pids)
    assert error["code"] == "resource_limit", inspect(error)
  end

  test "bulk read rejects non-object nested requests", context do
    body =
      context.source_uuid
      |> read_body(1)
      |> Map.put("requests", [%{"id" => "document"}, nil])

    conn =
      json_request(
        :post,
        read_path(context.source_uuid, 1, "reads/documents/bulk"),
        body
      )

    error = assert_typed_4xx(conn, context.process_pids)
    assert error["code"] == "invalid_request"
  end

  defp generation_request(:put, path, body), do: json_request(:put, path, body)
  defp generation_request(method, path, _body), do: wire_request(method, path, nil)

  defp provision_body(source_uuid, generation) do
    %{
      "source_uuid" => source_uuid,
      "shadow_uuid" => ElixirDB.UUID.v4(),
      "generation" => generation,
      "operation_id" => ElixirDB.UUID.v4(),
      "attachment_store_type" => "external_cas",
      "attachment_location" => "/tmp/elixirdb-shadow-safety",
      "specification_digest" => String.duplicate("a", 64)
    }
  end

  defp read_body(source_uuid, generation) do
    %{
      "source_uuid" => source_uuid,
      "shadow_uuid" => ElixirDB.UUID.v4(),
      "generation" => generation,
      "operation_id" => ElixirDB.UUID.v4(),
      "id" => "document",
      "name" => "attachment"
    }
  end

  defp generation_path(source_uuid, generation),
    do: "/v1/control-plane/shadows/#{source_uuid}/generations/#{generation}"

  defp read_path(source_uuid, generation, suffix),
    do: generation_path(source_uuid, generation) <> "/#{suffix}"

  defp json_request(method, path, body), do: wire_request(method, path, body)

  defp public_json_request(method, path, body),
    do: raw_request(method, path, encode(body), "application/json")

  defp wire_request(method, path, nil) do
    method
    |> Plug.Test.conn(path)
    |> put_headers(ElixirDB.TestReplicationWire.accept_headers())
    |> put_control_auth()
    |> Router.call([])
  end

  defp wire_request(method, path, body) do
    encoded = ElixirDB.TestReplicationWire.encode!(body)

    method
    |> Plug.Test.conn(path, encoded.body)
    |> put_headers(ElixirDB.TestReplicationWire.json_headers(encoded))
    |> put_control_auth()
    |> Router.call([])
  end

  defp raw_request(method, path, body, content_type) do
    method
    |> Plug.Test.conn(path, body)
    |> Plug.Conn.put_req_header("content-type", content_type)
    |> put_control_auth()
    |> Router.call([])
  end

  defp put_headers(conn, headers) do
    Enum.reduce(headers, conn, fn {name, value}, acc ->
      Plug.Conn.put_req_header(acc, name, value)
    end)
  end

  defp put_control_auth(conn),
    do: Plug.Conn.put_req_header(conn, "authorization", "Bearer #{@control_token}")

  defp assert_typed_4xx(conn, process_pids) do
    assert conn.status in 400..499,
           "expected a 4xx response, got #{conn.status}: #{inspect(response_payload(conn))}"

    assert [content_type] = Plug.Conn.get_resp_header(conn, "content-type")
    assert String.starts_with?(content_type, "application/json")

    assert {:ok, %{"request_id" => request_id, "error" => error}} =
             conn
             |> response_payload()
             |> decode_payload()

    assert is_binary(request_id) and request_id != ""
    assert is_map(error) and is_binary(error["code"])
    assert is_binary(error["message"])
    assert is_boolean(error["retryable"])

    assert_processes_unchanged(process_pids)
    error
  end

  defp assert_processes_unchanged(process_pids) do
    for {name, expected_pid} <- process_pids do
      actual_pid = Process.whereis(process_name(name))

      assert actual_pid == expected_pid,
             "#{name} restarted during a malformed shadow control request"

      assert Process.alive?(actual_pid),
             "#{name} exited during a malformed shadow control request"
    end
  end

  defp process_name(:catalog), do: DatabaseCatalog
  defp process_name(:shadow_supervisor), do: ElixirDB.Shadow.Supervisor

  defp configure_control_auth(previous) do
    digest = :crypto.hash(:sha256, @control_token) |> Base.encode16(case: :lower)

    next =
      previous
      |> Kernel.||([])
      |> Keyword.put(:control_token_digests, [digest])

    Application.put_env(:elixir_db, :shadow_worker, next)
  end

  defp restore_shadow_worker(nil), do: Application.delete_env(:elixir_db, :shadow_worker)

  defp restore_shadow_worker(previous),
    do: Application.put_env(:elixir_db, :shadow_worker, previous)

  defp max_bulk_operations,
    do: ElixirDB.Config.host_limits()[:max_bulk_operations] || 500

  defp response_payload(conn) do
    encoding = conn |> Plug.Conn.get_resp_header("content-encoding") |> List.first()

    if is_binary(encoding) and String.contains?(encoding, "zstd") do
      ElixirDB.TestReplicationWire.decode_response(conn.resp_headers, conn.resp_body)
    else
      conn.resp_body
    end
  end

  defp decode_payload(payload) when is_map(payload), do: {:ok, payload}
  defp decode_payload(payload) when is_binary(payload), do: decode(payload)

  defp encode(term), do: IO.iodata_to_binary(JSON.encode_to_iodata!(term))
  defp decode(body), do: StrictDecoder.decode(body)
end
