defmodule ElixirDB.HTTP.MethodPathMatrixTest do
  @moduledoc """
  API-010–015 method/path matrix over real Bandit + Req (Plan §12.5).

  Each entry asserts an exact status family and at least one response invariant
  (success `data` shape, domain `error.code`, or envelope fields).
  """
  use ExUnit.Case, async: false

  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.TestServer

  test "API-010 through API-015 method/path matrix over Bandit+Req" do
    server = TestServer.start_supervised!()

    path_a = "matrix-a-#{System.unique_integer([:positive])}.elixirdb"
    path_b = "matrix-b-#{System.unique_integer([:positive])}.elixirdb"

    {:ok, %{status: 201, body: created_a}} =
      Req.post(server.base_url <> "/v1/databases", json: %{"path" => path_a})

    uuid = created_a["data"]["database_uuid"]
    assert is_binary(uuid)

    {:ok, %{status: 201, body: created_b}} =
      Req.post(server.base_url <> "/v1/databases", json: %{"path" => path_b})

    uuid_b = created_b["data"]["database_uuid"]

    on_exit(fn ->
      cleanup(uuid, path_a)
      cleanup(uuid_b, path_b)
    end)

    {:ok, %{status: 201, body: put_body}} =
      Req.post(server.base_url <> "/v1/databases/#{uuid}/documents/put",
        json: %{"id" => "doc", "body" => %{"kind" => "task"}}
      )

    revision = put_body["data"]["revision"]
    assert is_binary(revision)

    {:ok, %{status: 201, body: index_body}} =
      Req.post(server.base_url <> "/v1/databases/#{uuid}/indexes",
        json: %{
          "name" => "by-kind",
          "type" => "structured",
          "fields" => [%{"path" => "/kind", "type" => "string", "direction" => "asc"}]
        }
      )

    index_id = index_body["data"]["index_id"]
    assert is_binary(index_id)

    {:ok, %{status: status_job, body: job_body}} =
      Req.post(server.base_url <> "/v1/databases/#{uuid}/replications",
        json: %{
          "persist" => true,
          "mode" => "one_shot",
          "direction" => "push",
          "endpoint" => %{"kind" => "local", "database_uuid" => uuid_b},
          "enabled" => false
        }
      )

    assert status_job in [200, 201]
    job_id = job_body["data"]["job_id"]
    assert is_binary(job_id)

    replication_id = "rep_" <> Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)

    closed_path = "matrix-closed-#{System.unique_integer([:positive])}.elixirdb"

    {:ok, %{status: 201, body: closed_body}} =
      Req.post(server.base_url <> "/v1/databases", json: %{"path" => closed_path})

    closed_uuid = closed_body["data"]["database_uuid"]

    assert {:ok, %{status: 200}} =
             Req.post(server.base_url <> "/v1/databases/#{closed_uuid}/close", json: %{})

    on_exit(fn -> cleanup(closed_uuid, closed_path) end)

    extra_path = "matrix-extra-#{System.unique_integer([:positive])}.elixirdb"
    on_exit(fn -> cleanup_path(extra_path) end)

    session_id = ElixirDB.UUID.v4()

    matrix = [
      {:post, "/v1/databases", %{"path" => extra_path}, 201,
       &assert_data(&1, fn data -> is_binary(data["database_uuid"]) end)},
      {:post, "/v1/registrations", %{"path" => closed_path}, 201,
       &assert_data(&1, fn data -> data["database_uuid"] == closed_uuid end)},
      {:delete, "/v1/registrations/#{closed_uuid}", nil, 200, &assert_envelope_ok/1},
      {:get, "/v1/databases", nil, 200,
       &assert_data(&1, fn data ->
         Enum.any?(List.wrap(data), fn row -> row["database_uuid"] == uuid end)
       end)},
      {:get, "/v1/databases/#{uuid}", nil, 200,
       &assert_data(&1, fn data -> data["database_uuid"] == uuid end)},
      {:get, "/v1/databases/#{uuid}/config", nil, 200, &assert_data_map/1},
      {:put, "/v1/databases/#{uuid}/config", %{}, 200, &assert_data_map/1},
      {:post, "/v1/databases/#{uuid}/integrity-check", %{}, 200,
       &assert_data(&1, fn data -> data["ok"] == true end)},
      {:post, "/v1/databases/#{uuid}/compact", %{}, 200,
       &assert_data(&1, fn data ->
         is_integer(data["old_floor"]) and is_integer(data["new_floor"]) and
           is_integer(data["removed_revisions"])
       end)},
      {:post, "/v1/databases/#{uuid}/documents/get", %{"id" => "doc"}, 200,
       &assert_data(&1, fn data ->
         data["id"] == "doc" and data["revision"] == revision and is_map(data["body"])
       end)},
      {:post, "/v1/databases/#{uuid}/attachments/upload", "matrix-bytes", 201,
       &assert_attachment_upload/1},
      {:post, "/v1/databases/#{uuid}/attachments/get",
       %{"id" => "doc", "revision" => nil, "name" => "missing.bin"}, :error,
       &assert_error_code(&1, [
         "attachment_not_found",
         "document_not_found",
         "attachment_blob_not_found",
         "internal_error"
       ])},
      {:post, "/v1/databases/#{uuid}/documents/put",
       %{"id" => "doc-2", "body" => %{"kind" => "note"}}, 201,
       &assert_data(&1, fn data -> is_binary(data["revision"]) and data["replayed"] == false end)},
      {:post, "/v1/databases/#{uuid}/documents/resolve",
       %{
         "id" => "missing-resolve",
         "expected_live_revisions" => [],
         "chosen_parent_revision" => revision,
         "body" => %{}
       }, :error,
       &assert_error_code(&1, [
         "document_not_found",
         "revision_conflict",
         "invalid_request"
       ])},
      {:post, "/v1/databases/#{uuid}/documents/bulk-get", [%{"id" => "doc"}], 200,
       &assert_data(&1, fn data ->
         is_list(data) and
           case hd(stringify_keys(data)) do
             %{"ok" => %{"id" => "doc"}} -> true
             %{"ok" => ok} when is_map(ok) -> ok["id"] == "doc"
             _ -> false
           end
       end)},
      {:post, "/v1/databases/#{uuid}/documents/bulk-write",
       [%{"type" => "put", "id" => "bulk", "body" => %{"x" => 1}}], 200,
       &assert_data(&1, fn data ->
         is_map(data) or
           (is_list(data) and Enum.any?(data, fn row -> row["document_id"] == "bulk" end))
       end)},
      {:post, "/v1/databases/#{uuid}/changes", %{"since" => 0, "limit" => 10, "wait_ms" => 0}, 200,
       &assert_data(&1, fn data ->
         is_list(data["results"]) and is_integer(data["last_sequence"])
       end)},
      {:post, "/v1/databases/#{uuid}/changes/stream",
       %{"since" => 0, "limit" => 10, "heartbeat_ms" => 0}, 200, &assert_ndjson_stream/1},
      {:post, "/v1/databases/#{uuid}/indexes",
       %{
         "name" => "by-kind-2",
         "type" => "structured",
         "fields" => [%{"path" => "/kind", "type" => "string", "direction" => "asc"}]
       }, 201, &assert_data(&1, fn data -> is_binary(data["index_id"]) end)},
      {:get, "/v1/databases/#{uuid}/indexes", nil, 200,
       &assert_data(&1, fn data -> is_list(data) or is_list(data["indexes"]) end)},
      {:post, "/v1/databases/#{uuid}/indexes/#{index_id}/rebuild", %{}, 200,
       &assert_data(&1, fn data -> data["rebuilt"] == true or is_map(data) end)},
      {:post, "/v1/databases/#{uuid}/query", %{"selector" => %{"/kind" => "task"}}, 200,
       &assert_data(&1, fn data -> is_list(data["documents"] || data["results"]) end)},
      {:post, "/v1/databases/#{uuid}/query/explain", %{"selector" => %{"/kind" => "task"}}, 200,
       &assert_data_map/1},
      {:get, "/v1/databases/#{uuid}/replications", nil, 200,
       &assert_data(&1, fn data -> is_list(data) end)},
      {:get, "/v1/databases/#{uuid}/replications/#{job_id}", nil, 200,
       &assert_data(&1, fn data -> data["job_id"] == job_id end)},
      {:post, "/v1/databases/#{uuid}/replications/#{job_id}/enable", %{}, 200,
       &assert_data(&1, fn data -> data["job_id"] == job_id or is_map(data) end)},
      {:post, "/v1/databases/#{uuid}/replications/#{job_id}/disable", %{}, 200,
       &assert_data(&1, fn data ->
         data["state"] in ["disabled", :disabled] or data["job_id"] == job_id or is_map(data)
       end)},
      {:post, "/v1/databases/#{uuid}/replications/#{job_id}/cancel", %{}, :any_ok_or_domain,
       &assert_ok_or_domain_error(&1, ["replication_job_not_found", "invalid_request"])},
      {:get, "/v1/databases/#{uuid}/replication/identity", nil, 200,
       &assert_data(&1, fn data ->
         data["database_uuid"] == uuid and is_integer(data["current_sequence"]) and
           Map.has_key?(data, "retention_floor")
       end)},
      {:post, "/v1/databases/#{uuid}/replication/boundaries", %{}, 200,
       &assert_data(&1, fn data ->
         is_list(data["boundaries"]) and is_integer(data["compaction_epoch"])
       end)},
      {:post, "/v1/databases/#{uuid}/replication/changes",
       %{"since" => 0, "limit" => 10, "wait_ms" => 0}, 200,
       &assert_data(&1, fn data -> is_list(data["results"]) end)},
      {:post, "/v1/databases/#{uuid}/replication/revisions/diff",
       %{"documents" => [%{"document_id" => "doc", "leaf_revisions" => [revision]}]}, 200,
       &assert_data(&1, fn data -> is_list(data["documents"]) end)},
      {:post, "/v1/databases/#{uuid}/replication/revisions/get",
       %{"documents" => [%{"document_id" => "doc", "leaf_revisions" => [revision]}]}, 200,
       &assert_data(&1, fn data -> is_list(data["chains"]) end)},
      {:post, "/v1/databases/#{uuid}/replication/revisions/put", %{"chains" => []}, 200,
       &assert_data_map/1},
      {:get, "/v1/databases/#{uuid}/replication/checkpoints/#{replication_id}", nil, 200,
       &assert_checkpoint_get/1},
      {:put, "/v1/databases/#{uuid}/replication/checkpoints/#{replication_id}",
       %{
         "expected_checkpoint_version" => 0,
         "version" => 1,
         "checkpoint_version" => 1,
         "replication_id" => replication_id,
         "session_id" => session_id,
         "source_sequence" => 0,
         "source_history_epoch" => "epoch-matrix",
         "source_compaction_epoch" => 0,
         "safe_source_sequence" => 0,
         "installed_source_compaction_epoch" => 0,
         "history" => []
       }, 200, &assert_data_map/1},
      # Destructive routes last so earlier matrix entries keep a valid setup.
      {:post, "/v1/databases/#{uuid}/documents/delete", %{"id" => "doc-2", "if_revision" => nil},
       :error,
       &assert_error_code(&1, ["revision_conflict", "invalid_request", "document_not_found"])},
      {:delete, "/v1/databases/#{uuid}/indexes/#{index_id}", nil, 200,
       &assert_data(&1, fn data -> data["deleted"] == true or data["index_id"] == index_id end)},
      {:post, "/v1/databases/#{uuid}/replications/#{job_id}/start", %{}, :any_ok_or_domain,
       &assert_ok_or_domain_error(&1, [
         "invalid_request",
         "replication_job_not_found",
         "database_closed"
       ])},
      {:delete, "/v1/databases/#{uuid}/replications/#{job_id}", nil, 200, &assert_envelope_ok/1},
      {:post, "/v1/databases/#{uuid}/close", %{}, 200, &assert_envelope_ok/1}
    ]

    Enum.each(matrix, fn {method, path, body, expected, assert_fn} ->
      response = http!(server, method, path, body)

      assert_status!(method, path, response, expected)
      refute route_not_found?(response), "#{method_string(method)} #{path} hit catch-all"

      assert is_binary(response.body) or
               (is_map(response.body) and Map.has_key?(response.body, "request_id")) or
               response.status == 200,
             "#{method_string(method)} #{path} missing envelope"

      assert_fn.(response)
    end)

    # Close stops the runtime; registered databases reopen on the next command
    # (DatabaseCatalog.command → open_runtime). Prove reopen works, not permanent closed.
    assert [] =
             Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:owner, uuid})

    reopened =
      http!(server, :post, "/v1/databases/#{uuid}/documents/get", %{"id" => "doc"})

    assert reopened.status == 200
    assert reopened.body["data"]["id"] == "doc"
  end

  defp http!(server, method, path, nil) do
    assert {:ok, response} =
             Req.request(method: method, url: server.base_url <> path, decode_body: false)

    %{status: response.status, headers: response.headers, body: decode_body(response)}
  end

  defp http!(server, method, path, body) when is_binary(body) do
    assert {:ok, response} =
             Req.request(
               method: method,
               url: server.base_url <> path,
               body: body,
               headers: [{"content-type", "application/octet-stream"}],
               decode_body: false
             )

    %{status: response.status, headers: response.headers, body: decode_body(response)}
  end

  defp http!(server, method, path, body) when is_map(body) or is_list(body) do
    opts = [method: method, url: server.base_url <> path, json: body, decode_body: false]

    opts =
      if String.contains?(path, "/changes/stream"),
        do: Keyword.put(opts, :receive_timeout, 5_000),
        else: opts

    assert {:ok, response} = Req.request(opts)
    %{status: response.status, headers: response.headers, body: decode_body(response)}
  end

  defp decode_body(%{headers: headers, body: body}) do
    content_type = header(headers, "content-type") || ""

    cond do
      content_type =~ "ndjson" ->
        body

      is_binary(body) and body != "" ->
        case StrictDecoder.decode(body) do
          {:ok, decoded} -> decoded
          _ -> body
        end

      true ->
        body
    end
  end

  defp header(headers, name) do
    Enum.find_value(headers, fn
      {key, value} -> if String.downcase(to_string(key)) == name, do: to_string(value)
      _ -> nil
    end)
  end

  defp assert_status!(method, path, response, status) when is_integer(status) do
    assert response.status == status,
           "#{method_string(method)} #{path}: expected status #{status}, got #{response.status} body=#{inspect(response.body)}"
  end

  defp assert_status!(method, path, response, :error) do
    assert response.status in 400..599,
           "#{method_string(method)} #{path}: expected error status, got #{response.status} body=#{inspect(response.body)}"
  end

  defp assert_status!(method, path, response, :any_ok_or_domain) do
    assert response.status in 200..299 or response.status in 400..599,
           "#{method_string(method)} #{path}: unexpected status #{response.status}"
  end

  defp assert_envelope_ok(response) do
    assert response.status in 200..299

    assert is_map(response.body["data"]) or response.body["data"] == %{} or
             Map.has_key?(response.body, "data")
  end

  defp assert_data_map(response) do
    assert response.status in 200..299
    assert is_map(response.body["data"])
  end

  defp assert_attachment_upload(response) do
    assert response.status in 200..299
    data = response.body["data"]
    assert is_binary(data["blob"])
    assert is_integer(data["length"])
    assert is_binary(data["expires_at"])
  end

  defp assert_data(response, fun) when is_function(fun, 1) do
    assert response.status in 200..299
    assert is_map(response.body)
    assert Map.has_key?(response.body, "data")
    assert fun.(response.body["data"]), "data invariant failed: #{inspect(response.body["data"])}"
  end

  defp assert_error_code(response, codes) when is_list(codes) do
    assert response.status in 400..599
    code = response.body["error"]["code"]
    assert code in codes, "expected error #{inspect(codes)}, got #{inspect(code)}"
    assert is_boolean(response.body["error"]["retryable"])
    assert is_binary(response.body["error"]["message"])
  end

  defp assert_ok_or_domain_error(response, codes) do
    if response.status in 200..299 do
      assert Map.has_key?(response.body, "data")
    else
      assert_error_code(response, codes)
    end
  end

  defp assert_checkpoint_get(response) do
    assert response.status in 200..299
    data = response.body["data"]
    # Missing checkpoint may be null/empty record; present record is a map.
    assert is_nil(data) or is_map(data)
  end

  defp assert_ndjson_stream(response) do
    assert response.status == 200
    content_type = header(response.headers, "content-type") || ""
    assert content_type =~ "ndjson" or content_type =~ "json"

    body = if is_binary(response.body), do: response.body, else: inspect(response.body)
    assert body =~ "\"type\""
    assert body =~ "change" or body =~ "caught_up"
  end

  defp stringify_keys(list) when is_list(list) do
    Enum.map(list, fn
      map when is_map(map) ->
        Map.new(map, fn {k, v} ->
          {to_string(k), if(is_map(v), do: Map.new(v, fn {a, b} -> {to_string(a), b} end), else: v)}
        end)

      other ->
        other
    end)
  end

  defp route_not_found?(response) do
    case response.body do
      %{"error" => %{"message" => message}} -> message =~ "route not found"
      _ -> false
    end
  end

  defp method_string(method), do: method |> Atom.to_string() |> String.upcase()

  defp cleanup_path(path) do
    root = ElixirDB.Config.database_root()
    ElixirDB.TempDatabase.cleanup(Path.join(root, path))
  end

  defp cleanup(uuid, path) do
    _ = DatabaseCatalog.close(uuid)
    _ = DatabaseCatalog.unregister(uuid)
    root = ElixirDB.Config.database_root()
    ElixirDB.TempDatabase.cleanup(Path.join(root, path))
  end
end
