defmodule ElixirDB.HTTP.QueryStreamTest do
  use ElixirDB.Observability.OtelCase, async: false

  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.Runtime.DatabaseCatalog

  test "query stream emits snapshot, caught_up, upsert, and closed events" do
    server = ElixirDB.TestServer.start_supervised!()
    path = "query-stream-#{System.unique_integer([:positive])}.elixirdb"

    {:ok, create_resp} =
      Req.post(server.base_url <> "/v1/databases", json: %{"path" => path})

    assert create_resp.status == 201
    uuid = create_resp.body["data"]["database_uuid"]

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      root = ElixirDB.Config.database_root()
      ElixirDB.TempDatabase.cleanup(Path.join(root, path))
    end)

    {:ok, put_resp} =
      Req.post(server.base_url <> "/v1/databases/#{uuid}/documents/put",
        json: %{"id" => "doc", "body" => %{"kind" => "task", "title" => "one"}}
      )

    assert put_resp.status == 201
    revision = put_resp.body["data"]["revision"]

    parent = self()
    url = server.base_url <> "/v1/databases/#{uuid}/query/stream"

    task =
      Task.async(fn ->
        collect_stream_events(
          url,
          %{"query" => %{"selector" => %{"/kind" => "task"}}, "heartbeat_ms" => 50},
          parent
        )
      end)

    assert_receive {:ndjson_event,
                    %{
                      "type" => "snapshot",
                      "document" => %{"id" => "doc", "revision" => ^revision}
                    }},
                   5_000

    assert_receive {:ndjson_event, %{"type" => "caught_up", "sequence" => sequence}}, 5_000
    assert is_integer(sequence)

    {:ok, update_resp} =
      Req.post(server.base_url <> "/v1/databases/#{uuid}/documents/put",
        json: %{
          "id" => "doc",
          "if_revision" => revision,
          "body" => %{"kind" => "task", "title" => "two"}
        }
      )

    assert update_resp.status == 201
    new_revision = update_resp.body["data"]["revision"]

    assert_receive {:ndjson_event,
                    %{
                      "type" => "upsert",
                      "document" => %{"id" => "doc", "revision" => ^new_revision}
                    }},
                   5_000

    assert_receive {:ndjson_event, %{"type" => "heartbeat"}}, 5_000

    assert :ok = DatabaseCatalog.close(uuid)
    assert_receive {:ndjson_event, %{"type" => "closed"}}, 5_000

    _ = Task.shutdown(task, :brutal_kill)
  end

  test "query stream rejects unknown fields and forbidden query keys" do
    server = ElixirDB.TestServer.start_supervised!()
    path = "query-stream-reject-#{System.unique_integer([:positive])}.elixirdb"

    {:ok, create_resp} =
      Req.post(server.base_url <> "/v1/databases", json: %{"path" => path})

    assert create_resp.status == 201
    uuid = create_resp.body["data"]["database_uuid"]

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      root = ElixirDB.Config.database_root()
      ElixirDB.TempDatabase.cleanup(Path.join(root, path))
    end)

    url = server.base_url <> "/v1/databases/#{uuid}/query/stream"

    {:ok, unknown} =
      Req.post(url, json: %{"query" => %{"selector" => %{}}, "extra" => 1})

    assert unknown.status == 400

    {:ok, forbidden} =
      Req.post(url, json: %{"query" => %{"selector" => %{}, "limit" => 1}})

    assert forbidden.status == 400
  end

  defp collect_stream_events(url, body, parent) do
    buffer_key = make_ref()
    Process.put(buffer_key, "")

    try do
      case Req.post(url,
             json: body,
             receive_timeout: 15_000,
             into: fn
               {:data, data}, {req, resp} ->
                 emit_stream_chunk(parent, buffer_key, IO.iodata_to_binary(data))
                 {:cont, {req, resp}}

               {:trailer, _}, acc ->
                 {:cont, acc}
             end
           ) do
        {:ok, _resp} -> :ok
        {:error, reason} -> send(parent, {:ndjson_stream_error, reason})
      end
    after
      Process.delete(buffer_key)
    end
  end

  defp emit_stream_chunk(parent, buffer_key, chunk) do
    parts = String.split(Process.get(buffer_key, "") <> chunk, "\n")
    {complete, [rest]} = Enum.split(parts, -1)
    Process.put(buffer_key, rest)

    for line when line != "" <- complete do
      case StrictDecoder.decode(line) do
        {:ok, event} when is_map(event) -> send(parent, {:ndjson_event, event})
        _ -> :ok
      end
    end
  end
end
