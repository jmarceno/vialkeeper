defmodule ElixirDB.HTTP.NdjsonChangesTest do
  @moduledoc """
  CHANGE-008 NDJSON stream events over an ephemeral Bandit TestServer.

  Asserts all event types with wire fields: change, caught_up, heartbeat, closed, error.
  """
  use ExUnit.Case, async: false

  alias ElixirDB.Runtime.DatabaseCatalog

  test "changes stream emits change, caught_up, heartbeat, closed, and error events" do
    server = ElixirDB.TestServer.start_supervised!()
    path = "ndjson-#{System.unique_integer([:positive])}.db"

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
        json: %{"id" => "doc", "body" => %{"n" => 1}}
      )

    assert put_resp.status == 201
    revision = put_resp.body["data"]["revision"]
    put_sequence = put_resp.body["data"]["sequence"]
    assert is_binary(revision)
    assert is_integer(put_sequence) and put_sequence >= 1

    parent = self()
    url = server.base_url <> "/v1/databases/#{uuid}/changes/stream"

    # Probe content-type on a short non-follow stream.
    {:ok, probe} =
      Req.post(url,
        json: %{"since" => 0, "limit" => 10, "heartbeat_ms" => 0},
        decode_body: false
      )

    content_type =
      Enum.find_value(probe.headers, fn {k, v} ->
        if String.downcase(to_string(k)) == "content-type", do: to_string(v)
      end)

    assert content_type =~ "application/x-ndjson"

    task =
      Task.async(fn ->
        collect_stream_events(url, %{"since" => 0, "limit" => 10, "heartbeat_ms" => 30}, parent)
      end)

    assert_receive {:ndjson_event,
                    %{
                      "type" => "change",
                      "change" => change
                    }},
                   2_000

    assert change["document_id"] == "doc" or change[:document_id] == "doc"

    assert (change["winning_revision"] || change[:winning_revision]) == revision or
             revision in leaf_revisions(change)

    assert is_integer(change["sequence"] || change[:sequence])
    assert is_list(change["leaf_revisions"] || change[:leaf_revisions])

    assert_receive {:ndjson_event, %{"type" => "caught_up", "sequence" => caught_seq}}, 2_000
    assert is_integer(caught_seq) and caught_seq >= put_sequence

    assert_receive {:ndjson_event, %{"type" => "heartbeat"} = heartbeat}, 2_000
    refute Map.has_key?(heartbeat, "sequence")
    refute Map.has_key?(heartbeat, "document_id")
    refute Map.has_key?(heartbeat, "change")

    assert :ok = DatabaseCatalog.close(uuid)
    assert_receive {:ndjson_event, %{"type" => "closed"}}, 2_000

    _ = Task.shutdown(task, :brutal_kill)

    # Re-open and force an in-stream error by killing the notifier after catch-up.
    assert {:ok, _} = DatabaseCatalog.register(path)

    error_parent = self()

    error_task =
      Task.async(fn ->
        collect_stream_events(
          url,
          %{"since" => 0, "limit" => 10, "heartbeat_ms" => 40},
          error_parent
        )
      end)

    assert_receive {:ndjson_event, %{"type" => "caught_up", "sequence" => seq2}}, 2_000
    assert is_integer(seq2) and seq2 >= 1
    assert_receive {:ndjson_event, %{"type" => "heartbeat"}}, 2_000

    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:notifier, uuid}) do
      [{pid, _}] -> Process.exit(pid, :kill)
      [] -> flunk("expected change notifier to be running")
    end

    assert_receive {:ndjson_event, %{"type" => "error", "error" => error}}, 3_000
    assert error["code"] in ["database_closed", "database_unavailable", "internal_error"]
    assert is_boolean(error["retryable"])
    assert is_binary(error["message"])

    _ = Task.shutdown(error_task, :brutal_kill)
  end

  defp leaf_revisions(change) do
    (change["leaf_revisions"] || change[:leaf_revisions] || [])
    |> Enum.map(fn leaf -> leaf["revision"] || leaf[:revision] end)
  end

  defp collect_stream_events(url, body, parent) do
    case Req.post(url,
           json: body,
           receive_timeout: 15_000,
           into: fn
             {:data, data}, {req, resp} ->
               for line <- String.split(IO.iodata_to_binary(data), "\n", trim: true) do
                 case ElixirDB.JSON.StrictDecoder.decode(line) do
                   {:ok, event} when is_map(event) -> send(parent, {:ndjson_event, event})
                   _ -> :ok
                 end
               end

               {:cont, {req, resp}}

             {:trailer, _}, acc ->
               {:cont, acc}
           end
         ) do
      {:ok, _resp} -> :ok
      {:error, reason} -> send(parent, {:ndjson_stream_error, reason})
    end
  end
end
