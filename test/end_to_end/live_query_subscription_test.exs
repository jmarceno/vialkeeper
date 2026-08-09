defmodule ElixirDB.EndToEnd.LiveQuerySubscriptionTest do
  @moduledoc """
  Mandatory live-query subscription E2E from the implementation plan §23.

  Uses real Bandit HTTP, a real `.elixirdb` bundle, SQLite indexes, and NDJSON
  chunked streaming.
  """

  use ExUnit.Case, async: false

  alias ElixirDB.Eventual
  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.Query.SubscriptionHub
  alias ElixirDB.Query.Subscriptions
  alias ElixirDB.Replication
  alias ElixirDB.Runtime.{AttachmentCoordinator, DatabaseCatalog}
  alias ElixirDB.TestServer

  @tag :slow
  test "mandatory live query subscription end-to-end scenario" do
    server = TestServer.start_supervised!()
    root = ElixirDB.Config.database_root()
    path = "live-query-#{System.unique_integer([:positive])}.elixirdb"
    uuid = create_database!(server, path)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(Path.join(root, path))
    end)

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :update_config,
                %{
                  "subscriptions" => %{"max_buffered_events" => 4},
                  "retention" => %{
                    "mode" => "stable_frontier",
                    "history_depth" => 0,
                    "peer_expiry_ms" => 86_400_000,
                    "schedule" => "disabled"
                  }
                }}
             )

    create_index!(server, uuid, "by-type", [
      %{"path" => "/type", "type" => "string", "direction" => "asc"}
    ])

    assert %{status: 201, body: seed_a} =
             put_document!(server, uuid, "a", %{
               "type" => "task",
               "status" => "open",
               "title" => "A"
             })

    assert %{status: 201} =
             put_document!(server, uuid, "note", %{"type" => "note", "title" => "N"})

    parent = self()
    stream_url = server.base_url <> "/v1/databases/#{uuid}/query/stream"

    # Subscription A — full bodies
    task_a =
      Task.async(fn ->
        collect_stream(
          stream_url,
          %{"query" => %{"selector" => %{"/type" => "task"}}, "heartbeat_ms" => 100},
          parent,
          :a
        )
      end)

    assert_receive {:stream, :a, %{"type" => "snapshot", "document" => %{"id" => "a"}}}, 5_000
    assert_receive {:stream, :a, %{"type" => "caught_up", "sequence" => seq_a}}, 5_000
    assert is_integer(seq_a)

    # Subscription B — projected fields
    task_b =
      Task.async(fn ->
        collect_stream(
          stream_url,
          %{
            "query" => %{
              "selector" => %{"/type" => "task"},
              "fields" => ["/title", "/status"]
            },
            "heartbeat_ms" => 100
          },
          parent,
          :b
        )
      end)

    assert_receive {:stream, :b,
                    %{
                      "type" => "snapshot",
                      "document" => %{"id" => "a", "fields" => fields}
                    }},
                   5_000

    assert Map.has_key?(fields, "/title") or Map.has_key?(fields, "title")
    assert_receive {:stream, :b, %{"type" => "caught_up"}}, 5_000

    # Nonmatching create — neither emits a membership event.
    assert %{status: 201, body: note_body} =
             put_document!(server, uuid, "grow", %{"type" => "note", "title" => "grow"})

    # Update to match
    grow_rev = note_body["data"]["revision"]

    assert %{status: 201, body: grow_match_body} =
             put_document!(
               server,
               uuid,
               "grow",
               %{"type" => "task", "status" => "open", "title" => "grown"},
               grow_rev
             )

    grow_match_rev = grow_match_body["data"]["revision"]

    assert_receive {:stream, :a,
                    %{
                      "type" => "upsert",
                      "document" => %{"id" => "grow", "revision" => ^grow_match_rev}
                    }},
                   5_000

    assert_receive {:stream, :b,
                    %{
                      "type" => "upsert",
                      "document" => %{"id" => "grow", "revision" => ^grow_match_rev}
                    }},
                   5_000

    # Update existing member while matching
    a_rev = seed_a["data"]["revision"]

    assert %{status: 201, body: a_updated} =
             put_document!(
               server,
               uuid,
               "a",
               %{"type" => "task", "status" => "open", "title" => "A2"},
               a_rev
             )

    assert_receive {:stream, :a,
                    %{
                      "type" => "upsert",
                      "document" => %{"id" => "a", "revision" => rev_a2}
                    }},
                   5_000

    assert rev_a2 == a_updated["data"]["revision"]
    assert_receive {:stream, :b, %{"type" => "upsert", "document" => %{"id" => "a"}}}, 5_000

    # Leave membership
    assert %{status: 201, body: a_left} =
             put_document!(
               server,
               uuid,
               "a",
               %{"type" => "note", "title" => "gone"},
               rev_a2
             )

    assert_receive {:stream, :a, %{"type" => "remove", "id" => "a"}}, 5_000
    assert_receive {:stream, :b, %{"type" => "remove", "id" => "a"}}, 5_000
    _ = a_left

    # Delete matching document
    {:ok, get_grow} =
      Req.post(server.base_url <> "/v1/databases/#{uuid}/documents/get",
        json: %{"id" => "grow"}
      )

    grow_live = get_grow.body["data"]["revision"]

    assert %{status: 200} = delete_document!(server, uuid, "grow", grow_live)
    assert_receive {:stream, :a, %{"type" => "remove", "id" => "grow"}}, 5_000
    assert_receive {:stream, :b, %{"type" => "remove", "id" => "grow"}}, 5_000

    # Rapid revisions with hub suspended
    assert %{status: 201, body: rapid_seed} =
             put_document!(server, uuid, "rapid", %{"type" => "task", "status" => "open", "n" => 0})

    assert_receive {:stream, :a, %{"type" => "upsert", "document" => %{"id" => "rapid"}}}, 5_000
    assert_receive {:stream, :b, %{"type" => "upsert", "document" => %{"id" => "rapid"}}}, 5_000

    [{hub, _}] =
      Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:query_subscription_hub, uuid})

    :sys.suspend(hub)
    r0 = rapid_seed["data"]["revision"]

    assert %{status: 201, body: r1_body} =
             put_document!(
               server,
               uuid,
               "rapid",
               %{"type" => "task", "status" => "open", "n" => 1},
               r0
             )

    assert %{status: 201, body: r2_body} =
             put_document!(
               server,
               uuid,
               "rapid",
               %{"type" => "task", "status" => "open", "n" => 2},
               r1_body["data"]["revision"]
             )

    :sys.resume(hub)

    assert_receive {:stream, :a,
                    %{
                      "type" => "upsert",
                      "document" => %{"id" => "rapid", "revision" => rev1, "body" => %{"n" => 1}}
                    }},
                   5_000

    assert_receive {:stream, :a,
                    %{
                      "type" => "upsert",
                      "document" => %{"id" => "rapid", "revision" => rev2, "body" => %{"n" => 2}}
                    }},
                   5_000

    assert rev1 == r1_body["data"]["revision"]
    assert rev2 == r2_body["data"]["revision"]

    # Slow consumer via process API (HTTP loop would keep returning credits).
    assert {:ok, slow} =
             Subscriptions.open(
               uuid,
               %{
                 "query" => %{"selector" => %{"/type" => "task"}},
                 "heartbeat_ms" => 30_000
               },
               self()
             )

    assert {:ok, %{type: :caught_up}} = drain_subscription(slow)

    Enum.reduce(1..8, r2_body["data"]["revision"], fn n, rev ->
      assert %{status: 201, body: body} =
               put_document!(
                 server,
                 uuid,
                 "rapid",
                 %{"type" => "task", "status" => "open", "n" => n + 10},
                 rev
               )

      body["data"]["revision"]
    end)

    assert {:error, %{type: :error, error: %ElixirDB.Error{code: :subscription_overloaded}}} =
             Subscriptions.next(slow, 5_000)

    assert_receive {:stream, :a, %{"type" => "upsert", "document" => %{"id" => "rapid"}}}, 5_000

    # History truncation reset for A
    :sys.suspend(hub)

    assert %{status: 201} =
             put_document!(server, uuid, "post", %{"type" => "task", "status" => "open"})

    assert {:ok, %{new_floor: floor}} =
             DatabaseCatalog.command(uuid, {:command, :compact_retention, %{}})

    assert floor > 0
    :sys.resume(hub)

    assert_receive {:stream, :a, %{"type" => "reset"}}, 5_000
    assert_receive {:stream, :a, %{"type" => "snapshot"}}, 5_000
    assert_receive {:stream, :a, %{"type" => "caught_up"}}, 5_000

    wait_for_attachment_gc(uuid)

    # Disconnect A's HTTP client — subscription process must disappear promptly.
    subscription_pids = Map.keys(:sys.get_state(hub).subscriptions)
    assert Enum.count_until(subscription_pids, 3) == 2
    count_before = SubscriptionHub.count(uuid)
    _ = Task.shutdown(task_a, :brutal_kill)

    Eventual.eventually(
      fn ->
        remaining = Map.keys(:sys.get_state(hub).subscriptions)

        case subscription_pids -- remaining do
          [gone] ->
            if not Process.alive?(gone) and SubscriptionHub.count(uuid) == count_before - 1 do
              :ok
            else
              false
            end

          _ ->
            false
        end
      end,
      timeout: 5_000,
      message: "expected HTTP disconnect to remove subscription A promptly"
    )

    # Open several new subscriptions, then close the database.
    task_c =
      Task.async(fn ->
        collect_stream(
          stream_url,
          %{"query" => %{"selector" => %{"/type" => "task"}}, "heartbeat_ms" => 100},
          parent,
          :c
        )
      end)

    task_d =
      Task.async(fn ->
        collect_stream(
          stream_url,
          %{"query" => %{"selector" => %{"/type" => "task"}}, "heartbeat_ms" => 100},
          parent,
          :d
        )
      end)

    assert_receive {:stream, :c, %{"type" => "caught_up"}}, 5_000
    assert_receive {:stream, :d, %{"type" => "caught_up"}}, 5_000

    assert :ok = DatabaseCatalog.close(uuid)
    assert_receive {:stream, :b, %{"type" => "closed"}}, 5_000
    assert_receive {:stream, :c, %{"type" => "closed"}}, 5_000
    assert_receive {:stream, :d, %{"type" => "closed"}}, 5_000

    _ = Task.shutdown(task_b, :brutal_kill)
    _ = Task.shutdown(task_c, :brutal_kill)
    _ = Task.shutdown(task_d, :brutal_kill)

    # Reopen proves no persisted subscription state
    assert {:ok, _} = DatabaseCatalog.open(uuid)
    assert 0 = SubscriptionHub.count(uuid)

    assert {:ok, %{current_sequence: current_sequence}} =
             DatabaseCatalog.command(uuid, {:command, :identity, %{}})

    assert {:ok, %{documents: documents}} =
             ElixirDB.Query.execute(uuid, %{"selector" => %{"/type" => "task"}})

    assert Enum.any?(documents, &(&1.id == "post"))

    assert {:ok, %{results: []}} =
             ElixirDB.Changes.read(uuid, %{since: current_sequence, limit: 10})

    assert {:ok, %{ok: true}} =
             DatabaseCatalog.command(uuid, {:command, :integrity_check, %{}})

    # Replication remains usable while a live target subscription is active.
    replication_source_path =
      "live-query-replication-source-#{System.unique_integer([:positive])}.elixirdb"

    replication_target_path =
      "live-query-replication-target-#{System.unique_integer([:positive])}.elixirdb"

    assert {:ok, replication_source} = DatabaseCatalog.create(replication_source_path)
    assert {:ok, replication_target} = DatabaseCatalog.create(replication_target_path)

    on_exit(fn ->
      for {identity, path} <- [
            {replication_source, replication_source_path},
            {replication_target, replication_target_path}
          ] do
        _ = DatabaseCatalog.close(identity.database_uuid)
        _ = DatabaseCatalog.unregister(identity.database_uuid)
        ElixirDB.TempDatabase.cleanup(Path.join(root, path))
      end
    end)

    assert {:ok, _} =
             ElixirDB.Documents.put(replication_source.database_uuid, %{
               id: "replicated",
               body: %{"type" => "task", "title" => "replicated"}
             })

    assert {:ok, target_subscription} =
             Subscriptions.open(
               replication_target.database_uuid,
               %{"query" => %{"selector" => %{"/type" => "task"}}, "heartbeat_ms" => 30_000},
               self()
             )

    assert {:ok, %{type: :caught_up}} = drain_subscription(target_subscription)

    assert {:ok, %{status: :completed}} =
             Replication.one_shot(
               replication_source.database_uuid,
               replication_target.database_uuid
             )

    assert {:ok, %{type: :upsert, document: %{id: "replicated"}}} =
             next_until_document(target_subscription, "replicated")
  end

  defp drain_subscription(pid) do
    case Subscriptions.next(pid, 5_000) do
      {:ok, %{type: :snapshot}} -> drain_subscription(pid)
      {:ok, %{type: :caught_up} = event} -> {:ok, event}
      other -> other
    end
  end

  defp next_until_document(pid, id) do
    case Subscriptions.next(pid, 5_000) do
      {:ok, %{type: :upsert, document: %{id: ^id}} = event} -> {:ok, event}
      {:ok, _event} -> next_until_document(pid, id)
      other -> other
    end
  end

  defp create_database!(server, path) do
    {:ok, resp} = Req.post(server.base_url <> "/v1/databases", json: %{"path" => path})
    assert resp.status == 201
    resp.body["data"]["database_uuid"]
  end

  defp create_index!(server, uuid, name, fields) do
    {:ok, resp} =
      Req.post(server.base_url <> "/v1/databases/#{uuid}/indexes",
        json: %{"name" => name, "type" => "structured", "fields" => fields}
      )

    assert resp.status == 201
    resp.body["data"]["index_id"]
  end

  defp put_document!(server, uuid, id, body, if_revision \\ nil) do
    payload = %{"id" => id, "body" => body}
    payload = if if_revision, do: Map.put(payload, "if_revision", if_revision), else: payload

    {:ok, resp} =
      Req.post(server.base_url <> "/v1/databases/#{uuid}/documents/put", json: payload)

    %{status: resp.status, body: resp.body}
  end

  defp delete_document!(server, uuid, id, if_revision) do
    {:ok, resp} =
      Req.post(server.base_url <> "/v1/databases/#{uuid}/documents/delete",
        json: %{"id" => id, "if_revision" => if_revision}
      )

    %{status: resp.status, body: resp.body}
  end

  defp collect_stream(url, body, parent, tag) do
    Process.put({:ndjson_buffer, tag}, "")

    case Req.post(url,
           json: body,
           receive_timeout: 60_000,
           into: fn
             {:data, data}, {req, resp} ->
               emit_ndjson_lines(parent, tag, IO.iodata_to_binary(data))
               {:cont, {req, resp}}

             {:trailer, _}, acc ->
               {:cont, acc}
           end
         ) do
      {:ok, _} -> :ok
      {:error, reason} -> send(parent, {:stream_error, tag, reason})
    end
  end

  defp emit_ndjson_lines(parent, tag, chunk) do
    buffer = Process.get({:ndjson_buffer, tag}, "")
    parts = String.split(buffer <> chunk, "\n")
    {complete, [rest]} = Enum.split(parts, -1)
    Process.put({:ndjson_buffer, tag}, rest)

    Enum.each(complete, fn
      "" ->
        :ok

      line ->
        case StrictDecoder.decode(line) do
          {:ok, event} when is_map(event) -> send(parent, {:stream, tag, event})
          _ -> :ok
        end
    end)
  end

  defp wait_for_attachment_gc(uuid) do
    Eventual.eventually(
      fn ->
        case AttachmentCoordinator.status(uuid) do
          %{gc_barrier: false, gc_active: false, gc_queued: false, gc_scheduled: false} -> true
          _ -> false
        end
      end,
      timeout: 5_000,
      message: "attachment GC after compact did not become idle"
    )
  end
end
