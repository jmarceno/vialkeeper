defmodule ElixirDB.EndToEnd.TwoServerHttpConvergenceTest do
  @moduledoc """
  Plan §12.6 / Architecture §21 step 9: two real Bandit servers, remote
  replication wire only, restart during continuous replication, resume through
  checkpoint reconciliation.

  Uses `ElixirDB.TestServer` + Req — not Plug.Test as fake servers.
  On outage: cancel/disable the continuous worker so resume must go through
  `JobManager.start` after Bandit restart (not an in-BEAM reconnect).
  """
  use ExUnit.Case, async: false

  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.TestServer

  @tag :slow
  test "two Bandit servers converge over remote wire across mid-replication restart" do
    root = ElixirDB.Config.database_root()
    prefix = "e2e-two-http-#{System.unique_integer([:positive])}"
    a_path = prefix <> "-a.db"
    b_path = prefix <> "-b.db"

    for path <- [a_path, b_path] do
      ElixirDB.TempDatabase.cleanup(Path.join(root, path))
    end

    server_a = TestServer.start_supervised!()
    server_b = TestServer.start_supervised!()
    refute server_a.port == server_b.port
    port_a = server_a.port
    port_b = server_b.port

    a_uuid = create_database!(server_a, a_path)
    b_uuid = create_database!(server_b, b_path)

    on_exit(fn ->
      _ = maybe_disable_jobs(a_uuid)
      _ = DatabaseCatalog.close(a_uuid)
      _ = DatabaseCatalog.close(b_uuid)
      _ = DatabaseCatalog.unregister(a_uuid)
      _ = DatabaseCatalog.unregister(b_uuid)
      ElixirDB.TempDatabase.cleanup(Path.join(root, a_path))
      ElixirDB.TempDatabase.cleanup(Path.join(root, b_path))
    end)

    assert {:ok, %{"revision" => first_rev}} =
             put_document!(server_a, a_uuid, "seed", %{"n" => 1, "phase" => "pre-restart"})

    assert {:ok, %{"job_id" => job_id}} =
             put_replication_job!(server_a, a_uuid, %{
               "persist" => true,
               "mode" => "continuous",
               "direction" => "push",
               "enabled" => true,
               "wait_ms" => 100,
               "retry" => %{
                 "max_attempts" => 32,
                 "base_delay_ms" => 50,
                 "max_delay_ms" => 400,
                 "jitter_ms" => 10
               },
               "endpoint" => %{
                 "kind" => "remote",
                 "database_uuid" => b_uuid,
                 "base_url" => server_b.base_url
               }
             })

    wait_for_document!(server_b, b_uuid, "seed", first_rev, %{"n" => 1})

    ElixirDB.Eventual.eventually(
      fn ->
        case ElixirDB.Replication.JobManager.get(a_uuid, job_id) do
          {:ok, %{state: state}} when state in [:waiting, :backoff, :handshake, :read_changes] ->
            true

          _ ->
            false
        end
      end,
      timeout: 15_000,
      message: "continuous job never entered an active replication state"
    )

    assert {:ok, %{"revision" => mid_rev}} =
             put_document!(server_a, a_uuid, "during", %{"n" => 2, "phase" => "before-stop"})

    wait_for_document!(server_b, b_uuid, "during", mid_rev, %{"n" => 2})

    # Architecture §21 step 9 — stop BOTH HTTP servers, then cancel/disable the
    # continuous worker so recovery cannot be a silent reconnect.
    assert :ok = TestServer.stop(server_a)
    assert :ok = TestServer.stop(server_b)

    assert {:error, _} = Req.get("http://127.0.0.1:#{port_a}/v1/databases", retry: false)
    assert {:error, _} = Req.get("http://127.0.0.1:#{port_b}/v1/databases", retry: false)

    assert {:ok, _} = ElixirDB.Replication.JobManager.disable(a_uuid, job_id)

    ElixirDB.Eventual.eventually(
      fn ->
        case ElixirDB.Replication.JobManager.get(a_uuid, job_id) do
          {:ok, %{state: :disabled}} -> true
          _ -> false
        end
      end,
      timeout: 10_000,
      message: "continuous job was not disabled after outage"
    )

    assert {:ok, %{revision: offline_rev}} =
             ElixirDB.Documents.put(a_uuid, %{
               id: "offline",
               body: %{"n" => 3, "phase" => "servers-down"}
             })

    # While the remote wire is down and the worker is disabled, B must not have
    # the offline write.
    assert {:error, %ElixirDB.Error{code: :document_not_found}} =
             ElixirDB.Documents.get(b_uuid, %{id: "offline"})

    server_a2 = TestServer.start_supervised!(port: port_a)
    server_b2 = TestServer.start_supervised!(port: port_b)
    assert server_a2.port == port_a
    assert server_b2.port == port_b

    # Explicit resume path: enable + start after Bandit restart.
    assert {:ok, _} = ElixirDB.Replication.JobManager.enable(a_uuid, job_id)

    ElixirDB.Eventual.eventually(
      fn ->
        case ElixirDB.Replication.JobManager.get(a_uuid, job_id) do
          {:ok, %{state: state}}
          when state in [
                 :waiting,
                 :backoff,
                 :handshake,
                 :read_changes,
                 :diff,
                 :fetch_chains,
                 :import,
                 :checkpoint_target,
                 :checkpoint_source,
                 :idle
               ] ->
            true

          _ ->
            false
        end
      end,
      timeout: 15_000,
      message: "continuous job never restarted after JobManager.enable/start"
    )

    # Resume through checkpoint reconciliation — offline write arrives after restart.
    wait_for_document!(server_b2, b_uuid, "offline", offline_rev, %{"n" => 3})

    assert {:ok, %{"revision" => post_rev}} =
             put_document!(server_a2, a_uuid, "post-restart", %{
               "n" => 4,
               "phase" => "after-restart"
             })

    wait_for_document!(server_b2, b_uuid, "post-restart", post_rev, %{"n" => 4})

    assert {:ok, replication_id} =
             ElixirDB.Replication.Id.calculate(a_uuid, b_uuid, "push", "continuous")

    final_sequence = source_sequence!(a_uuid)
    assert final_sequence >= 4

    ElixirDB.Eventual.eventually(
      fn ->
        case checkpoint_source_sequence(a_uuid, replication_id) do
          {:ok, seq} when seq == final_sequence ->
            case checkpoint_source_sequence(b_uuid, replication_id) do
              {:ok, ^final_sequence} -> true
              _ -> false
            end

          _ ->
            false
        end
      end,
      timeout: 15_000,
      message: "A and B checkpoints must equal final source sequence #{final_sequence}"
    )

    assert {:ok, ^final_sequence} = checkpoint_source_sequence(a_uuid, replication_id)
    assert {:ok, ^final_sequence} = checkpoint_source_sequence(b_uuid, replication_id)

    assert {:ok, %{"revision" => ^first_rev, "body" => %{"n" => 1}}} =
             get_document!(server_b2, b_uuid, "seed")

    assert {:ok, %{"revision" => ^mid_rev, "body" => %{"n" => 2}}} =
             get_document!(server_b2, b_uuid, "during")

    assert {:ok, %{"revision" => ^offline_rev, "body" => %{"n" => 3}}} =
             get_document!(server_b2, b_uuid, "offline")

    assert {:ok, %{"revision" => ^post_rev, "body" => %{"n" => 4}}} =
             get_document!(server_b2, b_uuid, "post-restart")

    assert_leaf_sets_equal!(a_uuid, b_uuid)

    assert {:ok, %{ok: true}} =
             DatabaseCatalog.command(b_uuid, {:command, :integrity_check, %{}})

    _ = ElixirDB.Replication.JobManager.disable(a_uuid, job_id)
  end

  defp create_database!(server, path) do
    assert {:ok, %{status: 201, body: body}} =
             Req.post(server.base_url <> "/v1/databases", json: %{"path" => path})

    assert %{"data" => %{"database_uuid" => uuid}} = body
    uuid
  end

  defp put_document!(server, uuid, id, doc_body) do
    assert {:ok, %{status: status, body: body}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/documents/put",
               json: %{"id" => id, "body" => doc_body}
             )

    assert status in [200, 201]
    assert %{"data" => data} = body
    {:ok, data}
  end

  defp get_document!(server, uuid, id) do
    case Req.post(server.base_url <> "/v1/databases/#{uuid}/documents/get",
           json: %{"id" => id}
         ) do
      {:ok, %{status: 200, body: %{"data" => data}}} -> {:ok, data}
      other -> other
    end
  end

  defp put_replication_job!(server, uuid, definition) do
    assert {:ok, %{status: 201, body: body}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/replications", json: definition)

    assert %{"data" => data} = body
    {:ok, data}
  end

  defp wait_for_document!(server, uuid, id, revision, body_subset) do
    ElixirDB.Eventual.eventually(
      fn ->
        case get_document!(server, uuid, id) do
          {:ok, %{"revision" => ^revision, "body" => body}} ->
            Enum.all?(body_subset, fn {k, v} -> body[k] == v end)

          _ ->
            false
        end
      end,
      timeout: 20_000,
      message: "document #{id} revision #{revision} did not appear on remote server"
    )
  end

  defp source_sequence!(uuid) do
    assert {:ok, identity} = DatabaseCatalog.command(uuid, {:command, :identity, %{}})
    identity[:current_sequence] || identity["current_sequence"]
  end

  defp checkpoint_source_sequence(uuid, replication_id) do
    case DatabaseCatalog.command(
           uuid,
           {:command, :get_local_record, "checkpoints", replication_id}
         ) do
      {:ok, %{value: value}} when is_map(value) ->
        {:ok, value["source_sequence"] || value[:source_sequence]}

      {:ok, %{"value" => value}} when is_map(value) ->
        {:ok, value["source_sequence"] || value[:source_sequence]}

      other ->
        other
    end
  end

  defp assert_leaf_sets_equal!(source_uuid, target_uuid) do
    assert {:ok, %{results: source_changes}} =
             ElixirDB.Changes.read(source_uuid, %{since: 0, limit: 200})

    assert {:ok, %{results: target_changes}} =
             ElixirDB.Changes.read(target_uuid, %{since: 0, limit: 200})

    source_leaves = leaf_map(source_changes)
    target_leaves = leaf_map(target_changes)

    for {document_id, leaves} <- source_leaves do
      assert Map.get(target_leaves, document_id) == leaves,
             "leaf set mismatch for #{document_id}: source=#{inspect(leaves)} target=#{inspect(Map.get(target_leaves, document_id))}"
    end
  end

  defp leaf_map(changes) do
    Map.new(changes, fn change ->
      leaves =
        (change.leaf_revisions || change["leaf_revisions"] || [])
        |> Enum.map(fn leaf -> leaf[:revision] || leaf["revision"] end)
        |> Enum.reject(&is_nil/1)
        |> MapSet.new()

      {change.document_id || change["document_id"], leaves}
    end)
  end

  defp maybe_disable_jobs(uuid) do
    case ElixirDB.Replication.JobManager.list(uuid) do
      {:ok, jobs} ->
        Enum.each(jobs, fn job ->
          _ = ElixirDB.Replication.JobManager.disable(uuid, job.job_id)
        end)

      _ ->
        :ok
    end
  end
end
