defmodule VialKeeper.Manual.ContainerReplicationTest do
  @moduledoc """
  Opt-in replication drill across three release containers.

  Run it directly:

      mix test.container_replication
      mix test.container_replication --burst 48
      mix test.container_replication --duration 3600 --burst 48

  `mix test`, `mix check.fast`, `mix check.integration`, and `mix check.full`
  exclude this module. The body also refuses to run unless
  `mix test.container_replication` armed `VIAL_KEEPER_CONTAINER_REPLICATION`.
  """

  use ExUnit.Case, async: false

  @moduletag :container_replication

  alias VialKeeper.Eventual
  alias VialKeeper.TestSupport.ContainerReplicationCluster

  @duration_seconds System.get_env("VIAL_KEEPER_CONTAINER_REPLICATION_DURATION", "0")
                    |> String.to_integer()
  @tag timeout: 900_000 + @duration_seconds * 1_000
  test "three containers replicate through network loss, restart, and a burst" do
    assert System.get_env("VIAL_KEEPER_CONTAINER_REPLICATION") == "1",
           "run this drill with mix test.container_replication"

    cluster = ContainerReplicationCluster.start!()
    assert Enum.count_until(cluster.nodes, 3) == 3

    nodes = Enum.map(cluster.nodes, &await_and_create!(cluster, &1))
    start_mesh!(cluster, nodes)

    started = System.monotonic_time(:millisecond)
    run_rounds!(cluster, nodes, burst_count(), started, 1, [])
  end

  defp run_rounds!(cluster, nodes, count, started, round, previous_docs) do
    {seed_ms, seed} = replicate_seed!(cluster, nodes, round)
    second = replicate_from_node!(cluster, nodes, "n2", round)
    {partition_ms, partition} = partition_and_recover!(cluster, nodes, round)
    {restart_ms, restart} = stop_and_recover!(cluster, nodes, round)
    resumed = replicate_from_node!(cluster, nodes, "n3", round)
    phase_docs = [seed, second, partition, restart, resumed]
    converge_burst!(cluster, nodes, previous_docs ++ phase_docs)
    burst = burst!(cluster, nodes, count, round)
    assert_integrity!(cluster, nodes)
    elapsed = System.monotonic_time(:millisecond) - started

    IO.puts(
      "[container-replication] round=#{round} elapsed_ms=#{elapsed} " <>
        "nodes=#{length(nodes)} seed_ms=#{seed_ms} " <>
        "partition_catch_up_ms=#{partition_ms} restart_catch_up_ms=#{restart_ms} " <>
        "burst=#{burst.count} write_ms=#{burst.write_ms} docs_per_s=#{burst.docs_per_s} " <>
        "converge_ms=#{burst.converge_ms}"
    )

    if elapsed < @duration_seconds * 1_000 do
      run_rounds!(cluster, nodes, count, started, round + 1, phase_docs ++ burst.docs)
    end
  end

  defp await_and_create!(cluster, node) do
    await_ready!(cluster, node)
    uuid = create_database!(node, cluster.token)
    Map.put(node, :database_uuid, uuid)
  end

  defp await_ready!(cluster, node) do
    result =
      Eventual.await(
        fn ->
          case Req.get(node.base_url <> "/v1/databases", request_opts(cluster.token, 2_000)) do
            {:ok, %{status: 200}} -> true
            _ -> false
          end
        end,
        timeout: 180_000,
        interval: 500
      )

    assert result == :ok, ready_message(cluster, node)
  end

  defp ready_message(cluster, node) do
    logs =
      cluster
      |> ContainerReplicationCluster.logs(node)
      |> String.slice(-2_000, 2_000)

    "container #{node.name} did not become ready\n#{logs}"
  end

  defp create_database!(node, token) do
    assert {:ok, %{status: 201, body: %{"data" => %{"database_uuid" => uuid}}}} =
             Req.post(
               node.base_url <> "/v1/databases",
               [json: %{"path" => "#{node.name}.vialkeeper"}] ++ request_opts(token, 10_000)
             )

    uuid
  end

  defp start_mesh!(cluster, nodes) do
    pairs =
      for source <- nodes, target <- nodes, source.name != target.name do
        {source, target}
      end

    Enum.each(pairs, fn {source, target} ->
      assert {:ok, %{status: status, body: %{"data" => %{"job_id" => job_id}}}} =
               Req.post(
                 source.base_url <> "/v1/databases/#{source.database_uuid}/replications",
                 [
                   json: %{
                     "persist" => true,
                     "mode" => "continuous",
                     "direction" => "push",
                     "enabled" => true,
                     "wait_ms" => 100,
                     "retry" => %{
                       "max_attempts" => 32,
                       "base_delay_ms" => 50,
                       "max_delay_ms" => 500,
                       "jitter_ms" => 10
                     },
                     "endpoint" => %{
                       "kind" => "remote",
                       "database_uuid" => target.database_uuid,
                       "base_url" => target.peer_base_url,
                       "auth_token" => cluster.token
                     }
                   }
                 ] ++ request_opts(cluster.token, 10_000)
               )

      assert status in [200, 201]
      assert is_binary(job_id)
    end)
  end

  defp replicate_seed!(cluster, nodes, round) do
    source = node!(nodes, "n1")
    id = document_id(round, "seed")
    body = %{"phase" => "seed", "round" => round}
    started = System.monotonic_time(:millisecond)
    revision = put_document!(cluster, source, id, body)
    wait_peers!(cluster, nodes, source, id, revision, body, 90_000)
    {System.monotonic_time(:millisecond) - started, {id, revision, body}}
  end

  defp replicate_from_node!(cluster, nodes, name, round) do
    source = node!(nodes, name)
    id = document_id(round, "from-#{name}")
    body = %{"phase" => name, "round" => round}
    revision = put_document!(cluster, source, id, body)
    wait_peers!(cluster, nodes, source, id, revision, body, 90_000)
    {id, revision, body}
  end

  defp partition_and_recover!(cluster, nodes, round) do
    isolated = node!(nodes, "n2")
    writer = node!(nodes, "n1")
    observer = node!(nodes, "n3")
    :ok = ContainerReplicationCluster.isolate!(cluster, isolated)

    id = document_id(round, "during-partition")
    body = %{"phase" => "partition", "round" => round}
    revision = put_document!(cluster, writer, id, body)
    wait_document!(cluster, observer, id, revision, body, 90_000)
    refute_while_isolated!(cluster, isolated, id)

    :ok = ContainerReplicationCluster.rejoin!(cluster, isolated)
    started = System.monotonic_time(:millisecond)
    wait_document!(cluster, isolated, id, revision, body, 120_000)
    {System.monotonic_time(:millisecond) - started, {id, revision, body}}
  end

  defp stop_and_recover!(cluster, nodes, round) do
    stopped = node!(nodes, "n3")
    writer = node!(nodes, "n1")
    peer = node!(nodes, "n2")
    :ok = ContainerReplicationCluster.stop_container!(cluster, stopped)

    id = document_id(round, "during-stop")
    body = %{"phase" => "restart", "round" => round}
    revision = put_document!(cluster, writer, id, body)
    wait_document!(cluster, peer, id, revision, body, 90_000)

    assert {:error, _} =
             Req.get(stopped.base_url <> "/v1/databases", request_opts(cluster.token, 2_000))

    :ok = ContainerReplicationCluster.start_container!(cluster, stopped)
    await_ready!(cluster, stopped)
    started = System.monotonic_time(:millisecond)
    wait_document!(cluster, stopped, id, revision, body, 180_000)
    {System.monotonic_time(:millisecond) - started, {id, revision, body}}
  end

  defp burst!(cluster, nodes, count, round) do
    source = node!(nodes, "n1")
    started = System.monotonic_time(:millisecond)

    docs =
      Enum.map(1..count, fn index ->
        id = document_id(round, "burst-#{index}")
        body = %{"kind" => "burst", "i" => index, "round" => round}
        revision = put_document!(cluster, source, id, body)
        {id, revision, body}
      end)

    write_ms = max(System.monotonic_time(:millisecond) - started, 1)
    converge_started = System.monotonic_time(:millisecond)
    converge_burst!(cluster, nodes, docs)

    converge_ms = System.monotonic_time(:millisecond) - converge_started
    docs_per_s = Float.round(count * 1_000 / write_ms, 1)
    assert docs_per_s > 0
    assert converge_ms >= 0

    %{
      count: count,
      write_ms: write_ms,
      docs_per_s: docs_per_s,
      converge_ms: converge_ms,
      docs: docs
    }
  end

  defp converge_burst!(cluster, nodes, docs) do
    deadline = System.monotonic_time(:millisecond) + 180_000
    Enum.each(nodes, &converge_docs!(cluster, &1, docs, deadline))
  end

  defp converge_docs!(cluster, node, docs, deadline) do
    Enum.each(docs, fn {id, revision, body} ->
      timeout = deadline - System.monotonic_time(:millisecond)
      assert timeout > 0, "batch convergence exceeded 180000ms on #{node.name} at #{id}"
      wait_document!(cluster, node, id, revision, body, timeout)
    end)
  end

  defp assert_integrity!(cluster, nodes) do
    Enum.each(nodes, fn node ->
      assert {:ok, %{status: 200, body: %{"data" => %{"ok" => true}}}} =
               Req.post(
                 node.base_url <> "/v1/databases/#{node.database_uuid}/integrity-check",
                 [json: %{}] ++ request_opts(cluster.token, 30_000)
               )
    end)
  end

  defp put_document!(cluster, node, id, body) do
    assert {:ok, %{status: 201, body: %{"data" => %{"revision" => revision}}}} =
             Req.post(
               node.base_url <> "/v1/databases/#{node.database_uuid}/documents/put",
               [json: %{"id" => id, "body" => body}] ++ request_opts(cluster.token, 10_000)
             )

    assert is_binary(revision)
    revision
  end

  defp wait_peers!(cluster, nodes, source, id, revision, body, timeout) do
    Enum.each(nodes, fn node ->
      if node.name != source.name do
        wait_document!(cluster, node, id, revision, body, timeout)
      end
    end)
  end

  defp wait_document!(cluster, node, id, revision, body, timeout) do
    Eventual.eventually(
      fn ->
        case fetch_document(cluster, node, id) do
          {:ok, %{"revision" => ^revision, "body" => ^body}} -> true
          _ -> false
        end
      end,
      timeout: timeout,
      interval: 200,
      message: "document #{id} did not converge on #{node.name}"
    )
  end

  defp refute_while_isolated!(cluster, node, id) do
    result =
      Eventual.await(
        fn ->
          case fetch_document(cluster, node, id) do
            :missing -> false
            {:ok, data} -> {:leaked, data}
            {:error, reason} -> {:observer_down, reason}
          end
        end,
        timeout: 5_000,
        interval: 200
      )

    assert result == :timeout, isolated_message(node, id, result)
  end

  defp isolated_message(node, id, result) do
    "document #{id} was visible on #{node.name} while its container network was off: #{inspect(result)}"
  end

  defp fetch_document(cluster, node, id) do
    case Req.post(
           node.base_url <> "/v1/databases/#{node.database_uuid}/documents/get",
           [json: %{"id" => id}] ++ request_opts(cluster.token, 5_000)
         ) do
      {:ok, %{status: 200, body: %{"data" => data}}} ->
        {:ok, data}

      {:ok, %{status: 404, body: %{"error" => %{"code" => "document_not_found"}}}} ->
        :missing

      {:ok, response} ->
        {:error, {:http, response.status, response.body}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp request_opts(token, timeout) do
    [
      headers: [{"authorization", "Bearer " <> token}],
      retry: false,
      receive_timeout: timeout,
      connect_options: [timeout: min(timeout, 5_000)]
    ]
  end

  defp node!(nodes, name) do
    Enum.find(nodes, &(&1.name == name)) || flunk("missing container #{name}")
  end

  defp document_id(round, suffix), do: "round-#{round}-#{suffix}"

  defp burst_count do
    case System.get_env("VIAL_KEEPER_CONTAINER_REPLICATION_BURST") do
      nil ->
        24

      raw ->
        case Integer.parse(raw) do
          {count, ""} when count > 0 ->
            count

          _ ->
            flunk("VIAL_KEEPER_CONTAINER_REPLICATION_BURST must be a positive integer")
        end
    end
  end
end
