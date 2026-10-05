defmodule VialKeeper.Manual.ContainerReplicationTest do
  @moduledoc """
  Opt-in replication drill across three release containers.

  Run it directly:

      mix test.container_replication
      mix test.container_replication --burst 48

  `mix test`, `mix check.fast`, `mix check.integration`, and `mix check.full`
  exclude this module. The body also refuses to run unless
  `mix test.container_replication` armed `VIAL_KEEPER_CONTAINER_REPLICATION`.
  """

  use ExUnit.Case, async: false

  @moduletag :container_replication

  alias VialKeeper.Eventual
  alias VialKeeper.TestSupport.ContainerReplicationCluster

  @tag timeout: 900_000
  test "three containers replicate through network loss, restart, and a burst" do
    assert System.get_env("VIAL_KEEPER_CONTAINER_REPLICATION") == "1",
           "run this drill with mix test.container_replication"

    cluster = ContainerReplicationCluster.start!()
    assert Enum.count_until(cluster.nodes, 3) == 3

    nodes = Enum.map(cluster.nodes, &await_and_create!(cluster, &1))
    start_mesh!(cluster, nodes)

    seed_ms = replicate_seed!(cluster, nodes)
    replicate_from_second!(cluster, nodes)
    partition_ms = partition_and_recover!(cluster, nodes)
    restart_ms = stop_and_recover!(cluster, nodes)
    burst = burst!(cluster, nodes, burst_count())
    assert_integrity!(cluster, nodes)

    IO.puts(
      "[container-replication] nodes=#{length(nodes)} seed_ms=#{seed_ms} " <>
        "partition_catch_up_ms=#{partition_ms} restart_catch_up_ms=#{restart_ms} " <>
        "burst=#{burst.count} write_ms=#{burst.write_ms} docs_per_s=#{burst.docs_per_s} " <>
        "converge_ms=#{burst.converge_ms}"
    )
  end

  defp await_and_create!(cluster, node) do
    await_ready!(cluster, node)
    uuid = create_database!(node, cluster.token)
    Map.put(node, :database_uuid, uuid)
  end

  defp await_ready!(cluster, node) do
    Eventual.eventually(
      fn ->
        case Req.get(node.base_url <> "/v1/databases", request_opts(cluster.token, 2_000)) do
          {:ok, %{status: 200}} -> true
          _ -> false
        end
      end,
      timeout: 180_000,
      interval: 500,
      message: ready_message(cluster, node)
    )
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

  defp replicate_seed!(cluster, nodes) do
    source = node!(nodes, "n1")
    body = %{"phase" => "seed", "n" => 1}
    started = System.monotonic_time(:millisecond)
    revision = put_document!(cluster, source, "seed", body)
    wait_peers!(cluster, nodes, source, "seed", revision, body, 90_000)
    System.monotonic_time(:millisecond) - started
  end

  defp replicate_from_second!(cluster, nodes) do
    source = node!(nodes, "n2")
    body = %{"phase" => "second", "n" => 2}
    revision = put_document!(cluster, source, "from-n2", body)
    wait_peers!(cluster, nodes, source, "from-n2", revision, body, 90_000)
  end

  defp partition_and_recover!(cluster, nodes) do
    isolated = node!(nodes, "n2")
    writer = node!(nodes, "n1")
    observer = node!(nodes, "n3")
    :ok = ContainerReplicationCluster.isolate!(cluster, isolated)

    body = %{"phase" => "partition", "n" => 3}
    revision = put_document!(cluster, writer, "during-partition", body)
    wait_document!(cluster, observer, "during-partition", revision, body, 90_000)
    refute_while_isolated!(cluster, isolated, "during-partition")

    :ok = ContainerReplicationCluster.rejoin!(cluster, isolated)
    started = System.monotonic_time(:millisecond)
    wait_document!(cluster, isolated, "during-partition", revision, body, 120_000)
    System.monotonic_time(:millisecond) - started
  end

  defp stop_and_recover!(cluster, nodes) do
    stopped = node!(nodes, "n3")
    writer = node!(nodes, "n1")
    peer = node!(nodes, "n2")
    :ok = ContainerReplicationCluster.stop_container!(cluster, stopped)

    body = %{"phase" => "restart", "n" => 4}
    revision = put_document!(cluster, writer, "during-stop", body)
    wait_document!(cluster, peer, "during-stop", revision, body, 90_000)

    assert {:error, _} =
             Req.get(stopped.base_url <> "/v1/databases", request_opts(cluster.token, 2_000))

    :ok = ContainerReplicationCluster.start_container!(cluster, stopped)
    await_ready!(cluster, stopped)
    started = System.monotonic_time(:millisecond)
    wait_document!(cluster, stopped, "during-stop", revision, body, 180_000)
    System.monotonic_time(:millisecond) - started
  end

  defp burst!(cluster, nodes, count) do
    source = node!(nodes, "n1")
    started = System.monotonic_time(:millisecond)

    docs =
      Enum.map(1..count, fn index ->
        id = "burst-" <> String.pad_leading(Integer.to_string(index), 4, "0")
        body = %{"kind" => "burst", "i" => index}
        revision = put_document!(cluster, source, id, body)
        {id, revision, body}
      end)

    write_ms = max(System.monotonic_time(:millisecond) - started, 1)
    converge_started = System.monotonic_time(:millisecond)
    converge_burst!(cluster, nodes, source, docs)

    converge_ms = System.monotonic_time(:millisecond) - converge_started
    docs_per_s = Float.round(count * 1_000 / write_ms, 1)
    assert docs_per_s > 0
    assert converge_ms >= 0

    %{count: count, write_ms: write_ms, docs_per_s: docs_per_s, converge_ms: converge_ms}
  end

  defp converge_burst!(cluster, nodes, source, docs) do
    nodes
    |> Enum.reject(&(&1.name == source.name))
    |> Enum.each(&converge_docs!(cluster, &1, docs))
  end

  defp converge_docs!(cluster, node, docs) do
    Enum.each(docs, fn {id, revision, body} ->
      wait_document!(cluster, node, id, revision, body, 180_000)
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
