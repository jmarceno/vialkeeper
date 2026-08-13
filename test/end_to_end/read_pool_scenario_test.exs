defmodule ElixirDB.EndToEnd.ReadPoolScenarioTest do
  @moduledoc """
  End-to-end HTTP composition for concurrent snapshot reads, a concurrent
  write, and portable close through the public API.

  The read-pool probes provide deterministic overlap evidence without relying
  on elapsed-time assertions.
  """
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :slow

  alias ElixirDB.Runtime.{DatabaseCatalog, ReadPool}
  alias ElixirDB.TestServer

  setup do
    previous_limits = Application.get_env(:elixir_db, :host_limits)

    limits =
      (previous_limits || [])
      |> Keyword.put(:read_pool_size, 2)
      |> Keyword.put(:read_queue_limit, 8)

    Application.put_env(:elixir_db, :host_limits, limits)

    on_exit(fn ->
      Application.delete_env(:elixir_db, :read_pool_sync)
      Application.delete_env(:elixir_db, :read_pool_probe)
      Application.put_env(:elixir_db, :host_limits, previous_limits)
    end)

    {:ok, server: TestServer.start_supervised!()}
  end

  @tag read_pool_e2e_overlap: true
  @tag timeout: 30_000
  test "HTTP gets overlap each other and a put", %{server: server} do
    %{uuid: uuid} = database = create_database!(server)
    on_exit(fn -> cleanup_database(database) end)

    assert 201 = put_document!(server, uuid, "doc", %{"n" => 1}).status

    probe = make_ref()
    gate = make_ref()
    Application.put_env(:elixir_db, :read_pool_probe, {self(), probe})
    Application.put_env(:elixir_db, :read_pool_sync, {self(), gate, uuid, :get_document})

    first = Task.async(fn -> get_document!(server, uuid, "doc") end)
    second = Task.async(fn -> get_document!(server, uuid, "doc") end)

    workers =
      for _ <- 1..2 do
        assert_receive {^gate, :before_begin, worker}, 5_000
        worker
      end

    assert_receive {^probe, :read_pool_grant, :foreground, :get_document}, 2_000
    assert_receive {^probe, :read_pool_grant, :foreground, :get_document}, 2_000
    refute_receive {^probe, :read_pool_release, :get_document}, 0

    assert 201 = put_document!(server, uuid, "written-during-reads", %{"ok" => true}).status

    Application.delete_env(:elixir_db, :read_pool_sync)
    Enum.each(workers, &send(&1, {:go, gate}))

    assert %{status: 200, body: %{"data" => %{"id" => "doc"}}} = Task.await(first, 10_000)
    assert %{status: 200, body: %{"data" => %{"id" => "doc"}}} = Task.await(second, 10_000)

    assert %{status: 200, body: %{"data" => %{"id" => "written-during-reads"}}} =
             get_document!(server, uuid, "written-during-reads")
  end

  @tag read_pool_e2e_close: true
  @tag timeout: 30_000
  test "HTTP close waits for active and queued gets and leaves a portable bundle", %{server: server} do
    %{uuid: uuid, absolute: absolute} = database = create_database!(server)
    on_exit(fn -> cleanup_database(database) end)

    assert 201 = put_document!(server, uuid, "doc", %{"n" => 1}).status

    gate = make_ref()
    Application.put_env(:elixir_db, :read_pool_sync, {self(), gate, uuid, :get_document})

    first = Task.async(fn -> get_document!(server, uuid, "doc") end)
    second = Task.async(fn -> get_document!(server, uuid, "doc") end)

    workers =
      for _ <- 1..2 do
        assert_receive {^gate, :before_begin, worker}, 5_000
        worker
      end

    third = Task.async(fn -> get_document!(server, uuid, "doc") end)

    ElixirDB.Eventual.eventually(
      fn ->
        match?({:ok, %{active: 2, queued: 1}}, ReadPool.stats(uuid))
      end,
      timeout: 5_000,
      message: "close composition requires two active reads and one queued read"
    )

    closer =
      Task.async(fn ->
        Req.post(server.base_url <> "/v1/databases/#{uuid}/close", json: %{})
      end)

    assert %{status: 503} = Task.await(third, 10_000)

    Application.delete_env(:elixir_db, :read_pool_sync)
    Enum.each(workers, &send(&1, {:go, gate}))

    assert %{status: 200} = Task.await(first, 10_000)
    assert %{status: 200} = Task.await(second, 10_000)
    assert {:ok, %{status: 200}} = Task.await(closer, 10_000)

    sqlite = ElixirDB.TempDatabase.sqlite_path(absolute)
    refute File.exists?(sqlite <> "-wal")
    refute File.exists?(sqlite <> "-shm")
  end

  defp create_database!(server) do
    relative = "read-pool-e2e-#{System.unique_integer([:positive])}.elixirdb"
    absolute = Path.join(ElixirDB.Config.database_root(), relative)
    ElixirDB.TempDatabase.cleanup(absolute)

    {:ok, response} =
      Req.post(server.base_url <> "/v1/databases", json: %{"path" => relative})

    assert response.status == 201
    %{uuid: response.body["data"]["database_uuid"], absolute: absolute}
  end

  defp put_document!(server, uuid, id, body) do
    {:ok, response} =
      Req.post(server.base_url <> "/v1/databases/#{uuid}/documents/put",
        json: %{"id" => id, "body" => body}
      )

    response
  end

  defp get_document!(server, uuid, id) do
    {:ok, response} =
      Req.post(server.base_url <> "/v1/databases/#{uuid}/documents/get",
        json: %{"id" => id}
      )

    %{status: response.status, body: response.body}
  end

  defp cleanup_database(%{uuid: uuid, absolute: absolute}) do
    _ = DatabaseCatalog.close(uuid)
    _ = DatabaseCatalog.unregister(uuid)
    ElixirDB.TempDatabase.cleanup(absolute)
  end
end
