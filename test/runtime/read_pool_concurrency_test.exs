defmodule ElixirDB.Runtime.ReadPoolConcurrencyTest do
  @moduledoc """
  Concurrent snapshot reads overlap each other and a concurrent writer.
  """
  use ExUnit.Case, async: false

  alias ElixirDB.Eventual
  alias ElixirDB.Runtime.{DatabaseCatalog, Deadline}
  alias ElixirDB.View.Manager

  setup do
    previous_limits = Application.get_env(:elixir_db, :host_limits)

    limits =
      (previous_limits || [])
      |> Keyword.put(:read_pool_size, 2)
      |> Keyword.put(:read_queue_limit, 8)

    Application.put_env(:elixir_db, :host_limits, limits)

    relative = "read-pool-conc-#{System.unique_integer([:positive])}.elixirdb"
    absolute = Path.join(ElixirDB.Config.database_root(), relative)
    ElixirDB.TempDatabase.cleanup(absolute)

    assert {:ok, identity} = DatabaseCatalog.create(relative)
    assert {:ok, _} = DatabaseCatalog.open(identity.database_uuid)
    assert :ok = Manager.await_resumed(identity.database_uuid)

    on_exit(fn ->
      Application.delete_env(:elixir_db, :read_pool_sync)
      Application.delete_env(:elixir_db, :read_pool_probe)
      Application.put_env(:elixir_db, :host_limits, previous_limits)
      _ = DatabaseCatalog.close(identity.database_uuid)
      _ = DatabaseCatalog.unregister(identity.database_uuid)
      ElixirDB.TempDatabase.cleanup(absolute)
    end)

    {:ok, uuid: identity.database_uuid}
  end

  test "two concurrent gets grant before either release while a put completes", %{uuid: uuid} do
    assert {:ok, %{revision: rev}} =
             ElixirDB.Documents.put(uuid, %{id: "doc", body: %{"n" => 1}})

    probe = make_ref()
    gate = make_ref()
    Application.put_env(:elixir_db, :read_pool_probe, {self(), probe})
    Application.put_env(:elixir_db, :read_pool_sync, {self(), gate, uuid})

    first = Task.async(fn -> ElixirDB.Documents.get(uuid, %{id: "doc"}) end)
    second = Task.async(fn -> ElixirDB.Documents.get(uuid, %{id: "doc"}) end)

    workers =
      for _ <- 1..2 do
        assert_receive {^gate, :before_begin, worker}, 2_000
        worker
      end

    grants = drain_grants(probe)
    releases = drain_releases(probe)

    assert Enum.count_until(grants, 2) == 2,
           "expected two read-pool grants before either snapshot finished, got #{inspect(grants)}"

    assert releases == [],
           "no snapshot should release before the sync gate opens, got #{inspect(releases)}"

    assert {:ok, %{revision: later}} =
             ElixirDB.Documents.put(uuid, %{id: "doc", if_revision: rev, body: %{"n" => 2}})

    assert later != rev

    Enum.each(workers, &send(&1, {:go, gate}))

    assert {:ok, %{id: "doc"}} = Task.await(first)
    assert {:ok, %{id: "doc"}} = Task.await(second)

    Eventual.eventually(
      fn ->
        releases = drain_releases(probe)
        Enum.count_until(releases, 2) == 2
      end,
      timeout: 2_000,
      message: "both snapshots should release after the gate opens"
    )
  end

  test "get issued after put return sees the put", %{uuid: uuid} do
    assert {:ok, %{revision: rev}} =
             ElixirDB.Documents.put(uuid, %{id: "seen", body: %{"v" => "after"}})

    assert {:ok, %{revision: ^rev, body: %{"v" => "after"}}} =
             ElixirDB.Documents.get(uuid, %{id: "seen"})
  end

  test "expired direct pool request returns a deadline error and reuses its reader", %{uuid: uuid} do
    assert {:ok, _} = ElixirDB.Documents.put(uuid, %{id: "doc", body: %{"n" => 1}})

    gate = make_ref()
    request_ref = make_ref()
    Application.put_env(:elixir_db, :read_pool_owner_body_sync, {self(), gate, uuid})

    [{pool, _}] = Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:read_pool, uuid})

    caller =
      Task.async(fn ->
        GenServer.call(
          pool,
          {:execute, request_ref, :foreground, {:command, :get_document, %{document_id: "doc"}},
           Deadline.from_timeout(25), OpenTelemetry.Ctx.get_current()},
          5_000
        )
      end)

    assert_receive {^gate, :owner_body, worker}, 2_000
    timer = Process.send_after(self(), :deadline_probe, 40)
    assert_receive :deadline_probe, 1_000
    Process.cancel_timer(timer)
    send(worker, {:go, gate})

    assert {:error,
            %ElixirDB.Error{
              code: :internal_error,
              details: %{reason: :deadline_exhausted},
              retryable: true
            }} =
             Task.await(caller, 5_000)

    Application.delete_env(:elixir_db, :read_pool_owner_body_sync)
    assert {:ok, %{id: "doc"}} = ElixirDB.Documents.get(uuid, %{id: "doc"})
  end

  test "cancelled direct pool request does not reply and interrupts its reader", %{uuid: uuid} do
    assert {:ok, _} = ElixirDB.Documents.put(uuid, %{id: "doc", body: %{"n" => 1}})

    gate = make_ref()
    request_ref = make_ref()
    parent = self()
    Application.put_env(:elixir_db, :read_pool_owner_body_sync, {self(), gate, uuid})
    [{pool, _}] = Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:read_pool, uuid})

    pid =
      spawn(fn ->
        result =
          GenServer.call(
            pool,
            {:execute, request_ref, :foreground, {:command, :get_document, %{document_id: "doc"}},
             :infinity, OpenTelemetry.Ctx.get_current()},
            5_000
          )

        send(parent, {:cancelled_pool_result, result})
      end)

    assert_receive {^gate, :owner_body, worker}, 2_000
    assert :ok = GenServer.call(pool, {:cancel, request_ref})
    send(worker, {:go, gate})
    refute_receive {:cancelled_pool_result, _}, 500

    Process.exit(pid, :kill)
    Application.delete_env(:elixir_db, :read_pool_owner_body_sync)
    assert {:ok, %{id: "doc"}} = ElixirDB.Documents.get(uuid, %{id: "doc"})
  end

  defp drain_grants(ref, acc \\ []) do
    receive do
      {^ref, :read_pool_grant, class, op} -> drain_grants(ref, [{class, op} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp drain_releases(ref, acc \\ []) do
    receive do
      {^ref, :read_pool_release, op} -> drain_releases(ref, [op | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
