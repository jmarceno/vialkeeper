defmodule ElixirDB.Runtime.ReadPoolTest do
  @moduledoc """
  FIFO occupancy and overflow for the snapshot read pool: bounded capacity,
  overload errors, and close draining active snapshots before teardown.
  """
  use ExUnit.Case, async: false

  alias ElixirDB.Eventual
  alias ElixirDB.Runtime.{DatabaseCatalog, Deadline, ReadPool}
  alias ElixirDB.View.Manager

  setup do
    previous_limits = Application.get_env(:elixir_db, :host_limits)

    limits =
      (previous_limits || [])
      |> Keyword.put(:read_pool_size, 1)
      |> Keyword.put(:read_queue_limit, 1)

    Application.put_env(:elixir_db, :host_limits, limits)

    relative = "read-pool-#{System.unique_integer([:positive])}.elixirdb"
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

    {:ok, uuid: identity.database_uuid, absolute: absolute}
  end

  test "overflow at read_queue_limit is database_overloaded", %{uuid: uuid} do
    assert {:ok, _} = ElixirDB.Documents.put(uuid, %{id: "doc", body: %{"n" => 1}})

    gate = make_ref()
    Application.put_env(:elixir_db, :read_pool_sync, {self(), gate, uuid})

    holder =
      Task.async(fn ->
        ElixirDB.Documents.get(uuid, %{id: "doc"})
      end)

    assert_receive {^gate, :before_begin, worker}, 2_000
    assert is_pid(worker)

    Eventual.eventually(
      fn ->
        case ReadPool.stats(uuid) do
          {:ok, %{active: 1, queued: 0}} -> true
          _ -> false
        end
      end,
      timeout: 2_000,
      message: "expected one active snapshot before enqueue"
    )

    waiter =
      Task.async(fn ->
        ElixirDB.Documents.get(uuid, %{id: "doc"})
      end)

    Eventual.eventually(
      fn ->
        case ReadPool.stats(uuid) do
          {:ok, %{active: 1, queued: 1}} -> true
          _ -> false
        end
      end,
      timeout: 2_000,
      message: "occupancy must include the queued waiter"
    )

    assert {:error, %ElixirDB.Error{code: :database_overloaded}} =
             ElixirDB.Documents.get(uuid, %{id: "doc"})

    Application.delete_env(:elixir_db, :read_pool_sync)
    send(worker, {:go, gate})
    assert {:ok, %{body: %{"n" => 1}}} = Task.await(holder)
    assert {:ok, %{body: %{"n" => 1}}} = Task.await(waiter)
  end

  test "close drains snapshots and leaves no WAL sidecars", %{uuid: uuid, absolute: absolute} do
    assert {:ok, _} = ElixirDB.Documents.put(uuid, %{id: "doc", body: %{"n" => 1}})
    assert {:ok, _} = ElixirDB.Documents.get(uuid, %{id: "doc"})

    assert {:ok, %{workers: 1}} = ReadPool.stats(uuid)

    sqlite =
      ElixirDB.Storage.Registry.backend().artifact_path(absolute)

    [{runtime, _}] =
      Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:runtime, uuid})

    child_ids = Enum.map(Supervisor.which_children(runtime), &elem(&1, 0))

    assert {:read_pool_supervisor, uuid} in child_ids,
           "expected read pool supervisor child, got #{inspect(child_ids)}"

    assert :ok = DatabaseCatalog.close(uuid)

    refute ReadPool.enabled?(uuid)

    assert [] = Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:read_pool, uuid})
    assert [] = Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:read_worker, uuid, 1})

    refute File.exists?(sqlite <> "-wal"),
           "expected WAL sidecar gone after close"

    shm = sqlite <> "-shm"

    refute File.exists?(shm),
           "expected SHM sidecar gone after close, stat=#{inspect(File.stat(shm))}"
  end

  test "close fails queued readers then drains the active snapshot", %{uuid: uuid} do
    assert {:ok, _} = ElixirDB.Documents.put(uuid, %{id: "doc", body: %{"n" => 1}})

    gate = make_ref()
    Application.put_env(:elixir_db, :read_pool_sync, {self(), gate, uuid})

    holder =
      Task.async(fn ->
        ElixirDB.Documents.get(uuid, %{id: "doc"})
      end)

    assert_receive {^gate, :before_begin, worker}, 2_000

    waiter =
      Task.async(fn ->
        ElixirDB.Documents.get(uuid, %{id: "doc"})
      end)

    Eventual.eventually(
      fn ->
        case ReadPool.stats(uuid) do
          {:ok, %{active: 1, queued: 1}} -> true
          _ -> false
        end
      end,
      timeout: 2_000,
      message: "close drain requires a queued reader"
    )

    closer = Task.async(fn -> DatabaseCatalog.close(uuid) end)

    assert {:error, %ElixirDB.Error{code: :database_closed}} = Task.await(waiter, 5_000)

    send(worker, {:go, gate})
    assert {:ok, %{id: "doc"}} = Task.await(holder, 5_000)
    assert :ok = Task.await(closer, 10_000)
  end

  test "cancel_close after begin_close restores classified reads without close_readers", %{
    uuid: uuid
  } do
    assert {:ok, _} = ElixirDB.Documents.put(uuid, %{id: "doc", body: %{"n" => 1}})

    assert :ok = ReadPool.begin_close(uuid)
    assert {:ok, %{closing?: true}} = ReadPool.stats(uuid)

    assert {:error, %ElixirDB.Error{code: :database_closed}} =
             ReadPool.execute(
               uuid,
               :foreground,
               {:command, :get_document, %{document_id: "doc"}},
               Deadline.from_timeout(5_000)
             )

    assert :ok = ReadPool.cancel_close(uuid)
    assert {:ok, %{closing?: false}} = ReadPool.stats(uuid)

    assert {:ok, %{body: %{"n" => 1}}} = ElixirDB.Documents.get(uuid, %{id: "doc"})
  end
end
