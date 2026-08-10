defmodule ElixirDB.Runtime.CatalogCloseAdmissionTest do
  @moduledoc """
  Plan §15: admission close commits before external services and aborted
  eligibility must not wedge admission in closing state.
  """
  use ExUnit.Case, async: false

  alias ElixirDB.Eventual
  alias ElixirDB.Replication.JobManager
  alias ElixirDB.Runtime.{AttachmentCoordinator, DatabaseAdmission, DatabaseCatalog}

  setup do
    prefix = "catalog-close-admission-#{System.unique_integer([:positive])}"
    a_path = prefix <> "-a.elixirdb"
    b_path = prefix <> "-b.elixirdb"
    root = ElixirDB.Config.database_root()

    for path <- [a_path, b_path] do
      ElixirDB.TempDatabase.cleanup(Path.join(root, path))
    end

    {:ok, a} = DatabaseCatalog.create(a_path)
    {:ok, b} = DatabaseCatalog.create(b_path)
    assert {:ok, _} = DatabaseCatalog.open(a.database_uuid)
    assert {:ok, _} = DatabaseCatalog.open(b.database_uuid)

    on_exit(fn ->
      for {identity, path} <- [{a, a_path}, {b, b_path}] do
        _ = DatabaseCatalog.close(identity.database_uuid)
        _ = DatabaseCatalog.unregister(identity.database_uuid)
        ElixirDB.TempDatabase.cleanup(Path.join(root, path))
      end
    end)

    {:ok, a_uuid: a.database_uuid, b_uuid: b.database_uuid}
  end

  test "close commits admission drain before attachment coordinator begins closing", %{
    a_uuid: uuid
  } do
    parent = self()

    blocker =
      Task.async(fn ->
        DatabaseAdmission.execute_owner(uuid, :foreground, fn ->
          send(parent, {:blocked, self()})
          receive(do: (:finish -> :done))
        end)
      end)

    assert_receive {:blocked, executor_pid}, 2_000

    closer = Task.async(fn -> DatabaseCatalog.close(uuid) end)

    Eventual.eventually(
      fn ->
        case DatabaseAdmission.closing?(uuid) do
          {:ok, true} ->
            case AttachmentCoordinator.status(uuid) do
              %{closing: false} -> :ok
              _ -> false
            end

          _ ->
            false
        end
      end,
      timeout: 5_000,
      message: "expected admission closing before attachment coordinator closing began"
    )

    assert {:error, %ElixirDB.Error{code: :database_closed}} =
             GenServer.call(
               DatabaseAdmission.via(uuid),
               {:acquire, make_ref(), :foreground, System.monotonic_time(:millisecond) + 5_000,
                :permit},
               2_000
             )

    send(executor_pid, :finish)
    assert :done = Task.await(blocker, 5_000)
    assert :ok = Task.await(closer, 10_000)
  end

  test "aborted close eligibility leaves admission open", %{a_uuid: a_uuid, b_uuid: b_uuid} do
    assert {:ok, %{job_id: job_id}} =
             JobManager.put(a_uuid, %{
               "persist" => true,
               "mode" => "continuous",
               "direction" => "push",
               "endpoint" => %{"kind" => "local", "database_uuid" => b_uuid},
               "enabled" => true,
               "wait_ms" => 100
             })

    Eventual.eventually(
      fn ->
        case JobManager.get(a_uuid, job_id) do
          {:ok, %{state: state}} when state in [:waiting, :backoff, :read_changes] -> true
          _ -> false
        end
      end,
      timeout: 5_000,
      message: "continuous replication job did not become active"
    )

    assert {:error, %ElixirDB.Error{code: :database_not_closable}} = DatabaseCatalog.close(a_uuid)
    assert {:ok, false} = DatabaseAdmission.closing?(a_uuid)

    assert :still_open =
             DatabaseAdmission.execute_owner(a_uuid, :foreground, fn -> :still_open end)

    assert {:ok, %{state: :disabled}} = JobManager.disable(a_uuid, job_id)
  end

  test "cancel_close after begin_close restores admission for new work", %{a_uuid: uuid} do
    assert :ok = DatabaseAdmission.begin_close(uuid)
    assert {:ok, true} = DatabaseAdmission.closing?(uuid)

    assert {:error, %ElixirDB.Error{code: :database_closed}} =
             DatabaseAdmission.execute_owner(uuid, :foreground, fn -> :never end)

    assert :ok = DatabaseAdmission.cancel_close(uuid)
    assert {:ok, false} = DatabaseAdmission.closing?(uuid)

    assert :still_open =
             DatabaseAdmission.execute_owner(uuid, :foreground, fn -> :still_open end)
  end
end
