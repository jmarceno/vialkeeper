defmodule ElixirDB.Runtime.OwnerCrashTest do
  @moduledoc """
  Gap D3: owner crash and supervised restart.

  Killing `DatabaseOwner` with `:kill` must restart under `DatabaseRuntimeSupervisor`
  (`:rest_for_one`). After close, the file lease is released and the DB is usable again.
  """
  use ExUnit.Case, async: false

  alias ElixirDB.Runtime.{DatabaseCatalog, FileLease}

  setup do
    relative = "owner-crash-#{System.unique_integer([:positive])}.db"
    absolute = Path.join(ElixirDB.Config.database_root(), relative)
    ElixirDB.TempDatabase.cleanup(absolute)

    assert {:ok, identity} = DatabaseCatalog.create(relative)
    uuid = identity.database_uuid

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(absolute)
    end)

    {:ok, uuid: uuid, absolute: absolute}
  end

  test "owner kill restarts under supervisor; close releases lease; DB usable again", %{
    uuid: uuid,
    absolute: absolute
  } do
    assert {:ok, _} = DatabaseCatalog.open(uuid)

    assert [{owner_before, _}] =
             Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:owner, uuid})

    assert [{runtime, _}] =
             Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:runtime, uuid})

    lease_pid = file_lease_pid(runtime)
    assert is_pid(lease_pid)
    assert Process.alive?(owner_before)
    assert Process.alive?(lease_pid)

    assert {:error, %ElixirDB.Error{code: :database_in_use}} =
             GenServer.start(FileLease, absolute)

    assert {:ok, %{revision: revision}} =
             ElixirDB.Documents.put(uuid, %{id: "crash-doc", body: %{"n" => 1}})

    ref = Process.monitor(owner_before)
    Process.exit(owner_before, :kill)
    assert_receive {:DOWN, ^ref, :process, ^owner_before, :killed}, 2_000

    owner_after =
      ElixirDB.Eventual.eventually(
        fn ->
          case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:owner, uuid}) do
            [{pid, _}] when pid != owner_before ->
              if Process.alive?(pid), do: {:ok, pid}, else: false

            _ ->
              false
          end
        end,
        timeout: 5_000,
        message: "DatabaseOwner did not restart after kill"
      )

    assert owner_after != owner_before
    assert Process.alive?(lease_pid)

    # Admission restarts with the owner under :rest_for_one; wait until commands work.
    ElixirDB.Eventual.eventually(
      fn ->
        case ElixirDB.Documents.get(uuid, %{id: "crash-doc"}) do
          {:ok, %{revision: ^revision, body: %{"n" => 1}}} -> true
          _ -> false
        end
      end,
      timeout: 5_000,
      message: "document not readable after owner restart"
    )

    assert {:ok, %{revision: _}} =
             ElixirDB.Documents.put(uuid, %{
               id: "crash-doc",
               if_revision: revision,
               body: %{"n" => 2}
             })

    assert :ok = DatabaseCatalog.close(uuid)

    assert [] = Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:owner, uuid})
    refute Process.alive?(lease_pid)

    assert {:ok, lease} = GenServer.start(FileLease, absolute)
    assert :ok = GenServer.stop(lease)

    assert {:ok, _} = DatabaseCatalog.open(uuid)

    assert {:ok, %{body: %{"n" => 2}}} =
             ElixirDB.Documents.get(uuid, %{id: "crash-doc"})
  end

  defp file_lease_pid(runtime) do
    runtime
    |> Supervisor.which_children()
    |> Enum.find_value(fn
      {FileLease, pid, :worker, _} when is_pid(pid) -> pid
      _ -> nil
    end)
  end
end
