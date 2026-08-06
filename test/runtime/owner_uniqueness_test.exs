defmodule ElixirDB.Runtime.OwnerUniquenessTest do
  use ExUnit.Case, async: false

  alias ElixirDB.Runtime.{DatabaseCatalog, FileLease}

  setup do
    relative = "owner-#{System.unique_integer([:positive])}.db"
    absolute = Path.join(ElixirDB.Config.database_root(), relative)
    _ = File.rm(absolute)
    _ = File.rm(absolute <> ".lease")

    assert {:ok, identity} = DatabaseCatalog.create(relative)

    on_exit(fn ->
      _ = DatabaseCatalog.close(identity.database_uuid)
      _ = DatabaseCatalog.unregister(identity.database_uuid)
      _ = File.rm(absolute)
      _ = File.rm(absolute <> ".lease")
    end)

    {:ok, identity: identity, absolute: absolute, relative: relative}
  end

  test "catalog open registers a single owner; second lease fails", %{
    identity: identity,
    absolute: absolute
  } do
    assert {:ok, _} = DatabaseCatalog.open(identity.database_uuid)

    assert [{owner_pid, _}] =
             Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:owner, identity.database_uuid})

    assert Process.alive?(owner_pid)

    # Unlinked start so a failed lease init does not exit the test process.
    assert {:error, %ElixirDB.Error{code: :database_in_use}} =
             GenServer.start(FileLease, absolute)

    assert [{^owner_pid, _}] =
             Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:owner, identity.database_uuid})
  end

  test "duplicate UUID registration is rejected", %{
    identity: identity,
    absolute: absolute
  } do
    assert :ok = DatabaseCatalog.close(identity.database_uuid)

    copy = "owner-copy-#{System.unique_integer([:positive])}.db"
    copy_abs = Path.join(ElixirDB.Config.database_root(), copy)
    File.cp!(absolute, copy_abs)

    on_exit(fn ->
      _ = File.rm(copy_abs)
      _ = File.rm(copy_abs <> ".lease")
    end)

    assert {:error, %ElixirDB.Error{code: :duplicate_database_uuid}} =
             DatabaseCatalog.register(copy)

    assert {:ok, entries} = DatabaseCatalog.list()
    assert Enum.count(entries, &(&1.database_uuid == identity.database_uuid)) == 1
  end
end
