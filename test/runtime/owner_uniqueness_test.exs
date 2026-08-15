defmodule VialKeeper.Runtime.OwnerUniquenessTest do
  @moduledoc "Covers single-owner lease enforcement for database bundles."

  use ExUnit.Case, async: false

  @moduletag :integration

  alias VialKeeper.Runtime.DatabaseCatalog
  alias VialKeeper.Storage.SQLite.Ownership

  setup do
    relative = "owner-#{System.unique_integer([:positive])}.vialkeeper"
    absolute = Path.join(VialKeeper.Config.database_root(), relative)
    VialKeeper.TempDatabase.cleanup(absolute)

    assert {:ok, identity} = DatabaseCatalog.create(relative)

    on_exit(fn ->
      _ = DatabaseCatalog.close(identity.database_uuid)
      _ = DatabaseCatalog.unregister(identity.database_uuid)
      VialKeeper.TempDatabase.cleanup(absolute)
    end)

    {:ok, identity: identity, absolute: absolute, relative: relative}
  end

  test "catalog open registers a single owner; second lease fails", %{
    identity: identity,
    absolute: absolute
  } do
    assert {:ok, _} = DatabaseCatalog.open(identity.database_uuid)

    assert [{owner_pid, _}] =
             Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:owner, identity.database_uuid})

    assert Process.alive?(owner_pid)

    # Unlinked start so a failed lease init does not exit the test process.
    sqlite_path = VialKeeper.TempDatabase.sqlite_path(absolute)

    assert {:error, %VialKeeper.Error{code: :database_in_use}} =
             GenServer.start(Ownership, sqlite_path)

    assert [{^owner_pid, _}] =
             Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:owner, identity.database_uuid})
  end

  test "duplicate UUID registration is rejected", %{
    identity: identity,
    absolute: absolute
  } do
    assert :ok = DatabaseCatalog.close(identity.database_uuid)

    copy = "owner-copy-#{System.unique_integer([:positive])}.vialkeeper"
    copy_abs = Path.join(VialKeeper.Config.database_root(), copy)
    File.cp_r!(absolute, copy_abs)

    on_exit(fn ->
      VialKeeper.TempDatabase.cleanup(copy_abs)
    end)

    assert {:error, %VialKeeper.Error{code: :duplicate_database_uuid}} =
             DatabaseCatalog.register(copy)

    assert {:ok, entries} = DatabaseCatalog.list()
    assert Enum.count(entries, &(&1.database_uuid == identity.database_uuid)) == 1
  end
end
