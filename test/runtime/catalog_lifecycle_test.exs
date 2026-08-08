defmodule ElixirDB.Runtime.CatalogLifecycleTest do
  use ExUnit.Case, async: false

  alias ElixirDB.DatabaseBundle
  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.Storage.SQLite.Adapter

  setup do
    prefix = "uuid-mismatch-#{System.unique_integer([:positive])}"
    registered_path = prefix <> "-registered.elixirdb"
    replacement_path = prefix <> "-replacement.elixirdb"
    root = ElixirDB.Config.database_root()

    for path <- [registered_path, replacement_path] do
      ElixirDB.TempDatabase.cleanup(Path.join(root, path))
    end

    {:ok, registered} = DatabaseCatalog.create(registered_path)
    uuid = registered.database_uuid

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(Path.join(root, registered_path))
      ElixirDB.TempDatabase.cleanup(Path.join(root, replacement_path))
    end)

    {:ok,
     uuid: uuid, registered_path: registered_path, replacement_path: replacement_path, root: root}
  end

  test "swapped database file UUID marks registration unavailable", %{
    uuid: uuid,
    registered_path: registered_path,
    replacement_path: replacement_path,
    root: root
  } do
    registered_bundle = Path.join(root, registered_path)
    replacement_bundle = Path.join(root, replacement_path)
    registered_sqlite = ElixirDB.TempDatabase.sqlite_path(registered_bundle)
    replacement_sqlite = ElixirDB.TempDatabase.sqlite_path(replacement_bundle)

    assert :ok = DatabaseCatalog.close(uuid)

    assert {:ok, bundle} = DatabaseBundle.create(replacement_bundle)
    {:ok, other} = Adapter.create(DatabaseBundle.sqlite_path(bundle))
    {:ok, other_identity} = Adapter.identity(other)
    :ok = Adapter.close(other)
    assert other_identity.database_uuid != uuid

    File.cp!(replacement_sqlite, registered_sqlite)

    assert {:error, %ElixirDB.Error{code: :database_unavailable, details: details}} =
             DatabaseCatalog.open(uuid)

    assert details.reason == :uuid_mismatch
    assert details.expected == uuid
    assert details.actual == other_identity.database_uuid

    assert {:ok, entries} = DatabaseCatalog.list()
    entry = Enum.find(entries, &(&1.database_uuid == uuid))
    assert entry.state == :unavailable

    assert [] = Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:owner, uuid})
  end
end
