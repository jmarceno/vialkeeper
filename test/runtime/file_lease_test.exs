defmodule ElixirDB.Runtime.FileLeaseTest do
  @moduledoc """
  Gap D3: ownership lease exclusion (`database_in_use`).

  True cross-OS-process lease exclusion requires two OS processes holding the
  same companion `.lease` SQLite EXCLUSIVE lock. Within one BEAM VM we still
  prove acquire / exclusion / release / re-acquire using `FileLease` GenServers.
  """
  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.Runtime.FileLease
  alias ElixirDB.Storage.SQLite.Adapter

  setup do
    {:ok, bundle_path} = ElixirDB.TempDatabase.create(prefix: "elixirdb-lease")
    sqlite_path = ElixirDB.TempDatabase.sqlite_path(bundle_path)
    {:ok, adapter} = Adapter.create(sqlite_path, %{})
    :ok = Adapter.close(adapter)

    on_exit(fn ->
      ElixirDB.TempDatabase.cleanup(bundle_path)
    end)

    {:ok, path: sqlite_path, bundle_path: bundle_path}
  end

  test "acquire succeeds, second holder gets database_in_use, release allows re-acquire", %{
    path: path
  } do
    assert {:ok, first} = GenServer.start(FileLease, path)
    assert Process.alive?(first)

    assert {:error, %ElixirDB.Error{code: :database_in_use}} =
             GenServer.start(FileLease, path)

    assert Process.alive?(first)
    assert :ok = GenServer.stop(first)
    refute Process.alive?(first)

    assert {:ok, second} = GenServer.start(FileLease, path)
    assert Process.alive?(second)
    assert :ok = GenServer.stop(second)
  end

  test "catalog open holds the lease so a raw FileLease cannot steal ownership" do
    relative = "lease-catalog-#{System.unique_integer([:positive])}.elixirdb"
    absolute = Path.join(ElixirDB.Config.database_root(), relative)
    ElixirDB.TempDatabase.cleanup(absolute)

    assert {:ok, identity} = DatabaseCatalog.create(relative)

    on_exit(fn ->
      _ = DatabaseCatalog.close(identity.database_uuid)
      _ = DatabaseCatalog.unregister(identity.database_uuid)
      ElixirDB.TempDatabase.cleanup(absolute)
    end)

    assert {:ok, _} = DatabaseCatalog.open(identity.database_uuid)

    sqlite_path = ElixirDB.TempDatabase.sqlite_path(absolute)

    assert {:error, %ElixirDB.Error{code: :database_in_use}} =
             GenServer.start(FileLease, sqlite_path)

    assert :ok = DatabaseCatalog.close(identity.database_uuid)

    assert {:ok, lease} = GenServer.start(FileLease, sqlite_path)
    assert :ok = GenServer.stop(lease)
  end
end
