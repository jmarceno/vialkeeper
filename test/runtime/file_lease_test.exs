defmodule ElixirDB.Runtime.FileLeaseTest do
  @moduledoc """
  Gap D3: ownership lease exclusion (`database_in_use`).

  True cross-OS-process lease exclusion requires two OS processes holding the
  same companion `.lease` SQLite EXCLUSIVE lock. Within one BEAM VM we still
  prove acquire / exclusion / release / re-acquire using `FileLease` GenServers.
  """
  use ExUnit.Case, async: false

  alias ElixirDB.Runtime.FileLease
  alias ElixirDB.Storage.SQLite.Adapter

  setup do
    {:ok, path} = ElixirDB.TempDatabase.create(prefix: "elixirdb-lease")
    {:ok, adapter} = Adapter.create(path, %{})
    :ok = Adapter.close(adapter)

    on_exit(fn ->
      ElixirDB.TempDatabase.cleanup(path)
    end)

    {:ok, path: path}
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
    relative = "lease-catalog-#{System.unique_integer([:positive])}.db"
    absolute = Path.join(ElixirDB.Config.database_root(), relative)
    ElixirDB.TempDatabase.cleanup(absolute)

    assert {:ok, identity} = ElixirDB.Runtime.DatabaseCatalog.create(relative)

    on_exit(fn ->
      _ = ElixirDB.Runtime.DatabaseCatalog.close(identity.database_uuid)
      _ = ElixirDB.Runtime.DatabaseCatalog.unregister(identity.database_uuid)
      ElixirDB.TempDatabase.cleanup(absolute)
    end)

    assert {:ok, _} = ElixirDB.Runtime.DatabaseCatalog.open(identity.database_uuid)

    assert {:error, %ElixirDB.Error{code: :database_in_use}} =
             GenServer.start(FileLease, absolute)

    assert :ok = ElixirDB.Runtime.DatabaseCatalog.close(identity.database_uuid)

    assert {:ok, lease} = GenServer.start(FileLease, absolute)
    assert :ok = GenServer.stop(lease)
  end
end
