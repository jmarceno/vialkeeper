defmodule ElixirDB.Runtime.OwnershipContractTest do
  @moduledoc "Backend-neutral ownership exclusion using the sentinel (non-SQL) fake."
  use ExUnit.Case, async: false

  alias ElixirDB.DatabaseBundle
  alias ElixirDB.Runtime.Ownership
  alias ElixirDB.Storage.Sentinel.Adapter

  setup do
    previous = Application.get_env(:elixir_db, :storage_backend)
    Application.put_env(:elixir_db, :storage_backend, Adapter)

    root =
      Path.join(System.tmp_dir!(), "elixirdb-ownership-#{System.unique_integer([:positive])}")

    assert {:ok, _bundle} = DatabaseBundle.create(root)
    assert {:ok, adapter} = Adapter.create(root, %{})
    assert :ok = Adapter.close(adapter)

    on_exit(fn ->
      File.rm_rf(root)

      if is_nil(previous) do
        Application.delete_env(:elixir_db, :storage_backend)
      else
        Application.put_env(:elixir_db, :storage_backend, previous)
      end
    end)

    {:ok, root: root}
  end

  test "second Runtime.Ownership acquire fails while the first holds the lease", %{root: root} do
    assert {:ok, first} = Ownership.start_link(root)

    assert {:error, %ElixirDB.Error{code: :database_in_use}} =
             GenServer.start(ElixirDB.Storage.Sentinel.Ownership, root)

    assert :ok = GenServer.stop(first)
    assert {:ok, second} = Ownership.start_link(root)
    assert :ok = GenServer.stop(second)
  end
end
