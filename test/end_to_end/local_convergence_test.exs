defmodule ElixirDB.EndToEnd.LocalConvergenceTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.Runtime.DatabaseCatalog

  @tag :slow
  test "two-database local convergence replicates documents from A to B" do
    prefix = "e2e-conv-#{System.unique_integer([:positive])}"
    a_path = prefix <> "-a.elixirdb"
    b_path = prefix <> "-b.elixirdb"

    for path <- [a_path, b_path] do
      ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path))
    end

    {:ok, a} = DatabaseCatalog.create(a_path)
    {:ok, b} = DatabaseCatalog.create(b_path)

    on_exit(fn ->
      for {identity, path} <- [{a, a_path}, {b, b_path}] do
        _ = DatabaseCatalog.close(identity.database_uuid)
        _ = DatabaseCatalog.unregister(identity.database_uuid)
        ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path))
      end
    end)

    assert {:ok, %{revision: r1}} =
             ElixirDB.Documents.put(a.database_uuid, %{id: "alpha", body: %{"v" => 1}})

    assert {:ok, %{revision: r2}} =
             ElixirDB.Documents.put(a.database_uuid, %{id: "beta", body: %{"v" => 2}})

    assert {:ok, %{status: :completed}} =
             ElixirDB.Replication.one_shot(a.database_uuid, b.database_uuid)

    assert {:ok, %{revision: ^r1, body: %{"v" => 1}}} =
             ElixirDB.Documents.get(b.database_uuid, %{id: "alpha"})

    assert {:ok, %{revision: ^r2, body: %{"v" => 2}}} =
             ElixirDB.Documents.get(b.database_uuid, %{id: "beta"})
  end
end
