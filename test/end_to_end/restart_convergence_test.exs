defmodule ElixirDB.EndToEnd.RestartConvergenceTest do
  @moduledoc """
  Gap D8 companion: replicate, stop/restart owners via close/open, verify state.
  """
  use ExUnit.Case, async: false

  alias ElixirDB.Runtime.DatabaseCatalog

  @tag :slow
  test "replication converges across close/reopen of source and target owners" do
    prefix = "e2e-restart-#{System.unique_integer([:positive])}"
    a_path = prefix <> "-a.db"
    b_path = prefix <> "-b.db"
    root = ElixirDB.Config.database_root()

    for path <- [a_path, b_path] do
      _ = File.rm(Path.join(root, path))
      _ = File.rm(Path.join(root, path <> ".lease"))
    end

    {:ok, a} = DatabaseCatalog.create(a_path)
    {:ok, b} = DatabaseCatalog.create(b_path)

    on_exit(fn ->
      for {identity, path} <- [{a, a_path}, {b, b_path}] do
        _ = DatabaseCatalog.close(identity.database_uuid)
        _ = DatabaseCatalog.unregister(identity.database_uuid)
        _ = File.rm(Path.join(root, path))
        _ = File.rm(Path.join(root, path <> ".lease"))
      end
    end)

    assert {:ok, %{revision: first}} =
             ElixirDB.Documents.put(a.database_uuid, %{id: "doc", body: %{"n" => 1}})

    assert {:ok, %{status: :completed}} =
             ElixirDB.Replication.one_shot(a.database_uuid, b.database_uuid)

    assert {:ok, %{revision: ^first}} =
             ElixirDB.Documents.get(b.database_uuid, %{id: "doc"})

    # Stop owners (releases file leases) and reopen.
    assert :ok = DatabaseCatalog.close(a.database_uuid)
    assert :ok = DatabaseCatalog.close(b.database_uuid)
    assert {:ok, _} = DatabaseCatalog.open(a.database_uuid)
    assert {:ok, _} = DatabaseCatalog.open(b.database_uuid)

    assert {:ok, %{revision: ^first, body: %{"n" => 1}}} =
             ElixirDB.Documents.get(a.database_uuid, %{id: "doc"})

    assert {:ok, %{revision: ^first, body: %{"n" => 1}}} =
             ElixirDB.Documents.get(b.database_uuid, %{id: "doc"})

    assert {:ok, %{revision: second}} =
             ElixirDB.Documents.put(a.database_uuid, %{
               id: "doc",
               if_revision: first,
               body: %{"n" => 2}
             })

    assert {:ok, %{status: :completed}} =
             ElixirDB.Replication.one_shot(a.database_uuid, b.database_uuid)

    assert {:ok, %{revision: ^second, body: %{"n" => 2}}} =
             ElixirDB.Documents.get(b.database_uuid, %{id: "doc"})

    # Second restart cycle: checkpointed resume must still transfer later changes.
    assert :ok = DatabaseCatalog.close(a.database_uuid)
    assert :ok = DatabaseCatalog.close(b.database_uuid)
    assert {:ok, _} = DatabaseCatalog.open(a.database_uuid)
    assert {:ok, _} = DatabaseCatalog.open(b.database_uuid)

    assert {:ok, %{revision: third}} =
             ElixirDB.Documents.put(a.database_uuid, %{
               id: "doc",
               if_revision: second,
               body: %{"n" => 3}
             })

    assert {:ok, %{status: :completed}} =
             ElixirDB.Replication.one_shot(a.database_uuid, b.database_uuid)

    assert {:ok, %{revision: ^third, body: %{"n" => 3}}} =
             ElixirDB.Documents.get(b.database_uuid, %{id: "doc"})

    assert {:ok, %{ok: true}} =
             DatabaseCatalog.command(b.database_uuid, {:command, :integrity_check, %{}})
  end
end
