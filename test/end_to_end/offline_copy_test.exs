defmodule ElixirDB.EndToEnd.OfflineCopyTest do
  use ExUnit.Case, async: false

  alias ElixirDB.Runtime.DatabaseCatalog

  @tag :slow
  test "offline copy, registration, and reopen restores documents" do
    source_rel = "e2e-offline-src-#{System.unique_integer([:positive])}.db"
    dest_rel = "e2e-offline-dst-#{System.unique_integer([:positive])}.db"
    root = ElixirDB.Config.database_root()
    source_abs = Path.join(root, source_rel)
    dest_abs = Path.join(root, dest_rel)

    for path <- [source_abs, dest_abs, source_abs <> ".lease", dest_abs <> ".lease"] do
      _ = File.rm(path)
    end

    assert {:ok, source} = DatabaseCatalog.create(source_rel)

    assert {:ok, %{revision: revision}} =
             ElixirDB.Documents.put(source.database_uuid, %{
               id: "portable",
               body: %{"copied" => true}
             })

    assert :ok = DatabaseCatalog.close(source.database_uuid)
    File.cp!(source_abs, dest_abs)

    # Same UUID cannot register twice on this host; unregister the source route first.
    assert :ok = DatabaseCatalog.unregister(source.database_uuid)
    assert {:ok, restored} = DatabaseCatalog.register(dest_rel)
    assert restored.database_uuid == source.database_uuid

    on_exit(fn ->
      _ = DatabaseCatalog.close(restored.database_uuid)
      _ = DatabaseCatalog.unregister(restored.database_uuid)
      _ = File.rm(source_abs)
      _ = File.rm(dest_abs)
      _ = File.rm(source_abs <> ".lease")
      _ = File.rm(dest_abs <> ".lease")
    end)

    assert {:ok, %{revision: ^revision, body: %{"copied" => true}}} =
             ElixirDB.Documents.get(restored.database_uuid, %{id: "portable"})

    assert {:ok, %{ok: true}} =
             DatabaseCatalog.command(restored.database_uuid, {:command, :integrity_check, %{}})
  end
end
