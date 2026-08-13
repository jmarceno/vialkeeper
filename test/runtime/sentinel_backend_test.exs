defmodule ElixirDB.Runtime.SentinelBackendTest do
  @moduledoc "Proves the runtime can create/open/close/identify via a non-SQL backend."
  use ExUnit.Case, async: false

  alias ElixirDB.Runtime.{DatabaseCatalog, ReadPool}
  alias ElixirDB.Storage.Sentinel.Adapter

  setup do
    previous = Application.get_env(:elixir_db, :storage_backend)
    Application.put_env(:elixir_db, :storage_backend, Adapter)

    on_exit(fn ->
      if is_nil(previous) do
        Application.delete_env(:elixir_db, :storage_backend)
      else
        Application.put_env(:elixir_db, :storage_backend, previous)
      end
    end)

    :ok
  end

  test "catalog create/open/identity/close works with the sentinel backend" do
    relative = "sentinel-runtime-#{System.unique_integer([:positive])}.elixirdb"

    assert {:ok, identity} = DatabaseCatalog.create(relative)
    uuid = identity.database_uuid || identity["database_uuid"]
    assert is_binary(uuid)

    assert {:ok, opened} = DatabaseCatalog.open(uuid)
    assert opened.database_uuid == uuid
    assert {:ok, info} = DatabaseCatalog.info(uuid)
    assert info.database_uuid == uuid
    refute ReadPool.enabled?(uuid)

    assert :ok = DatabaseCatalog.close(uuid)
    assert :ok = DatabaseCatalog.unregister(uuid)
  end

  test "owner returns typed unsupported errors for missing storage capabilities" do
    relative = "sentinel-unsupported-#{System.unique_integer([:positive])}.elixirdb"

    assert {:ok, identity} = DatabaseCatalog.create(relative)
    uuid = identity.database_uuid || identity["database_uuid"]

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
    end)

    for command <- [
          {:command, :get_document, %{document_id: "missing"}},
          {:command, :read_changes, %{}},
          {:command, :execute_query, %{}},
          {:command, :compact_retention, %{}},
          {:command, :resolve_blob_metadata, %{digest: String.duplicate("a", 64)}},
          {:command, :list_views, %{}}
        ] do
      assert {:error, %ElixirDB.Error{code: :invalid_request}} =
               DatabaseCatalog.command(uuid, command)
    end
  end
end
