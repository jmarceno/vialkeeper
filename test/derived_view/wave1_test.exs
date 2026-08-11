defmodule ElixirDB.DerivedView.Wave1Test do
  @moduledoc "Covers derived bundle authority, definition validation, and external write protection."
  use ExUnit.Case, async: false

  alias ElixirDB.DerivedView.Definition
  alias ElixirDB.DerivedView.Path, as: DerivedPath
  alias ElixirDB.MaterializedViews
  alias ElixirDB.Replication.LocalEndpoint
  alias ElixirDB.Runtime.DatabaseCatalog

  setup do
    source_path = "derived-wave1-source-#{System.unique_integer([:positive])}.elixirdb"
    source_abs = Path.join(ElixirDB.Config.database_root(), source_path)
    ElixirDB.TempDatabase.cleanup(source_abs)

    {:ok, source} = DatabaseCatalog.create(source_path)
    source_uuid = source.database_uuid

    on_exit(fn ->
      _ = DatabaseCatalog.close(source_uuid)
      _ = DatabaseCatalog.unregister(source_uuid)
      ElixirDB.TempDatabase.cleanup(source_abs)
    end)

    {:ok, source_uuid: source_uuid}
  end

  test "normalizes bounded definitions and deterministic paths", %{source_uuid: source_uuid} do
    request = %{
      "name" => "Sales São / 2026",
      "sources" => [source_uuid],
      "map" => %{
        "key" => [%{"path" => "/month"}],
        "value" => %{"path" => "/amount"}
      },
      "reduce" => "_sum",
      "group_level" => 1
    }

    assert {:ok, definition} = Definition.normalize(request)
    assert definition.options["max_concurrent_sources"] == 1
    assert definition.reducer == :_sum
    assert definition.definition_digest == Definition.digest(definition)

    assert DerivedPath.for(request["name"], "11111111-1111-4111-8111-111111111111") ==
             "_derived/sales-sao-2026--11111111.derived.elixirdb"
  end

  test "ordinary creation records ordinary kind", _context do
    path = "derived-wave1-ordinary-#{System.unique_integer([:positive])}.elixirdb"
    absolute = Path.join(ElixirDB.Config.database_root(), path)
    on_exit(fn -> ElixirDB.TempDatabase.cleanup(absolute) end)

    assert {:ok, %{database_kind: :ordinary} = identity} = DatabaseCatalog.create(path)
    assert {:ok, listed} = DatabaseCatalog.list()

    assert Enum.any?(
             listed,
             &(&1.database_uuid == identity.database_uuid and &1.database_kind == :ordinary)
           )
  end

  test "derived creation persists complete state and survives a move", %{source_uuid: source_uuid} do
    request = %{
      name: "Moved Materialization",
      sources: [source_uuid],
      map: %{key: [%{"path" => "/month"}]}
    }

    assert {:ok, identity} = MaterializedViews.create(request)
    uuid = identity.database_uuid
    original = Path.join(ElixirDB.Config.database_root(), identity.database_path)
    moved = Path.join(ElixirDB.Config.database_root(), "renamed.derived.elixirdb")

    on_exit(fn -> ElixirDB.TempDatabase.cleanup(original) end)

    assert {:ok, %{database_kind: :derived}} =
             DatabaseCatalog.command(uuid, {:command, :identity, %{}})

    assert {:ok, entries} = DatabaseCatalog.list()
    assert Enum.any?(entries, &(&1.database_uuid == uuid and &1.database_kind == :derived))
    assert File.exists?(Path.join(original, "database.sqlite3"))

    assert {:ok, %{enabled: false}} =
             DatabaseCatalog.command(
               uuid,
               {:command, :set_derived_enabled, %{materialization_id: uuid, enabled: false}}
             )

    assert :ok = DatabaseCatalog.close(uuid)
    assert :ok = DatabaseCatalog.unregister(uuid)
    assert :ok = File.rename(original, moved)
    assert {:ok, %{database_kind: :derived}} = DatabaseCatalog.register("renamed.derived.elixirdb")
    assert {:ok, %{database_kind: :derived}} = DatabaseCatalog.info(uuid)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(moved)
    end)
  end

  test "derived database rejects public writes and byte streams before consumption", %{
    source_uuid: source_uuid
  } do
    request = %{
      name: "Read Only",
      sources: [source_uuid],
      map: %{key: [%{"path" => "/month"}]}
    }

    assert {:ok, identity} = MaterializedViews.create(request)
    uuid = identity.database_uuid
    bundle = Path.join(ElixirDB.Config.database_root(), identity.database_path)

    on_exit(fn ->
      _ = DatabaseCatalog.command(uuid, {:command, :set_derived_enabled, %{enabled: false}})
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(bundle)
    end)

    assert {:error, %ElixirDB.Error{code: :derived_database_read_only}} =
             ElixirDB.Documents.put(uuid, %{id: "public-write", body: %{}})

    assert {:error, %ElixirDB.Error{code: :derived_database_read_only}} =
             ElixirDB.Documents.delete(uuid, %{id: "public-write"})

    assert {:error, %ElixirDB.Error{code: :derived_database_read_only}} =
             ElixirDB.Documents.bulk_write(uuid, [])

    assert {:error, %ElixirDB.Error{code: :derived_database_read_only}} =
             DatabaseCatalog.command(uuid, {:command, :import_revision_chains, %{chains: []}})

    send_self = self()

    upload_source = fn ->
      send(send_self, :upload_source_consumed)
      :done
    end

    assert {:error, %ElixirDB.Error{code: :derived_database_read_only}} =
             ElixirDB.Attachments.upload_stream(uuid, upload_source)

    refute_received :upload_source_consumed

    digest = String.duplicate("a", 64)

    blob_source = fn ->
      send(send_self, :blob_source_consumed)
      :done
    end

    assert {:error, %ElixirDB.Error{code: :derived_database_read_only}} =
             ElixirDB.Attachments.put_blob(uuid, digest, 0, blob_source)

    refute_received :blob_source_consumed

    {:ok, source} = LocalEndpoint.new(source_uuid)
    {:ok, target} = LocalEndpoint.new(uuid)

    assert {:error, %ElixirDB.Error{code: :derived_database_read_only}} =
             ElixirDB.Replication.handshake(source, target, %{})
  end
end
