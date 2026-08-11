defmodule ElixirDB.DerivedView.MaterializationContractTest do
  @moduledoc "Covers derived replication, lifecycle ownership, and bundle portability."
  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.Documents
  alias ElixirDB.Error
  alias ElixirDB.Eventual
  alias ElixirDB.JSON.Canonical
  alias ElixirDB.MaterializedViews
  alias ElixirDB.Replication
  alias ElixirDB.Replication.{LocalEndpoint, RemoteEndpoint}
  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.Storage.SQLite.{Adapter, Connection}
  alias ElixirDB.TempDatabase
  alias ElixirDB.TestServer

  test "derived databases replicate as sources through local and remote endpoints" do
    prefix = "derived-replication-#{System.unique_integer([:positive])}"
    source_path = prefix <> "-source.elixirdb"
    local_target_path = prefix <> "-local-target.elixirdb"
    remote_target_path = prefix <> "-remote-target.elixirdb"
    root = ElixirDB.Config.database_root()

    for path <- [source_path, local_target_path, remote_target_path],
        do: TempDatabase.cleanup(Path.join(root, path))

    assert {:ok, source} = DatabaseCatalog.create(source_path)
    assert {:ok, _} = DatabaseCatalog.open(source.database_uuid)
    assert {:ok, local_target} = DatabaseCatalog.create(local_target_path)
    assert {:ok, remote_target} = DatabaseCatalog.create(remote_target_path)

    assert {:ok, derived} =
             MaterializedViews.create(%{
               name: "Replication Source",
               sources: [source.database_uuid],
               map: %{key: [%{"path" => "/kind"}], value: %{"path" => "/amount"}}
             })

    derived_path = Path.join(root, derived.database_path)

    on_exit(fn ->
      cleanup_registered(derived.database_uuid, derived_path)
      cleanup_registered(source.database_uuid, Path.join(root, source_path))
      cleanup_registered(local_target.database_uuid, Path.join(root, local_target_path))
      cleanup_registered(remote_target.database_uuid, Path.join(root, remote_target_path))
    end)

    assert {:ok, _source_revision} =
             Documents.put(source.database_uuid, %{
               id: "replicated",
               body: %{"kind" => "sale", "amount" => 12}
             })

    generated_id = map_id(source.database_uuid, "replicated")

    assert Eventual.eventually(
             fn ->
               match?(
                 {:ok, %{body: %{"value" => 12}}},
                 Documents.get(derived.database_uuid, %{id: generated_id})
               )
             end,
             message: "derived source did not materialize the replicated document"
           )

    assert {:ok, %{status: :completed}} =
             Replication.one_shot(derived.database_uuid, local_target.database_uuid)

    assert {:ok, %{body: %{"value" => 12}}} =
             Documents.get(local_target.database_uuid, %{id: generated_id})

    server = TestServer.start_supervised!()

    assert {:ok, remote_source} =
             RemoteEndpoint.new(%{
               "base_url" => server.base_url,
               "database_uuid" => derived.database_uuid
             })

    assert {:ok, remote_target_endpoint} = LocalEndpoint.new(remote_target.database_uuid)

    assert {:ok, %{status: :completed}} =
             Replication.one_shot_endpoints(remote_source, remote_target_endpoint)

    assert {:ok, %{body: %{"value" => 12}}} =
             Documents.get(remote_target.database_uuid, %{id: generated_id})

    assert {:error, %Error{code: :derived_database_read_only}} =
             Replication.one_shot(source.database_uuid, derived.database_uuid)
  end

  test "enabled materializers keep their database and source from closing" do
    prefix = "derived-lifecycle-#{System.unique_integer([:positive])}"
    source_path = prefix <> "-source.elixirdb"
    root = ElixirDB.Config.database_root()
    source_abs = Path.join(root, source_path)
    TempDatabase.cleanup(source_abs)

    assert {:ok, source} = DatabaseCatalog.create(source_path)
    assert {:ok, _} = DatabaseCatalog.open(source.database_uuid)

    assert {:ok, derived} =
             MaterializedViews.create(%{
               name: "Lifecycle Ownership",
               sources: [source.database_uuid],
               map: %{key: [%{"path" => "/kind"}]}
             })

    derived_abs = Path.join(root, derived.database_path)

    on_exit(fn ->
      cleanup_registered(derived.database_uuid, derived_abs)
      cleanup_registered(source.database_uuid, source_abs)
    end)

    assert {:error, %Error{code: :database_not_closable}} =
             DatabaseCatalog.close(derived.database_uuid)

    assert {:error, %Error{code: :database_not_closable, details: %{dependent_count: 1}}} =
             DatabaseCatalog.close(source.database_uuid)

    assert {:ok, %{"enabled" => false}} = MaterializedViews.disable(derived.database_uuid)
    assert :ok = DatabaseCatalog.close(derived.database_uuid)
    assert :ok = DatabaseCatalog.close(source.database_uuid)
  end

  test "a moved derived bundle stays readable without sources and resumes after registration" do
    prefix = "derived-portability-#{System.unique_integer([:positive])}"
    source_path = prefix <> "-source.elixirdb"
    moved_path = prefix <> "-moved.elixirdb"
    root = ElixirDB.Config.database_root()
    source_abs = Path.join(root, source_path)
    moved_abs = Path.join(root, moved_path)
    TempDatabase.cleanup(source_abs)
    TempDatabase.cleanup(moved_abs)

    assert {:ok, source} = DatabaseCatalog.create(source_path)

    assert {:ok, initial} =
             Documents.put(source.database_uuid, %{
               id: "portable",
               body: %{"kind" => "sale", "amount" => 3}
             })

    assert {:ok, derived} =
             MaterializedViews.create(%{
               name: "Portable Sales",
               sources: [source.database_uuid],
               map: %{key: [%{"path" => "/kind"}], value: %{"path" => "/amount"}}
             })

    derived_abs = Path.join(root, derived.database_path)
    generated_id = map_id(source.database_uuid, "portable")

    on_exit(fn ->
      cleanup_registered(derived.database_uuid, moved_abs)
      cleanup_registered(source.database_uuid, source_abs)
      TempDatabase.cleanup(derived_abs)
      TempDatabase.cleanup(moved_abs)
    end)

    assert Eventual.eventually(
             fn ->
               match?(
                 {:ok, %{body: %{"value" => 3}}},
                 Documents.get(derived.database_uuid, %{id: generated_id})
               )
             end,
             message: "derived bundle did not contain its initial generated document"
           )

    assert {:ok, before_move} = MaterializedViews.get(derived.database_uuid)
    assert {:ok, %{"enabled" => false}} = MaterializedViews.disable(derived.database_uuid)
    assert :ok = DatabaseCatalog.close(derived.database_uuid)
    assert :ok = DatabaseCatalog.close(source.database_uuid)

    assert {:ok, source_adapter} = Adapter.open(TempDatabase.sqlite_path(source_abs))

    try do
      for table <- ~w(derived_view derived_sources derived_rows derived_groups) do
        assert {:ok, [[0]]} =
                 Connection.query(source_adapter.conn, "SELECT COUNT(*) FROM #{table}")
      end
    after
      assert :ok = Adapter.close(source_adapter)
    end

    File.cp_r!(derived_abs, moved_abs)
    assert :ok = DatabaseCatalog.unregister(derived.database_uuid)
    assert :ok = DatabaseCatalog.unregister(source.database_uuid)
    assert {:ok, moved_identity} = DatabaseCatalog.register(moved_path)
    assert moved_identity.database_uuid == derived.database_uuid
    assert moved_identity.database_kind == :derived

    assert {:ok, after_move} = MaterializedViews.get(derived.database_uuid)
    assert after_move["database_kind"] == "derived"
    assert after_move["database_path"] == moved_path
    assert after_move["definition_digest"] == before_move["definition_digest"]
    assert after_move["sources"] |> hd() |> Map.fetch!("checkpoint_sequence") == 1

    assert {:ok, %{body: %{"value" => 3}}} =
             Documents.get(derived.database_uuid, %{id: generated_id})

    assert {:ok, %{"enabled" => true}} = MaterializedViews.enable(derived.database_uuid)

    assert Eventual.eventually(
             fn ->
               case MaterializedViews.get(derived.database_uuid) do
                 {:ok,
                  %{
                    "runtime_status" => "stale",
                    "sources" => [%{"last_error_code" => "database_not_registered"}]
                  }} ->
                   true

                 _ ->
                   false
               end
             end,
             message: "missing source was not reported as stale"
           )

    assert {:ok, restored_source} = DatabaseCatalog.register(source_path)
    assert restored_source.database_uuid == source.database_uuid

    assert {:ok, _updated} =
             Documents.put(source.database_uuid, %{
               id: "portable",
               if_revision: initial.revision,
               body: %{"kind" => "sale", "amount" => 8}
             })

    assert Eventual.eventually(
             fn ->
               match?(
                 {:ok, %{body: %{"value" => 8}}},
                 Documents.get(derived.database_uuid, %{id: generated_id})
               )
             end,
             message: "moved derived bundle did not resume after source registration"
           )
  end

  defp map_id(source_uuid, document_id) do
    digest = :crypto.hash(:sha256, Canonical.encode!([source_uuid, document_id]))
    "m-" <> Base.encode16(digest, case: :lower)
  end

  defp cleanup_registered(uuid, bundle_path) do
    disable_derived(uuid)
    _ = DatabaseCatalog.close(uuid)
    _ = DatabaseCatalog.unregister(uuid)
    TempDatabase.cleanup(bundle_path)
  end

  defp disable_derived(uuid) do
    case DatabaseCatalog.info(uuid) do
      {:ok, %{database_kind: :derived}} ->
        _ = DatabaseCatalog.command(uuid, {:command, :set_derived_enabled, %{enabled: false}})

      {:ok, %{"database_kind" => "derived"}} ->
        _ = DatabaseCatalog.command(uuid, {:command, :set_derived_enabled, %{enabled: false}})

      _ ->
        :ok
    end
  end
end
