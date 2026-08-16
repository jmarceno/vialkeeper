defmodule VialKeeper.Storage.PortsTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Storage.{BackendContext, OpaqueHandle, Ports, Registry, Transaction}

  alias VialKeeper.Storage.Ports.{
    ChangeLog,
    DocumentFacts,
    Errors,
    IndexCandidates,
    LocalRecords,
    RetentionRecords
  }

  alias VialKeeper.Storage.Memory.Lifecycle, as: MemoryLifecycle
  alias VialKeeper.Storage.Sentinel.Adapter, as: Sentinel
  alias VialKeeper.Storage.Sentinel.Lifecycle, as: SentinelLifecycle
  alias VialKeeper.Storage.SQLite.Adapter, as: SQLite
  alias VialKeeper.Storage.SQLite.DocumentFacts, as: SQLiteDocumentFacts
  alias VialKeeper.Storage.SQLite.Lifecycle, as: SQLiteLifecycle

  test "every port family has a behaviour module" do
    for family <- Ports.families() do
      behaviour = Ports.behaviour(family)
      assert is_atom(behaviour)
      assert Code.ensure_loaded?(behaviour)
      assert function_exported?(behaviour, :behaviour_info, 1)
      assert behaviour.behaviour_info(:callbacks) != []
    end
  end

  test "SQLite adapter composes every port family" do
    assert Registry.port_backend?(SQLite)

    for family <- Ports.families() do
      module = SQLite.port(family)
      assert Code.ensure_loaded?(module)

      behaviours =
        module.module_info(:attributes)
        |> Keyword.get_values(:behaviour)
        |> List.flatten()

      assert Ports.behaviour(family) in behaviours
    end
  end

  test "sentinel implements lifecycle, transaction, and ownership without SQL" do
    assert Registry.port_backend?(Sentinel)

    assert Map.keys(Sentinel.port_modules()) |> Enum.sort() == [
             :lifecycle,
             :ownership,
             :transaction
           ]

    for family <- [:lifecycle, :transaction, :ownership] do
      module = Sentinel.port(family)
      source = module.__info__(:compile)[:source] |> List.to_string() |> File.read!()
      refute source =~ "Exqlite"
      refute source =~ "BEGIN "
      refute source =~ "COMMIT"
      refute source =~ "PRAGMA"
    end
  end

  test "sentinel lifecycle and transaction ports work without a SQLite connection" do
    root =
      Path.join(
        System.tmp_dir!(),
        "vialkeeper-ports-sentinel-#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf(root) end)

    assert {:ok, %BackendContext{} = context} = SentinelLifecycle.create(root, %{})

    refute match?(%{conn: _}, BackendContext.backend_ref(context))
    assert BackendContext.backend(context) == Sentinel

    assert {:ok, identity} = SentinelLifecycle.identity(context)
    assert identity.engine == "none"

    assert {:ok, :ran} =
             Transaction.run(context, fn tx_context ->
               assert %BackendContext{} = tx_context
               ref = BackendContext.backend_ref(tx_context)
               refute Map.has_key?(Map.from_struct(ref), :conn)
               {:ok, :ran}
             end)

    assert :ok = SentinelLifecycle.close(context)
  end

  test "reader interruption is supported by SQLite and unsupported by memory and sentinel" do
    assert {:ok, sqlite} = SQLiteLifecycle.create(":memory:", %{storage_mode: :memory})
    assert :unsupported = SQLiteLifecycle.interrupt_reader(sqlite)
    assert :ok = SQLiteLifecycle.close(sqlite)

    root =
      Path.join(System.tmp_dir!(), "vialkeeper-ports-memory-#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm_rf(root) end)

    assert {:ok, memory} = MemoryLifecycle.create(root, %{})
    assert :unsupported = MemoryLifecycle.interrupt_reader(memory)
    assert :ok = MemoryLifecycle.close(memory)

    assert {:ok, sentinel} = SentinelLifecycle.create(root <> "-sentinel", %{})
    assert :unsupported = SentinelLifecycle.interrupt_reader(sentinel)
    assert :ok = SentinelLifecycle.close(sentinel)
  end

  test "SQLite lifecycle context keeps backend_ref opaque" do
    assert {:ok, context} = SQLiteLifecycle.create(":memory:", %{storage_mode: :memory})
    ref = BackendContext.backend_ref(context)

    assert is_struct(ref, OpaqueHandle)
    refute is_struct(ref, SQLite)
    refute Map.has_key?(ref, :conn)
    refute match?(%{conn: _}, ref)
    assert is_nil(Map.get(ref, :conn))
    refute function_exported?(BackendContext, :conn, 1)
    refute function_exported?(VialKeeper.Runtime.DatabaseOwner, :unwrap_handle!, 1)
    refute function_exported?(SQLite, :unwrap_handle!, 1)

    # Shared/runtime modules must not call OpaqueHandle.unwrap (Reach-enforced).
    owner_source = File.read!("lib/vial_keeper/runtime/database_owner.ex")
    refute owner_source =~ "OpaqueHandle.unwrap"
    refute owner_source =~ "unwrap_handle!"

    # Process dictionary must not hold the adapter payload.
    {:dictionary, entries} = Process.info(self(), :dictionary)

    refute Enum.any?(entries, fn
             {{VialKeeper.Storage.OpaqueHandle, _}, _} -> true
             {key, _} when is_tuple(key) -> inspect(key) =~ "OpaqueHandle"
             _ -> false
           end)

    # Direct unwrap from shared/test code is rejected (Context not on stack).
    assert_raise ArgumentError, fn -> OpaqueHandle.unwrap(ref) end

    # Bare GenServer unwrap without Context on the caller stack is rejected.
    assert {:error, :forbidden} =
             GenServer.call(VialKeeper.Storage.OpaqueHandle.Server, {:unwrap, ref})

    # Forged stacktrace payloads must not authorize unwrap.
    fake = [{VialKeeper.Storage.SQLite.Context, :unwrap, 1, []}]

    assert {:error, :forbidden} =
             GenServer.call(VialKeeper.Storage.OpaqueHandle.Server, {:unwrap, ref, fake})

    # Foreign process cannot unwrap.
    parent = self()

    _ =
      spawn(fn ->
        try do
          _ = OpaqueHandle.unwrap(ref)
          send(parent, {:opaque_probe, :unwrapped})
        rescue
          ArgumentError -> send(parent, {:opaque_probe, :blocked})
        end
      end)

    assert_receive {:opaque_probe, :blocked}, 1_000

    assert :ok = SQLiteLifecycle.close(context)
  end

  test "SQLite transaction port hides the connection from the caller callback" do
    assert {:ok, context} = SQLiteLifecycle.create(":memory:", %{storage_mode: :memory})

    assert {:ok, :hidden} =
             Transaction.run(context, fn tx_context ->
               assert %BackendContext{} = tx_context
               ref = BackendContext.backend_ref(tx_context)
               assert is_struct(ref, OpaqueHandle)
               refute Map.has_key?(ref, :conn)
               refute match?(%{conn: _}, ref)
               assert is_nil(Map.get(ref, :conn))
               refute function_exported?(BackendContext, :conn, 1)
               {:ok, :hidden}
             end)

    assert {:ok, doc} =
             Transaction.run(context, fn tx_context ->
               SQLiteDocumentFacts.ensure_document(tx_context, "doc-1")
             end)

    assert doc.document_id == "doc-1"
    assert is_map(doc.backend_meta)
    refute Map.has_key?(doc, :doc_key)
    refute Map.has_key?(doc, :conn)
    refute Map.has_key?(doc, :rowid)

    assert :ok = SQLiteLifecycle.close(context)
  end

  test "port behaviours expose list, retention delete, and candidate scan shapes" do
    assert {:list, 2} in LocalRecords.behaviour_info(:callbacks)
    assert {:delete_through_boundary, 2} in ChangeLog.behaviour_info(:callbacks)
    assert {:range_scan_candidates, 2} in IndexCandidates.behaviour_info(:callbacks)
    assert {:full_text_candidates, 2} in IndexCandidates.behaviour_info(:callbacks)
    assert {:list_ancestors, 3} in DocumentFacts.behaviour_info(:callbacks)
    assert {:get_compaction_result, 1} in RetentionRecords.behaviour_info(:callbacks)
    assert {:put_compaction_result, 2} in RetentionRecords.behaviour_info(:callbacks)
  end

  test "Ports.Errors normalizes untyped backend failures" do
    assert %VialKeeper.Error{code: :internal_error} = Errors.normalize({:sqlite, :busy})
    error = VialKeeper.Error.invalid_request("nope")
    assert Errors.normalize(error) == error
    assert {:error, %VialKeeper.Error{}} = Errors.wrap({:error, :boom})
    assert {:ok, 1} = Errors.wrap({:ok, 1})
  end

  test "generic port types do not advertise physical SQLite fields" do
    forbidden = [
      "Connection.handle",
      "doc_key",
      "rowid",
      "physical_name",
      "term_blob",
      "BEGIN IMMEDIATE",
      "storage_mode"
    ]

    for path <-
          Path.wildcard("lib/vial_keeper/storage/ports/*.ex")
          |> Enum.concat(["lib/vial_keeper/storage/transaction.ex"]) do
      source = File.read!(path)

      for marker <- forbidden do
        refute source =~ marker, "#{path} must not contain #{marker}"
      end
    end

    transaction_source = File.read!("lib/vial_keeper/storage/transaction.ex")
    refute transaction_source =~ "BEGIN"
    refute transaction_source =~ "COMMIT"
    refute transaction_source =~ "Exqlite"
  end
end
