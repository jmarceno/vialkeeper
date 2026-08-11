defmodule ElixirDB.Storage.PortsTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Storage.{BackendContext, Ports, Registry, Transaction}
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.Sentinel.Adapter, as: Sentinel
  alias ElixirDB.Storage.Sentinel.Lifecycle, as: SentinelLifecycle
  alias ElixirDB.Storage.SQLite.Adapter, as: SQLite
  alias ElixirDB.Storage.SQLite.DocumentFacts
  alias ElixirDB.Storage.SQLite.Lifecycle, as: SQLiteLifecycle

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
      Path.join(System.tmp_dir!(), "elixirdb-ports-sentinel-#{System.unique_integer([:positive])}")

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

  test "SQLite transaction port hides the connection from the caller callback" do
    assert {:ok, context} = SQLiteLifecycle.create(":memory:", %{storage_mode: :memory})

    assert {:ok, :hidden} =
             Transaction.run(context, fn tx_context ->
               assert %BackendContext{} = tx_context
               ref = BackendContext.backend_ref(tx_context)
               assert is_struct(ref)
               refute function_exported?(BackendContext, :conn, 1)
               {:ok, :hidden}
             end)

    assert {:ok, doc} =
             Transaction.run(context, fn tx_context ->
               DocumentFacts.ensure_document(tx_context, "doc-1")
             end)

    assert doc.document_id == "doc-1"
    assert is_map(doc.backend_meta)
    refute Map.has_key?(doc, :doc_key)
    refute Map.has_key?(doc, :conn)
    refute Map.has_key?(doc, :rowid)

    assert :ok = SQLiteLifecycle.close(context)
  end

  test "Ports.Errors normalizes untyped backend failures" do
    assert %ElixirDB.Error{code: :internal_error} = Errors.normalize({:sqlite, :busy})
    error = ElixirDB.Error.invalid_request("nope")
    assert Errors.normalize(error) == error
    assert {:error, %ElixirDB.Error{}} = Errors.wrap({:error, :boom})
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
          Path.wildcard("lib/elixir_db/storage/ports/*.ex") ++
            ["lib/elixir_db/storage/transaction.ex"] do
      source = File.read!(path)

      for marker <- forbidden do
        refute source =~ marker, "#{path} must not contain #{marker}"
      end
    end

    transaction_source = File.read!("lib/elixir_db/storage/transaction.ex")
    refute transaction_source =~ "BEGIN"
    refute transaction_source =~ "COMMIT"
    refute transaction_source =~ "Exqlite"
  end
end
