defmodule ElixirDB.Storage.BoundaryGuardTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Storage.{
    BackendContext,
    BoundaryGuard,
    PhysicalAllowlist,
    Ports,
    Registry,
    Sentinel.Adapter
  }

  @known_leak_paths [
    "lib/elixir_db/database_bundle.ex",
    "lib/elixir_db/runtime/database_owner.ex",
    "lib/elixir_db/runtime/database_catalog.ex",
    "lib/elixir_db/runtime/file_lease.ex",
    "lib/elixir_db/runtime/registration_manifest.ex",
    "lib/elixir_db/diagnostics.ex",
    "lib/elixir_db/application.ex"
  ]

  test "port families freeze the approved vocabulary" do
    assert :lifecycle in Ports.families()
    assert :transaction in Ports.families()
    assert :document_facts in Ports.families()
    assert Ports.family?(:ownership)
    refute Ports.family?(:sqlite)
  end

  test "physical allowlist classifies sqlite implementation and physical tests" do
    assert PhysicalAllowlist.allowed_path?("lib/elixir_db/storage/sqlite/adapter.ex")
    assert PhysicalAllowlist.allowed_path?("priv/sqlite/schema_v1.sql")
    assert PhysicalAllowlist.allowed_path?("lib/elixir_db/observability/instrumentation/sqlite.ex")
    assert PhysicalAllowlist.allowed_path?("test/storage_adapter/portability_test.exs")
    refute PhysicalAllowlist.allowed_path?("lib/elixir_db/runtime/database_owner.ex")
    refute PhysicalAllowlist.allowed_path?("lib/elixir_db/database_bundle.ex")
  end

  test "boundary guard reports known product/runtime leaks with file and line" do
    findings = BoundaryGuard.scan()
    leaking = BoundaryGuard.leaking_paths(findings)

    for path <- @known_leak_paths do
      assert path in leaking, "expected leak findings for #{path}, got #{inspect(leaking)}"

      path_findings = BoundaryGuard.findings_for(findings, path)
      assert path_findings != []
      assert Enum.all?(path_findings, &(&1.line >= 1))
      assert Enum.all?(path_findings, &is_atom(&1.pattern))
    end

    refute "lib/elixir_db/storage/sqlite/adapter.ex" in leaking
    refute "lib/elixir_db/storage/sentinel/adapter.ex" in leaking
  end

  test "classified physical tests exist on disk and carry the sqlite_physical tag" do
    for path <- PhysicalAllowlist.classified_physical_tests() do
      assert File.regular?(path), "missing classified physical test #{path}"
      source = File.read!(path)

      assert source =~ "@moduletag :sqlite_physical",
             "#{path} must declare @moduletag :sqlite_physical"
    end
  end

  test "sentinel backend create/open/close/identity without SQL types" do
    root =
      Path.join(System.tmp_dir!(), "elixirdb-sentinel-#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm_rf(root) end)

    assert {:ok, created} = Adapter.create(root, %{})
    assert {:ok, identity} = Adapter.identity(created)
    assert identity["backend"] == "sentinel"
    assert identity["engine"] == "none"
    assert :ok = Adapter.close(created)

    assert {:ok, reopened} = Adapter.open(root, %{})
    assert {:ok, reopened_identity} = Adapter.identity(reopened)
    assert reopened_identity["database_uuid"] == identity["database_uuid"]

    context = Adapter.to_context(reopened)
    assert %BackendContext{} = context
    assert BackendContext.backend(context) == Adapter
    assert BackendContext.bundle_root(context) == Path.expand(root)
    assert is_map(context.capabilities)
    assert context.capabilities.sql == false

    source = File.read!("lib/elixir_db/storage/sentinel/adapter.ex")
    refute source =~ "Exqlite"
    refute source =~ "PRAGMA"
    refute source =~ "SELECT "
    refute source =~ "database.sqlite3"
  end

  test "registry resolves configured backend modules" do
    previous = Application.get_env(:elixir_db, :storage_backend)

    on_exit(fn ->
      if is_nil(previous) do
        Application.delete_env(:elixir_db, :storage_backend)
      else
        Application.put_env(:elixir_db, :storage_backend, previous)
      end
    end)

    Application.put_env(:elixir_db, :storage_backend, Adapter)
    assert Registry.backend() == Adapter
    assert Registry.adapter_module?(Adapter)
  end
end
