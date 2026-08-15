defmodule VialKeeper.Runtime.CatalogManifestResilienceTest do
  @moduledoc """
  A poisoned registration manifest must never crash the DatabaseCatalog.

  The manifest is routing-only, reconstructible state, so the catalog degrades to
  an empty registration set instead of letting a corrupt or duplicate file take
  the application down at startup (availability). A malicious or accidental
  manifest must not be able to denial-of-service the server. Covers duplicate
  uuid/path entries plus the other unreadable shapes.
  """
  use ExUnit.Case, async: false

  alias VialKeeper.Runtime.DatabaseCatalog

  setup do
    previous = Application.get_env(:vial_keeper, :registration_manifest)

    dir =
      Path.join(
        System.tmp_dir!(),
        "vialkeeper-catalog-resilience-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    manifest = Path.join(dir, "registrations.json")

    on_exit(fn ->
      Application.put_env(:vial_keeper, :registration_manifest, previous)
      _ = File.rm_rf(dir)
    end)

    {:ok, manifest: manifest}
  end

  # Plant `body` as the manifest, boot a catalog against it, and assert it starts
  # alive and degrades to an empty registration set instead of crashing.
  defp assert_catalog_survives(manifest, body) do
    File.write!(manifest, body)
    Application.put_env(:vial_keeper, :registration_manifest, manifest)

    assert {:ok, pid} = GenServer.start_link(DatabaseCatalog, [])
    assert Process.alive?(pid)
    assert {:ok, []} = GenServer.call(pid, :list)

    GenServer.stop(pid)
  end

  test "duplicate path entries degrade to an empty catalog instead of crashing", %{
    manifest: manifest
  } do
    assert_catalog_survives(
      manifest,
      ~s|{"version":1,"databases":[| <>
        ~s|{"path":"same.vialkeeper","uuid":"11111111-1111-4111-8111-111111111111"},| <>
        ~s|{"path":"same.vialkeeper","uuid":"22222222-2222-4222-8222-222222222222"}]}|
    )
  end

  test "duplicate uuid entries degrade to an empty catalog instead of crashing", %{
    manifest: manifest
  } do
    assert_catalog_survives(
      manifest,
      ~s|{"version":1,"databases":[| <>
        ~s|{"path":"a.vialkeeper","uuid":"11111111-1111-4111-8111-111111111111"},| <>
        ~s|{"path":"b.vialkeeper","uuid":"11111111-1111-4111-8111-111111111111"}]}|
    )
  end

  test "corrupt JSON degrades to an empty catalog instead of crashing", %{manifest: manifest} do
    assert_catalog_survives(manifest, ~s|{"version":1,"databases":[not valid json|)
  end

  test "an entry missing its uuid degrades to an empty catalog instead of crashing", %{
    manifest: manifest
  } do
    assert_catalog_survives(manifest, ~s|{"version":1,"databases":[{"path":"a.vialkeeper"}]}|)
  end

  test "an unsupported manifest version degrades to an empty catalog instead of crashing", %{
    manifest: manifest
  } do
    assert_catalog_survives(manifest, ~s|{"version":2,"databases":[]}|)
  end
end
