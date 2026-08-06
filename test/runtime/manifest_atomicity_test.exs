defmodule ElixirDB.Runtime.ManifestAtomicityTest do
  @moduledoc """
  Gap D3: registration manifest temp+rename atomicity.

  Interrupted or failed writes must leave the prior `registrations.json` intact;
  orphan `.tmp.*` files must not affect `RegistrationManifest.read/0`.
  """
  use ExUnit.Case, async: false

  alias ElixirDB.Runtime.RegistrationManifest

  setup do
    previous = Application.get_env(:elixir_db, :registration_manifest)
    dir = Path.join(System.tmp_dir!(), "elixirdb-manifest-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    manifest = Path.join(dir, "registrations.json")
    Application.put_env(:elixir_db, :registration_manifest, manifest)

    on_exit(fn ->
      Application.put_env(:elixir_db, :registration_manifest, previous)
      File.chmod!(dir, 0o755)
      _ = File.rm_rf(dir)
    end)

    uuid_a = "11111111-1111-4111-8111-111111111111"
    uuid_b = "22222222-2222-4222-8222-222222222222"

    {:ok, dir: dir, manifest: manifest, uuid_a: uuid_a, uuid_b: uuid_b}
  end

  test "failed write after prior publish leaves prior manifest bytes intact", %{
    dir: dir,
    manifest: manifest,
    uuid_a: uuid_a,
    uuid_b: uuid_b
  } do
    assert :ok = RegistrationManifest.write([%{uuid: uuid_a, path: "prior.db"}])
    prior_bytes = File.read!(manifest)

    assert {:ok, [%{uuid: ^uuid_a, path: "prior.db"}]} = RegistrationManifest.read()

    File.chmod!(dir, 0o555)

    assert {:error, %ElixirDB.Error{code: :database_unavailable}} =
             RegistrationManifest.write([%{uuid: uuid_b, path: "next.db"}])

    File.chmod!(dir, 0o755)

    assert File.read!(manifest) == prior_bytes
    assert {:ok, [%{uuid: ^uuid_a, path: "prior.db"}]} = RegistrationManifest.read()
  end

  test "orphan temp file from interrupted rename does not replace or corrupt read", %{
    dir: dir,
    manifest: manifest,
    uuid_a: uuid_a
  } do
    assert :ok = RegistrationManifest.write([%{uuid: uuid_a, path: "stable.db"}])
    prior_bytes = File.read!(manifest)

    orphan = Path.join(dir, "registrations.json.tmp.#{System.unique_integer([:positive])}")
    File.write!(orphan, "{\"version\":1,\"databases\":[{\"uuid\":\"not-a-uuid\",\"path\":\"x.db\"}]}")

    assert File.exists?(orphan)
    assert File.read!(manifest) == prior_bytes
    assert {:ok, [%{uuid: ^uuid_a, path: "stable.db"}]} = RegistrationManifest.read()

    assert :ok =
             RegistrationManifest.write([
               %{uuid: uuid_a, path: "stable.db"},
               %{uuid: "33333333-3333-4333-8333-333333333333", path: "added.db"}
             ])

    assert {:ok, entries} = RegistrationManifest.read()
    assert length(entries) == 2
    assert Enum.any?(entries, &(&1.path == "stable.db"))
    assert Enum.any?(entries, &(&1.path == "added.db"))
  end
end
