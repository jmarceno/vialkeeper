defmodule VialKeeper.Runtime.ManifestAtomicityTest do
  @moduledoc """
  Gap D3: registration manifest temp+rename atomicity.

  Interrupted or failed writes must leave the prior `registrations.json` intact;
  orphan `.tmp.*` files must not affect `RegistrationManifest.read/0`.
  """
  use ExUnit.Case, async: false

  alias VialKeeper.Runtime.RegistrationManifest

  setup do
    previous = Application.get_env(:vial_keeper, :registration_manifest)
    dir = Path.join(System.tmp_dir!(), "vialkeeper-manifest-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    manifest = Path.join(dir, "registrations.json")
    Application.put_env(:vial_keeper, :registration_manifest, manifest)

    on_exit(fn ->
      Application.put_env(:vial_keeper, :registration_manifest, previous)
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
    assert :ok = RegistrationManifest.write([%{uuid: uuid_a, path: "prior.vialkeeper"}])
    prior_bytes = File.read!(manifest)

    assert {:ok, [%{uuid: ^uuid_a, path: "prior.vialkeeper"}]} = RegistrationManifest.read()

    File.chmod!(dir, 0o555)

    assert {:error, %VialKeeper.Error{code: :database_unavailable}} =
             RegistrationManifest.write([%{uuid: uuid_b, path: "next.vialkeeper"}])

    File.chmod!(dir, 0o755)

    assert File.read!(manifest) == prior_bytes
    assert {:ok, [%{uuid: ^uuid_a, path: "prior.vialkeeper"}]} = RegistrationManifest.read()
  end

  test "orphan temp file from interrupted rename does not replace or corrupt read", %{
    dir: dir,
    manifest: manifest,
    uuid_a: uuid_a
  } do
    assert :ok = RegistrationManifest.write([%{uuid: uuid_a, path: "stable.vialkeeper"}])
    prior_bytes = File.read!(manifest)

    orphan = Path.join(dir, "registrations.json.tmp.#{System.unique_integer([:positive])}")

    File.write!(
      orphan,
      "{\"version\":1,\"databases\":[{\"uuid\":\"not-a-uuid\",\"path\":\"x.vialkeeper\"}]}"
    )

    assert File.exists?(orphan)
    assert File.read!(manifest) == prior_bytes
    assert {:ok, [%{uuid: ^uuid_a, path: "stable.vialkeeper"}]} = RegistrationManifest.read()

    assert :ok =
             RegistrationManifest.write([
               %{uuid: uuid_a, path: "stable.vialkeeper"},
               %{uuid: "33333333-3333-4333-8333-333333333333", path: "added.vialkeeper"}
             ])

    assert {:ok, entries} = RegistrationManifest.read()
    assert [_, _] = entries
    assert Enum.any?(entries, &(&1.path == "stable.vialkeeper"))
    assert Enum.any?(entries, &(&1.path == "added.vialkeeper"))
  end
end
