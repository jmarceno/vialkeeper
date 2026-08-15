defmodule VialKeeper.Runtime.DiagnosticsTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Diagnostics

  test "runtime/0 records release metadata including the app version" do
    metadata = Diagnostics.runtime()

    assert Map.has_key?(metadata, :app_version)
    assert Map.has_key?(metadata, :elixir)
    assert Map.has_key?(metadata, :otp)
    assert Map.has_key?(metadata, :storage_backend)
    assert Map.has_key?(metadata, :backend)
    assert is_map(metadata.backend)
    assert metadata.backend.engine == "sqlite"
    assert Map.has_key?(metadata.backend, :sqlite)

    assert is_binary(metadata.app_version)
    assert metadata.app_version != ""
    assert metadata.app_version == Diagnostics.app_version()
  end

  test "app_version/0 matches the loaded :vial_keeper application vsn" do
    assert Diagnostics.app_version() ==
             :vial_keeper |> Application.spec(:vsn) |> List.to_string()
  end
end
