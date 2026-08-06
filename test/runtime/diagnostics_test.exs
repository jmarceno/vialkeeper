defmodule ElixirDB.Runtime.DiagnosticsTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Diagnostics

  test "runtime/0 records release metadata including the app version" do
    metadata = Diagnostics.runtime()

    assert Map.has_key?(metadata, :app_version)
    assert Map.has_key?(metadata, :elixir)
    assert Map.has_key?(metadata, :otp)
    assert Map.has_key?(metadata, :exqlite)
    assert Map.has_key?(metadata, :sqlite)
    assert Map.has_key?(metadata, :sqlite_compile_options)

    assert is_binary(metadata.app_version)
    assert metadata.app_version != ""
    assert metadata.app_version == Diagnostics.app_version()
  end

  test "app_version/0 matches the loaded :elixir_db application vsn" do
    assert Diagnostics.app_version() ==
             (:elixir_db |> Application.spec(:vsn) |> List.to_string())
  end
end
