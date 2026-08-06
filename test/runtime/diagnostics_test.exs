defmodule ElixirDB.Runtime.DiagnosticsTest do
  # Not async: the tests below mutate the process-global ELIXIRDB_GIT_REF env var.
  use ExUnit.Case, async: false

  alias ElixirDB.Diagnostics

  test "runtime/0 records release metadata including the git commit" do
    # Plan §3.1: the release record MUST include Elixir/OTP/Exqlite/SQLite versions, SQLite
    # compile options, and git commit.
    metadata = Diagnostics.runtime()

    assert Map.has_key?(metadata, :elixir)
    assert Map.has_key?(metadata, :otp)
    assert Map.has_key?(metadata, :exqlite)
    assert Map.has_key?(metadata, :sqlite)
    assert Map.has_key?(metadata, :sqlite_compile_options)
    assert Map.has_key?(metadata, :git_commit)

    assert is_binary(metadata.git_commit)
    assert metadata.git_commit != ""
  end

  test "git_commit/0 honors a non-empty ELIXIRDB_GIT_REF override" do
    with_env("ELIXIRDB_GIT_REF", "deadbeefcafebabe", fn ->
      assert Diagnostics.git_commit() == "deadbeefcafebabe"
    end)
  end

  test "git_commit/0 ignores an empty ELIXIRDB_GIT_REF and falls through" do
    # An empty env var must NOT short-circuit the fallback chain ("" is truthy in Elixir).
    with_env("ELIXIRDB_GIT_REF", "", fn ->
      expected = read_priv_git_ref() || "unknown"
      assert Diagnostics.git_commit() == expected
    end)
  end

  test "git_commit/0 falls back to unknown when no ref is resolvable" do
    # With the env var unset, the resolution is priv/git_ref (absent in this test build) then
    # "unknown". If a stray priv/git_ref exists in the build, accept that value instead of
    # asserting "unknown" falsely.
    with_env("ELIXIRDB_GIT_REF", nil, fn ->
      assert Diagnostics.git_commit() == (read_priv_git_ref() || "unknown")
    end)
  end

  # Helpers -------------------------------------------------------------------

  defp with_env(key, value, fun) do
    previous = System.get_env(key)

    try do
      if value, do: System.put_env(key, value), else: System.delete_env(key)
      fun.()
    after
      if previous, do: System.put_env(key, previous), else: System.delete_env(key)
    end
  end

  defp read_priv_git_ref do
    case :code.priv_dir(:elixir_db) do
      {:error, :bad_name} ->
        nil

      priv ->
        path = Path.join(priv, "git_ref")

        case File.read(path) do
          {:ok, content} ->
            ref = String.trim(content)
            if ref == "", do: nil, else: ref

          _ ->
            nil
        end
    end
  end
end
