defmodule ElixirDB.Contract.DatabaseBundleTest do
  use ExUnit.Case, async: true

  alias ElixirDB.DatabaseBundle
  alias ElixirDB.PathSafety

  test "within_root? rejects paths outside the bundle root" do
    root = Path.expand(System.tmp_dir!())

    assert PathSafety.within_root?(Path.join(root, "blobs"), root)
    refute PathSafety.within_root?(Path.expand("/etc/passwd"), root)
  end

  test "validate rejects bundle components that resolve outside the root" do
    root = Path.join(System.tmp_dir!(), "elixirdb-bundle-#{System.unique_integer([:positive])}")
    outside = Path.join(System.tmp_dir!(), "elixirdb-outside-#{System.unique_integer([:positive])}")

    on_exit(fn ->
      File.rm_rf(root)
      File.rm_rf(outside)
    end)

    assert {:ok, bundle} = DatabaseBundle.create(root)
    File.write!(DatabaseBundle.sqlite_path(bundle), "sqlite")

    File.mkdir_p!(outside)
    File.rm_rf!(Path.join(root, "blobs"))
    File.ln_s!(outside, Path.join(root, "blobs"))

    assert {:error, %ElixirDB.Error{code: :integrity_violation}} = DatabaseBundle.validate(root)
  end

  test "prepare_for_open reclaims stale temporary uploads" do
    root =
      Path.join(
        System.tmp_dir!(),
        "elixirdb-bundle-#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf(root) end)

    assert {:ok, bundle} = DatabaseBundle.create(root)
    File.write!(DatabaseBundle.sqlite_path(bundle), "sqlite")
    stale = Path.join(DatabaseBundle.tmp_path(bundle), "stale-upload")
    File.write!(stale, "partial")
    File.touch!(stale, {{2000, 1, 1}, {0, 0, 0}})

    assert {:ok, _} = DatabaseBundle.prepare_for_open(root)
    refute File.exists?(stale)
  end
end
