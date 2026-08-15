defmodule VialKeeper.Runtime.AtomicWriteTest do
  @moduledoc """
  `AtomicWrite.write/2` durability contract: success path, parent-directory
  auto-creation, and failed-write isolation (no temp leftovers, prior file kept).
  """
  use ExUnit.Case, async: true

  alias VialKeeper.Runtime.AtomicWrite

  setup do
    dir = Path.join(System.tmp_dir!(), "vialkeeper-atomic-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> _ = File.rm_rf(dir) end)
    {:ok, dir: dir}
  end

  test "writes contents and the file is readable", %{dir: dir} do
    path = Path.join(dir, "out.bin")
    assert :ok = AtomicWrite.write(path, "hello")
    assert "hello" == File.read!(path)
  end

  test "creates missing parent directories", %{dir: dir} do
    path = Path.join([dir, "nested", "deep", "out.bin"])
    assert :ok = AtomicWrite.write(path, "nested")
    assert "nested" == File.read!(path)
  end

  test "replaces an existing file atomically", %{dir: dir} do
    path = Path.join(dir, "out.bin")
    assert :ok = AtomicWrite.write(path, "old")
    assert :ok = AtomicWrite.write(path, "new")
    assert "new" == File.read!(path)
  end

  test "failed write leaves no temp file behind", %{dir: dir} do
    path = Path.join(dir, "out.bin")
    assert :ok = AtomicWrite.write(path, "prior")

    # Force a failure by making the directory read-only so rename cannot land.
    # The temp file is written inside `dir`, so a read-only dir blocks the
    # rename step. Permissions are restored in the setup on_exit handler.
    File.chmod!(dir, 0o500)
    assert {:error, _} = AtomicWrite.write(path, "next")
    File.chmod!(dir, 0o755)

    # Prior content is intact and no .tmp.* leftovers remain.
    assert "prior" == File.read!(path)

    assert [] ==
             File.ls!(dir)
             |> Enum.filter(&String.starts_with?(&1, "out.bin.tmp."))
  end
end
