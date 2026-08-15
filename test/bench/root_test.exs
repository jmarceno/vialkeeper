defmodule VialKeeper.Bench.RootTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Bench.{Root, Tmp}

  setup do
    parent = unique_dir("approved")
    repo = unique_dir("repo")
    {:ok, approved_parent: parent, repo_root: repo}
  end

  test "configure writes matching pointer and marker", %{
    approved_parent: parent,
    repo_root: repo
  } do
    root = Path.join(parent, "vk")
    {:ok, ctx} = Root.configure(root, env(parent, repo))

    assert ctx.root == Path.expand(root)
    assert File.regular?(ctx.pointer_path)
    assert File.regular?(ctx.marker_path)
    assert {:ok, loaded} = Root.load(env(parent, repo))
    assert loaded.root_id == ctx.root_id
  end

  test "rejects relative root", %{approved_parent: parent, repo_root: repo} do
    assert {:error, message} = Root.configure("relative", env(parent, repo))
    assert message =~ "absolute"
  end

  test "rejects /mnt/other/downloads-evil as a prefix escape" do
    assert {:error, message} =
             Root.configure("/mnt/other/downloads-evil/vk",
               approved_parent: "/mnt/other/downloads",
               repo_root: unique_dir("repo")
             )

    assert message =~ "canonical descendant"
  end

  test "rejects root inside the repository", %{approved_parent: parent} do
    repo = Path.join(parent, "checkout")
    File.mkdir_p!(repo)
    inside = Path.join(repo, "oops")
    assert {:error, message} = Root.configure(inside, env(parent, repo))
    assert message =~ "inside the VialKeeper repository"
  end

  test "rejects root that contains the repository", %{approved_parent: parent} do
    # Place the fake repo under the candidate root.
    nested_repo = Path.join(parent, "vk-contains/repo")
    File.mkdir_p!(nested_repo)

    assert {:error, message} =
             Root.configure(Path.join(parent, "vk-contains"), env(parent, nested_repo))

    assert message =~ "contain the VialKeeper repository"
  end

  test "rejects non-empty unmarked directory", %{approved_parent: parent, repo_root: repo} do
    root = Path.join(parent, "dirty")
    File.mkdir_p!(root)
    File.write!(Path.join(root, "stale.txt"), "nope")

    assert {:error, message} = Root.configure(root, env(parent, repo))
    assert message =~ "non-empty unmarked"
  end

  test "rejects stale pointer UUID mismatch", %{approved_parent: parent, repo_root: repo} do
    root = Path.join(parent, "vk")
    {:ok, ctx} = Root.configure(root, env(parent, repo))

    File.write!(
      ctx.marker_path,
      ~s({"schema_version":1,"project":"vialkeeper","root_id":"00000000-0000-4000-8000-000000000000"}\n)
    )

    assert {:error, message} = Root.load(env(parent, repo))
    assert message =~ "does not match"
  end

  test "rejects missing destination marker", %{approved_parent: parent, repo_root: repo} do
    root = Path.join(parent, "vk")
    {:ok, ctx} = Root.configure(root, env(parent, repo))
    File.rm!(ctx.marker_path)

    assert {:error, message} = Root.load(env(parent, repo))
    assert message =~ "marker is missing"
  end

  test "requires --reuse-existing to attach to an already marked root", %{
    approved_parent: parent,
    repo_root: repo
  } do
    root = Path.join(parent, "vk")
    {:ok, first} = Root.configure(root, env(parent, repo))
    File.rm!(first.pointer_path)

    other_repo = unique_dir("repo2")

    assert {:error, message} = Root.configure(root, env(parent, other_repo))
    assert message =~ "reuse-existing"

    {:ok, reused} = Root.configure(root, env(parent, other_repo) ++ [reuse_existing: true])
    assert reused.root_id == first.root_id
  end

  test "dataset cleanup cannot escape datasets/", %{approved_parent: parent, repo_root: repo} do
    root = Path.join(parent, "vk")
    {:ok, ctx} = Root.configure(root, env(parent, repo))
    outside = Path.join(parent, "not-datasets")
    File.mkdir_p!(outside)

    assert {:error, _} = Root.remove_dataset!(ctx, "..", "not-datasets")
  end

  test "work and report paths stay under the configured root", %{
    approved_parent: parent,
    repo_root: repo
  } do
    root = Path.join(parent, "vk")
    {:ok, ctx} = Root.configure(root, env(parent, repo))

    {:ok, work} = Root.work_run_path(ctx, "fts", "run-1")
    {:ok, report} = Root.report_path(ctx, "out.json")
    {:ok, staging} = Root.staging_path(ctx, "trec-covid", "abc")
    {:ok, cache} = Root.cache_path(ctx, "trec-covid", "v1")

    assert Root.descendant?(work, ctx.root)
    assert Root.descendant?(report, ctx.root)
    assert Root.descendant?(staging, ctx.root)
    assert Root.descendant?(cache, ctx.root)
    refute String.starts_with?(work, File.cwd!())
  end

  test "refuses a symlink component", %{approved_parent: parent, repo_root: repo} do
    real = Path.join(parent, "real")
    File.mkdir_p!(real)
    link = Path.join(parent, "link")
    File.ln_s!(real, link)

    assert {:error, message} = Root.configure(Path.join(link, "vk"), env(parent, repo))
    assert message =~ "symlink"
  end

  test "load fails when no pointer exists", %{approved_parent: parent, repo_root: repo} do
    assert {:error, message} = Root.load(env(parent, repo))
    assert message =~ "not configured"
  end

  test "fails closed when free space cannot be queried", %{
    approved_parent: parent,
    repo_root: repo
  } do
    root = Path.join(parent, "vk")

    assert {:error, message} =
             Root.configure(
               root,
               env(parent, repo) ++ [available_bytes_fun: fn _ -> {:error, :unavailable} end]
             )

    assert message =~ "free space"
  end

  test "rejects relative '.' and '..' roots", %{approved_parent: parent, repo_root: repo} do
    assert {:error, message} = Root.configure(".", env(parent, repo))
    assert message =~ "absolute"

    assert {:error, _} = Root.configure("/mnt/other/downloads/../downloads/vk", env(parent, repo))
  end

  test "rejects a forged destination marker schema", %{approved_parent: parent, repo_root: repo} do
    root = Path.join(parent, "vk")
    {:ok, ctx} = Root.configure(root, env(parent, repo))

    File.write!(
      ctx.marker_path,
      ~s({"schema_version":99,"project":"vialkeeper","root_id":"#{ctx.root_id}"}\n)
    )

    assert {:error, message} = Root.load(env(parent, repo))
    assert message =~ "schema_version"
  end

  test "cleanup with a manipulated dataset name cannot delete the root", %{
    approved_parent: parent,
    repo_root: repo
  } do
    root = Path.join(parent, "vk")
    {:ok, ctx} = Root.configure(root, env(parent, repo))
    marker = ctx.marker_path

    assert {:error, _} = Root.remove_dataset!(ctx, "datasets", "..")
    assert File.regular?(marker)
  end

  test "hard-coded approved parent rejects /mnt/other/downloads-evil" do
    assert {:error, message} =
             Root.configure("/mnt/other/downloads-evil/vk", repo_root: unique_dir("repo"))

    assert message =~ "canonical descendant"
  end

  test "hard-coded approved parent rejects a path outside /mnt/other/downloads" do
    assert {:error, message} =
             Root.configure("/var/tmp/vk-bench-outside", repo_root: unique_dir("repo"))

    assert message =~ "canonical descendant"
  end

  defp env(parent, repo) do
    [approved_parent: parent, repo_root: repo]
  end

  defp unique_dir(prefix), do: Tmp.dir(prefix)
end
