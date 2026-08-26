defmodule VialKeeper.WebUI.ReleaseSmokeTest do
  @moduledoc """
  Production-release offline smoke for the embedded administration console.

  Builds a release, copies only the release artifact plus a fresh database root
  into a tree that lacks repository `assets/`, and proves shell/assets/home still
  load from embedded BEAM modules.
  """
  use ExUnit.Case, async: false

  @moduletag :integration

  alias VialKeeper.Eventual
  alias VialKeeper.TestSupport.ProdRelease
  alias VialKeeper.WebUI.Assets

  @moduletag :slow

  test "release serves embedded UI without repository assets tree" do
    work =
      Path.join(System.tmp_dir!(), "vialkeeper-ui-release-#{System.unique_integer([:positive])}")

    File.mkdir_p!(work)
    on_exit(fn -> File.rm_rf(work) end)

    release_dst = Path.join(work, "rel")
    ProdRelease.copy_to!(release_dst)

    db_root = Path.join(work, "data")
    File.mkdir_p!(db_root)

    refute File.exists?(Path.join(work, "assets"))
    refute File.exists?(Path.join(release_dst, "assets/web_ui"))

    port = ProdRelease.allocate_loopback_port!()
    ProdRelease.write_host_toml!(db_root, port, web_ui: true)
    assert File.exists?(ProdRelease.bin_path(release_dst))
    assert :ok = ProdRelease.start_daemon!(release_dst, db_root)

    on_exit(fn -> ProdRelease.stop_daemon(release_dst, db_root) end)

    base = ProdRelease.base_url(port)

    Eventual.eventually(
      fn ->
        case Req.get(base <> "/ui", receive_timeout: 1_000) do
          {:ok, %{status: 200}} -> true
          _ -> false
        end
      end,
      timeout: 30_000,
      message: "release UI did not become ready"
    )

    assert {:ok, %{status: 200, body: shell}} = Req.get(base <> "/ui")
    assert shell =~ "VialKeeper"
    assert shell =~ "/ui/assets/htmx.min.js"
    refute shell =~ "cdn.jsdelivr"
    refute shell =~ "unpkg.com"

    assert {:ok, %{status: 200, body: htmx}} = Req.get(base <> "/ui/assets/htmx.min.js")
    assert byte_size(htmx) == Assets.htmx_expected_size()

    assert {:ok, %{status: 200, body: home}} = Req.get(base <> "/ui/fragments/home")
    assert home =~ "Console" or home =~ "Overview" or home =~ "Databases"
  end
end
