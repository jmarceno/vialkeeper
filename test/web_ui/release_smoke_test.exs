defmodule ElixirDB.WebUI.ReleaseSmokeTest do
  @moduledoc """
  Production-release offline smoke for the embedded administration console.

  Builds a release, copies only the release artifact plus a fresh database root
  into a tree that lacks repository `assets/`, and proves shell/assets/home still
  load from embedded BEAM modules.
  """
  use ExUnit.Case, async: false

  alias ElixirDB.Eventual
  alias ElixirDB.WebUI.Assets

  @moduletag :slow

  test "release serves embedded UI without repository assets tree" do
    project = File.cwd!()
    work = Path.join(System.tmp_dir!(), "elixirdb-ui-release-#{System.unique_integer([:positive])}")
    File.mkdir_p!(work)
    on_exit(fn -> File.rm_rf(work) end)

    {output, status} =
      System.cmd("mix", ["release.build"],
        cd: project,
        env: [{"MIX_ENV", "prod"}],
        stderr_to_stdout: true
      )

    assert status == 0, output
    release_src = Path.join(project, "_build/prod/rel/elixir_db")
    assert File.dir?(release_src)

    release_dst = Path.join(work, "rel")
    File.cp_r!(release_src, release_dst)

    db_root = Path.join(work, "data")
    File.mkdir_p!(db_root)

    refute File.exists?(Path.join(work, "assets"))
    refute File.exists?(Path.join(release_dst, "assets/web_ui"))

    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, {_ip, port}} = :inet.sockname(listen)
    :ok = :gen_tcp.close(listen)

    File.write!(
      Path.join(db_root, "host.toml"),
      """
      [listener]
      ip = "127.0.0.1"
      port = #{port}

      [web_ui]
      enabled = true

      [auth]
      enabled = false
      tokens = []
      """
    )

    bin = Path.join(release_dst, "bin/elixir_db")
    assert File.exists?(bin)

    {start_out, start_status} =
      System.cmd(bin, ["daemon"],
        env: [{"ELIXIR_DB_ROOT", db_root}],
        stderr_to_stdout: true
      )

    assert start_status == 0, start_out

    on_exit(fn ->
      _ = System.cmd(bin, ["stop"], env: [{"ELIXIR_DB_ROOT", db_root}], stderr_to_stdout: true)
    end)

    base = "http://127.0.0.1:#{port}"

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
    assert shell =~ "ElixirDB"
    assert shell =~ "/ui/assets/htmx.min.js"
    refute shell =~ "cdn.jsdelivr"
    refute shell =~ "unpkg.com"

    assert {:ok, %{status: 200, body: htmx}} = Req.get(base <> "/ui/assets/htmx.min.js")
    assert byte_size(htmx) == Assets.htmx_expected_size()

    assert {:ok, %{status: 200, body: home}} = Req.get(base <> "/ui/fragments/home")
    assert home =~ "Console" or home =~ "Overview" or home =~ "Databases"
  end
end
