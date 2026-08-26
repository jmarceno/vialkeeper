defmodule VialKeeper.TestSupport.ProdRelease do
  @moduledoc """
  Builds, copies, and drives the production OTP release in integration tests.
  """

  import ExUnit.Assertions

  alias VialKeeper.TestSupport.ContainerEngine

  @release_rel_path "_build/prod/rel/vial_keeper"

  @doc """
  Returns the project-relative release directory after rebuilding it from the
  current checkout.
  """
  @spec ensure_built!() :: String.t()
  def ensure_built! do
    project = File.cwd!()
    release_src = Path.join(project, @release_rel_path)

    {output, status} =
      System.cmd("mix", ["release.build"],
        cd: project,
        env: [{"MIX_ENV", "prod"}],
        stderr_to_stdout: true
      )

    assert status == 0, output
    assert File.dir?(release_src)
    release_src
  end

  @doc """
  Copies the built release tree into `dest_dir` and returns that path.
  """
  @spec copy_to!(String.t()) :: String.t()
  def copy_to!(dest_dir) do
    _copied = File.cp_r!(ensure_built!(), dest_dir)
    dest_dir
  end

  @doc """
  Ensures a release artifact that runs inside the drill restore container.

  Reuses a host-built release when it already starts in the pinned glibc image;
  otherwise builds a portable release inside the matching Elixir builder image.
  """
  @spec ensure_portable_for_drill!(String.t()) :: String.t()
  def ensure_portable_for_drill!(dest_dir) do
    _cmd = ContainerEngine.require_engine!()
    :ok = File.mkdir_p!(dest_dir)

    host_release = ensure_built!()

    cond do
      ContainerEngine.release_runs?(dest_dir) ->
        dest_dir

      File.dir?(host_release) and ContainerEngine.release_runs?(host_release) ->
        _removed = File.rm_rf!(dest_dir)
        :ok = File.mkdir_p!(dest_dir)
        _copied = File.cp_r!(host_release, dest_dir)
        dest_dir

      true ->
        build_portable_release!(dest_dir)
        dest_dir
    end
  end

  @spec bin_path(String.t()) :: String.t()
  def bin_path(release_dir), do: Path.join(release_dir, "bin/vial_keeper")

  @doc """
  Binds an ephemeral loopback TCP port and returns its number.
  """
  @spec allocate_loopback_port!() :: pos_integer()
  def allocate_loopback_port! do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, {_ip, port}} = :inet.sockname(listen)
    :ok = :gen_tcp.close(listen)
    port
  end

  @doc """
  Writes a minimal `host.toml` under `root` for loopback HTTP tests.
  """
  @spec write_host_toml!(String.t(), pos_integer(), keyword()) :: :ok
  def write_host_toml!(root, port, opts \\ []) do
    web_ui = Keyword.get(opts, :web_ui, false)

    File.write!(
      Path.join(root, "host.toml"),
      """
      [listener]
      ip = "127.0.0.1"
      port = #{port}

      [web_ui]
      enabled = #{web_ui}

      [auth]
      enabled = false
      tokens = []
      """
    )
  end

  @doc """
  Starts `bin/vial_keeper daemon` with `VIAL_KEEPER_ROOT` set to `root`.
  """
  @spec start_daemon!(String.t(), String.t()) :: :ok
  def start_daemon!(release_dir, root) do
    bin = bin_path(release_dir)

    {output, status} =
      System.cmd(bin, ["daemon"], env: [{"VIAL_KEEPER_ROOT", root}], stderr_to_stdout: true)

    assert status == 0, output
    :ok
  end

  @doc """
  Stops a daemon started via `start_daemon!/2`.
  """
  @spec stop_daemon!(String.t(), String.t()) :: :ok
  def stop_daemon!(release_dir, root) do
    bin = bin_path(release_dir)

    {output, status} =
      System.cmd(bin, ["stop"],
        env: [{"VIAL_KEEPER_ROOT", root}],
        stderr_to_stdout: true
      )

    assert status == 0, output
    :ok
  end

  @doc "Stops a test daemon during cleanup, ignoring an already-stopped release."
  @spec stop_daemon(String.t(), String.t()) :: :ok
  def stop_daemon(release_dir, root) do
    bin = bin_path(release_dir)
    _result = System.cmd(bin, ["stop"], env: [{"VIAL_KEEPER_ROOT", root}], stderr_to_stdout: true)
    :ok
  end

  @doc """
  Returns the HTTP base URL for a release daemon rooted at `root` on `port`.
  """
  @spec base_url(pos_integer()) :: String.t()
  def base_url(port), do: "http://127.0.0.1:#{port}"

  defp build_portable_release!(dest_dir) do
    cmd = ContainerEngine.require_engine!()
    project = File.cwd!()
    :ok = ContainerEngine.ensure_builder_image!()

    script = """
    set -euo pipefail
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq
    apt-get install -y -qq curl build-essential pkg-config libssl-dev >/dev/null
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
    . "$HOME/.cargo/env"
    rm -rf /build
    cp -a /project/. /build/
    cd /build
    rm -rf _build native/tantivy_ex/native/tantivy_ex/target
    mkdir -p /tmp/tantivy-target
    ln -sfn /tmp/tantivy-target native/tantivy_ex/native/tantivy_ex/target
    MIX_ENV=prod mix local.hex --force
    MIX_ENV=prod mix local.rebar --force
    MIX_ENV=prod mix release.build
    rm -rf /out/*
    cp -a _build/prod/rel/vial_keeper/. /out/
    """

    {output, status} =
      System.cmd(
        cmd,
        [
          "run",
          "--rm",
          "-v",
          "#{project}:/project",
          "-v",
          "#{dest_dir}:/out",
          "-w",
          "/project",
          ContainerEngine.builder_image(),
          "/bin/bash",
          "-lc",
          script
        ],
        stderr_to_stdout: true
      )

    assert status == 0, output

    assert ContainerEngine.release_runs?(dest_dir),
           "portable release build did not start in the restore image"
  end
end
