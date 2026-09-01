defmodule VialKeeper.TestSupport.ContainerEngine do
  @moduledoc """
  Docker/Podman helpers for clean-host integration drills.

  `mix check.full` requires a working container engine. When neither Docker nor
  Podman responds to `info`, helpers fail closed with an explicit message.
  """

  import ExUnit.Assertions

  @image "docker.io/library/debian@sha256:abc9cb88a5587630d7f915f47b23b0668fe250fbfc6457aa4d52b534c1bbf73f"

  @builder_image "docker.io/hexpm/elixir@sha256:9804c9fd6cefea19e2b1095763057d08d634cac29a0994503a468427a64e5e12"

  @missing_engine_message """
  mix check.full requires Docker or Podman with a running daemon (docker info / podman info).
  Install and start one engine, then rerun mix check.full.
  """

  @doc """
  Returns `"docker"` or `"podman"` when an engine daemon is reachable.
  """
  @spec resolve() :: {:ok, String.t()} | :missing
  def resolve do
    cond do
      engine_available?("docker") -> {:ok, "docker"}
      engine_available?("podman") -> {:ok, "podman"}
      true -> :missing
    end
  end

  @doc """
  Like `resolve/0`, but flunks when no engine is available.
  """
  @spec require_engine!() :: String.t()
  def require_engine! do
    case resolve() do
      {:ok, cmd} -> cmd
      :missing -> flunk(@missing_engine_message)
    end
  end

  @doc """
  Returns true when the bind-mounted release starts inside the restore image.
  """
  @spec release_runs?(String.t()) :: boolean()
  def release_runs?(release_dir) when is_binary(release_dir) do
    bin = Path.join(release_dir, "bin/vial_keeper")

    if File.regular?(bin) do
      cmd = require_engine!()
      ensure_image!()

      probe_root =
        Path.join(System.tmp_dir!(), "vialkeeper-probe-#{System.unique_integer([:positive])}")

      File.mkdir_p!(probe_root)

      {output, status} =
        System.cmd(
          cmd,
          [
            "run",
            "--rm",
            "-v",
            "#{release_dir}:/opt/vial_keeper:ro",
            "-v",
            "#{probe_root}:/var/lib/vialkeeper",
            "-e",
            "VIAL_KEEPER_ROOT=/var/lib/vialkeeper"
          ] ++
            host_id_env() ++
            [
              @image,
              "/bin/sh",
              "-lc",
              install_runtime_deps_command() <>
                "chown -R ${HOST_UID}:${HOST_GID} /var/lib/vialkeeper && " <>
                "exec setpriv --reuid=${HOST_UID} --regid=${HOST_GID} --clear-groups -- " <>
                "/opt/vial_keeper/bin/vial_keeper eval 'IO.puts(:portable_ok)'"
            ],
          stderr_to_stdout: true
        )

      _ = File.rm_rf(probe_root)
      status == 0 and output =~ "portable_ok"
    else
      false
    end
  end

  @doc "Returns the digest-pinned Debian runtime image used for portable release probes."
  @spec runtime_image() :: String.t()
  def runtime_image, do: @image

  @doc "Returns the digest-pinned Elixir builder image for portable release builds."
  @spec builder_image() :: String.t()
  def builder_image, do: @builder_image

  @doc """
  Ensures the pinned glibc base image is present locally.
  """
  @spec ensure_image!() :: :ok
  def ensure_image! do
    cmd = require_engine!()
    ensure_image!(cmd, @image)
  end

  @doc "Ensures the digest-pinned portable-release builder image is present locally."
  @spec ensure_builder_image!() :: :ok
  def ensure_builder_image! do
    cmd = require_engine!()
    ensure_image!(cmd, @builder_image)
  end

  @doc """
  Starts a detached container running the release in the foreground (`start`).

  Options:
  * `:name` — unique container name
  * `:release_dir` — host path bind-mounted read-only at `/opt/vial_keeper`
  * `:data_root` — host path bind-mounted at `/var/lib/vialkeeper`
  """
  @spec run!(keyword()) :: String.t()
  def run!(opts) do
    cmd = require_engine!()
    ensure_image!()

    name = Keyword.fetch!(opts, :name)
    release_mount = Keyword.fetch!(opts, :release_dir)
    data_mount = Keyword.fetch!(opts, :data_root)

    args =
      [
        "run",
        "-d",
        "--rm",
        "--name",
        name,
        "--network",
        "host",
        "-v",
        "#{release_mount}:/opt/vial_keeper:ro",
        "-v",
        "#{data_mount}:/var/lib/vialkeeper",
        "-e",
        "VIAL_KEEPER_ROOT=/var/lib/vialkeeper"
      ] ++
        host_id_env() ++
        [
          @image,
          "/bin/sh",
          "-lc",
          install_runtime_deps_command() <>
            "chown -R ${HOST_UID}:${HOST_GID} /var/lib/vialkeeper && " <>
            "exec setpriv --reuid=${HOST_UID} --regid=${HOST_GID} --clear-groups -- " <>
            "/opt/vial_keeper/bin/vial_keeper start"
        ]

    {output, status} = System.cmd(cmd, args, stderr_to_stdout: true)
    assert status == 0, output
    String.trim(output)
  end

  @doc """
  Stops a running container, ignoring errors when it is already gone.
  """
  @spec stop(String.t()) :: :ok
  def stop(name) when is_binary(name) do
    case resolve() do
      {:ok, cmd} ->
        _ = System.cmd(cmd, ["stop", name], stderr_to_stdout: true)
        :ok

      :missing ->
        :ok
    end
  end

  @doc """
  Runs `bin/vial_keeper eval` inside a running container.
  """
  @spec eval!(String.t(), String.t()) :: {String.t(), 0}
  def eval!(name, expression) when is_binary(name) and is_binary(expression) do
    cmd = require_engine!()
    {uid, gid} = current_ids!()

    {output, status} =
      System.cmd(
        cmd,
        [
          "exec",
          name,
          "setpriv",
          "--reuid=#{uid}",
          "--regid=#{gid}",
          "--clear-groups",
          "--",
          "/opt/vial_keeper/bin/vial_keeper",
          "eval",
          expression
        ],
        stderr_to_stdout: true
      )

    assert status == 0, output
    {output, status}
  end

  defp host_id_env do
    {uid, gid} = current_ids!()

    [
      "-e",
      "HOST_UID=#{uid}",
      "-e",
      "HOST_GID=#{gid}"
    ]
  end

  defp install_runtime_deps_command do
    "export DEBIAN_FRONTEND=noninteractive && " <>
      "apt-get update -qq && apt-get install -y -qq libncurses6 util-linux >/dev/null && "
  end

  defp engine_available?(command) do
    case System.find_executable(command) do
      nil ->
        false

      _executable ->
        {_output, status} = System.cmd(command, ["info"], stderr_to_stdout: true)
        status == 0
    end
  end

  defp ensure_image!(cmd, image) do
    case System.cmd(cmd, ["image", "inspect", image], stderr_to_stdout: true) do
      {_output, 0} ->
        :ok

      {_output, _status} ->
        {output, status} = System.cmd(cmd, ["pull", image], stderr_to_stdout: true)
        assert status == 0, "failed to pull #{image}: #{output}"
        :ok
    end
  end

  defp current_ids! do
    {uid_out, 0} = System.cmd("id", ["-u"])
    {gid_out, 0} = System.cmd("id", ["-g"])
    {String.trim(uid_out), String.trim(gid_out)}
  end
end
