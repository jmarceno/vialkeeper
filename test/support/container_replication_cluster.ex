defmodule VialKeeper.TestSupport.ContainerReplicationCluster do
  @moduledoc """
  Starts a private bridge network of release containers for the opt-in
  replication drill.

  Each container is the digest-pinned Debian runtime image used by the
  clean-host restore drill. That image is a small glibc userspace; the OTP
  release is built against glibc, so Alpine cannot run it. The drill requires
  at least three containers.

  Containers publish loopback ports for the test process and replicate to each
  other's bridge IPs. `isolate!/2` drops traffic to and from the other
  containers with iptables inside the target network namespace, leaving the
  loopback published port usable. `stop_container!/2` and `start_container!/2`
  power a container off and on without deleting its data root.
  """

  import ExUnit.Assertions
  import ExUnit.Callbacks

  alias VialKeeper.Eventual
  alias VialKeeper.TestSupport.{ContainerEngine, ProdRelease}

  @internal_port 4000
  @start_timeout_ms 240_000

  @type node_info :: %{
          name: String.t(),
          container: String.t(),
          host_port: pos_integer(),
          base_url: String.t(),
          peer_base_url: String.t(),
          data_root: String.t(),
          ip: String.t()
        }

  @type t :: %__MODULE__{
          engine: String.t(),
          network: String.t(),
          release_dir: String.t(),
          token: String.t(),
          nodes: [node_info()]
        }

  defstruct [:engine, :network, :release_dir, :token, :nodes]

  @doc """
  Builds a portable release when needed and starts `count` containers (default
  and minimum 3) on a fresh bridge network.
  """
  @spec start!(keyword()) :: t()
  def start!(opts \\ []) when is_list(opts) do
    count = container_count!(opts)
    id = System.unique_integer([:positive])
    work = Path.join(System.tmp_dir!(), "vialkeeper-container-repl-#{id}")
    network = "vk-repl-#{id}"
    containers = Enum.map(1..count, &container_name(id, &1))
    engine = ContainerEngine.require_engine!()

    on_exit(fn -> cleanup(engine, network, containers, work) end)

    :ok = File.mkdir_p!(work)
    release_dir = Path.join(work, "rel")
    _release = ProdRelease.ensure_portable_for_drill!(release_dir)
    :ok = ContainerEngine.ensure_image!()
    _created = engine_cmd!(engine, ["network", "create", network])

    token = Base.encode16(:crypto.strong_rand_bytes(32), case: :lower)
    digest = :crypto.hash(:sha256, token) |> Base.encode16(case: :lower)

    specs =
      containers
      |> Enum.with_index(1)
      |> Enum.map(fn {container, index} ->
        data_root = Path.join(work, "node-#{index}")
        :ok = File.mkdir_p!(data_root)
        write_host_toml!(data_root, digest)

        %{
          engine: engine,
          network: network,
          release_dir: release_dir,
          container: container,
          name: "n#{index}",
          data_root: data_root,
          host_port: ProdRelease.allocate_loopback_port!()
        }
      end)

    nodes =
      specs
      |> Task.async_stream(&start_node!/1, timeout: @start_timeout_ms, ordered: true)
      |> Enum.map(&unwrap_start!/1)

    %__MODULE__{
      engine: engine,
      network: network,
      release_dir: release_dir,
      token: token,
      nodes: nodes
    }
  end

  @doc "Drops forwarded traffic between `node` and every other container."
  @spec isolate!(t(), node_info()) :: :ok
  def isolate!(%__MODULE__{} = cluster, %{container: container} = node) do
    peers = Enum.reject(cluster.nodes, &(&1.container == container))

    Enum.each(peers, fn peer ->
      _input = exec!(cluster, node, ["iptables", "-I", "INPUT", "-s", peer.ip, "-j", "DROP"])
      _output = exec!(cluster, node, ["iptables", "-I", "OUTPUT", "-d", peer.ip, "-j", "DROP"])
    end)

    :ok
  end

  @doc "Restores forwarded traffic removed by `isolate!/2`."
  @spec rejoin!(t(), node_info()) :: :ok
  def rejoin!(%__MODULE__{} = cluster, %{container: container} = node) do
    peers = Enum.reject(cluster.nodes, &(&1.container == container))

    Enum.each(peers, fn peer ->
      _input = exec!(cluster, node, ["iptables", "-D", "INPUT", "-s", peer.ip, "-j", "DROP"])
      _output = exec!(cluster, node, ["iptables", "-D", "OUTPUT", "-d", peer.ip, "-j", "DROP"])
    end)

    :ok
  end

  @doc "Stops a container without removing its data root."
  @spec stop_container!(t(), node_info()) :: :ok
  def stop_container!(%__MODULE__{engine: engine}, %{container: container}) do
    _stopped = engine_cmd!(engine, ["stop", "-t", "20", container])
    :ok
  end

  @doc "Starts a container previously stopped with `stop_container!/2`."
  @spec start_container!(t(), node_info()) :: :ok
  def start_container!(%__MODULE__{engine: engine}, %{container: container}) do
    _started = engine_cmd!(engine, ["start", container])
    :ok
  end

  @doc "Returns the container's combined stdout and stderr."
  @spec logs(t(), node_info()) :: String.t()
  def logs(%__MODULE__{engine: engine}, %{container: container}) do
    {output, _status} = System.cmd(engine, ["logs", container], stderr_to_stdout: true)
    output
  end

  defp container_count!(opts) do
    case Keyword.get(opts, :count, 3) do
      count when is_integer(count) and count >= 3 ->
        count

      other ->
        flunk("container replication requires at least 3 containers, got #{inspect(other)}")
    end
  end

  defp container_name(id, index), do: "vk#{id}n#{index}"

  defp start_node!(spec) do
    _started =
      engine_cmd!(
        spec.engine,
        [
          "run",
          "-d",
          "--name",
          spec.container,
          "--network",
          spec.network,
          "--cap-add",
          "NET_ADMIN",
          "-p",
          "127.0.0.1:#{spec.host_port}:#{@internal_port}",
          "-v",
          "#{spec.release_dir}:/opt/vial_keeper:ro",
          "-v",
          "#{spec.data_root}:/var/lib/vialkeeper",
          "-e",
          "VIAL_KEEPER_ROOT=/var/lib/vialkeeper"
        ] ++
          host_id_env() ++ [ContainerEngine.runtime_image(), "/bin/sh", "-lc", bootstrap_command()]
      )

    ip = wait_for_ip!(spec)

    %{
      name: spec.name,
      container: spec.container,
      host_port: spec.host_port,
      base_url: "http://127.0.0.1:#{spec.host_port}",
      peer_base_url: "http://#{ip}:#{@internal_port}",
      data_root: spec.data_root,
      ip: ip
    }
  end

  defp unwrap_start!({:ok, node}), do: node

  defp unwrap_start!({:exit, reason}),
    do: flunk("container failed to start: #{inspect(reason)}")

  defp wait_for_ip!(spec) do
    Eventual.eventually(
      fn ->
        case container_ip(spec.engine, spec.network, spec.container) do
          ip when ip in ["", "<no value>"] -> false
          ip -> ip
        end
      end,
      timeout: 15_000,
      interval: 200,
      message: "container #{spec.container} did not receive a bridge IP"
    )
  end

  defp container_ip(engine, network, container) do
    format = ~s[{{(index .NetworkSettings.Networks "#{network}").IPAddress}}]

    case System.cmd(engine, ["inspect", "-f", format, container], stderr_to_stdout: true) do
      {output, 0} -> String.trim(output)
      {_output, _status} -> ""
    end
  end

  defp exec!(%__MODULE__{engine: engine}, %{container: container}, args) do
    engine_cmd!(engine, ["exec", container | args])
  end

  defp write_host_toml!(root, digest) do
    :ok =
      File.write!(Path.join(root, "host.toml"), """
      [listener]
      ip = "0.0.0.0"
      port = #{@internal_port}

      [web_ui]
      enabled = false

      [auth]
      enabled = true
      tokens = ["#{digest}"]
      """)
  end

  defp bootstrap_command do
    """
    set -eu
    if ! dpkg -s libncurses6 >/dev/null 2>&1 || ! command -v setpriv >/dev/null 2>&1 || ! command -v iptables >/dev/null 2>&1; then
      export DEBIAN_FRONTEND=noninteractive
      apt-get update -qq
      apt-get install -y -qq libncurses6 util-linux iptables >/dev/null
    fi
    chown -R ${HOST_UID}:${HOST_GID} /var/lib/vialkeeper
    exec setpriv --reuid=${HOST_UID} --regid=${HOST_GID} --clear-groups -- /opt/vial_keeper/bin/vial_keeper start
    """
  end

  defp host_id_env do
    {uid, 0} = System.cmd("id", ["-u"])
    {gid, 0} = System.cmd("id", ["-g"])

    ["-e", "HOST_UID=#{String.trim(uid)}", "-e", "HOST_GID=#{String.trim(gid)}"]
  end

  defp engine_cmd!(engine, args) do
    case System.cmd(engine, args, stderr_to_stdout: true) do
      {output, 0} ->
        String.trim(output)

      {output, status} ->
        detail = String.slice(output, -2_000, 2_000)
        flunk("#{engine} #{Enum.join(args, " ")} exited #{status}: #{detail}")
    end
  end

  defp cleanup(engine, network, containers, work) do
    Enum.each(containers, fn name ->
      _removed = System.cmd(engine, ["rm", "-f", name], stderr_to_stdout: true)
    end)

    _network = System.cmd(engine, ["network", "rm", network], stderr_to_stdout: true)
    _work = File.rm_rf(work)
    :ok
  end
end
