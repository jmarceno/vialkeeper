defmodule ElixirDB.Application do
  @moduledoc "OTP application supervisor for the ElixirDB runtime."
  use Application

  alias ElixirDB.Storage.OpaqueHandle.Server, as: OpaqueHandleServer

  @impl true
  def start(_type, _args) do
    _ = ElixirDB.Diagnostics.validate_backend!()
    listener = Application.get_env(:elixir_db, :listener, ip: {127, 0, 0, 1}, port: 4000)

    enforce_listener_safety!(listener)

    http_server = http_server_child_spec(listener)

    children = [
      OpaqueHandleServer,
      # Starts the OpenTelemetry SDK + exporter if configured, else no-op.
      ElixirDB.Observability.Supervisor,
      {Registry, keys: :unique, name: ElixirDB.Runtime.DatabaseRegistry},
      ElixirDB.Runtime.DatabaseSupervisor,
      {Registry, keys: :unique, name: ElixirDB.Replication.WorkerRegistry},
      ElixirDB.Replication.WorkerSupervisor,
      ElixirDB.Replication.JobManager,
      {Task.Supervisor, name: ElixirDB.TaskSupervisor},
      ElixirDB.Runtime.DatabaseCatalog,
      ElixirDB.DerivedView.Supervisor,
      ElixirDB.DerivedView.Manager,
      http_server
    ]

    opts = [strategy: :one_for_one, name: ElixirDB.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # CONFIG-005 failsafe: refuse to start on a non-loopback interface unless
  # authentication or TLS is enabled, or the operator sets the explicit
  # allow_insecure_remote override.
  #
  # `listener_safety_error/1` is the pure decision (testable without starting
  # Bandit); `enforce_listener_safety!/1` raises on a non-nil error.
  @doc false
  @spec listener_safety_error(keyword()) :: :ok | {:error, String.t()}
  def listener_safety_error(listener) do
    ip = Keyword.get(listener, :ip, {127, 0, 0, 1})

    if loopback?(ip) do
      :ok
    else
      auth = Application.get_env(:elixir_db, :auth, [])
      tls = Application.get_env(:elixir_db, :tls, [])
      security = Application.get_env(:elixir_db, :security, [])

      auth_enabled = Keyword.get(auth, :enabled, false)
      tls_enabled = Keyword.get(tls, :enabled, false)
      allow_insecure = Keyword.get(security, :allow_insecure_remote, false)

      if auth_enabled or tls_enabled or allow_insecure do
        :ok
      else
        {:error,
         """
         ElixirDB refuses to start on a non-loopback interface without
         authentication or TLS enabled.

         Listener IP: #{inspect(ip)}.

         Enable [auth] or [tls] in host.toml, or set
         [security] allow_insecure_remote = true to override (not recommended).
         """}
      end
    end
  end

  defp enforce_listener_safety!(listener) do
    case listener_safety_error(listener) do
      :ok -> :ok
      {:error, message} -> raise message
    end
  end

  defp loopback?({127, _, _, _}), do: true
  defp loopback?({0, 0, 0, 0, 0, 0, 0, 1}), do: true
  defp loopback?(_), do: false

  defp http_server_child_spec(listener) do
    tls = Application.get_env(:elixir_db, :tls, [])

    if Keyword.get(tls, :enabled, false) do
      root = ElixirDB.HostConfig.database_root()

      certfile = ElixirDB.HostConfig.resolve_path(root, tls[:certfile])
      keyfile = ElixirDB.HostConfig.resolve_path(root, tls[:keyfile])

      Bandit.child_spec(
        [plug: ElixirDB.HTTP.Router, scheme: :https, certfile: certfile, keyfile: keyfile] ++
          listener
      )
    else
      Bandit.child_spec([plug: ElixirDB.HTTP.Router, scheme: :http] ++ listener)
    end
  end
end
