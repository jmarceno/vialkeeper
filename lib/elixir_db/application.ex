defmodule ElixirDB.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    _ = ElixirDB.Diagnostics.validate_sqlite!()
    listener = Application.get_env(:elixir_db, :listener, ip: {127, 0, 0, 1}, port: 4000)
    http_server = Bandit.child_spec([plug: ElixirDB.HTTP.Router, scheme: :http] ++ listener)

    children = [
      # Starts the OpenTelemetry SDK + exporter if configured, else no-op.
      ElixirDB.Observability.Supervisor,
      {Registry, keys: :unique, name: ElixirDB.Runtime.DatabaseRegistry},
      ElixirDB.Runtime.DatabaseSupervisor,
      {Registry, keys: :unique, name: ElixirDB.Replication.WorkerRegistry},
      ElixirDB.Replication.WorkerSupervisor,
      {Task.Supervisor, name: ElixirDB.TaskSupervisor},
      ElixirDB.Runtime.DatabaseCatalog,
      http_server
    ]

    opts = [strategy: :one_for_one, name: ElixirDB.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
