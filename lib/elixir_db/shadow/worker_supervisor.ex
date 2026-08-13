defmodule ElixirDB.Shadow.WorkerSupervisor do
  @moduledoc "Supervision boundary for worker-side shadow services."
  use DynamicSupervisor

  def start_link(opts \\ []), do: DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @spec start_worker(keyword()) :: DynamicSupervisor.on_start_child()
  def start_worker(opts),
    do: DynamicSupervisor.start_child(__MODULE__, {ElixirDB.Shadow.Worker, opts})

  @spec start_replicator(map(), keyword()) :: DynamicSupervisor.on_start_child()
  def start_replicator(request, opts \\ []) when is_map(request) do
    DynamicSupervisor.start_child(
      __MODULE__,
      {ElixirDB.Shadow.Replicator, Keyword.put(opts, :request, request)}
    )
  end

  @impl true
  def init(_opts),
    do: DynamicSupervisor.init(strategy: :one_for_one, max_restarts: 3, max_seconds: 5)
end
