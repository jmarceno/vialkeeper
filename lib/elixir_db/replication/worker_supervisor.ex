defmodule ElixirDB.Replication.WorkerSupervisor do
  @moduledoc false
  use DynamicSupervisor

  def start_link(_args \\ []), do: DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__)

  @impl true
  def init(_args), do: DynamicSupervisor.init(strategy: :one_for_one)
end
