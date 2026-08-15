defmodule VialKeeper.Search.Supervisor do
  @moduledoc "Dynamically supervises per-database full-text search owners."
  use DynamicSupervisor

  alias VialKeeper.Search.Owner

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(_opts \\ []), do: DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__)

  @spec start_owner(binary(), binary() | nil) :: {:ok, pid()} | {:error, term()}
  def start_owner(uuid, tmp_path) when is_binary(uuid) do
    case Owner.whereis(uuid) do
      pid when is_pid(pid) -> {:ok, pid}
      :undefined -> DynamicSupervisor.start_child(__MODULE__, Owner.child_spec({uuid, tmp_path}))
    end
  end

  @spec stop_owner(binary()) :: :ok
  def stop_owner(uuid) when is_binary(uuid) do
    case Owner.whereis(uuid) do
      pid when is_pid(pid) ->
        _ = DynamicSupervisor.terminate_child(__MODULE__, pid)
        :ok

      :undefined ->
        :ok
    end
  end

  @impl true
  def init(_args), do: DynamicSupervisor.init(strategy: :one_for_one)
end
