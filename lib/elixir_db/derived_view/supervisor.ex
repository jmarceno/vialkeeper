defmodule ElixirDB.DerivedView.Supervisor do
  @moduledoc "Dynamically supervises materialized-view sessions for open derived databases."
  use DynamicSupervisor

  alias ElixirDB.DerivedView.SessionSupervisor
  alias ElixirDB.Error

  @registry ElixirDB.Runtime.DatabaseRegistry

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(_opts \\ []), do: DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__)

  @spec start_session(binary()) :: {:ok, pid()} | {:error, term()}
  def start_session(uuid) when is_binary(uuid) do
    case session_pid(uuid) do
      {:ok, pid} -> {:ok, pid}
      :error -> DynamicSupervisor.start_child(__MODULE__, SessionSupervisor.child_spec(uuid))
    end
  end

  @spec stop_session(binary()) :: :ok | {:error, term()}
  def stop_session(uuid) when is_binary(uuid) do
    case session_pid(uuid) do
      {:ok, pid} -> DynamicSupervisor.terminate_child(__MODULE__, pid)
      :error -> :ok
    end
  end

  @spec session_pid(binary()) :: {:ok, pid()} | :error
  def session_pid(uuid) when is_binary(uuid) do
    case Registry.lookup(@registry, {:derived_session, uuid}) do
      [{pid, _}] -> {:ok, pid}
      [] -> :error
    end
  end

  @spec task_supervisor_pid(binary()) :: {:ok, pid()} | {:error, Error.t()}
  def task_supervisor_pid(uuid) when is_binary(uuid) do
    case Registry.lookup(@registry, {:derived_task_supervisor, uuid}) do
      [{pid, _}] -> {:ok, pid}
      [] -> {:error, Error.database_closed("derived session task supervisor is not running")}
    end
  end

  @impl true
  def init(_args), do: DynamicSupervisor.init(strategy: :one_for_one)
end
