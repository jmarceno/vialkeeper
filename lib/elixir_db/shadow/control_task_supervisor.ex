defmodule ElixirDB.Shadow.ControlTaskSupervisor do
  @moduledoc "Bounded supervision boundary for asynchronous shadow control work."

  def start_link(opts \\ []),
    do: Task.Supervisor.start_link(Keyword.put_new(opts, :name, __MODULE__))

  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}, type: :supervisor}
  end

  @spec async((-> term())) :: Task.t()
  def async(fun) when is_function(fun, 0) do
    Task.Supervisor.async_nolink(supervisor(), fun)
  end

  defp supervisor do
    cond do
      Process.whereis(__MODULE__) -> __MODULE__
      Process.whereis(ElixirDB.TaskSupervisor) -> ElixirDB.TaskSupervisor
      true -> raise "shadow control task supervisor is not running"
    end
  end
end
