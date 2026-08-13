defmodule ElixirDB.Shadow.ControlTaskSupervisor do
  @moduledoc "Bounded supervision boundary for asynchronous shadow control work."
  def start_link(opts \\ []),
    do: Task.Supervisor.start_link(Keyword.put_new(opts, :name, __MODULE__))

  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}, type: :supervisor}
  end
end
