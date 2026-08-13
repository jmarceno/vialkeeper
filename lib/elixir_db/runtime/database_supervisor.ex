defmodule ElixirDB.Runtime.DatabaseSupervisor do
  @moduledoc """
  Dynamic supervisor for independent database runtime processes.

  The restart budget absorbs the short retry burst that can occur while a
  killed runtime's descendants release their registered names. It remains
  bounded so a persistently crashing runtime cannot loop indefinitely.
  """
  use DynamicSupervisor

  def start_link(_args \\ []), do: DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__)

  @impl true
  def init(_args),
    do: DynamicSupervisor.init(strategy: :one_for_one, max_restarts: 20, max_seconds: 5)
end
