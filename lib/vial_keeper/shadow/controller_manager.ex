defmodule VialKeeper.Shadow.ControllerManager do
  @moduledoc "Starts source-local reconcilers from durable desired state after host boot."
  use GenServer

  alias VialKeeper.Shadow.Reconciler

  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @impl true
  def init(_opts) do
    send(self(), :recover)
    {:ok, %{}}
  end

  @impl true
  def handle_info(:recover, state) do
    :ok = Reconciler.recover_desired()
    {:noreply, state}
  end
end
