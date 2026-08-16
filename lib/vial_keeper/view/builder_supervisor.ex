defmodule VialKeeper.View.BuilderSupervisor do
  @moduledoc """
  Dynamic supervisor that owns one `VialKeeper.View.Builder` process per view.
  """
  use DynamicSupervisor

  alias VialKeeper.Runtime.ChildSpec
  alias VialKeeper.View.Builder

  @spec child_spec(binary()) :: map()
  def child_spec(uuid),
    do:
      ChildSpec.supervisor(
        {:view_builder_supervisor, uuid},
        {__MODULE__, :start_link, [uuid]},
        :permanent
      )

  @spec start_link(binary()) :: Supervisor.on_start()
  def start_link(uuid),
    do: DynamicSupervisor.start_link(__MODULE__, uuid, name: via(uuid))

  @spec via(binary()) :: {:via, module(), term()}
  def via(uuid),
    do: {:via, Registry, {VialKeeper.Runtime.DatabaseRegistry, {:view_builder_supervisor, uuid}}}

  @spec start_builder(binary(), binary()) :: DynamicSupervisor.on_start_child()
  @spec start_builder(binary(), binary(), keyword()) :: DynamicSupervisor.on_start_child()
  def start_builder(uuid, view_id, opts \\ []) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:view_builder_supervisor, uuid}) do
      [{supervisor, _}] ->
        DynamicSupervisor.start_child(
          supervisor,
          Builder.child_spec(Keyword.merge([uuid: uuid, view_id: view_id], opts))
        )

      [] ->
        {:error, VialKeeper.Error.database_closed("view builder supervisor is not running")}
    end
  end

  @impl true
  def init(_uuid), do: DynamicSupervisor.init(strategy: :one_for_one)
end
