defmodule ElixirDB.View.BuilderSupervisor do
  @moduledoc """
  Dynamic supervisor that owns one `ElixirDB.View.Builder` process per view.
  """
  use DynamicSupervisor

  alias ElixirDB.Runtime.ChildSpec
  alias ElixirDB.View.Builder

  def child_spec(uuid),
    do:
      ChildSpec.supervisor(
        {:view_builder_supervisor, uuid},
        {__MODULE__, :start_link, [uuid]},
        :permanent
      )

  def start_link(uuid),
    do: DynamicSupervisor.start_link(__MODULE__, uuid, name: via(uuid))

  def via(uuid),
    do: {:via, Registry, {ElixirDB.Runtime.DatabaseRegistry, {:view_builder_supervisor, uuid}}}

  def start_builder(uuid, view_id, opts \\ []) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:view_builder_supervisor, uuid}) do
      [{supervisor, _}] ->
        DynamicSupervisor.start_child(
          supervisor,
          Builder.child_spec(Keyword.merge([uuid: uuid, view_id: view_id], opts))
        )

      [] ->
        {:error, ElixirDB.Error.database_closed("view builder supervisor is not running")}
    end
  end

  @impl true
  def init(_uuid), do: DynamicSupervisor.init(strategy: :one_for_one)
end
