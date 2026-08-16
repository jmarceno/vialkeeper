defmodule VialKeeper.Runtime.ReadPoolSupervisor do
  @moduledoc """
  Supervises one database's snapshot read pool and its readonly workers.

  The pool starts after `DatabaseOwner` so the writer has already opened the
  file and set WAL. A worker crash restarts that worker; an owner restart still
  tears down this tree with the rest of the runtime.
  """
  use Supervisor

  alias VialKeeper.Runtime.{ChildSpec, ReadPool, ReadWorker}

  @spec child_spec(binary(), pos_integer(), pos_integer()) :: map()
  def child_spec(uuid, pool_size, queue_limit)
      when is_binary(uuid) and is_integer(pool_size) and pool_size > 0 and
             is_integer(queue_limit) and queue_limit > 0 do
    ChildSpec.supervisor(
      {:read_pool_supervisor, uuid},
      {__MODULE__, :start_link, [{uuid, pool_size, queue_limit}]},
      :permanent,
      VialKeeper.Config.shutdown_timeout()
    )
  end

  @spec start_link({binary(), pos_integer(), pos_integer()}) :: Supervisor.on_start()
  def start_link({uuid, pool_size, queue_limit}),
    do: Supervisor.start_link(__MODULE__, {uuid, pool_size, queue_limit}, name: via(uuid))

  defp via(uuid),
    do: {:via, Registry, {VialKeeper.Runtime.DatabaseRegistry, {:read_pool_supervisor, uuid}}}

  @impl true
  def init({uuid, pool_size, queue_limit}) do
    workers =
      for index <- 1..pool_size do
        ReadWorker.child_spec({uuid, index})
      end

    children = [
      ChildSpec.worker(
        {:read_pool, uuid},
        {ReadPool, :start_link, [{uuid, pool_size, queue_limit}]},
        :permanent
      )
      | workers
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
