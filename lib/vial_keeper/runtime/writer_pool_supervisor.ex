defmodule VialKeeper.Runtime.WriterPoolSupervisor do
  @moduledoc """
  Supervises one database's writer pool and its writer slots.

  The pool exists only when it can help: the effective writer count
  `min(writer_pool_size, backend max_writers)` must exceed one and the backend
  must open extra writer connections (a disk database). Otherwise this
  supervisor is not started and concurrent writes run on `DatabaseOwner`.

  It starts after `DatabaseOwner` and the read pool. A slot crash restarts that
  slot; an owner restart tears this tree down with the rest of the runtime.
  """
  use Supervisor

  alias VialKeeper.Runtime.{ChildSpec, DatabaseOwner, WriterPool, WriterSlot}
  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.Lifecycle

  @spec child_spec(binary(), pos_integer(), pos_integer()) :: map()
  def child_spec(uuid, pool_size, queue_limit)
      when is_binary(uuid) and is_integer(pool_size) and pool_size > 0 and
             is_integer(queue_limit) and queue_limit > 0 do
    ChildSpec.supervisor(
      {:writer_pool_supervisor, uuid},
      {__MODULE__, :start_link, [{uuid, pool_size, queue_limit}]},
      :permanent,
      VialKeeper.Config.shutdown_timeout()
    )
  end

  @spec start_link({binary(), pos_integer(), pos_integer()}) :: Supervisor.on_start() | :ignore
  def start_link({uuid, pool_size, queue_limit}),
    do: Supervisor.start_link(__MODULE__, {uuid, pool_size, queue_limit}, name: via(uuid))

  defp via(uuid),
    do: {:via, Registry, {VialKeeper.Runtime.DatabaseRegistry, {:writer_pool_supervisor, uuid}}}

  @doc """
  Returns how many writer slots the database gets:
  `min(writer_pool_size, backend max_writers)`.
  """
  @spec effective_writers(BackendContext.t(), pos_integer()) :: pos_integer()
  def effective_writers(%BackendContext{} = owner_context, pool_size),
    do: min(pool_size, Lifecycle.writer_capabilities(owner_context).max_writers)

  @impl true
  def init({uuid, pool_size, queue_limit}) do
    with {:ok, %BackendContext{} = owner} <- DatabaseOwner.writer_source(uuid),
         writers when writers > 1 <- effective_writers(owner, pool_size),
         true <- extra_writers?(owner) do
      slots = for index <- 1..writers, do: WriterSlot.child_spec({uuid, index})

      children = [
        ChildSpec.worker(
          {:writer_pool, uuid},
          {WriterPool, :start_link, [{uuid, writers, queue_limit}]},
          :permanent
        )
        | slots
      ]

      Supervisor.init(children, strategy: :rest_for_one)
    else
      _single_writer -> :ignore
    end
  end

  # Disk databases accept extra writer connections; memory ones do not.
  defp extra_writers?(owner),
    do: Lifecycle.writer_capabilities(owner).sequence_persistence == :separate_connection
end
