defmodule ElixirDB.Runtime.DatabaseAdmission do
  @moduledoc false
  use GenServer

  def start_link({uuid, limit}),
    do: GenServer.start_link(__MODULE__, {uuid, limit}, name: via(uuid))

  def via(uuid), do: {:via, Registry, {ElixirDB.Runtime.DatabaseRegistry, {:admission, uuid}}}

  def with_token(uuid, fun) when is_function(fun, 0) do
    with {:ok, counter, limit} <- lookup(uuid), :ok <- acquire(uuid, counter, limit) do
      try do
        fun.()
      after
        release(counter)
      end
    end
  end

  defp lookup(uuid) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:admission, uuid}) do
      [{_pid, %{counter: counter, limit: limit}}] -> {:ok, counter, limit}
      [] -> {:error, ElixirDB.Error.database_closed("database admission is closed")}
    end
  end

  def active_count(uuid) do
    case lookup(uuid) do
      {:ok, counter, _limit} -> {:ok, :atomics.get(counter, 1)}
      {:error, _} = error -> error
    end
  end

  @impl true
  def init({uuid, limit}) do
    counter = :atomics.new(1, signed: true)

    _ =
      Registry.update_value(
        ElixirDB.Runtime.DatabaseRegistry,
        {:admission, uuid},
        fn _ -> %{counter: counter, limit: limit} end
      )

    {:ok, %{uuid: uuid, limit: limit, counter: counter}}
  end

  defp acquire(uuid, counter, limit) do
    count = :atomics.add_get(counter, 1, 1)

    if count <= limit do
      :ok
    else
      _ = :atomics.add_get(counter, 1, -1)
      ElixirDB.Observability.Instrumentation.Database.overload(uuid)
      {:error, ElixirDB.Error.database_overloaded("database admission limit reached")}
    end
  end

  defp release(counter), do: :atomics.add_get(counter, 1, -1)
end
