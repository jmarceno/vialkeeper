defmodule ElixirDB.Storage.OpaqueHandle.Server do
  @moduledoc """
  Private process backing `ElixirDB.Storage.OpaqueHandle`.

  The server stores payloads in a private ETS table. General unwrap,
  replacement, and deletion requests are authorized from backend context
  modules. A lower-overhead unwrap request exists for backend Context modules
  and is statically confined there by Reach. The server is an implementation
  detail of the storage boundary and is not a general-purpose term registry.
  """
  use GenServer

  @allowed_callers [
    ElixirDB.Storage.SQLite.Context,
    ElixirDB.Storage.Sentinel.Context,
    ElixirDB.Storage.Memory.Context
  ]

  @spec start_link(term()) :: GenServer.on_start()
  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @spec wrap(term()) :: ElixirDB.Storage.OpaqueHandle.t()
  def wrap(term), do: GenServer.call(__MODULE__, {:wrap, term})

  @spec unwrap(ElixirDB.Storage.OpaqueHandle.t()) ::
          {:ok, term()} | {:error, :missing | :forbidden}
  def unwrap(%ElixirDB.Storage.OpaqueHandle{} = handle) do
    GenServer.call(__MODULE__, {:unwrap, handle})
  end

  @doc false
  @spec backend_unwrap(ElixirDB.Storage.OpaqueHandle.t()) :: {:ok, term()} | {:error, :missing}
  def backend_unwrap(%ElixirDB.Storage.OpaqueHandle{} = handle) do
    GenServer.call(__MODULE__, {:backend_unwrap, handle})
  end

  @spec replace(ElixirDB.Storage.OpaqueHandle.t(), term()) ::
          {:ok, ElixirDB.Storage.OpaqueHandle.t()} | {:error, :missing | :forbidden}
  def replace(%ElixirDB.Storage.OpaqueHandle{} = handle, term) do
    GenServer.call(__MODULE__, {:replace, handle, term})
  end

  @spec drop(ElixirDB.Storage.OpaqueHandle.t()) :: :ok | {:error, :forbidden}
  def drop(%ElixirDB.Storage.OpaqueHandle{} = handle) do
    GenServer.call(__MODULE__, {:drop, handle})
  end

  @impl true
  def init(:ok) do
    tid = :ets.new(__MODULE__.Table, [:set, :private])
    {:ok, tid}
  end

  @impl true
  def handle_call({:wrap, term}, _from, tid) do
    id = make_ref()
    true = :ets.insert(tid, {id, term})
    {:reply, %ElixirDB.Storage.OpaqueHandle{id: id}, tid}
  end

  def handle_call({:unwrap, %ElixirDB.Storage.OpaqueHandle{id: id}}, {from_pid, _}, tid) do
    with :ok <- authorize_caller(from_pid),
         [{^id, term}] <- :ets.lookup(tid, id) do
      {:reply, {:ok, term}, tid}
    else
      [] -> {:reply, {:error, :missing}, tid}
      {:error, _} = error -> {:reply, error, tid}
    end
  end

  def handle_call({:backend_unwrap, %ElixirDB.Storage.OpaqueHandle{id: id}}, _from, tid) do
    case :ets.lookup(tid, id) do
      [{^id, term}] -> {:reply, {:ok, term}, tid}
      [] -> {:reply, {:error, :missing}, tid}
    end
  end

  def handle_call(
        {:replace, %ElixirDB.Storage.OpaqueHandle{id: id} = handle, term},
        {from_pid, _},
        tid
      ) do
    case authorize_caller(from_pid) do
      :ok ->
        case :ets.lookup(tid, id) do
          [{^id, _}] ->
            true = :ets.insert(tid, {id, term})
            {:reply, {:ok, handle}, tid}

          [] ->
            {:reply, {:error, :missing}, tid}
        end

      {:error, _} = error ->
        {:reply, error, tid}
    end
  end

  def handle_call({:drop, %ElixirDB.Storage.OpaqueHandle{id: id}}, {from_pid, _}, tid) do
    case authorize_caller(from_pid) do
      :ok ->
        true = :ets.delete(tid, id)
        {:reply, :ok, tid}

      {:error, _} = error ->
        {:reply, error, tid}
    end
  end

  def handle_call(_other, _from, tid), do: {:reply, {:error, :forbidden}, tid}

  defp authorize_caller(from_pid) when is_pid(from_pid) do
    stack =
      case Process.info(from_pid, :current_stacktrace) do
        {:current_stacktrace, frames} when is_list(frames) -> frames
        _ -> []
      end

    if Enum.any?(stack, fn {mod, _fun, _arity, _loc} -> mod in @allowed_callers end) do
      :ok
    else
      {:error, :forbidden}
    end
  end
end
