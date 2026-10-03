defmodule VialKeeper.Storage.OpaqueHandle.Server do
  @moduledoc """
  Private process backing `VialKeeper.Storage.OpaqueHandle`.

  The server owns the payload ETS table and is its only writer. General
  unwrap, replacement, and deletion requests are authorized from backend
  context modules. Backend Context modules read payloads with
  `backend_unwrap/1`, a direct lookup in the caller that is statically
  confined to those modules by Reach: it sits on every storage port call, so
  it must neither serialize all databases through this process nor pay a
  message round trip. The server is an implementation detail of the storage
  boundary and is not a general-purpose term registry.
  """
  use GenServer

  @table_key {__MODULE__, :table}

  @allowed_callers [
    VialKeeper.Storage.SQLite.Context,
    VialKeeper.Storage.Sentinel.Context,
    VialKeeper.Storage.Memory.Context
  ]

  @spec start_link(term()) :: GenServer.on_start()
  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @spec wrap(term()) :: VialKeeper.Storage.OpaqueHandle.t()
  def wrap(term),
    do: GenServer.call(__MODULE__, {:wrap, term}, VialKeeper.Config.request_timeout_ms())

  @spec unwrap(VialKeeper.Storage.OpaqueHandle.t()) ::
          {:ok, term()} | {:error, :missing | :forbidden}
  def unwrap(%VialKeeper.Storage.OpaqueHandle{} = handle) do
    GenServer.call(__MODULE__, {:unwrap, handle}, VialKeeper.Config.request_timeout_ms())
  end

  @doc false
  @spec backend_unwrap(VialKeeper.Storage.OpaqueHandle.t()) :: {:ok, term()} | {:error, :missing}
  def backend_unwrap(%VialKeeper.Storage.OpaqueHandle{id: id}) do
    case :ets.lookup(:persistent_term.get(@table_key), id) do
      [{^id, term}] -> {:ok, term}
      [] -> {:error, :missing}
    end
  end

  @spec replace(VialKeeper.Storage.OpaqueHandle.t(), term()) ::
          {:ok, VialKeeper.Storage.OpaqueHandle.t()} | {:error, :missing | :forbidden}
  def replace(%VialKeeper.Storage.OpaqueHandle{} = handle, term) do
    GenServer.call(__MODULE__, {:replace, handle, term}, VialKeeper.Config.request_timeout_ms())
  end

  @spec drop(VialKeeper.Storage.OpaqueHandle.t()) :: :ok | {:error, :forbidden}
  def drop(%VialKeeper.Storage.OpaqueHandle{} = handle) do
    GenServer.call(__MODULE__, {:drop, handle}, VialKeeper.Config.request_timeout_ms())
  end

  @impl true
  def init(:ok) do
    tid = :ets.new(__MODULE__.Table, [:set, :protected, read_concurrency: true])
    :ok = :persistent_term.put(@table_key, tid)
    {:ok, tid}
  end

  @impl true
  def handle_call({:wrap, term}, _from, tid) do
    id = make_ref()
    true = :ets.insert(tid, {id, term})
    {:reply, %VialKeeper.Storage.OpaqueHandle{id: id}, tid}
  end

  def handle_call({:unwrap, %VialKeeper.Storage.OpaqueHandle{id: id}}, {from_pid, _}, tid) do
    with :ok <- authorize_caller(from_pid),
         [{^id, term}] <- :ets.lookup(tid, id) do
      {:reply, {:ok, term}, tid}
    else
      [] -> {:reply, {:error, :missing}, tid}
      {:error, _} = error -> {:reply, error, tid}
    end
  end

  def handle_call(
        {:replace, %VialKeeper.Storage.OpaqueHandle{id: id} = handle, term},
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

  def handle_call({:drop, %VialKeeper.Storage.OpaqueHandle{id: id}}, {from_pid, _}, tid) do
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
