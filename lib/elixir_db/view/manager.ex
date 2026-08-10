defmodule ElixirDB.View.Manager do
  @moduledoc """
  Per-database coordinator that starts and stops view builders.

  This GenServer is the only process allowed to start or stop builders and it
  resolves create/delete lifecycle races.
  """
  use GenServer

  alias ElixirDB.Runtime.{ChildSpec, DatabaseCatalog}
  alias ElixirDB.View.BuilderSupervisor

  def child_spec(uuid),
    do: ChildSpec.worker({:view_manager, uuid}, {__MODULE__, :start_link, [uuid]}, :permanent)

  def start_link(uuid), do: GenServer.start_link(__MODULE__, uuid, name: via(uuid))

  def via(uuid),
    do: {:via, Registry, {ElixirDB.Runtime.DatabaseRegistry, {:view_manager, uuid}}}

  @spec start_builder(binary(), binary()) :: :ok | {:error, ElixirDB.Error.t()}
  def start_builder(uuid, view_id) when is_binary(uuid) and is_binary(view_id) do
    call(uuid, {:start_builder, view_id})
  end

  @spec start_builder(binary(), binary(), keyword()) :: :ok | {:error, ElixirDB.Error.t()}
  def start_builder(uuid, view_id, opts)
      when is_binary(uuid) and is_binary(view_id) and is_list(opts) do
    call(uuid, {:start_builder, view_id, opts})
  end

  @spec await_resumed(binary()) :: :ok | {:error, ElixirDB.Error.t()}
  def await_resumed(uuid) when is_binary(uuid), do: call(uuid, :await_resumed)

  @spec stop_builder(binary(), binary()) :: :ok | {:error, ElixirDB.Error.t()}
  def stop_builder(uuid, view_id) when is_binary(uuid) and is_binary(view_id) do
    call(uuid, {:stop_builder, view_id})
  end

  @spec close(binary()) :: :ok
  def close(uuid) when is_binary(uuid) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:view_manager, uuid}) do
      [{pid, _}] ->
        try do
          GenServer.call(pid, :close, 30_000)
        catch
          :exit, _ -> :ok
        end

      [] ->
        :ok
    end
  end

  @spec builder_pid(binary(), binary()) :: {:ok, pid()} | :error
  def builder_pid(uuid, view_id) when is_binary(uuid) and is_binary(view_id) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:view_builder, uuid, view_id}) do
      [{pid, _}] -> {:ok, pid}
      [] -> :error
    end
  end

  defp call(uuid, message) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:view_manager, uuid}) do
      [{pid, _}] -> GenServer.call(pid, message)
      [] -> {:error, ElixirDB.Error.database_closed("view manager is not running")}
    end
  end

  @impl true
  def init(uuid) do
    state = %{
      uuid: uuid,
      builders: %{},
      pending_starts: MapSet.new(),
      pending_stops: MapSet.new(),
      resume_status: :pending,
      resume_result: nil,
      resume_waiters: []
    }

    {:ok, state, {:continue, :resume_builders}}
  end

  @impl true
  def handle_call(:close, from, state) do
    state =
      Enum.reduce(Map.keys(state.builders), state, fn view_id, acc ->
        schedule_stop(acc, view_id)
      end)

    if map_size(state.builders) == 0 do
      {:stop, :normal, :ok, state}
    else
      {:noreply, %{state | close_from: from}}
    end
  end

  def handle_call(:await_resumed, from, %{resume_status: :pending} = state) do
    {:noreply, %{state | resume_waiters: [from | state.resume_waiters]}}
  end

  def handle_call(:await_resumed, _from, %{resume_status: :done, resume_result: result} = state) do
    {:reply, result, state}
  end

  def handle_call({:start_builder, view_id}, _from, state) do
    handle_start_builder(view_id, [], state)
  end

  def handle_call({:start_builder, view_id, opts}, _from, state) do
    handle_start_builder(view_id, opts, state)
  end

  def handle_call({:stop_builder, view_id}, _from, state) do
    {:reply, :ok, schedule_stop(state, view_id)}
  end

  defp handle_start_builder(view_id, opts, state) do
    state =
      if MapSet.member?(state.pending_stops, view_id) and not Map.has_key?(state.builders, view_id) do
        %{state | pending_stops: MapSet.delete(state.pending_stops, view_id)}
      else
        state
      end

    if MapSet.member?(state.pending_stops, view_id) do
      {:reply, :ok, state}
    else
      {:reply, :ok, schedule_start(state, view_id, opts)}
    end
  end

  @impl true
  def handle_continue(:resume_builders, state), do: resume_persisted_builders(state)

  @impl true
  def handle_info(:resume_builders, state) do
    resume_persisted_builders(state)
  end

  def handle_info({:do_start_builder, view_id, opts}, %{uuid: uuid} = state) do
    cond do
      Map.has_key?(state.builders, view_id) ->
        {:noreply, %{state | pending_starts: MapSet.delete(state.pending_starts, view_id)}}

      MapSet.member?(state.pending_stops, view_id) ->
        {:noreply,
         %{
           state
           | pending_starts: MapSet.delete(state.pending_starts, view_id),
             pending_stops: MapSet.delete(state.pending_stops, view_id)
         }}

      true ->
        case BuilderSupervisor.start_builder(uuid, view_id, opts) do
          {:ok, pid} ->
            ref = Process.monitor(pid)
            send(self(), {:builder_started, view_id, pid, ref})
            {:noreply, state}

          {:error, {:already_started, pid}} ->
            ref = Process.monitor(pid)
            send(self(), {:builder_started, view_id, pid, ref})
            {:noreply, state}

          {:error, _reason} ->
            {:noreply, %{state | pending_starts: MapSet.delete(state.pending_starts, view_id)}}
        end
    end
  end

  def handle_info({:builder_started, view_id, pid, ref}, state) do
    if MapSet.member?(state.pending_stops, view_id) do
      _ = Process.demonitor(ref, [:flush])
      _ = terminate_builder(pid)
      {:noreply, %{state | pending_stops: MapSet.delete(state.pending_stops, view_id)}}
    else
      monitor_ref = Process.monitor(pid)
      builders = Map.put(state.builders, view_id, %{pid: pid, ref: ref, monitor_ref: monitor_ref})

      state = %{
        state
        | pending_starts: MapSet.delete(state.pending_starts, view_id),
          builders: builders
      }

      {:noreply, maybe_finish_close(state)}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case find_builder_by_monitor(state, ref) do
      {view_id, _entry} ->
        stopping? = MapSet.member?(state.pending_stops, view_id)

        state =
          state
          |> Map.update!(:builders, &Map.delete(&1, view_id))
          |> Map.update!(:pending_stops, &MapSet.delete(&1, view_id))
          |> maybe_finish_close()

        state = if stopping?, do: state, else: schedule_start(state, view_id)

        {:noreply, state}

      nil ->
        {:noreply, state}
    end
  end

  def handle_info(:stop_after_close, state), do: {:stop, :normal, state}

  defp resume_persisted_builders(state) do
    case maintenance(state.uuid, {:command, :list_views, %{}}) do
      {:ok, views} ->
        state = Enum.reduce(views, state, &resume_view/2)
        {:noreply, finish_resume(state, :ok)}

      {:error, %ElixirDB.Error{code: :database_overloaded}} ->
        Process.send_after(self(), :resume_builders, 25)
        {:noreply, state}

      {:error, %ElixirDB.Error{} = error} ->
        {:noreply, finish_resume(state, {:error, error})}
    end
  end

  defp finish_resume(%{resume_waiters: waiters} = state, result) do
    Enum.each(waiters, &GenServer.reply(&1, result))
    %{state | resume_status: :done, resume_result: result, resume_waiters: []}
  end

  defp resume_view(%{"view_id" => view_id}, acc) do
    if MapSet.member?(acc.pending_stops, view_id), do: acc, else: schedule_start(acc, view_id)
  end

  defp schedule_start(state, view_id, opts \\ []) do
    cond do
      Map.has_key?(state.builders, view_id) ->
        state

      MapSet.member?(state.pending_starts, view_id) ->
        state

      true ->
        send(self(), {:do_start_builder, view_id, opts})
        %{state | pending_starts: MapSet.put(state.pending_starts, view_id)}
    end
  end

  defp schedule_stop(state, view_id) do
    state = %{
      state
      | pending_stops: MapSet.put(state.pending_stops, view_id),
        pending_starts: MapSet.delete(state.pending_starts, view_id)
    }

    case Map.get(state.builders, view_id) do
      %{pid: pid, monitor_ref: monitor_ref} ->
        _ = Process.demonitor(monitor_ref, [:flush])
        _ = terminate_builder(pid)
        %{state | builders: Map.delete(state.builders, view_id)}

      nil ->
        state
    end
  end

  defp terminate_builder(pid) when is_pid(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid, :shutdown, 5_000)
    :ok
  end

  defp find_builder_by_monitor(state, ref) do
    Enum.find(state.builders, fn {_view_id, %{monitor_ref: monitor_ref}} -> monitor_ref == ref end)
  end

  defp maybe_finish_close(%{close_from: from} = state) when not is_nil(from) do
    if map_size(state.builders) == 0 do
      GenServer.reply(from, :ok)
      send(self(), :stop_after_close)
    end

    state
  end

  defp maybe_finish_close(state), do: state

  defp maintenance(uuid, command) do
    DatabaseCatalog.command_as(uuid, :maintenance, command)
  end
end
