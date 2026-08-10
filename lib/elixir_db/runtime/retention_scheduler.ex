defmodule ElixirDB.Runtime.RetentionScheduler do
  @moduledoc false
  use GenServer

  alias ElixirDB.Observability.Instrumentation.Compact
  alias ElixirDB.Runtime.{DatabaseAdmission, DatabaseCatalog}

  def start_link(uuid), do: GenServer.start_link(__MODULE__, uuid, name: via(uuid))

  def via(uuid),
    do: {:via, Registry, {ElixirDB.Runtime.DatabaseRegistry, {:retention_scheduler, uuid}}}

  @doc "Reloads schedule from the database identity and resets the timer."
  @spec reschedule(binary()) :: :ok
  def reschedule(uuid) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:retention_scheduler, uuid}) do
      [{pid, _}] -> GenServer.cast(pid, :reschedule)
      [] -> :ok
    end
  end

  @impl true
  def init(uuid) do
    {:ok, schedule_state(uuid, nil)}
  end

  @impl true
  def handle_cast(:reschedule, state) do
    {:noreply, schedule_state(state.uuid, state.timer_ref)}
  end

  @impl true
  def handle_info(:scheduled_compact, state) do
    _ = run_scheduled_compact(state.uuid)
    {:noreply, schedule_state(state.uuid, state.timer_ref)}
  end

  @impl true
  def terminate(_reason, %{timer_ref: ref}) when is_reference(ref), do: cancel_timer(ref)
  def terminate(_reason, _state), do: :ok

  defp schedule_state(uuid, timer_ref) do
    cancel_timer(timer_ref)

    case schedule_ms(uuid) do
      nil ->
        %{uuid: uuid, timer_ref: nil}

      ms ->
        ref = Process.send_after(self(), :scheduled_compact, ms)
        %{uuid: uuid, timer_ref: ref}
    end
  end

  defp schedule_ms(uuid) do
    # Route through per-database admission as :maintenance without catalog.command:
    # schedule_state runs during runtime startup while DatabaseCatalog may still be
    # inside {:open, _} / {:ensure_command_target, _}.
    case DatabaseAdmission.execute(uuid, :maintenance, {:command, :identity, %{}}) do
      {:ok, %{config: config}} when is_map(config) ->
        retention_schedule_ms(config)

      _ ->
        nil
    end
  end

  defp retention_schedule_ms(config) do
    retention = Map.get(config, "retention", %{})
    mode = Map.get(retention, "mode")
    schedule = Map.get(retention, "schedule")

    if mode == "stable_frontier" and is_integer(schedule) and schedule > 0,
      do: schedule,
      else: nil
  end

  defp run_scheduled_compact(uuid) do
    Compact.requested(uuid, :scheduled)

    DatabaseCatalog.command_as(
      uuid,
      :maintenance,
      {:command, :compact_retention, %{trigger: :scheduled}}
    )
    |> case do
      {:ok, _} -> :ok
      {:error, _} -> :ok
    end
  end

  defp cancel_timer(nil), do: :ok

  defp cancel_timer(ref) when is_reference(ref) do
    _ = Process.cancel_timer(ref)
    :ok
  end
end
