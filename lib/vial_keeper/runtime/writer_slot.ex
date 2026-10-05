defmodule VialKeeper.Runtime.WriterSlot do
  @moduledoc """
  Owns one extra writer connection and runs concurrent document writes on it.

  The connection never leaves this process. Each job is authorized like an
  owner command, runs through `VialKeeper.Runtime.MutationCommands` in the
  backend's concurrent transaction mode, and is retried on a write-write
  conflict with a bounded backoff. Writer-side caches are dropped whenever the
  pool's cache epoch changed since the previous job.
  """
  use GenServer

  require Logger
  alias VialKeeper.Deadline
  alias VialKeeper.Error
  alias VialKeeper.MapAccess

  alias VialKeeper.Runtime.{
    ChildSpec,
    DatabaseCommandPolicy,
    DatabaseOwner,
    MutationCommands,
    ShadowBinding,
    WriterPool
  }

  alias VialKeeper.Runtime.WriterPool.Job
  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.Lifecycle

  @max_attempts 8
  @max_backoff_ms 50

  @type args :: {binary(), pos_integer()}

  @spec child_spec(args()) :: map()
  def child_spec({uuid, index} = arg) when is_binary(uuid) and is_integer(index) and index > 0 do
    {:writer_slot, uuid, index}
    |> ChildSpec.worker({__MODULE__, :start_link, [arg]}, :permanent)
    |> Map.put(:shutdown, VialKeeper.Config.shutdown_timeout())
  end

  @spec start_link(args()) :: GenServer.on_start() | :ignore
  def start_link({uuid, index}) when is_binary(uuid) and is_integer(index) and index > 0 do
    case writer_source(uuid) do
      {:ok, %BackendContext{} = owner} -> open_and_start(uuid, index, owner)
      :ignore -> :ignore
    end
  end

  @spec via(binary(), pos_integer()) :: {:via, module(), term()}
  def via(uuid, index),
    do: {:via, Registry, {VialKeeper.Runtime.DatabaseRegistry, {:writer_slot, uuid, index}}}

  @impl true
  def init({uuid, index, %BackendContext{} = context}) do
    :ok = WriterPool.register(uuid, self())

    {:ok,
     %{
       uuid: uuid,
       index: index,
       context: %{context | write_mode: :concurrent},
       cache_epoch: WriterPool.cache_epoch(uuid)
     }}
  end

  @impl true
  def handle_call(:close_writer, _from, %{context: nil} = state), do: {:reply, :ok, state}

  def handle_call(:close_writer, _from, %{context: context} = state) do
    _ = Lifecycle.close_writer(context)
    {:reply, :ok, %{state | context: nil}}
  end

  @impl true
  def handle_cast({:run, %Job{} = job}, %{context: nil} = state) do
    finish_job(state.uuid, job, {{:error, Error.database_closed("database is closed")}, 0})
    {:noreply, state}
  end

  def handle_cast({:run, %Job{} = job}, state) do
    state = refresh_writer_caches(state)
    sync_before_write(state.uuid, job)
    {:noreply, attempt(state, job, 1)}
  end

  @impl true
  def handle_info({:retry, %Job{} = job, attempt}, state),
    do: {:noreply, attempt(state, job, attempt)}

  @impl true
  def terminate(_reason, %{context: nil}), do: :ok

  def terminate(_reason, %{context: context}) do
    _ = Lifecycle.close_writer(context)
    :ok
  end

  # A write-write conflict rolled back and aborted its reservation; the job
  # runs again with a fresh reservation after `min(50, 2^attempt)` ms plus up
  # to 1 ms of jitter, while its deadline allows. Every conflict emits
  # `[:vial_keeper, :writer, :write_conflict]` with whether it was retried.
  defp attempt(state, %Job{} = job, attempt) do
    case run_job(state, job) do
      {{:error, %Error{code: :write_conflict}}, _max_used} when attempt < @max_attempts ->
        retry_or_give_up(state, job, attempt)

      {{:error, %Error{code: :write_conflict}}, _max_used} ->
        conflict_event(state.uuid, attempt, :exhausted)
        finish_job(state.uuid, job, {{:error, conflicts_exhausted()}, 0})

      outcome ->
        finish_job(state.uuid, job, outcome)
    end

    state
  end

  defp retry_or_give_up(state, job, attempt) do
    delay = min(@max_backoff_ms, Integer.pow(2, attempt)) + :rand.uniform(2) - 1

    if time_for?(job.deadline_ms, delay) do
      conflict_event(state.uuid, attempt, :retry)
      _ = Process.send_after(self(), {:retry, job, attempt + 1}, delay)
      :ok
    else
      conflict_event(state.uuid, attempt, :exhausted)
      finish_job(state.uuid, job, {{:error, conflicts_exhausted()}, 0})
    end
  end

  defp conflict_event(uuid, attempt, outcome) do
    :telemetry.execute(
      [:vial_keeper, :writer, :write_conflict],
      %{count: 1},
      %{database_uuid: uuid, attempt: attempt, outcome: outcome}
    )
  end

  defp time_for?(:infinity, _delay), do: true
  defp time_for?(deadline_ms, delay), do: Deadline.remaining(deadline_ms) > delay

  defp run_job(state, %Job{} = job) do
    if Deadline.exhausted?(job.deadline_ms) do
      {{:error, deadline_error()}, 0}
    else
      with_trace_context(job.trace_context, fn -> authorized_write(state, job) end)
    end
  catch
    kind, reason ->
      Logger.error("writer slot command raised",
        kind: kind,
        reason: Exception.format(kind, reason, __STACKTRACE__)
      )

      {{:error,
        Error.internal_error("database command failed", %{cause: inspect(reason), kind: kind})}, 0}
  end

  defp authorized_write(state, %Job{command: command, authority: authority} = job) do
    database_kind = MapAccess.get(state.context.identity, :database_kind, :ordinary)

    with :ok <- DatabaseCommandPolicy.authorize(database_kind, authority, command),
         :ok <- ShadowBinding.check(database_kind, state.context, authority, state.uuid) do
      write(state, command, job.deadline_ms)
    else
      {:error, %Error{} = error} -> {{:error, error}, 0}
    end
  end

  defp write(state, command, deadline) do
    if MutationCommands.handles?(command),
      do: MutationCommands.execute(command, state.context, state.uuid, deadline),
      else: {{:error, Error.invalid_request("unknown database command")}, 0}
  end

  defp finish_job(uuid, %Job{} = job, outcome) do
    if WriterPool.complete(uuid, self(), job) == :reply,
      do: GenServer.reply(job.from, {:written, outcome})

    :ok
  end

  # Test gate: `{pid, ref, uuid}` under `:writer_slot_sync` makes every job on
  # that database announce itself and wait for `{:go, ref}` before it writes.
  defp sync_before_write(uuid, %Job{document_ids: document_ids}) do
    case Application.get_env(:vial_keeper, :writer_slot_sync) do
      {pid, ref, ^uuid} when is_pid(pid) ->
        send(pid, {ref, :before_write, self(), document_ids})

        receive do
          {:go, ^ref} -> :ok
        end

      _ ->
        :ok
    end
  end

  defp refresh_writer_caches(state) do
    epoch = WriterPool.refresh_writer_caches(state.uuid, state.context, state.cache_epoch)
    %{state | cache_epoch: epoch}
  end

  defp open_and_start(uuid, index, owner) do
    case Lifecycle.open_writer(owner) do
      {:ok, %BackendContext{} = writer} ->
        GenServer.start_link(__MODULE__, {uuid, index, writer}, name: via(uuid, index))

      {:error, :unsupported_writers} ->
        :ignore

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp writer_source(uuid) do
    case DatabaseOwner.writer_source(uuid) do
      {:ok, %BackendContext{} = owner} -> {:ok, owner}
      {:error, _} -> :ignore
    end
  catch
    :exit, _reason -> :ignore
  end

  defp with_trace_context(trace_context, fun) when is_function(fun, 0) do
    token = OpenTelemetry.Ctx.attach(trace_context)

    try do
      fun.()
    after
      OpenTelemetry.Ctx.detach(token)
    end
  end

  defp conflicts_exhausted,
    do: Error.database_overloaded("write conflict retries exhausted")

  defp deadline_error do
    Error.new(
      :internal_error,
      "database command timed out",
      %{reason: :deadline_exhausted},
      retryable: true
    )
  end
end
