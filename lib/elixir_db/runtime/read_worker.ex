defmodule ElixirDB.Runtime.ReadWorker do
  @moduledoc """
  Owns one readonly snapshot connection and runs classified reads on it.

  The connection never leaves this process. Snapshot bodies are interrupted by
  the pool when their deadline expires or their request is cancelled. The
  interrupted job returns the retryable deadline error and its snapshot is
  rolled back before the worker serves the next job.
  """
  use GenServer

  alias ElixirDB.Commands
  alias ElixirDB.Error
  alias ElixirDB.MapAccess

  alias ElixirDB.Runtime.{
    ChildSpec,
    DatabaseCommandPolicy,
    DatabaseOwner,
    DatabaseReadDispatch,
    Deadline,
    ReadPool,
    ShadowBinding
  }

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Lifecycle
  alias ElixirDB.Storage.Transaction

  @type args :: {binary(), pos_integer()}

  def child_spec({uuid, index} = arg) when is_binary(uuid) and is_integer(index) and index > 0 do
    {:read_worker, uuid, index}
    |> ChildSpec.worker({__MODULE__, :start_link, [arg]}, :permanent)
    |> Map.put(:shutdown, ElixirDB.Config.shutdown_timeout())
  end

  def start_link({uuid, index}) when is_binary(uuid) and is_integer(index) and index > 0 do
    case reader_source(uuid) do
      {:ok, %BackendContext{} = writer} ->
        open_and_start(uuid, index, writer)

      :ignore ->
        :ignore
    end
  end

  def via(uuid, index),
    do: {:via, Registry, {ElixirDB.Runtime.DatabaseRegistry, {:read_worker, uuid, index}}}

  @impl true
  def init({uuid, index, %BackendContext{} = context}) do
    interrupt_fun = fn -> Lifecycle.interrupt_reader(context) end
    :ok = ReadPool.register(uuid, self(), interrupt_fun)
    {:ok, %{uuid: uuid, index: index, context: context}}
  end

  @impl true
  def handle_call(:close_reader, _from, %{context: nil} = state), do: {:reply, :ok, state}

  def handle_call(:close_reader, _from, %{context: context} = state) do
    _ = Lifecycle.close_reader(context)
    {:reply, :ok, %{state | context: nil}}
  end

  @impl true
  def handle_cast({:run, job}, %{context: nil} = state) do
    ReadPool.complete(
      state.uuid,
      self(),
      job,
      {:error, Error.database_closed("database is closed")}
    )

    {:noreply, state}
  end

  def handle_cast({:run, job}, state) do
    result = execute_job(state, job)
    ReadPool.complete(state.uuid, self(), job, enforce_deadline(result, job))
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %{context: nil}), do: :ok

  def terminate(_reason, %{context: context}) do
    _ = Lifecycle.close_reader(context)
    :ok
  end

  defp open_and_start(uuid, index, writer) do
    case Lifecycle.open_reader(writer) do
      {:ok, %BackendContext{} = reader} ->
        bound = %{reader | identity: writer.identity, bundle_root: writer.bundle_root}
        GenServer.start_link(__MODULE__, {uuid, index, bound}, name: via(uuid, index))

      {:error, :unsupported_readers} ->
        :ignore

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp reader_source(uuid) do
    case DatabaseOwner.reader_source(uuid) do
      {:ok, %BackendContext{} = writer} -> {:ok, writer}
      {:error, _} -> :ignore
    end
  catch
    :exit, _reason -> :ignore
  end

  defp execute_job(state, job) do
    %{
      authority: authority,
      command: command,
      deadline_ms: deadline_ms,
      probe_op: probe_op,
      trace_context: trace_context
    } = job

    if Deadline.exhausted?(deadline_ms) do
      deadline_error()
    else
      with_trace_context(trace_context, fn ->
        run_read(state, authority, command, probe_op)
      end)
    end
  end

  defp enforce_deadline(result, %{deadline_ms: deadline_ms}) do
    if Deadline.exhausted?(deadline_ms), do: {:error, deadline_error()}, else: result
  end

  defp run_read(state, authority, command, probe_op) do
    normalized = Commands.normalize(command)
    database_kind = MapAccess.get(state.context.identity, :database_kind, :ordinary)

    sync_before_begin(state.uuid, probe_op)

    with :ok <- DatabaseCommandPolicy.authorize(database_kind, authority, normalized) do
      run_authorized_read(state, authority, normalized, database_kind, probe_op)
    end
  catch
    error_kind, reason ->
      {:error,
       Error.internal_error("read pool command failed", %{
         kind: error_kind,
         reason: inspect(reason)
       })}
  end

  defp run_authorized_read(state, authority, normalized, database_kind, probe_op) do
    Transaction.run_snapshot(state.context, fn snapshot ->
      snapshot_read(snapshot, authority, normalized, database_kind, state.uuid, probe_op)
    end)
  end

  defp snapshot_read(snapshot, authority, normalized, database_kind, uuid, probe_op) do
    sync_owner_body(uuid, probe_op)

    with :ok <- ShadowBinding.check(database_kind, snapshot, authority, uuid) do
      DatabaseReadDispatch.run(snapshot, normalized)
    end
  end

  defp sync_before_begin(uuid, probe_op) when is_binary(uuid) do
    case Application.get_env(:elixir_db, :read_pool_sync) do
      {pid, ref, ^uuid, only_op} when is_pid(pid) and only_op == probe_op ->
        wait_for_sync_gate(pid, ref, :before_begin)

      {pid, ref, ^uuid} when is_pid(pid) ->
        wait_for_sync_gate(pid, ref, :before_begin)

      {pid, ref} when is_pid(pid) ->
        wait_for_sync_gate(pid, ref, :before_begin)

      _ ->
        :ok
    end
  end

  defp sync_owner_body(uuid, probe_op) when is_binary(uuid) do
    case Application.get_env(:elixir_db, :read_pool_owner_body_sync) do
      {pid, ref, ^uuid, only_op} when is_pid(pid) and only_op == probe_op ->
        wait_for_sync_gate(pid, ref, :owner_body)

      {pid, ref, ^uuid} when is_pid(pid) ->
        wait_for_sync_gate(pid, ref, :owner_body)

      {pid, ref} when is_pid(pid) ->
        wait_for_sync_gate(pid, ref, :owner_body)

      _ ->
        :ok
    end
  end

  defp wait_for_sync_gate(pid, ref, event) do
    send(pid, {ref, event, self()})

    receive do
      {:go, ^ref} -> :ok
    end
  end

  defp with_trace_context(trace_context, fun) when is_function(fun, 0) do
    token = OpenTelemetry.Ctx.attach(trace_context)

    try do
      fun.()
    after
      OpenTelemetry.Ctx.detach(token)
    end
  end

  defp deadline_error do
    Error.new(
      :internal_error,
      "database command timed out",
      %{reason: :deadline_exhausted},
      retryable: true
    )
  end
end
