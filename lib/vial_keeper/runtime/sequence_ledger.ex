defmodule VialKeeper.Runtime.SequenceLedger do
  @moduledoc """
  Hands out change sequences for one database from memory.

  A write reserves the most sequences it may use before its transaction
  begins (`reserve/3`) and finishes the reservation after `COMMIT` or
  `ROLLBACK` (`complete/3`). Reservations may finish in any order. The
  visible watermark W is the highest sequence at or below which every
  reservation has finished; change-feed readers only see rows up to W.
  Unused sequences become permanent holes.

  Before handing out a sequence above the persisted reserved-through value P,
  the ledger durably raises P by a block of 4096 on a connection of its own,
  so P is always at or above every stored sequence and a restart never reuses
  one.

  The data version V increases once per committed reservation that may have
  changed documents. It starts at `P + 1` on every open, which is above every
  earlier value. Query bookmarks use it.

  W and V live in a view cell published through
  `VialKeeper.Storage.Services.Sequences.publish_view/3`, so
  readers get them without a message.
  """
  use GenServer

  require Logger

  alias VialKeeper.Deadline
  alias VialKeeper.Error
  alias VialKeeper.Runtime.{ChangeNotifier, ChildSpec}
  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.Lifecycle
  alias VialKeeper.Storage.Ports.Access
  alias VialKeeper.Storage.Services.Sequences

  @block 4096

  @type outcome :: :committed | :aborted
  @type view :: %{visible: non_neg_integer(), data_version: pos_integer()}

  @spec child_spec(binary()) :: map()
  def child_spec(uuid) when is_binary(uuid) do
    {:sequence_ledger, uuid}
    |> ChildSpec.worker({__MODULE__, :start_link, [uuid]}, :permanent)
    |> Map.put(:shutdown, VialKeeper.Config.shutdown_timeout())
  end

  @spec start_link(binary()) :: GenServer.on_start()
  def start_link(uuid) when is_binary(uuid),
    do: GenServer.start_link(__MODULE__, uuid, name: via(uuid))

  @spec via(binary()) :: {:via, module(), term()}
  def via(uuid),
    do: {:via, Registry, {VialKeeper.Runtime.DatabaseRegistry, {:sequence_ledger, uuid}}}

  @doc """
  Loads the persisted reserved-through value through the owner's context,
  opens the ledger's own persistence connection when the backend needs one,
  and persists the first block. Called by `DatabaseOwner` after it opens
  storage, and again after every owner restart.
  """
  @spec initialize(binary(), BackendContext.t()) :: :ok | {:error, Error.t()}
  def initialize(uuid, %BackendContext{} = owner_context) when is_binary(uuid),
    do: call(uuid, {:initialize, owner_context}, VialKeeper.Config.shutdown_timeout())

  @doc """
  Reserves `count` contiguous sequences for the calling process, which the
  ledger monitors. Fails with a retryable error, handing nothing out, when
  the reservation cannot be persisted.
  """
  @spec reserve(binary(), pos_integer(), Deadline.t()) ::
          {:ok, reference(), pos_integer(), pos_integer()} | {:error, Error.t()}
  def reserve(uuid, count, deadline)
      when is_binary(uuid) and is_integer(count) and count > 0 do
    if Deadline.exhausted?(deadline),
      do: {:error, deadline_error()},
      else: call(uuid, {:reserve, count}, Deadline.call_timeout(deadline))
  end

  @doc """
  Finishes a reservation after its transaction committed or rolled back.

  `used_through` is the highest sequence the write stored (0 for none), or
  `:all` when unknown. While the reservation is still the newest one handed
  out, its numbers above `used_through` are taken back, so a write that
  changed nothing leaves no hole at the end of the feed. Any other unused
  number is a permanent hole.
  """
  @spec complete(binary(), reference(), outcome(), non_neg_integer() | :all) :: :ok
  def complete(uuid, token, outcome, used_through \\ :all)
      when is_binary(uuid) and is_reference(token) and outcome in [:committed, :aborted] and
             (used_through == :all or (is_integer(used_through) and used_through >= 0)) do
    message = {:complete, token, outcome, used_through}

    case call(uuid, message, VialKeeper.Config.shutdown_timeout()) do
      :ok -> :ok
      # A dead ledger restarted every writer with it; nothing is left to finish.
      {:error, %Error{}} -> :ok
    end
  end

  @doc """
  Waits until `sequence` is visible. Returns `:ok` at the deadline too: the
  write it waits for has committed either way.
  """
  @spec await_visible(binary(), non_neg_integer(), Deadline.t()) :: :ok
  def await_visible(_uuid, 0, _deadline), do: :ok

  def await_visible(uuid, sequence, deadline)
      when is_binary(uuid) and is_integer(sequence) and sequence > 0 do
    case view(uuid) do
      {:ok, %{visible: visible}} when visible >= sequence ->
        :ok

      _not_yet ->
        if Deadline.exhausted?(deadline) do
          :ok
        else
          _ = call(uuid, {:await, sequence}, Deadline.call_timeout(deadline))
          :ok
        end
    end
  catch
    :exit, _deadline_or_closed -> :ok
  end

  @doc "Returns the visible watermark and data version without a message."
  @spec view(binary()) :: {:ok, view()} | :none
  def view(uuid) when is_binary(uuid), do: Sequences.ledger_view(uuid)

  def view(_uuid), do: :none

  @doc "Returns the visible watermark W, or `nil` when the ledger is not running."
  @spec visible(binary()) :: non_neg_integer() | nil
  def visible(uuid) do
    case view(uuid) do
      {:ok, %{visible: visible}} -> visible
      :none -> nil
    end
  end

  @doc "Returns the data version V, or `nil` when the ledger is not running."
  @spec data_version(binary()) :: pos_integer() | nil
  def data_version(uuid) do
    case view(uuid) do
      {:ok, %{data_version: version}} -> version
      :none -> nil
    end
  end

  @doc """
  Runs `fun` holding a reservation of `count` sequences.

  The reservation is placed in this process for
  `VialKeeper.Storage.Services.Sequences.take/2`, then completed as
  `:committed` when `fun` returns `{:ok, _}` and `:aborted` otherwise.
  Returns `fun`'s result and the highest sequence it used (0 for none).
  `count == 0` runs `fun` without the ledger.
  """
  @spec with_reservation(binary(), non_neg_integer(), Deadline.t(), (-> result)) ::
          {result | {:error, Error.t()}, non_neg_integer()}
        when result: term()
  def with_reservation(_uuid, 0, _deadline, fun) when is_function(fun, 0), do: {fun.(), 0}

  def with_reservation(uuid, count, deadline, fun)
      when is_binary(uuid) and is_integer(count) and count > 0 and is_function(fun, 0) do
    case reserve(uuid, count, deadline) do
      {:ok, token, first, last} ->
        :ok = Sequences.put_reservation(uuid, token, first, last)
        run_reserved(uuid, token, fun)

      {:error, _} = error ->
        {error, 0}
    end
  end

  defp run_reserved(uuid, token, fun) do
    result = fun.()
    {result, finish(uuid, token, outcome(result))}
  catch
    kind, reason ->
      _ = finish(uuid, token, :aborted)
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  defp finish(uuid, token, outcome) do
    max_used =
      case Sequences.pop_reservation(uuid) do
        %{max_used: max_used} -> max_used
        nil -> 0
      end

    # A rolled-back write stored nothing, whatever numbers it took.
    used_through = if outcome == :committed, do: max_used, else: 0
    :ok = complete(uuid, token, outcome, used_through)
    max_used
  end

  defp outcome({:ok, _value}), do: :committed
  defp outcome(_error), do: :aborted

  @impl true
  def init(uuid) do
    Process.flag(:trap_exit, true)

    {:ok,
     %{
       uuid: uuid,
       cell: Sequences.new_view_cell(),
       initialized?: false,
       next: 1,
       persisted: 0,
       visible: 0,
       version: 1,
       outstanding: :gb_trees.empty(),
       by_token: %{},
       by_monitor: %{},
       waiters: :gb_trees.empty(),
       persist_ctx: nil
     }}
  end

  @impl true
  def handle_call({:initialize, owner_context}, _from, state) do
    case load(state, owner_context) do
      {:ok, state} -> {:reply, :ok, state}
      {:error, _} = error -> {:reply, error, state}
    end
  end

  def handle_call(_request, _from, %{initialized?: false} = state),
    do: {:reply, {:error, closed_error()}, state}

  def handle_call({:reserve, count}, {pid, _tag}, state) do
    last = state.next + count - 1

    case ensure_persisted(state, last) do
      {:ok, state} ->
        {token, state} = add_reservation(state, pid, count, last)
        {:reply, {:ok, token, last - count + 1, last}, state}

      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:complete, token, outcome, used_through}, _from, state) do
    {:reply, :ok, finish_reservation(state, token, outcome, used_through)}
  end

  def handle_call({:await, sequence}, from, state) do
    if state.visible >= sequence do
      {:reply, :ok, state}
    else
      {:noreply, %{state | waiters: add_waiter(state.waiters, sequence, from)}}
    end
  end

  @impl true
  def handle_info({:DOWN, monitor_ref, :process, _pid, _reason}, state) do
    case Map.fetch(state.by_monitor, monitor_ref) do
      # A dead writer can never commit: its connection rolls back when closed.
      {:ok, token} -> {:noreply, finish_reservation(state, token, :aborted, :all)}
      :error -> {:noreply, state}
    end
  end

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    :ok = Sequences.withdraw_view(state.uuid, self())
    close_persist_ctx(state.persist_ctx)
    :ok
  end

  defp load(state, owner_context) do
    with {:ok, high_water} <- Sequences.high_water(owner_context),
         {:ok, persist_ctx} <- open_persist_ctx(state.persist_ctx, owner_context) do
      state = %{
        state
        | initialized?: true,
          persist_ctx: persist_ctx,
          persisted: high_water,
          next: max(state.next, high_water + 1),
          visible: max(state.visible, high_water),
          version: max(state.version, high_water + 1)
      }

      with {:ok, state} <- persist(state, state.next - 1 + @block) do
        state = publish_view(state)
        :ok = Sequences.publish_view(state.uuid, self(), state.cell)
        {:ok, state}
      end
    end
  end

  defp open_persist_ctx(previous, owner_context) do
    close_persist_ctx(previous)

    case Lifecycle.writer_capabilities(owner_context).sequence_persistence do
      :separate_connection -> Lifecycle.open_writer(owner_context)
      :none -> {:ok, nil}
    end
    |> case do
      {:ok, persist_ctx} -> {:ok, persist_ctx}
      {:error, :unsupported_writers} -> {:ok, nil}
      {:error, %Error{}} = error -> error
    end
  end

  defp close_persist_ctx(nil), do: :ok

  defp close_persist_ctx(%BackendContext{} = persist_ctx) do
    _ = Lifecycle.close_writer(persist_ctx)
    :ok
  end

  defp ensure_persisted(%{persist_ctx: nil} = state, _last), do: {:ok, state}

  defp ensure_persisted(%{persisted: persisted} = state, last) when last <= persisted,
    do: {:ok, state}

  defp ensure_persisted(state, last) do
    case persist(state, last + @block) do
      {:ok, state} ->
        {:ok, state}

      {:error, %Error{} = error} ->
        Logger.warning("sequence reservation could not be persisted",
          database_uuid: state.uuid,
          reason: error.message
        )

        {:error, Error.database_overloaded("sequence reservation could not be persisted")}
    end
  end

  defp persist(%{persist_ctx: nil} = state, _through), do: {:ok, state}

  defp persist(state, through) do
    port = Access.port(state.persist_ctx, :change_log)

    case port.persist_sequence_reservation(state.persist_ctx, through) do
      :ok -> {:ok, %{state | persisted: max(state.persisted, through)}}
      {:error, _} = error -> error
    end
  end

  defp add_reservation(state, pid, count, last) do
    token = make_ref()
    monitor_ref = Process.monitor(pid)
    first = state.next

    state = %{
      state
      | next: last + 1,
        outstanding: :gb_trees.insert(first, {last, token, monitor_ref, count}, state.outstanding),
        by_token: Map.put(state.by_token, token, first),
        by_monitor: Map.put(state.by_monitor, monitor_ref, token)
    }

    {token, state}
  end

  defp finish_reservation(state, token, outcome, used_through) do
    case Map.pop(state.by_token, token) do
      {nil, _by_token} ->
        state

      {first, by_token} ->
        {{last, ^token, monitor_ref, count}, outstanding} =
          :gb_trees.take(first, state.outstanding)

        Process.demonitor(monitor_ref, [:flush])

        %{
          state
          | outstanding: outstanding,
            next: take_back_unused(state.next, first, last, used_through),
            by_token: by_token,
            by_monitor: Map.delete(state.by_monitor, monitor_ref),
            version: bump_version(state.version, outcome, count)
        }
        |> advance_visible()
    end
  end

  # Only the newest reservation can return numbers: nothing above it was
  # handed out, and every returned number is still covered by the persisted
  # reserved-through value.
  defp take_back_unused(next, first, last, used_through)
       when is_integer(used_through) and last == next - 1,
       do: max(first, used_through + 1)

  defp take_back_unused(next, _first, _last, _used_through), do: next

  defp bump_version(version, :committed, count) when count >= 1, do: version + 1
  defp bump_version(version, _outcome, _count), do: version

  defp advance_visible(state) do
    visible =
      if :gb_trees.is_empty(state.outstanding) do
        state.next - 1
      else
        {first, _reservation} = :gb_trees.smallest(state.outstanding)
        first - 1
      end

    if visible > state.visible do
      state = publish_view(%{state | visible: visible})
      ChangeNotifier.publish(state.uuid, visible)
      reply_waiters(state)
    else
      publish_view(state)
    end
  end

  defp publish_view(state) do
    :ok = Sequences.store_view(state.cell, state.visible, state.version)
    state
  end

  defp add_waiter(waiters, sequence, from) do
    case :gb_trees.lookup(sequence, waiters) do
      {:value, froms} -> :gb_trees.update(sequence, [from | froms], waiters)
      :none -> :gb_trees.insert(sequence, [from], waiters)
    end
  end

  defp reply_waiters(state) do
    if :gb_trees.is_empty(state.waiters) do
      state
    else
      case :gb_trees.smallest(state.waiters) do
        {sequence, froms} when sequence <= state.visible ->
          Enum.each(froms, &GenServer.reply(&1, :ok))
          reply_waiters(%{state | waiters: :gb_trees.delete(sequence, state.waiters)})

        _later ->
          state
      end
    end
  end

  defp call(uuid, message, timeout) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:sequence_ledger, uuid}) do
      [{pid, _value}] ->
        try do
          GenServer.call(pid, message, timeout)
        catch
          :exit, reason -> translate_exit(reason)
        end

      [] ->
        {:error, closed_error()}
    end
  end

  defp translate_exit(reason) do
    cond do
      Deadline.genserver_call_timeout?(reason) -> {:error, deadline_error()}
      Deadline.no_process_exit?(reason) -> {:error, closed_error()}
      true -> exit(reason)
    end
  end

  defp closed_error, do: Error.database_closed("database sequence ledger is not running")

  defp deadline_error do
    Error.new(
      :internal_error,
      "database command timed out",
      %{reason: :deadline_exhausted},
      retryable: true
    )
  end
end
