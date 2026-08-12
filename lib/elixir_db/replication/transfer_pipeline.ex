defmodule ElixirDB.Replication.TransferPipeline do
  @moduledoc """
  Bounded concurrent replication transfer pipeline.

  Fetches revision chains and streams missing attachment blobs under private
  per-run task supervision, then returns deterministic chains for import.
  """

  alias ElixirDB.Attachments.Manifest
  alias ElixirDB.Error
  alias ElixirDB.MapAccess
  alias ElixirDB.Observability.Instrumentation.Replication, as: ReplicationModule
  alias ElixirDB.Replication.BlobRepresentationStream

  defmodule ChainChunk do
    @moduledoc false
    @enforce_keys [:ordinal, :documents]
    defstruct [:ordinal, :documents]

    @type t :: %__MODULE__{ordinal: non_neg_integer(), documents: [map()]}
  end

  defmodule BlobObligation do
    @moduledoc false
    @enforce_keys [:digest, :length]
    defstruct [:digest, :length]

    @type t :: %__MODULE__{digest: binary(), length: non_neg_integer()}
  end

  defmodule State do
    @moduledoc false
    @enforce_keys [:phase]
    defstruct phase: :idle,
              source: nil,
              target: nil,
              task_supervisor: nil,
              chain_chunks: [],
              chain_queue: [],
              chain_tasks: %{},
              completed_chains: %{},
              blob_diff_queue: [],
              blob_diff_tasks: %{},
              blob_queue: [],
              blob_tasks: %{},
              reserved_bytes: 0,
              cancel_requested: false,
              max_chain_fetches: 4,
              max_blob_transfers: 4,
              max_transfer_bytes_in_flight: 1_073_741_824,
              blob_diff_batch_size: 100,
              trace_context: nil,
              phase_hook: nil,
              replication_id: nil,
              blob_obligations: [],
              seen_blob_digests: %{},
              chain_chunks_total: 0,
              max_chain_concurrency_observed: 0,
              blob_count: 0,
              max_blob_concurrency_observed: 0,
              logical_blob_bytes: 0,
              peak_reserved_transfer_bytes: 0

    @type phase ::
            :idle
            | {:chain, non_neg_integer()}
            | {:blob, binary()}
            | {:blob_diff, non_neg_integer()}
    @type t :: %__MODULE__{
            phase: phase(),
            source: term(),
            target: term(),
            task_supervisor: pid() | nil,
            chain_chunks: [ChainChunk.t()],
            chain_queue: [ChainChunk.t()],
            chain_tasks: map(),
            completed_chains: map(),
            blob_diff_queue: [BlobObligation.t()],
            blob_diff_tasks: map(),
            blob_queue: [BlobObligation.t()],
            blob_tasks: map(),
            reserved_bytes: non_neg_integer(),
            cancel_requested: boolean(),
            max_chain_fetches: pos_integer(),
            max_blob_transfers: pos_integer(),
            max_transfer_bytes_in_flight: pos_integer(),
            blob_diff_batch_size: pos_integer(),
            trace_context: term(),
            phase_hook: (atom(), map() -> :ok | {:error, Error.t()}) | nil,
            replication_id: binary() | nil,
            blob_obligations: [BlobObligation.t()],
            seen_blob_digests: %{optional(binary()) => non_neg_integer()},
            chain_chunks_total: non_neg_integer(),
            max_chain_concurrency_observed: non_neg_integer(),
            blob_count: non_neg_integer(),
            max_blob_concurrency_observed: non_neg_integer(),
            logical_blob_bytes: non_neg_integer(),
            peak_reserved_transfer_bytes: non_neg_integer()
          }
  end

  @type diff_document :: %{required(:document_id) => term(), required(:leaf_revisions) => list()}

  @spec partition_chain_fetches([diff_document()], map()) :: [ChainChunk.t()]
  def partition_chain_fetches(documents, config) when is_list(documents) and is_map(config) do
    if documents == [] do
      []
    else
      batch_documents = positive_config(config, :batch_documents, 100)
      max_fetches = positive_config(config, :max_concurrent_chain_fetches, 4)
      target = ceil_div(length(documents), max_fetches) |> min(batch_documents) |> max(1)

      documents
      |> Enum.chunk_every(target)
      |> Enum.with_index()
      |> Enum.map(fn {chunk, ordinal} -> %ChainChunk{ordinal: ordinal, documents: chunk} end)
    end
  end

  @spec aggregate_chains(map() | [{non_neg_integer(), [map()]}]) :: [map()]
  def aggregate_chains(completed) when is_map(completed) do
    completed
    |> Enum.sort_by(fn {ordinal, _chains} -> ordinal end)
    |> Enum.flat_map(fn {_ordinal, chains} -> List.wrap(chains) end)
  end

  def aggregate_chains(completed) when is_list(completed) do
    completed
    |> Enum.sort_by(fn {ordinal, _chains} -> ordinal end)
    |> Enum.flat_map(fn {_ordinal, chains} -> List.wrap(chains) end)
  end

  @spec blob_obligations([map()]) ::
          {:ok, [BlobObligation.t()]} | {:error, Error.t()}
  def blob_obligations(chains) when is_list(chains) do
    with {:ok, attachments} <- collect_chain_attachments(chains) do
      deduplicate_attachments(attachments)
    end
    |> case do
      {:ok, lengths} ->
        {:ok,
         lengths
         |> Enum.sort_by(fn {digest, _length} -> digest end)
         |> Enum.map(fn {digest, length} ->
           %BlobObligation{digest: digest, length: length}
         end)}

      error ->
        error
    end
  end

  def blob_obligations(_chains), do: {:error, integrity_error("revision chain metadata is invalid")}

  @spec extract_blob_obligations([map()]) ::
          {:ok, [BlobObligation.t()]} | {:error, Error.t()}
  def extract_blob_obligations(chains), do: blob_obligations(chains)

  @spec partition_blob_digests([binary()], pos_integer()) :: [[binary()]]
  def partition_blob_digests(digests, batch_size)
      when is_list(digests) and is_integer(batch_size) and batch_size > 0 do
    digests
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.chunk_every(batch_size)
  end

  @doc false
  @spec run(term(), term(), map(), map()) :: {:ok, map()} | {:error, Error.t()}
  def run(source_endpoint, target_endpoint, context, config)
      when is_map(context) and is_map(config) do
    documents = MapAccess.get(context, :documents, [])

    if is_list(documents) do
      run_document_transfer(source_endpoint, target_endpoint, context, config, documents)
      |> public_result()
    else
      {:error, Error.invalid_request("replication documents must be a list")}
    end
  end

  def run(_source_endpoint, _target_endpoint, _context, _config),
    do: {:error, Error.invalid_request("replication transfer context is invalid")}

  @doc "Requests cooperative cancellation of a running transfer phase."
  @spec cancel(pid()) :: :ok
  def cancel(phase_pid) when is_pid(phase_pid) do
    send(phase_pid, :elixir_db_replication_transfer_cancel)
    :ok
  end

  defp run_document_transfer(source_endpoint, target_endpoint, context, config, documents) do
    chunks = partition_chain_fetches(documents, config)

    preloaded_chains =
      if MapAccess.get(context, :transfer_preloaded, false) do
        MapAccess.get(context, :chains, [])
      else
        []
      end

    cond do
      not is_list(preloaded_chains) ->
        {:error, Error.invalid_request("preloaded revision chains must be a list")}

      chunks == [] and preloaded_chains == [] ->
        transfer_result(config, context, fn ->
          {:ok, Map.put(context, :chains, []), empty_measurements()}
        end)

      true ->
        transfer_result(
          config,
          context,
          fn ->
            run_supervised_transfer(
              source_endpoint,
              target_endpoint,
              context,
              config,
              chunks,
              preloaded_chains
            )
          end
        )
    end
  end

  defp public_result({:ok, context, _measurements}), do: {:ok, context}
  defp public_result({:error, %Error{} = error, _measurements}), do: {:error, error}
  defp public_result({:error, %Error{} = error}), do: {:error, error}

  defp transfer_result(config, context, fun) do
    replication_id = MapAccess.get(context, :replication_id) || Map.get(config, :replication_id, "")
    ReplicationModule.transfer_span(replication_id || "", fun)
  end

  defp empty_measurements do
    [
      chain_chunks: 0,
      max_chain_concurrency_observed: 0,
      blob_count: 0,
      max_blob_concurrency_observed: 0,
      logical_blob_bytes: 0,
      peak_reserved_transfer_bytes: 0
    ]
  end

  defp run_supervised_transfer(
         source_endpoint,
         target_endpoint,
         context,
         config,
         chunks,
         preloaded_chains
       ) do
    with {:ok, {completed_chains, blob_diff_queue, seen_blob_digests}} <-
           preloaded_state(chunks, preloaded_chains),
         {:ok, task_supervisor} <- Task.Supervisor.start_link() do
      state = %State{
        phase: {:chain, 0},
        source: source_endpoint,
        target: target_endpoint,
        task_supervisor: task_supervisor,
        chain_chunks: chunks,
        chain_queue: chunks,
        completed_chains: completed_chains,
        blob_diff_queue: blob_diff_queue,
        seen_blob_digests: seen_blob_digests,
        max_chain_fetches: positive_config(config, :max_concurrent_chain_fetches, 4),
        max_blob_transfers: positive_config(config, :max_concurrent_blob_transfers, 4),
        max_transfer_bytes_in_flight:
          positive_config(config, :max_transfer_bytes_in_flight, 1_073_741_824),
        blob_diff_batch_size: positive_config(config, :batch_documents, 100),
        trace_context: OpenTelemetry.Ctx.get_current(),
        phase_hook: Map.get(config, :phase_hook),
        replication_id: MapAccess.get(context, :replication_id) || Map.get(config, :replication_id),
        chain_chunks_total: length(chunks),
        max_chain_concurrency_observed: 0,
        blob_count: 0,
        max_blob_concurrency_observed: 0,
        logical_blob_bytes: 0,
        peak_reserved_transfer_bytes: 0
      }

      try do
        case run_transfer_loop(state) do
          {:ok, completed, transfer_measurements} ->
            {:ok, Map.put(context, :chains, aggregate_chains(completed)), transfer_measurements}

          {:error, error, transfer_measurements} ->
            {:error, error, transfer_measurements}
        end
      after
        stop_task_supervisor(task_supervisor)
      end
    end
  end

  defp preloaded_state([], chains) do
    with {:ok, obligations} <- blob_obligations(chains),
         {:ok, new_obligations, seen} <- discover_new_obligations(obligations, %{}) do
      {:ok, {%{0 => chains}, new_obligations, seen}}
    end
  end

  defp preloaded_state(_chunks, _chains), do: {:ok, {%{}, [], %{}}}

  defp run_transfer_loop(state) do
    state = receive_cancel(state)

    if state.cancel_requested do
      fail_tasks(state, Error.database_closed("replication transfer was cancelled"))
    else
      run_scheduled_transfer(state)
    end
  end

  defp run_scheduled_transfer(state) do
    state
    |> schedule_chain_tasks()
    |> continue_after_chain_schedule()
  end

  defp continue_after_chain_schedule(state) do
    case receive_cancel(state) do
      %{cancel_requested: true} = state ->
        fail_tasks(state, Error.database_closed("replication transfer was cancelled"))

      state ->
        state
        |> schedule_blob_diff_tasks()
        |> continue_after_diff_schedule()
    end
  end

  defp continue_after_diff_schedule(state) do
    case receive_cancel(state) do
      %{cancel_requested: true} = state ->
        fail_tasks(state, Error.database_closed("replication transfer was cancelled"))

      state ->
        state
        |> schedule_blob_tasks()
        |> finish_scheduled_transfer(state)
    end
  end

  defp finish_scheduled_transfer({:error, error}, state), do: fail_tasks(state, error)

  defp finish_scheduled_transfer(state, _original_state) do
    if transfer_complete?(state) do
      {:ok, state.completed_chains, measurements(state)}
    else
      receive_one_task_result(state)
    end
  end

  defp receive_cancel(state) do
    receive do
      :elixir_db_replication_transfer_cancel -> %{state | cancel_requested: true}
    after
      0 -> state
    end
  end

  defp schedule_chain_tasks(%State{chain_queue: []} = state), do: state
  defp schedule_chain_tasks(%State{cancel_requested: true} = state), do: state

  defp schedule_chain_tasks(%State{} = state) do
    available = max(state.max_chain_fetches - map_size(state.chain_tasks), 0)
    {chunks, queue} = Enum.split(state.chain_queue, available)

    tasks =
      Enum.reduce(chunks, state.chain_tasks, fn chunk, tasks ->
        task = start_chain_task(state, chunk)

        Map.put(tasks, task.ref, {task, {:chain, chunk.ordinal}})
      end)

    %{
      state
      | chain_queue: queue,
        chain_tasks: tasks,
        max_chain_concurrency_observed: max(state.max_chain_concurrency_observed, map_size(tasks))
    }
  end

  defp schedule_blob_diff_tasks(%State{} = state) do
    if state.cancel_requested or state.blob_diff_queue == [] or map_size(state.blob_diff_tasks) > 0 do
      state
    else
      {batch, queue} =
        state.blob_diff_queue
        |> Enum.sort_by(& &1.digest)
        |> Enum.split(state.blob_diff_batch_size)

      if batch == [], do: state, else: start_blob_diff_task(state, queue, batch)
    end
  end

  defp start_blob_diff_task(state, queue, batch) do
    task =
      Task.Supervisor.async_nolink(state.task_supervisor, fn ->
        run_blob_diff_task(state, batch)
      end)

    %{
      state
      | blob_diff_queue: queue,
        blob_diff_tasks: Map.put(state.blob_diff_tasks, task.ref, {task, {:blob_diff, 0, batch}})
    }
  end

  defp schedule_blob_tasks(%State{} = state) do
    if state.cancel_requested do
      state
    else
      schedule_blob_tasks_admitted(state)
    end
  end

  defp schedule_blob_tasks_admitted(%State{} = state) do
    available = max(state.max_blob_transfers - map_size(state.blob_tasks), 0)

    {to_start, queue} =
      take_blob_starts(
        state.blob_queue,
        available,
        state.reserved_bytes,
        state.max_transfer_bytes_in_flight,
        []
      )

    {tasks, reserved} =
      Enum.reduce(to_start, {state.blob_tasks, state.reserved_bytes}, fn obligation,
                                                                         {tasks, reserved} ->
        task =
          Task.Supervisor.async_nolink(state.task_supervisor, fn ->
            run_blob_task(state, obligation)
          end)

        {Map.put(tasks, task.ref, {task, {:blob, obligation.digest, obligation.length}}),
         reserved + obligation.length}
      end)

    state = %{
      state
      | blob_queue: queue,
        blob_tasks: tasks,
        reserved_bytes: reserved,
        blob_count: state.blob_count + length(to_start),
        logical_blob_bytes: state.logical_blob_bytes + Enum.reduce(to_start, 0, &(&1.length + &2)),
        max_blob_concurrency_observed: max(state.max_blob_concurrency_observed, map_size(tasks)),
        peak_reserved_transfer_bytes: max(state.peak_reserved_transfer_bytes, reserved)
    }

    case {to_start, state.blob_queue, map_size(state.blob_tasks)} do
      {[], [%BlobObligation{length: length} | _], 0}
      when length > state.max_transfer_bytes_in_flight ->
        {:error, Error.resource_limit("blob exceeds transfer byte budget")}

      _ ->
        state
    end
  end

  defp take_blob_starts(queue, 0, _reserved, _max_bytes, started),
    do: {Enum.reverse(started), queue}

  defp take_blob_starts([], _available, _reserved, _max_bytes, started),
    do: {Enum.reverse(started), []}

  defp take_blob_starts([obligation | rest], available, reserved, max_bytes, started) do
    started_bytes = Enum.reduce(started, 0, &(&1.length + &2))

    if reserved + started_bytes + obligation.length <= max_bytes do
      take_blob_starts(rest, available - 1, reserved, max_bytes, [obligation | started])
    else
      # FIFO head-of-line: keep this blob and every later blob queued until budget frees.
      {Enum.reverse(started), [obligation | rest]}
    end
  end

  defp receive_one_task_result(state) do
    receive do
      :elixir_db_replication_transfer_cancel ->
        fail_tasks(state, Error.database_closed("replication transfer was cancelled"))

      {ref, {:ok, chains}} when is_map_key(state.chain_tasks, ref) ->
        {{_task, {:chain, ordinal}}, tasks} = Map.pop!(state.chain_tasks, ref)
        _ = Process.demonitor(ref, [:flush])
        state = %{state | chain_tasks: tasks}

        case discover_blob_obligations(state, chains) do
          {:ok, state} ->
            run_transfer_loop(update_chain_state(state, ordinal, chains))

          {:error, error} ->
            fail_tasks(state, error)
        end

      {ref, {:error, %Error{} = error}} when is_map_key(state.chain_tasks, ref) ->
        fail_tasks(state, error)

      {ref, {:error, reason}} when is_map_key(state.chain_tasks, ref) ->
        fail_tasks(state, normalize_chain_error(reason))

      {ref, {:ok, missing}} when is_map_key(state.blob_diff_tasks, ref) ->
        {{_task, {:blob_diff, _ordinal, batch}}, tasks} = Map.pop!(state.blob_diff_tasks, ref)
        _ = Process.demonitor(ref, [:flush])

        case missing_blob_obligations(batch, missing) do
          {:ok, obligations} ->
            state = %{state | blob_diff_tasks: tasks, blob_queue: state.blob_queue ++ obligations}
            run_transfer_loop(state)

          {:error, error} ->
            fail_tasks(%{state | blob_diff_tasks: tasks}, error)
        end

      {ref, :ok} when is_map_key(state.blob_tasks, ref) ->
        {{_task, {:blob, _digest, length}}, tasks} = Map.pop!(state.blob_tasks, ref)
        _ = Process.demonitor(ref, [:flush])

        state = %{
          state
          | blob_tasks: tasks,
            reserved_bytes: state.reserved_bytes - length
        }

        run_transfer_loop(state)

      {ref, {:error, %Error{} = error}}
      when is_map_key(state.blob_diff_tasks, ref) or is_map_key(state.blob_tasks, ref) ->
        fail_tasks(state, error)

      {ref, {:error, _reason}}
      when is_map_key(state.blob_diff_tasks, ref) or is_map_key(state.blob_tasks, ref) ->
        fail_tasks(state, Error.internal_error("replication blob transfer failed"))

      {:DOWN, ref, :process, _pid, :normal} when is_map_key(state.chain_tasks, ref) ->
        receive_one_task_result(state)

      {:DOWN, ref, :process, _pid, reason} when is_map_key(state.chain_tasks, ref) ->
        fail_tasks(state, normalize_chain_error(reason))

      {:DOWN, ref, :process, _pid, :normal}
      when is_map_key(state.blob_diff_tasks, ref) or is_map_key(state.blob_tasks, ref) ->
        receive_one_task_result(state)

      {:DOWN, ref, :process, _pid, reason}
      when is_map_key(state.blob_diff_tasks, ref) or is_map_key(state.blob_tasks, ref) ->
        fail_tasks(state, normalize_blob_error(reason))
    end
  end

  defp update_chain_state(state, ordinal, chains) do
    %{
      state
      | completed_chains: Map.put(state.completed_chains, ordinal, chains),
        phase: {:chain, ordinal + 1}
    }
  end

  defp fetch_chain_chunk(source, %ChainChunk{ordinal: ordinal, documents: documents}, phase_hook) do
    context = %{ordinal: ordinal, documents: documents}

    with :ok <- invoke_phase_hook(phase_hook, :before_chain_fetch, context),
         result <- endpoint_call(source, :get_revision_chains, [%{documents: documents}]),
         {:ok, chains} <- normalize_chain_response(result),
         :ok <- invoke_phase_hook(phase_hook, :after_chain_fetch, Map.put(context, :chains, chains)) do
      {:ok, chains}
    end
  end

  defp normalize_chain_response({:ok, response}) when is_map(response) do
    chains = MapAccess.get(response, :chains)

    if is_list(chains) do
      {:ok, chains}
    else
      {:error, Error.invalid_request("revision chain response is invalid")}
    end
  end

  defp normalize_chain_response({:error, %Error{} = error}), do: {:error, error}

  defp normalize_chain_response({:error, _reason}),
    do: {:error, Error.internal_error("revision chain fetch failed")}

  defp normalize_chain_response(_response),
    do: {:error, Error.invalid_request("revision chain response is invalid")}

  defp fail_tasks(state, error) do
    Enum.each(all_tasks(state), fn {_ref, {task, _metadata}} ->
      _ = Task.Supervisor.terminate_child(state.task_supervisor, task.pid)
    end)

    Enum.each(all_tasks(state), fn {ref, _task} ->
      _ = Process.demonitor(ref, [:flush])
    end)

    _state = release_blob_reservations(state)

    {:error, error, measurements(state)}
  end

  defp measurements(state) do
    [
      chain_chunks: state.chain_chunks_total,
      max_chain_concurrency_observed: state.max_chain_concurrency_observed,
      blob_count: state.blob_count,
      max_blob_concurrency_observed: state.max_blob_concurrency_observed,
      logical_blob_bytes: state.logical_blob_bytes,
      peak_reserved_transfer_bytes: state.peak_reserved_transfer_bytes
    ]
  end

  defp release_blob_reservations(state) do
    %{state | reserved_bytes: 0, blob_tasks: %{}}
  end

  defp all_tasks(state),
    do: Map.merge(state.chain_tasks, Map.merge(state.blob_diff_tasks, state.blob_tasks))

  defp transfer_complete?(state) do
    state.chain_queue == [] and map_size(state.chain_tasks) == 0 and
      state.blob_diff_queue == [] and map_size(state.blob_diff_tasks) == 0 and
      state.blob_queue == [] and map_size(state.blob_tasks) == 0
  end

  defp discover_blob_obligations(state, chains) do
    with {:ok, obligations} <- blob_obligations(chains) do
      with {:ok, new_obligations, seen} <-
             discover_new_obligations(obligations, state.seen_blob_digests) do
        {:ok,
         %{
           state
           | seen_blob_digests: seen,
             blob_diff_queue: state.blob_diff_queue ++ new_obligations
         }}
      end
    end
  end

  defp discover_new_obligations(obligations, seen) do
    Enum.reduce_while(obligations, {:ok, [], seen}, fn obligation, {:ok, acc, seen} ->
      length = obligation.length

      case Map.fetch(seen, obligation.digest) do
        :error ->
          {:cont, {:ok, [obligation | acc], Map.put(seen, obligation.digest, obligation.length)}}

        {:ok, ^length} ->
          {:cont, {:ok, acc, seen}}

        {:ok, _other_length} ->
          {:halt, {:error, conflicting_blob_length_error(obligation.digest)}}
      end
    end)
    |> case do
      {:ok, obligations, seen} -> {:ok, Enum.reverse(obligations), seen}
      error -> error
    end
  end

  defp conflicting_blob_length_error(digest),
    do:
      Error.integrity_violation("attachment digest has conflicting logical lengths", %{
        digest: digest
      })

  defp diff_blob_batch(target, batch, phase_hook) do
    context = %{digests: Enum.map(batch, & &1.digest)}

    with :ok <- invoke_phase_hook(phase_hook, :before_diff_blobs, context),
         {:ok, missing} <-
           normalize_blob_diff(endpoint_call(target, :diff_blobs, [context.digests])),
         :ok <-
           invoke_phase_hook(phase_hook, :after_diff_blobs, Map.put(context, :missing, missing)) do
      {:ok, missing}
    end
  end

  defp normalize_blob_diff({:ok, missing}) when is_list(missing), do: {:ok, missing}
  defp normalize_blob_diff({:error, %Error{} = error}), do: {:error, error}

  defp normalize_blob_diff({:error, _reason}),
    do: {:error, Error.internal_error("blob diff failed")}

  defp normalize_blob_diff(_response),
    do: {:error, Error.invalid_request("blob diff response is invalid")}

  defp missing_blob_obligations(batch, missing) when is_list(missing) do
    obligations = Map.new(batch, &{&1.digest, &1})

    missing
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.reduce_while({:ok, []}, &append_missing_obligation(&1, obligations, &2))
    |> reverse_obligations()
  end

  defp missing_blob_obligations(_batch, _missing),
    do: {:error, Error.invalid_request("blob diff response is invalid")}

  defp append_missing_obligation(digest, obligations, {:ok, acc}) do
    case Map.fetch(obligations, digest) do
      {:ok, obligation} -> {:cont, {:ok, [obligation | acc]}}
      :error -> {:halt, {:error, Error.integrity_violation("blob diff returned an unknown digest")}}
    end
  end

  defp reverse_obligations({:ok, obligations}), do: {:ok, Enum.reverse(obligations)}
  defp reverse_obligations(error), do: error

  defp run_transfer_task(message, fun) when is_binary(message) and is_function(fun, 0) do
    run_caught_transfer_task(message, fun)
  end

  defp run_caught_transfer_task(message, fun) do
    fun.()
  catch
    :error, _reason -> {:error, Error.internal_error(message)}
    :exit, {:elixir_db_transfer_stream_error, %Error{} = error} -> {:error, error}
    _kind, _reason -> {:error, Error.internal_error(message)}
  end

  defp start_chain_task(state, chunk) do
    Task.Supervisor.async_nolink(state.task_supervisor, fn -> run_chain_task(state, chunk) end)
  end

  defp run_chain_task(state, chunk) do
    run_transfer_task("replication chain fetch failed", fn ->
      with_trace_context(state.trace_context, fn ->
        fetch_chain_chunk(state.source, chunk, state.phase_hook)
      end)
    end)
  end

  defp run_blob_diff_task(state, batch) do
    run_transfer_task("replication blob diff failed", fn ->
      with_trace_context(state.trace_context, fn ->
        diff_blob_batch(state.target, batch, state.phase_hook)
      end)
    end)
  end

  defp run_blob_task(state, obligation) do
    run_transfer_task("replication blob transfer failed", fn ->
      with_trace_context(state.trace_context, fn ->
        transfer_blob(
          state.source,
          state.target,
          obligation,
          state.phase_hook,
          state.replication_id
        )
      end)
    end)
  end

  defp with_trace_context(trace_context, fun) when is_function(fun, 0) do
    token = OpenTelemetry.Ctx.attach(trace_context)

    try do
      fun.()
    after
      OpenTelemetry.Ctx.detach(token)
    end
  end

  defp transfer_blob(
         source,
         target,
         %BlobObligation{length: length} = obligation,
         phase_hook,
         replication_id
       ) do
    ReplicationModule.blob_transfer_span(
      replication_id || "",
      [logical_bytes: length],
      fn -> do_transfer_blob(source, target, obligation, phase_hook) end
    )
  end

  defp do_transfer_blob(source, target, %BlobObligation{digest: digest, length: length}, phase_hook) do
    context = %{digest: digest, length: length}

    with :ok <- invoke_phase_hook(phase_hook, :before_blob_transfer, context),
         :ok <- invoke_phase_hook(phase_hook, :before_open_blob, context),
         {:ok, %BlobRepresentationStream{} = stream} <-
           endpoint_call(source, :open_blob_representation, [digest]),
         :ok <- invoke_phase_hook(phase_hook, :after_open_blob, Map.put(context, :stream, stream)),
         :ok <- validate_blob_stream(stream, digest, length),
         :ok <- record_payload_length(stream),
         :ok <- invoke_phase_hook(phase_hook, :before_put_blob, context),
         :ok <-
           normalize_put_blob(endpoint_call(target, :put_blob_representation, [stream])),
         :ok <- invoke_phase_hook(phase_hook, :after_put_blob, context) do
      :ok
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, _reason} -> {:error, Error.internal_error("blob transfer failed")}
      _response -> {:error, Error.invalid_request("blob stream response is invalid")}
    end
  end

  defp invoke_phase_hook(nil, _phase, _context), do: :ok

  defp invoke_phase_hook(phase_hook, phase, context) when is_function(phase_hook, 2) do
    case phase_hook.(phase, context) do
      :ok -> :ok
      {:error, %Error{} = error} -> {:error, error}
      {:error, _reason} -> {:error, Error.internal_error("transfer phase hook failed")}
      _other -> {:error, Error.internal_error("transfer phase hook returned invalid result")}
    end
  end

  defp invoke_phase_hook(_phase_hook, _phase, _context), do: :ok

  defp validate_blob_stream(
         %BlobRepresentationStream{
           logical_digest: digest,
           logical_length: length,
           format_version: 1,
           encoding: encoding
         },
         digest,
         length
       )
       when encoding in [:raw, :zstd],
       do: :ok

  defp validate_blob_stream(%BlobRepresentationStream{}, _digest, _length),
    do: {:error, Error.integrity_violation("blob stream metadata does not match manifest")}

  defp record_payload_length(%BlobRepresentationStream{payload_length: payload_length}) do
    _ = ElixirDB.Observability.Tracer.set_attributes(payload_length: payload_length)
    :ok
  end

  defp normalize_put_blob(:ok), do: :ok
  defp normalize_put_blob({:ok, _result}), do: :ok
  defp normalize_put_blob({:error, %Error{} = error}), do: {:error, error}

  defp normalize_put_blob({:error, _reason}),
    do: {:error, Error.internal_error("blob install failed")}

  defp normalize_put_blob(_response),
    do: {:error, Error.invalid_request("blob install response is invalid")}

  defp stop_task_supervisor(task_supervisor) do
    _ = Supervisor.stop(task_supervisor)
    :ok
  end

  defp endpoint_call(%module{} = endpoint, function, args) when is_atom(module),
    do: apply(module, function, [endpoint | args])

  defp normalize_chain_error(%Error{} = error), do: error
  defp normalize_chain_error(_reason), do: Error.internal_error("replication chain fetch failed")

  defp normalize_blob_error({:elixir_db_transfer_stream_error, %Error{} = error}), do: error
  defp normalize_blob_error(%Error{} = error), do: error
  defp normalize_blob_error(_reason), do: Error.internal_error("replication blob transfer failed")

  defp collect_chain_attachments(chains) do
    Enum.reduce_while(chains, {:ok, []}, fn chain, {:ok, acc} ->
      case chain_attachments(chain) do
        {:ok, attachments} -> {:cont, {:ok, prepend_all(attachments, acc)}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp chain_attachments(chain) when is_map(chain) do
    case Map.fetch(chain, :revisions) do
      :error ->
        case Map.fetch(chain, "revisions") do
          :error -> {:ok, []}
          {:ok, revisions} when is_list(revisions) -> collect_revision_attachments(revisions)
          {:ok, _revisions} -> {:error, integrity_error("revision chain metadata is invalid")}
        end

      {:ok, revisions} when is_list(revisions) ->
        collect_revision_attachments(revisions)

      {:ok, _revisions} ->
        {:error, integrity_error("revision chain metadata is invalid")}
    end
  end

  defp chain_attachments(_chain),
    do: {:error, integrity_error("revision chain metadata is invalid")}

  defp attachment_entries(attachments) when is_map(attachments) do
    Enum.reduce_while(attachments, {:ok, []}, fn {_name, entry}, {:ok, acc} ->
      case normalize_attachment_entry(entry) do
        {:ok, pair} -> {:cont, {:ok, [pair | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp attachment_entries(_attachments),
    do: {:error, integrity_error("attachment metadata is invalid")}

  defp normalize_attachment_entry(entry) when is_map(entry) do
    digest = MapAccess.get(entry, :digest)
    length = MapAccess.get_first(entry, [:length, :logical_size])

    case Manifest.validate_digest(digest) do
      {:ok, digest} when is_integer(length) and length >= 0 ->
        {:ok, {digest, length}}

      {:ok, _digest} ->
        {:error, integrity_error("attachment metadata is invalid")}

      {:error, _reason} ->
        {:error, integrity_error("attachment metadata is invalid")}
    end
  end

  defp normalize_attachment_entry(_entry),
    do: {:error, integrity_error("attachment metadata is invalid")}

  defp collect_revision_attachments(revisions) do
    Enum.reduce_while(revisions, {:ok, []}, fn revision, {:ok, acc} ->
      case revision_attachments(revision) do
        {:ok, entries} -> {:cont, {:ok, prepend_all(entries, acc)}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp revision_attachments(revision) when is_map(revision) do
    revision
    |> MapAccess.get(:attachments, %{})
    |> attachment_entries()
  end

  defp revision_attachments(_revision),
    do: {:error, integrity_error("revision metadata is invalid")}

  defp deduplicate_attachments(attachments) do
    attachments
    |> Enum.sort_by(fn {digest, _length} -> digest end)
    |> Enum.reduce_while({:ok, %{}}, fn {digest, length}, {:ok, seen} ->
      case Map.fetch(seen, digest) do
        :error ->
          {:cont, {:ok, Map.put(seen, digest, length)}}

        {:ok, ^length} ->
          {:cont, {:ok, seen}}

        {:ok, _other_length} ->
          {:halt,
           {:error,
            Error.integrity_violation("attachment digest has conflicting logical lengths", %{
              digest: digest
            })}}
      end
    end)
  end

  defp prepend_all(items, acc), do: Enum.reduce(items, acc, fn item, list -> [item | list] end)

  defp integrity_error(message), do: Error.integrity_violation(message)

  defp positive_config(config, key, default) do
    replication = MapAccess.get(config, :replication, config)
    value = MapAccess.get(replication, key, default)
    if is_integer(value) and value > 0, do: value, else: default
  end

  defp ceil_div(value, divisor), do: div(value + divisor - 1, divisor)
end
