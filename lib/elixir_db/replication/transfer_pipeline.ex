defmodule ElixirDB.Replication.TransferPipeline do
  @moduledoc """
  Pure contracts and planning helpers for bounded replication transfers.

  The worker still owns the existing sequential transfer phases. This module
  only defines the deterministic data that the concurrent pipeline will use.
  """

  alias ElixirDB.Attachments.Manifest
  alias ElixirDB.Error
  alias ElixirDB.MapAccess

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
              max_chain_fetches: 4,
              trace_context: nil,
              blob_obligations: [],
              seen_blob_digests: MapSet.new()

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
            max_chain_fetches: pos_integer(),
            trace_context: term(),
            blob_obligations: [BlobObligation.t()],
            seen_blob_digests: MapSet.t()
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
      chunks = partition_chain_fetches(documents, config)

      if chunks == [] do
        {:ok, Map.put(context, :chains, [])}
      else
        {:ok, task_supervisor} = Task.Supervisor.start_link([])

        state = %State{
          phase: {:chain, 0},
          source: source_endpoint,
          target: target_endpoint,
          task_supervisor: task_supervisor,
          chain_chunks: chunks,
          chain_queue: chunks,
          max_chain_fetches: positive_config(config, :max_concurrent_chain_fetches, 4),
          trace_context: OpenTelemetry.Ctx.get_current()
        }

        try do
          case run_chain_loop(state) do
            {:ok, completed} ->
              {:ok, Map.put(context, :chains, aggregate_chains(completed))}

            {:error, error} ->
              {:error, error}
          end
        after
          stop_task_supervisor(task_supervisor)
        end
      end
    else
      {:error, Error.invalid_request("replication documents must be a list")}
    end
  end

  def run(_source_endpoint, _target_endpoint, _context, _config),
    do: {:error, Error.invalid_request("replication transfer context is invalid")}

  defp run_chain_loop(state) do
    state = schedule_chain_tasks(state)

    if state.chain_queue == [] and map_size(state.chain_tasks) == 0 do
      {:ok, state.completed_chains}
    else
      receive_one_task_result(state)
    end
  end

  defp schedule_chain_tasks(%State{chain_queue: []} = state), do: state

  defp schedule_chain_tasks(%State{} = state) do
    available = max(state.max_chain_fetches - map_size(state.chain_tasks), 0)
    {chunks, queue} = Enum.split(state.chain_queue, available)

    tasks =
      Enum.reduce(chunks, state.chain_tasks, fn chunk, tasks ->
        task =
          Task.Supervisor.async_nolink(state.task_supervisor, fn ->
            token = OpenTelemetry.Ctx.attach(state.trace_context)

            try do
              fetch_chain_chunk(state.source, chunk)
            after
              OpenTelemetry.Ctx.detach(token)
            end
          end)

        Map.put(tasks, task.ref, {task, chunk.ordinal})
      end)

    %{state | chain_queue: queue, chain_tasks: tasks}
  end

  defp receive_one_task_result(state) do
    receive do
      {ref, {:ok, chains}} when is_map_key(state.chain_tasks, ref) ->
        {{_task, ordinal}, tasks} = Map.pop!(state.chain_tasks, ref)
        _ = Process.demonitor(ref, [:flush])
        state = %{state | chain_tasks: tasks}
        state = update_chain_state(state, ordinal, chains)
        run_chain_loop(state)

      {ref, {:error, %Error{} = error}} when is_map_key(state.chain_tasks, ref) ->
        fail_chain_tasks(state, ref, error)

      {ref, {:error, reason}} when is_map_key(state.chain_tasks, ref) ->
        fail_chain_tasks(state, ref, normalize_chain_error(reason))

      {:DOWN, ref, :process, _pid, :normal} when is_map_key(state.chain_tasks, ref) ->
        receive_one_task_result(state)

      {:DOWN, ref, :process, _pid, reason} when is_map_key(state.chain_tasks, ref) ->
        fail_chain_tasks(state, ref, normalize_chain_error(reason))
    end
  end

  defp update_chain_state(state, ordinal, chains) do
    %{
      state
      | completed_chains: Map.put(state.completed_chains, ordinal, chains),
        phase: {:chain, ordinal + 1}
    }
  end

  defp fetch_chain_chunk(source, %ChainChunk{documents: documents}) do
    case endpoint_call(source, :get_revision_chains, [%{documents: documents}]) do
      {:ok, response} when is_map(response) ->
        chains = MapAccess.get(response, :chains)

        if is_list(chains) do
          {:ok, chains}
        else
          {:error, Error.invalid_request("revision chain response is invalid")}
        end

      {:error, %Error{} = error} ->
        {:error, error}

      {:error, _reason} ->
        {:error, Error.internal_error("revision chain fetch failed")}

      _response ->
        {:error, Error.invalid_request("revision chain response is invalid")}
    end
  end

  defp fail_chain_tasks(state, failed_ref, error) do
    Enum.each(state.chain_tasks, fn {ref, {task, _ordinal}} ->
      if ref != failed_ref do
        _ = Task.Supervisor.terminate_child(state.task_supervisor, task.pid)
      end

      _ = Process.demonitor(ref, [:flush])
    end)

    {:error, error}
  end

  defp stop_task_supervisor(task_supervisor) do
    _ = Supervisor.stop(task_supervisor)
    :ok
  end

  defp endpoint_call(%module{} = endpoint, function, args) when is_atom(module),
    do: apply(module, function, [endpoint | args])

  defp normalize_chain_error(%Error{} = error), do: error
  defp normalize_chain_error(_reason), do: Error.internal_error("replication chain fetch failed")

  defp collect_chain_attachments(chains) do
    Enum.reduce_while(chains, {:ok, []}, fn chain, {:ok, acc} ->
      case chain_attachments(chain) do
        {:ok, attachments} -> {:cont, {:ok, prepend_all(attachments, acc)}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp chain_attachments(chain) when is_map(chain) do
    revisions = MapAccess.get(chain, :revisions)

    if is_list(revisions) do
      collect_revision_attachments(revisions)
    else
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
