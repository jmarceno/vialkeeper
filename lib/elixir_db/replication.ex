defmodule ElixirDB.Replication do
  @moduledoc "One-shot and continuous replication orchestration."
  alias ElixirDB.Replication.{CheckpointReconciler, Id}
  alias ElixirDB.JSON.Canonical

  @default_batch 100

  @doc "Ordered worker phase states from Plan §7.7 (excluding terminal idle entry)."
  def phases do
    [
      :idle,
      :handshake,
      :read_changes,
      :diff,
      :fetch_chains,
      :import,
      :checkpoint_target,
      :checkpoint_source,
      :waiting,
      :backoff,
      :completed,
      :failed
    ]
  end

  def one_shot(source_uuid, target_uuid, options \\ %{}) do
    with {:ok, source} <- ElixirDB.Replication.LocalEndpoint.new(source_uuid),
         {:ok, target} <- ElixirDB.Replication.LocalEndpoint.new(target_uuid) do
      run(source, target, options)
    end
  end

  def one_shot_endpoints(source, target, options \\ %{}), do: run(source, target, options)

  @doc "Run one captured batch sequence, or keep waiting when mode is continuous."
  def run(source, target, options \\ %{}) do
    session_id = option(options, :session_id, ElixirDB.UUID.v4())
    options = put_option(options, :session_id, session_id)

    with {:ok, context} <- handshake(source, target, options) do
      process_from_handshake(source, target, context, options)
    end
  end

  @doc "Handshake: identities, compatibility, replication id, checkpoint reconciliation."
  def handshake(source, target, options) do
    with :ok <- phase_hook(options, :handshake, %{}),
         {:ok, source_identity} <- endpoint_call(source, :identity, []),
         {:ok, target_identity} <- endpoint_call(target, :identity, []),
         :ok <- compatible(source_identity, target_identity),
         {:ok, replication_id} <-
           Id.calculate(
             source_uuid(source_identity),
             source_uuid(target_identity),
             option(options, :direction, "push"),
             option(options, :mode, "one_shot")
           ),
         {:ok, source_checkpoint} <- endpoint_call(source, :get_checkpoint, [replication_id]),
         {:ok, target_checkpoint} <- endpoint_call(target, :get_checkpoint, [replication_id]) do
      since =
        CheckpointReconciler.common_sequence(value(source_checkpoint), value(target_checkpoint))

      terminal = get(source_identity, :current_sequence) || 0

      {:ok,
       %{
         session_id: option(options, :session_id, ElixirDB.UUID.v4()),
         replication_id: replication_id,
         since: since,
         terminal: terminal,
         source_checkpoint: source_checkpoint,
         target_checkpoint: target_checkpoint,
         selected: [],
         documents: [],
         chains: [],
         imported: nil
       }}
    end
  end

  @doc "Read a bounded change batch from the source after `context.since`."
  def read_changes(source, context, options) do
    limit = option(options, :batch, @default_batch)
    wait_ms = option(options, :wait_ms_for_read, 0)

    request =
      if wait_ms > 0,
        do: %{since: context.since, limit: limit, wait_ms: wait_ms},
        else: %{since: context.since, limit: limit}

    with :ok <- phase_hook(options, :read_changes, context),
         {:ok, changes} <- endpoint_call(source, :read_changes, [request]),
         {:ok, selected} <- take_batch_bytes(terminal_changes(changes, context.terminal), options),
         :ok <- ensure_progress(selected, context.since, context.terminal) do
      {:ok, %{context | selected: selected}}
    end
  end

  @doc "Diff selected change leaves against the target."
  def diff(target, context, options) do
    with :ok <- phase_hook(options, :diff, context),
         {:ok, documents} <- target_diff(target, context.selected) do
      {:ok, %{context | documents: documents}}
    end
  end

  @doc "Fetch complete revision chains for missing documents from the source."
  def fetch_chains(source, context, options) do
    with :ok <- phase_hook(options, :fetch_chains, context),
         {:ok, chains} <-
           endpoint_call(source, :get_revision_chains, [%{documents: context.documents}]) do
      {:ok, %{context | chains: get(chains, :chains) || []}}
    end
  end

  @doc "Import chains into the target and confirm durable commit (REPL-004)."
  def import_chains(target, context, options) do
    with :ok <- phase_hook(options, :import, context),
         {:ok, imported} <-
           endpoint_call(target, :import_revision_chains, [%{chains: context.chains}]),
         {:ok, _confirmed} <-
           endpoint_call(target, :confirm_durable_commit, [%{imported: imported}]) do
      {:ok, %{context | imported: imported}}
    end
  end

  @doc "CAS-write the target checkpoint for the current batch sequence."
  def checkpoint_target(source, target, context, options) do
    with :ok <- phase_hook(options, :checkpoint_target, context),
         {:ok, prepared} <- prepare_checkpoint(source, target, context, options),
         {:ok, target_result} <-
           endpoint_call(target, :put_checkpoint, [
             context.replication_id,
             prepared.target_request
           ]) do
      {:ok,
       Map.merge(context, %{
         checkpoint_prepared: prepared,
         target_checkpoint_result: target_result,
         source_checkpoint: prepared.source_current,
         target_checkpoint: prepared.target_current
       })}
    end
  end

  @doc "CAS-write the source checkpoint after the target checkpoint succeeded."
  def checkpoint_source(source, context, options) do
    prepared = Map.fetch!(context, :checkpoint_prepared)

    with :ok <- phase_hook(options, :checkpoint_source, context),
         {:ok, source_result} <-
           endpoint_call(source, :put_checkpoint, [
             context.replication_id,
             prepared.source_request
           ]) do
      :telemetry.execute(
        [:elixir_db, :replication, :checkpoint],
        %{documents: prepared.documents, revisions: prepared.revisions},
        %{replication_id: context.replication_id, source_sequence: prepared.sequence}
      )

      next = prepared.sequence

      {:ok,
       context
       |> Map.put(:since, next)
       |> Map.put(:source_checkpoint_result, source_result)
       |> Map.put(:selected, [])
       |> Map.put(:documents, [])
       |> Map.put(:chains, [])
       |> Map.put(:imported, nil)
       |> Map.delete(:checkpoint_prepared)
       |> Map.delete(:target_checkpoint_result)}
    end
  end

  @doc "Wait for source changes beyond the current checkpoint (continuous mode)."
  def wait_for_changes(source, context, options) do
    with :ok <- phase_hook(options, :waiting, context),
         {:ok, changes} <-
           endpoint_call(source, :read_changes, [
             %{
               since: context.since,
               limit: option(options, :batch, @default_batch),
               wait_ms: option(options, :wait_ms, 1_000)
             }
           ]) do
      selected = get(changes, :results) || []

      terminal =
        case endpoint_call(source, :identity, []) do
          {:ok, identity} -> get(identity, :current_sequence) || context.since
          _ -> context.since
        end

      {:ok, %{context | terminal: terminal, selected: selected}}
    end
  end

  @doc "Advance after checkpoint_source: more batches, waiting, or completed."
  def next_after_checkpoint(context, options) do
    continuous? = option(options, :mode, "one_shot") in ["continuous", :continuous]

    cond do
      context.since < context.terminal ->
        {:continue_batch, context}

      continuous? ->
        {:waiting, context}

      true ->
        {:completed,
         %{
           status: :completed,
           replication_id: context.replication_id,
           source_sequence: context.since
         }}
    end
  end

  @doc "Whether handshake found the source already caught up to the terminal sequence."
  def caught_up?(%{since: since, terminal: terminal}), do: since >= terminal

  defp process_from_handshake(source, target, context, options) do
    if caught_up?(context) do
      with {:ok, context} <- checkpoint_target(source, target, context, options),
           {:ok, context} <- checkpoint_source(source, context, options) do
        case next_after_checkpoint(context, options) do
          {:completed, result} ->
            {:ok, result}

          {:waiting, context} ->
            wait_loop(source, target, context, options)

          {:continue_batch, context} ->
            process_batches(source, target, context, options)
        end
      end
    else
      process_batches(source, target, context, options)
    end
  end

  defp process_batches(source, target, context, options) do
    with {:ok, context} <- read_changes(source, context, options),
         {:ok, context} <- diff(target, context, options),
         {:ok, context} <- fetch_chains(source, context, options),
         {:ok, context} <- import_chains(target, context, options),
         {:ok, context} <- checkpoint_target(source, target, context, options),
         {:ok, context} <- checkpoint_source(source, context, options) do
      case next_after_checkpoint(context, options) do
        {:completed, result} ->
          {:ok, result}

        {:waiting, context} ->
          wait_loop(source, target, context, options)

        {:continue_batch, context} ->
          process_batches(source, target, context, options)
      end
    end
  end

  defp wait_loop(source, target, context, options) do
    case wait_for_changes(source, context, options) do
      {:ok, context} ->
        if context.selected == [] and context.terminal <= context.since do
          wait_loop(source, target, context, options)
        else
          process_batches(source, target, %{context | selected: []}, options)
        end

      {:error, error} ->
        {:error, error}
    end
  end

  defp prepare_checkpoint(source, target, context, _options) do
    sequence =
      case context.selected do
        [_ | _] = selected -> get(List.last(selected), :sequence)
        _ -> context.since
      end

    documents = length(context.selected)
    imported = context.imported

    with {:ok, source_current} <- endpoint_call(source, :get_checkpoint, [context.replication_id]),
         {:ok, target_current} <- endpoint_call(target, :get_checkpoint, [context.replication_id]),
         source_value <- value(source_current),
         target_value <- value(target_current),
         session_id <- context.session_id,
         entry <-
           checkpoint_entry(source_value, target_value, session_id, sequence, documents, imported),
         history <- checkpoint_history(source_value, target_value, entry),
         payload <- %{
           "version" => 1,
           "replication_id" => context.replication_id,
           "session_id" => session_id,
           "source_sequence" => sequence,
           "history" => history
         },
         target_payload <-
           Map.put(payload, "checkpoint_version", record_version(target_current) + 1),
         source_payload <-
           Map.put(payload, "checkpoint_version", record_version(source_current) + 1) do
      {:ok,
       %{
         sequence: sequence,
         documents: documents,
         revisions: imported_count(imported),
         source_current: source_current,
         target_current: target_current,
         target_request: checkpoint_request(target_payload, target_current),
         source_request: checkpoint_request(source_payload, source_current)
       }}
    end
  end

  defp checkpoint_request(payload, current),
    do: Map.put(payload, "expected_checkpoint_version", record_version(current))

  # REPL-007: retain at most the ten most recent completed sessions, keyed by
  # session identity `{session_id, source_sequence}` (same key used by checkpoint_entry/6).
  defp checkpoint_history(source, target, entry) do
    (List.wrap(source && (source[:history] || source["history"])) ++
       List.wrap(target && (target[:history] || target["history"])) ++ [entry])
    |> Enum.uniq_by(fn item -> {get(item, :session_id), get(item, :source_sequence)} end)
    |> Enum.sort_by(&get(&1, :source_sequence), :desc)
    |> Enum.take(10)
  end

  defp checkpoint_entry(source, target, session_id, sequence, documents, imported) do
    existing =
      (List.wrap(source && (source[:history] || source["history"])) ++
         List.wrap(target && (target[:history] || target["history"])))
      |> Enum.find(fn item ->
        get(item, :session_id) == session_id and get(item, :source_sequence) == sequence
      end)

    existing ||
      %{
        "session_id" => session_id,
        "source_sequence" => sequence,
        "documents_read" => documents,
        "revisions_written" => imported_count(imported),
        "completed_at" => DateTime.utc_now() |> DateTime.to_iso8601()
      }
  end

  defp ensure_progress([], since, terminal) when since < terminal,
    do: {:error, ElixirDB.Error.database_unavailable("source changes did not make progress")}

  defp ensure_progress(_selected, _since, _terminal), do: :ok

  defp terminal_changes(changes, terminal) do
    Enum.filter(get(changes, :results) || [], &(get(&1, :sequence) <= terminal))
  end

  defp take_batch_bytes(changes, options) do
    configured =
      option(
        options,
        :batch_bytes,
        get_in(ElixirDB.Config.defaults(), ["replication", "batch_bytes"]) || 4_194_304
      )

    default = get_in(ElixirDB.Config.defaults(), ["replication", "batch_bytes"]) || 4_194_304
    configured = if is_integer(configured) and configured > 0, do: configured, else: default

    maximum =
      min(configured, ElixirDB.Config.host_limits()[:max_replication_batch_bytes] || 16_777_216)

    Enum.reduce_while(changes, {[], 0}, fn change, {selected, size} ->
      case Canonical.encode(stringify_keys(change)) do
        {:ok, encoded} ->
          next_size = size + byte_size(encoded)

          cond do
            next_size <= maximum ->
              {:cont, {[change | selected], next_size}}

            selected == [] ->
              {:halt,
               {:error,
                ElixirDB.Error.payload_too_large("replication batch exceeds the byte limit")}}

            true ->
              {:halt, {:done, Enum.reverse(selected)}}
          end

        {:error, error} ->
          {:halt, {:error, error}}
      end
    end)
    |> case do
      {:error, _} = error -> error
      {:done, selected} -> {:ok, selected}
      {selected, _size} -> {:ok, Enum.reverse(selected)}
    end
  end

  defp target_diff(_target, []), do: {:ok, []}

  defp target_diff(target, changes) do
    request = %{
      documents:
        Enum.map(changes, fn change ->
          %{
            document_id: get(change, :document_id),
            leaf_revisions: Enum.map(get(change, :leaf_revisions) || [], &get(&1, :revision))
          }
        end)
    }

    with {:ok, result} <- endpoint_call(target, :diff_revisions, [request]) do
      {:ok,
       Enum.map(get(result, :documents) || [], fn document ->
         %{
           document_id: get(document, :document_id),
           leaf_revisions: get(document, :missing_revisions) || []
         }
       end)}
    end
  end

  defp phase_hook(options, phase, context) do
    case option(options, :phase_hook, nil) do
      nil ->
        :ok

      fun when is_function(fun, 2) ->
        case fun.(phase, context) do
          :ok -> :ok
          {:error, _} = error -> error
          other -> {:error, ElixirDB.Error.internal_error("phase hook returned #{inspect(other)}")}
        end

      _ ->
        :ok
    end
  end

  defp stringify_keys(value) when is_map(value),
    do: Map.new(value, fn {key, child} -> {to_string(key), stringify_keys(child)} end)

  defp stringify_keys(value) when is_list(value), do: Enum.map(value, &stringify_keys/1)
  defp stringify_keys(value), do: value

  defp record_version(nil), do: 0
  defp record_version(%{version: version}) when is_integer(version), do: version
  defp record_version(%{"version" => version}) when is_integer(version), do: version
  defp record_version(_), do: 0

  defp imported_count(nil), do: 0
  defp imported_count(imported), do: get(imported, :revisions_inserted) || 0

  defp compatible(source, target) do
    source_uuid = source_uuid(source)
    target_uuid = source_uuid(target)

    cond do
      source_uuid == target_uuid ->
        {:error,
         ElixirDB.Error.replication_incompatible("source and target database UUIDs must differ")}

      version(source, :replication_protocol_major) != version(target, :replication_protocol_major) ->
        {:error, ElixirDB.Error.replication_incompatible("replication protocol versions differ")}

      version(source, :revision_algorithm_version) != version(target, :revision_algorithm_version) ->
        {:error, ElixirDB.Error.replication_incompatible("revision algorithms differ")}

      version(source, :canonicalization_version) != version(target, :canonicalization_version) ->
        {:error, ElixirDB.Error.replication_incompatible("canonicalization versions differ")}

      true ->
        :ok
    end
  end

  defp endpoint_call(%ElixirDB.Replication.LocalEndpoint{} = endpoint, function, args),
    do: apply(ElixirDB.Replication.LocalEndpoint, function, [endpoint | args])

  defp endpoint_call(%ElixirDB.Replication.RemoteEndpoint{} = endpoint, function, args),
    do: apply(ElixirDB.Replication.RemoteEndpoint, function, [endpoint | args])

  defp source_uuid(map), do: get(map, :database_uuid)
  defp version(map, key), do: get(map, key)
  defp get(map, key) when is_map(map), do: map[key] || map[Atom.to_string(key)]
  defp value(nil), do: nil
  defp value(%{value: value}), do: value
  defp value(%{"value" => value}), do: value

  defp option(options, key, default) when is_map(options),
    do: Map.get(options, key, Map.get(options, Atom.to_string(key), default))

  defp option(options, key, default) when is_list(options),
    do: Keyword.get(options, key, default)

  defp put_option(options, key, value) when is_map(options), do: Map.put(options, key, value)
  defp put_option(options, key, value) when is_list(options), do: Keyword.put(options, key, value)
end
