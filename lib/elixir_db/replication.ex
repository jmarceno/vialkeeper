defmodule ElixirDB.Replication do
  @moduledoc "One-shot and continuous replication orchestration."
  alias ElixirDB.Replication.{CheckpointReconciler, Id}
  alias ElixirDB.JSON.Canonical

  @default_batch 100

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
    report_phase(options, :running)

    with {:ok, source_identity} <- endpoint_call(source, :identity, []),
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
      process_batches(source, target, replication_id, since, terminal, options)
    end
  end

  defp process_batches(source, target, replication_id, since, terminal, options)
       when since >= terminal do
    with :ok <- maybe_checkpoint(source, target, replication_id, since, 0, nil, options) do
      if option(options, :mode, "one_shot") in ["continuous", :continuous] do
        wait_for_next_batch(source, target, replication_id, since, options)
      else
        {:ok, %{status: :completed, replication_id: replication_id, source_sequence: since}}
      end
    end
  end

  defp process_batches(source, target, replication_id, since, terminal, options) do
    limit = option(options, :batch, @default_batch)

    with {:ok, changes} <- endpoint_call(source, :read_changes, [%{since: since, limit: limit}]),
         {:ok, selected} <- take_batch_bytes(terminal_changes(changes, terminal), options),
         :ok <- ensure_progress(selected, since, terminal),
         {:ok, documents} <- target_diff(target, selected),
         {:ok, chains} <- endpoint_call(source, :get_revision_chains, [%{documents: documents}]),
         {:ok, imported} <-
           endpoint_call(target, :import_revision_chains, [%{chains: get(chains, :chains) || []}]),
         next <- get(List.last(selected), :sequence),
         :ok <-
           maybe_checkpoint(
             source,
             target,
             replication_id,
             next,
             length(selected),
             imported,
             options
           ) do
      if next >= terminal and option(options, :mode, "one_shot") not in ["continuous", :continuous],
        do: {:ok, %{status: :completed, replication_id: replication_id, source_sequence: next}},
        else: process_batches(source, target, replication_id, next, terminal, options)
    end
  end

  defp wait_for_next_batch(source, target, replication_id, since, options) do
    report_phase(options, :waiting)
    wait_ms = option(options, :wait_ms, 1_000)
    limit = option(options, :batch, @default_batch)

    case endpoint_call(source, :read_changes, [%{since: since, limit: limit, wait_ms: wait_ms}]) do
      {:ok, changes} ->
        selected = get(changes, :results) || []

        terminal =
          case endpoint_call(source, :identity, []) do
            {:ok, identity} -> get(identity, :current_sequence) || since
            _ -> since
          end

        if selected == [] and terminal <= since do
          wait_for_next_batch(source, target, replication_id, since, options)
        else
          report_phase(options, :running)
          process_batches(source, target, replication_id, since, terminal, options)
        end

      {:error, error} ->
        {:error, error}
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

  defp maybe_checkpoint(source, target, replication_id, sequence, documents, imported, options) do
    with {:ok, source_current} <- endpoint_call(source, :get_checkpoint, [replication_id]),
         {:ok, target_current} <- endpoint_call(target, :get_checkpoint, [replication_id]),
         source_value <- value(source_current),
         target_value <- value(target_current),
         session_id <- option(options, :session_id, ElixirDB.UUID.v4()),
         entry <-
           checkpoint_entry(source_value, target_value, session_id, sequence, documents, imported),
         history <- checkpoint_history(source_value, target_value, entry),
         payload <- %{
           "version" => 1,
           "replication_id" => replication_id,
           "session_id" => session_id,
           "source_sequence" => sequence,
           "history" => history
         },
         target_payload <-
           Map.put(payload, "checkpoint_version", record_version(target_current) + 1),
         source_payload <-
           Map.put(payload, "checkpoint_version", record_version(source_current) + 1),
         {:ok, _target_result} <-
           endpoint_call(target, :put_checkpoint, [
             replication_id,
             checkpoint_request(target_payload, target_current)
           ]),
         {:ok, _source_result} <-
           endpoint_call(source, :put_checkpoint, [
             replication_id,
             checkpoint_request(source_payload, source_current)
           ]) do
      :telemetry.execute(
        [:elixir_db, :replication, :checkpoint],
        %{documents: documents, revisions: imported_count(imported)},
        %{replication_id: replication_id, source_sequence: sequence}
      )

      :ok
    end
  end

  defp checkpoint_request(payload, current),
    do: Map.put(payload, "expected_checkpoint_version", record_version(current))

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

  defp report_phase(options, phase) do
    case option(options, :job_id, nil) do
      nil -> :ok
      job_id -> ElixirDB.Replication.JobManager.report(job_id, phase, %{})
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
