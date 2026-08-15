defmodule VialKeeper.Replication.JobManager do
  @moduledoc "Persistent replication definitions and transient worker lifecycle."
  use GenServer

  alias VialKeeper.Domain.ReplicationEndpoint
  alias VialKeeper.JSON.Stringify
  alias VialKeeper.MapAccess
  alias VialKeeper.Replication.Id
  alias VialKeeper.Replication.LocalEndpoint
  alias VialKeeper.Replication.RemoteEndpoint
  alias VialKeeper.Runtime.DatabaseCatalog
  @table :vial_keeper_replication_jobs

  @active_states [
    :idle,
    :handshake,
    :install_boundaries,
    :bootstrap,
    :read_changes,
    :diff,
    :transfer,
    :import,
    :checkpoint_target,
    :checkpoint_source,
    :report_peer,
    :waiting,
    :backoff
  ]

  def start_link(_args \\ []), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  @impl true
  def init(_args) do
    _ = ensure_table()
    {:ok, %{}}
  end

  def list(uuid) do
    table = ensure_table()

    with {:ok, jobs} <- list_persisted_jobs(uuid) do
      {:ok,
       jobs
       |> Enum.map(&running_job(&1, table))
       |> Enum.map(&redact_job/1)}
    end
  end

  def resume(uuid) do
    case list_persisted_jobs(uuid) do
      {:ok, jobs} ->
        Enum.each(jobs, &resume_job(uuid, &1))

        :ok

      {:error, _} ->
        :ok
    end
  end

  defp resume_job(uuid, job) do
    definition = job.definition

    if job.enabled and option(definition, :mode, "one_shot") in ["continuous", :continuous],
      do: start(uuid, job.job_id)
  end

  def get(uuid, job_id) do
    with {:ok, jobs} <- list(uuid) do
      case Enum.find(jobs, &(&1.job_id == job_id)) do
        nil -> {:error, VialKeeper.Error.replication_job_not_found("replication job not found")}
        job -> {:ok, job}
      end
    end
  end

  @doc """
  Returns the stored remote token for the same-node Web UI edit merge.

  This raw value must never be included in an HTTP response.
  """
  @spec stored_remote_auth_token(binary(), binary()) :: binary() | nil
  def stored_remote_auth_token(uuid, job_id) do
    case fetch_job(uuid, job_id) do
      {:ok, job} ->
        job
        |> Map.get(:definition, %{})
        |> Map.get("endpoint", %{})
        |> Map.get("auth_token")
        |> present_token()

      _ ->
        nil
    end
  end

  def active?(uuid) do
    ensure_table()
    |> :ets.tab2list()
    |> Enum.any?(fn
      {_job_id, state, _pid, ^uuid, _replication_id, _details} -> active_state?(state)
      {_job_id, state, _pid, ^uuid, _replication_id} -> active_state?(state)
      _ -> false
    end)
  end

  @doc """
  Returns a bounded in-memory summary of tracked replication workers.

  Reads only the JobManager ETS table; it does not open databases or load
  persisted job definitions.
  """
  @spec runtime_summary() :: %{
          tracked: non_neg_integer(),
          active: non_neg_integer(),
          by_state: %{optional(atom()) => non_neg_integer()}
        }
  def runtime_summary do
    entries = ensure_table() |> :ets.tab2list()

    Enum.reduce(entries, %{tracked: 0, active: 0, by_state: %{}}, fn entry, acc ->
      state =
        case entry do
          {_job_id, state, _pid, _uuid, _replication_id, _details} -> state
          {_job_id, state, _pid, _uuid, _replication_id} -> state
          _ -> :unknown
        end

      by_state = Map.update(acc.by_state, state, 1, &(&1 + 1))
      active = if active_state?(state), do: acc.active + 1, else: acc.active

      %{tracked: acc.tracked + 1, active: active, by_state: by_state}
    end)
  end

  def put(uuid, definition) do
    with {:ok, normalized} <- normalize_definition(definition),
         persist <- option(normalized, :persist, true),
         mode <- option(normalized, :mode, "one_shot"),
         true <- is_boolean(persist),
         :ok <- validate_persistence(persist, mode),
         normalized <- preserve_roundtrip_token(uuid, normalized) do
      job_id = option(normalized, :job_id, VialKeeper.UUID.v4())
      normalized = Map.put(normalized, "job_id", job_id)

      persist_or_start(uuid, normalized, job_id, persist)
    end
  end

  defp persist_or_start(uuid, normalized, job_id, true) do
    enabled = option(normalized, :enabled, false)

    with {:ok, _} <-
           DatabaseCatalog.command(
             uuid,
             {:command, :put_job, job_record(job_id, normalized, enabled)}
           ) do
      persisted_result(uuid, job_id, enabled)
    end
  end

  defp persist_or_start(uuid, normalized, _job_id, false),
    do: start_unpersisted(uuid, normalized)

  defp persisted_result(uuid, job_id, true), do: start(uuid, job_id)
  defp persisted_result(_uuid, job_id, false), do: {:ok, %{job_id: job_id, state: :disabled}}

  def start(uuid, job_id) do
    table = ensure_table()

    with {:ok, jobs} <- list_persisted_jobs(uuid),
         {:ok, job} <- find_job(jobs, job_id),
         :ok <-
           if(job.enabled,
             do: :ok,
             else: {:error, VialKeeper.Error.invalid_request("replication job is disabled")}
           ),
         {:ok, options} <- worker_options(uuid, job),
         :ok <- ensure_worker_available(options.replication_id),
         {:ok, pid} <- start_worker(options) do
      true = :ets.insert(table, {job_id, :idle, pid, uuid, options.replication_id, %{}})
      :gen_statem.cast(pid, :start)
      {:ok, %{job_id: job_id, state: :idle}}
    end
  end

  def cancel(uuid, job_id) do
    cancel_entry(uuid, job_id, :ets.lookup(ensure_table(), job_id))
  end

  defp cancel_entry(uuid, job_id, [
         {entry_job_id, state, _pid, entry_uuid, _replication_id, _details}
       ])
       when entry_job_id == job_id and (is_nil(uuid) or uuid == entry_uuid) and
              state in @active_states do
    case cancel_if_active(entry_uuid, job_id) do
      :ok -> {:ok, %{job_id: job_id, state: :failed}}
      {:error, _} = error -> error
    end
  end

  defp cancel_entry(uuid, job_id, [
         {entry_job_id, state, _pid, entry_uuid, _replication_id, details}
       ])
       when entry_job_id == job_id and (is_nil(uuid) or uuid == entry_uuid) do
    case await_worker_termination(entry_uuid, job_id) do
      :ok -> {:ok, Map.merge(%{job_id: job_id, state: state}, details || %{})}
      {:error, _} = error -> error
    end
  end

  defp cancel_entry(_uuid, _job_id, []),
    do: {:error, VialKeeper.Error.replication_job_not_found("replication job is not running")}

  def cancel(job_id), do: cancel(nil, job_id)

  def enable(uuid, job_id) do
    with {:ok, job} <- fetch_job(uuid, job_id),
         definition <- Map.put(job.definition, "enabled", true),
         {:ok, _} <- put_persisted(uuid, job_id, definition, true) do
      start(uuid, job_id)
    end
  end

  def disable(uuid, job_id) do
    with {:ok, job} <- fetch_job(uuid, job_id),
         :ok <- cancel_if_active(uuid, job_id),
         definition <- Map.put(job.definition, "enabled", false),
         {:ok, _} <- put_persisted(uuid, job_id, definition, false) do
      mark_disabled(uuid, job_id)
      {:ok, %{job_id: job_id, state: :disabled}}
    end
  end

  def delete(uuid, job_id) do
    case :ets.lookup(ensure_table(), job_id) do
      [{^job_id, state, _pid, ^uuid, _, _details}] when state in @active_states ->
        {:error, VialKeeper.Error.database_not_closable("replication job is active")}

      _ ->
        DatabaseCatalog.command(uuid, {:command, :delete_job, job_id})
    end
  end

  def report(job_id, state, details \\ %{}) do
    table = ensure_table()

    case :ets.lookup(table, job_id) do
      [{^job_id, _old_state, pid, uuid, replication_id, _old_details}] ->
        :ets.insert(table, {job_id, state, pid, uuid, replication_id, details})
        :ok

      [] ->
        :ok
    end
  end

  defp start_unpersisted(uuid, definition) do
    job_id = definition["job_id"]
    job = job_record(job_id, definition, true)

    with {:ok, options} <- worker_options(uuid, job),
         :ok <- ensure_worker_available(options.replication_id),
         {:ok, pid} <- start_worker(options) do
      true = :ets.insert(ensure_table(), {job_id, :idle, pid, uuid, options.replication_id, %{}})
      :gen_statem.cast(pid, :start)
      {:ok, %{job_id: job_id, state: :idle, persisted: false}}
    end
  end

  defp start_worker(options) do
    max_workers = VialKeeper.Config.host_limits()[:max_replication_workers] || 32

    if Registry.count(VialKeeper.Replication.WorkerRegistry) >= max_workers do
      {:error, VialKeeper.Error.resource_limit("maximum replication worker count reached")}
    else
      case DynamicSupervisor.start_child(
             VialKeeper.Replication.WorkerSupervisor,
             {VialKeeper.Replication.Worker, options}
           ) do
        {:ok, pid} ->
          {:ok, pid}

        {:error, {:shutdown, %VialKeeper.Error{} = error}} ->
          {:error, error}

        {:error, {:already_started, _pid}} ->
          {:error,
           VialKeeper.Error.replication_already_running("replication worker is already running")}

        {:error, reason} ->
          {:error,
           VialKeeper.Error.internal_error("replication worker could not start", %{
             cause: inspect(reason)
           })}
      end
    end
  end

  defp ensure_worker_available(replication_id) do
    case Registry.lookup(VialKeeper.Replication.WorkerRegistry, replication_id) do
      [] ->
        :ok

      _ ->
        {:error,
         VialKeeper.Error.replication_already_running("replication worker is already running")}
    end
  end

  defp send_cancel(pid) do
    if Process.alive?(pid), do: :gen_statem.cast(pid, :cancel)
    :ok
  end

  defp cancel_if_active(uuid, job_id) do
    case :ets.lookup(ensure_table(), job_id) do
      [{^job_id, state, pid, ^uuid, replication_id, _details}] when state in @active_states ->
        stop_worker(state, pid, replication_id, job_id)

      [{^job_id, _state, _pid, ^uuid, _replication_id, _details}] ->
        await_worker_termination(uuid, job_id)

      _ ->
        :ok
    end
  end

  defp await_worker_termination(uuid, job_id) do
    case :ets.lookup(ensure_table(), job_id) do
      [{^job_id, state, pid, ^uuid, replication_id, _details}] ->
        stop_worker(state, pid, replication_id, job_id)

      _ ->
        :ok
    end
  end

  defp stop_worker(state, pid, replication_id, job_id) do
    ref = Process.monitor(pid)
    if state in @active_states, do: send_cancel(pid)

    with :ok <- await_worker_exit(ref, pid, job_id) do
      await_worker_registry_release(replication_id, pid)
    end
  end

  defp await_worker_exit(ref, pid, job_id) do
    receive do
      {:DOWN, ^ref, :process, ^pid, _reason} ->
        :ok
    after
      VialKeeper.Config.shutdown_timeout() ->
        Process.demonitor(ref, [:flush])

        {:error,
         VialKeeper.Error.database_not_closable("replication worker did not stop", %{
           job_id: job_id
         })}
    end
  end

  defp await_worker_registry_release(replication_id, pid) do
    deadline = System.monotonic_time(:millisecond) + VialKeeper.Config.shutdown_timeout()
    await_worker_registry_release(replication_id, pid, deadline)
  end

  defp await_worker_registry_release(replication_id, pid, deadline) do
    case Registry.lookup(VialKeeper.Replication.WorkerRegistry, replication_id) do
      [] ->
        :ok

      [{^pid, _}] ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error,
           VialKeeper.Error.database_not_closable(
             "replication worker registry entry did not clear",
             %{
               replication_id: replication_id
             }
           )}
        else
          Process.sleep(1)
          await_worker_registry_release(replication_id, pid, deadline)
        end

      _entries ->
        {:error,
         VialKeeper.Error.replication_already_running("replication worker is already running")}
    end
  end

  defp mark_disabled(uuid, job_id) do
    case :ets.lookup(ensure_table(), job_id) do
      [{^job_id, _state, pid, ^uuid, replication_id, details}] ->
        true = :ets.insert(ensure_table(), {job_id, :disabled, pid, uuid, replication_id, details})
        :ok

      [] ->
        :ok
    end
  end

  defp put_persisted(uuid, job_id, definition, enabled),
    do:
      DatabaseCatalog.command(
        uuid,
        {:command, :put_job, job_record(job_id, definition, enabled)}
      )

  defp job_record(job_id, definition, enabled),
    do: %{job_id: job_id, definition: definition, enabled: enabled}

  @redacted_token "[redacted]"

  defp list_persisted_jobs(uuid),
    do: DatabaseCatalog.command(uuid, {:command, :list_jobs, %{}})

  defp fetch_job(uuid, job_id) do
    with {:ok, jobs} <- list_persisted_jobs(uuid) do
      find_job(jobs, job_id)
    end
  end

  defp running_job(job, table) when is_map(job),
    do: Map.put(job, :state, runtime_state(table, job.job_id, job.enabled))

  defp redact_job(job) when is_map(job),
    do: Map.update(job, :definition, %{}, &redact_definition/1)

  defp redact_definition(%{"endpoint" => %{"auth_token" => token} = endpoint} = definition)
       when is_binary(token) and token != "" do
    %{definition | "endpoint" => %{endpoint | "auth_token" => @redacted_token}}
  end

  defp redact_definition(definition), do: definition

  defp present_token(token) when is_binary(token) and token != "", do: token
  defp present_token(_), do: nil

  defp preserve_roundtrip_token(uuid, definition) do
    case roundtrip_target(definition) do
      {:preserve, job_id, endpoint} ->
        merge_stored_token(uuid, job_id, endpoint, definition)

      nil ->
        definition
    end
  end

  defp roundtrip_target(definition) do
    job_id = Map.get(definition, "job_id")

    if is_binary(job_id) and job_id != "" do
      endpoint = Map.get(definition, "endpoint", %{})
      kind = Map.get(endpoint, "kind")
      presented = Map.get(endpoint, "auth_token")

      if kind == "remote" and (not is_binary(presented) or presented in ["", @redacted_token]) do
        {:preserve, job_id, endpoint}
      else
        nil
      end
    else
      nil
    end
  end

  defp merge_stored_token(uuid, job_id, endpoint, definition) do
    case stored_remote_auth_token(uuid, job_id) do
      token when is_binary(token) and token != "" ->
        Map.put(definition, "endpoint", Map.put(endpoint, "auth_token", token))

      _ ->
        definition
    end
  end

  defp find_job(jobs, job_id) do
    case Enum.find(jobs, &(&1.job_id == job_id)) do
      nil -> {:error, VialKeeper.Error.replication_job_not_found("replication job not found")}
      job -> {:ok, job}
    end
  end

  defp worker_options(uuid, job) do
    definition = job.definition
    endpoint = option(definition, :endpoint, %{})
    direction = option(definition, :direction, "push")
    mode = option(definition, :mode, "continuous")

    with {:ok, local} <- LocalEndpoint.new(uuid),
         {:ok, counterpart} <- endpoint_from_definition(endpoint),
         {:ok, source_target} <- endpoints_for_direction(local, counterpart, direction),
         :ok <- validate_target_kind(source_target.target),
         {:ok, replication_id} <-
           Id.calculate(
             source_uuid(source_target.source),
             source_uuid(source_target.target),
             direction,
             mode
           ) do
      {:ok,
       %{
         source: source_target.source,
         target: source_target.target,
         replication_id: replication_id,
         mode: mode,
         direction: direction,
         batch:
           option(
             option(definition, :batch, %{}),
             :documents,
             replication_default(:batch_documents, 100)
           ),
         wait_ms: option(definition, :wait_ms, 1_000),
         retry: option(definition, :retry, %{}),
         batch_bytes:
           option(
             option(definition, :batch, %{}),
             :bytes,
             replication_default(:batch_bytes, 4_194_304)
           ),
         max_concurrent_chain_fetches:
           option(
             definition,
             :max_concurrent_chain_fetches,
             replication_default(:max_concurrent_chain_fetches, 4)
           ),
         max_concurrent_blob_transfers:
           option(
             definition,
             :max_concurrent_blob_transfers,
             replication_default(:max_concurrent_blob_transfers, 4)
           ),
         max_transfer_bytes_in_flight:
           option(
             definition,
             :max_transfer_bytes_in_flight,
             replication_default(:max_transfer_bytes_in_flight, 1_073_741_824)
           ),
         batch_documents:
           option(
             definition,
             :batch_documents,
             option(
               option(definition, :batch, %{}),
               :documents,
               replication_default(:batch_documents, 100)
             )
           ),
         job_id: job.job_id
       }}
    end
  end

  defp endpoints_for_direction(local, counterpart, "pull"),
    do: {:ok, %{source: counterpart, target: local}}

  defp endpoints_for_direction(local, counterpart, _),
    do: {:ok, %{source: local, target: counterpart}}

  defp validate_target_kind(%LocalEndpoint{} = target) do
    case LocalEndpoint.identity(target) do
      {:ok, %{database_kind: :derived}} ->
        {:error,
         VialKeeper.Error.derived_database_read_only(
           "derived databases cannot be replication targets"
         )}

      {:ok, _identity} ->
        :ok

      {:error, _} = error ->
        error
    end
  end

  defp validate_target_kind(_target), do: :ok

  defp source_uuid(%{database_uuid: uuid}), do: uuid

  defp endpoint_from_definition(definition) do
    case ReplicationEndpoint.new(definition) do
      {:ok, %{kind: :local, database_uuid: uuid}} ->
        LocalEndpoint.new(uuid)

      {:ok, %{kind: :remote} = endpoint} ->
        RemoteEndpoint.new(%{
          database_uuid: endpoint.database_uuid,
          base_url: endpoint.base_url,
          auth_token: endpoint.auth_token
        })

      {:error, error} ->
        {:error, error}
    end
  end

  defp normalize_definition(definition) when is_map(definition) do
    allowed = [
      "job_id",
      "persist",
      "mode",
      "direction",
      "endpoint",
      "enabled",
      "batch",
      "retry",
      "wait_ms",
      "max_concurrent_chain_fetches",
      "max_concurrent_blob_transfers",
      "max_transfer_bytes_in_flight",
      "batch_documents",
      :job_id,
      :persist,
      :mode,
      :direction,
      :endpoint,
      :enabled,
      :batch,
      :retry,
      :wait_ms,
      :max_concurrent_chain_fetches,
      :max_concurrent_blob_transfers,
      :max_transfer_bytes_in_flight,
      :batch_documents
    ]

    if Enum.all?(Map.keys(definition), &(&1 in allowed)) do
      normalized = Stringify.keys(definition)
      endpoint = normalized["endpoint"]

      with true <- normalized["mode"] in ["one_shot", "continuous"],
           true <- normalized["direction"] in ["push", "pull"],
           {:ok, _} <- ReplicationEndpoint.new(endpoint),
           true <- is_boolean(Map.get(normalized, "enabled", false)),
           :ok <- validate_job_options(normalized) do
        {:ok, normalized}
      else
        _ -> {:error, VialKeeper.Error.invalid_request("replication definition is invalid")}
      end
    else
      {:error, VialKeeper.Error.invalid_request("replication definition contains an unknown field")}
    end
  end

  defp normalize_definition(_),
    do: {:error, VialKeeper.Error.invalid_request("replication definition must be an object")}

  defp validate_persistence(false, "continuous"),
    do: {:error, VialKeeper.Error.invalid_request("unpersisted replication must be one_shot")}

  defp validate_persistence(_, _), do: :ok

  defp validate_job_options(definition) do
    batch = Map.get(definition, "batch", %{})
    retry = Map.get(definition, "retry", %{})
    batch_keys = ["documents", "bytes"]
    retry_keys = ["max_attempts", "base_delay_ms", "max_delay_ms", "jitter_ms"]
    max_documents = VialKeeper.Config.host_limits()[:max_replication_batch_documents] || 500
    max_bytes = VialKeeper.Config.host_limits()[:max_replication_batch_bytes] || 16_777_216
    max_attempts = VialKeeper.Config.host_limits()[:max_replication_attempts] || 32
    max_delay = VialKeeper.Config.host_limits()[:max_replication_delay_ms] || 300_000
    max_wait = VialKeeper.Config.host_limits()[:max_wait_ms] || 30_000

    with true <- is_map(batch) and Enum.all?(Map.keys(batch), &(&1 in batch_keys)),
         true <- is_map(retry) and Enum.all?(Map.keys(retry), &(&1 in retry_keys)),
         true <-
           positive_bounded?(
             Map.get(batch, "documents", replication_default(:batch_documents, 100)),
             max_documents
           ),
         true <-
           positive_bounded?(
             Map.get(batch, "bytes", replication_default(:batch_bytes, 4_194_304)),
             max_bytes
           ),
         true <-
           positive_bounded?(
             Map.get(definition, "batch_documents", replication_default(:batch_documents, 100)),
             max_documents
           ),
         :ok <- validate_transfer_job_options(definition),
         true <- positive_bounded?(Map.get(retry, "max_attempts", 8), max_attempts),
         true <- positive_integer?(Map.get(retry, "base_delay_ms", 100)),
         true <- positive_bounded?(Map.get(retry, "max_delay_ms", 30_000), max_delay),
         true <- positive_integer?(Map.get(retry, "jitter_ms", 250)),
         true <-
           Map.get(retry, "max_delay_ms", 30_000) >=
             Map.get(retry, "base_delay_ms", 100),
         true <- positive_bounded?(Map.get(definition, "wait_ms", 1_000), max_wait) do
      :ok
    else
      {:error, _} = error -> error
      _ -> {:error, VialKeeper.Error.invalid_request("replication job options are invalid")}
    end
  end

  defp validate_transfer_job_options(definition) do
    max_chain_fetches =
      VialKeeper.Config.host_limits()[:max_replication_concurrent_chain_fetches] || 32

    max_blob_transfers =
      VialKeeper.Config.host_limits()[:max_replication_concurrent_blob_transfers] || 32

    max_transfer_bytes =
      VialKeeper.Config.host_limits()[:max_replication_transfer_bytes_in_flight] || 4_294_967_296

    with true <-
           positive_bounded?(
             Map.get(
               definition,
               "max_concurrent_chain_fetches",
               replication_default(:max_concurrent_chain_fetches, 4)
             ),
             max_chain_fetches
           ),
         true <-
           positive_bounded?(
             Map.get(
               definition,
               "max_concurrent_blob_transfers",
               replication_default(:max_concurrent_blob_transfers, 4)
             ),
             max_blob_transfers
           ),
         true <-
           positive_bounded?(
             Map.get(
               definition,
               "max_transfer_bytes_in_flight",
               replication_default(:max_transfer_bytes_in_flight, 1_073_741_824)
             ),
             max_transfer_bytes
           ) do
      :ok
    else
      _ -> {:error, VialKeeper.Error.invalid_request("replication job options are invalid")}
    end
  end

  defp positive_integer?(value), do: is_integer(value) and value > 0
  defp positive_bounded?(value, maximum), do: positive_integer?(value) and value <= maximum

  defp runtime_state(table, job_id, enabled) do
    case :ets.lookup(table, job_id) do
      [{^job_id, state, _pid, _uuid, _replication_id, _details} | _] ->
        state

      [{^job_id, state, _pid, _uuid, _replication_id} | _] ->
        state

      [] ->
        if(enabled, do: :idle, else: :disabled)
    end
  end

  defp active_state?(state), do: state in @active_states

  defp ensure_table do
    case :ets.whereis(@table) do
      :undefined -> :ets.new(@table, [:named_table, :public, :set])
      _ -> @table
    end
  end

  defp option(map, key, default) when is_map(map),
    do: MapAccess.get(map, key, default)

  defp option(_map, _key, default), do: default

  defp replication_default(key, default) do
    VialKeeper.Config.defaults()
    |> Map.get("replication", %{})
    |> Map.get(Atom.to_string(key), default)
  end
end
