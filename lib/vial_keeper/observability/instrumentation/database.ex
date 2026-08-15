defmodule VialKeeper.Observability.Instrumentation.Database do
  @moduledoc """
  Emitters for database events:

    * `vial_keeper.database.open`        — span + counter
    * `vial_keeper.database.command`     — span + histogram
    * `vial_keeper.database.overload`    — counter only (not a unit of work)
    * `vial_keeper.database.admission.wait` — histogram (queue wait, not owner work)
    * `vial_keeper.database.read_pool.quiesce.duration` — histogram (exclusive drain)

  Instrumentation lives at the service/owner boundary (the catalog and owner),
  never inside a physical storage adapter.
  """

  alias VialKeeper.Commands
  alias VialKeeper.Observability.{Meters, Tracer}

  @open_span "vial_keeper.database.open"
  @command_span "vial_keeper.database.command"

  @doc """
  Wraps an open attempt in the `vial_keeper.database.open` span. `uuid` is the
  requested database uuid. The outcome (`:ok` | `:rejected`) is set from the
  result; rejected opens (unavailable/in_use) keep span status UNSET — they are
  expected application outcomes, not failures.
  """
  @spec open(binary(), (-> {:ok, term()} | {:error, VialKeeper.Error.t()})) ::
          {:ok, term()} | {:error, VialKeeper.Error.t()}
  def open(uuid, fun) when is_binary(uuid) and is_function(fun, 0) do
    Tracer.with_span(@open_span, [db_uuid: uuid], fn ->
      result = fun.()
      emit_open_outcome(uuid, result)
      result
    end)
  end

  defp emit_open_outcome(uuid, {:ok, _}) do
    Meters.add(:"vial_keeper.database.open.count", db_uuid: uuid, outcome: :ok)
    _ = Tracer.set_attributes(db_uuid: uuid, outcome: :ok)
    :ok
  end

  defp emit_open_outcome(uuid, {:error, %VialKeeper.Error{code: code}}) do
    Meters.add(:"vial_keeper.database.open.count", db_uuid: uuid, outcome: :rejected)
    _ = Tracer.set_attributes(db_uuid: uuid, outcome: :rejected, error_code: code)
    :ok
  end

  @doc """
  Wraps a database command in the `vial_keeper.database.command` span and records
  `vial_keeper.database.command.duration`. `command.type` is derived from the
  command struct. Span status follows the observability error policy: only
  `:internal_error` sets ERROR; expected domain errors stay UNSET.
  """
  @spec command(binary(), term(), (-> {:ok, term()} | {:error, VialKeeper.Error.t()})) ::
          {:ok, term()} | {:error, VialKeeper.Error.t()}
  def command(uuid, command, fun) when is_binary(uuid) and is_function(fun, 0) do
    command_type = command_type(command)
    started = System.monotonic_time()

    Tracer.with_span(@command_span, [db_uuid: uuid, command_type: command_type], fn ->
      result = fun.()
      duration = System.monotonic_time() - started

      emit_command_result(uuid, command_type, duration, result)
      result
    end)
  end

  defp emit_command_result(uuid, command_type, duration, {:ok, value}) do
    outcome = command_outcome(value)

    Meters.record(:"vial_keeper.database.command.duration", duration,
      db_uuid: uuid,
      command_type: command_type,
      outcome: outcome
    )

    _ = Tracer.set_attributes(outcome: outcome)
    :ok
  end

  defp emit_command_result(uuid, command_type, duration, {:error, %VialKeeper.Error{} = error}) do
    Meters.record(:"vial_keeper.database.command.duration", duration,
      db_uuid: uuid,
      command_type: command_type,
      error_code: error.code
    )

    _ = Tracer.record_error(error)
    _ = Tracer.apply_error_status(error)
    :ok
  end

  # Commands that acknowledge with a bare :ok (e.g. deletes) or other success
  # shapes. Treat as a normal :ok outcome.
  defp emit_command_result(uuid, command_type, duration, _other) do
    Meters.record(:"vial_keeper.database.command.duration", duration,
      db_uuid: uuid,
      command_type: command_type,
      outcome: :ok
    )

    :ok
  end

  # TX-006: a normal acknowledged mutation is :ok; an idempotent replay that
  # matched an existing identical revision is :replayed.
  defp command_outcome(%{replayed: true}), do: :replayed
  defp command_outcome(_), do: :ok

  @doc "Increments the overload counter for `uuid`. No span (overload is not work)."
  @spec overload(binary()) :: :ok
  def overload(uuid) when is_binary(uuid) do
    Meters.add(:"vial_keeper.database.overload.count", db_uuid: uuid)
  end

  @admission_outcomes [:granted, :rejected, :cancelled, :closed]

  defguardp valid_wait_measurement(
              uuid,
              class,
              outcome,
              duration,
              queue_depth_at_enqueue,
              queue_depth_at_grant
            )
            when is_binary(uuid) and is_atom(class) and outcome in @admission_outcomes and
                   is_integer(duration) and duration >= 0 and
                   is_integer(queue_depth_at_enqueue) and queue_depth_at_enqueue >= 0 and
                   is_integer(queue_depth_at_grant) and queue_depth_at_grant >= 0

  @doc """
  Records `vial_keeper.database.admission.wait` for one admission-queue outcome.

  `duration` is a native monotonic-time delta. Depth attributes are bounded by
  the host admission limit.
  """
  @spec admission_wait(
          binary(),
          atom(),
          :granted | :rejected | :cancelled | :closed,
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: :ok
  def admission_wait(uuid, class, outcome, duration, queue_depth_at_enqueue, queue_depth_at_grant)
      when valid_wait_measurement(
             uuid,
             class,
             outcome,
             duration,
             queue_depth_at_enqueue,
             queue_depth_at_grant
           ) do
    Meters.record(:"vial_keeper.database.admission.wait", duration,
      db_uuid: uuid,
      admission_class: class,
      outcome: outcome,
      queue_depth_at_enqueue: queue_depth_at_enqueue,
      queue_depth_at_grant: queue_depth_at_grant
    )
  end

  @doc """
  Records `vial_keeper.database.read_pool.quiesce.duration` for one exclusive drain.

  `duration` is a native monotonic-time delta. No document ids or payloads.
  """
  @spec read_pool_quiesce(binary(), non_neg_integer()) :: :ok
  def read_pool_quiesce(uuid, duration)
      when is_binary(uuid) and is_integer(duration) and duration >= 0 do
    Meters.record(:"vial_keeper.database.read_pool.quiesce.duration", duration, db_uuid: uuid)
  end

  @doc """
  Records `vial_keeper.database.read_pool.wait` for one read-pool queue outcome.

  `duration` is a native monotonic-time delta. Depth attributes are bounded by
  the host read queue limit.
  """
  @spec read_pool_wait(
          binary(),
          atom(),
          :granted | :rejected | :cancelled | :closed,
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: :ok
  def read_pool_wait(uuid, class, outcome, duration, queue_depth_at_enqueue, queue_depth_at_grant)
      when valid_wait_measurement(
             uuid,
             class,
             outcome,
             duration,
             queue_depth_at_enqueue,
             queue_depth_at_grant
           ) do
    Meters.record(:"vial_keeper.database.read_pool.wait", duration,
      db_uuid: uuid,
      admission_class: class,
      outcome: outcome,
      queue_depth_at_enqueue: queue_depth_at_enqueue,
      queue_depth_at_grant: queue_depth_at_grant
    )
  end

  @doc "Adjusts `vial_keeper.database.read_pool.active` (running snapshots) by `delta`."
  @spec read_pool_active(binary(), integer()) :: :ok
  def read_pool_active(uuid, delta) when is_binary(uuid) and is_integer(delta) do
    Meters.updown_add(:"vial_keeper.database.read_pool.active", delta, db_uuid: uuid)
  end

  @doc "Adjusts `vial_keeper.database.read_pool.queued` (waiting reads) by `delta`."
  @spec read_pool_queued(binary(), integer()) :: :ok
  def read_pool_queued(uuid, delta) when is_binary(uuid) and is_integer(delta) do
    Meters.updown_add(:"vial_keeper.database.read_pool.queued", delta, db_uuid: uuid)
  end

  @doc """
  Maps a command (tagged tuple or struct) to a stable `command.type` atom.

  Public for tests and for `Instrumentation.Query` (which needs the index/query
  type from the command).
  """
  @spec command_type(term()) :: atom()
  def command_type(command) do
    # Normalize tagged tuples to their struct form so the type matches the
    # owner's dispatch (Commands.normalize/1 is the single source of truth).
    normalized = Commands.normalize(command)
    command_type_for_struct(normalized)
  end

  defp command_type_for_struct(%Commands.Identity{}), do: :identity
  defp command_type_for_struct(%Commands.UpdateConfig{}), do: :update_config
  defp command_type_for_struct(%Commands.IntegrityCheck{}), do: :integrity_check
  defp command_type_for_struct(%Commands.GetDocument{}), do: :get
  defp command_type_for_struct(%Commands.GetRevision{}), do: :get_revision
  defp command_type_for_struct(%Commands.PutDocument{}), do: :put
  defp command_type_for_struct(%Commands.DeleteDocument{}), do: :delete
  defp command_type_for_struct(%Commands.ResolveConflict{}), do: :resolve
  defp command_type_for_struct(%Commands.BulkWrite{}), do: :bulk_write
  defp command_type_for_struct(%Commands.ReadChanges{}), do: :read_changes
  defp command_type_for_struct(%Commands.DiffRevisions{}), do: :diff_revisions
  defp command_type_for_struct(%Commands.GetRevisionChains{}), do: :get_revision_chains
  defp command_type_for_struct(%Commands.ImportRevisionChains{}), do: :import_revision_chains
  defp command_type_for_struct(%Commands.GetCheckpoint{}), do: :get_checkpoint
  defp command_type_for_struct(%Commands.PutCheckpoint{}), do: :put_checkpoint
  defp command_type_for_struct(%Commands.GetLocalRecord{}), do: :get_local_record
  defp command_type_for_struct(%Commands.PutLocalRecord{}), do: :put_local_record
  defp command_type_for_struct(%Commands.ListIndexes{}), do: :list_indexes
  defp command_type_for_struct(%Commands.CreateIndex{}), do: :create_index
  defp command_type_for_struct(%Commands.DeleteIndex{}), do: :delete_index
  defp command_type_for_struct(%Commands.RebuildIndex{}), do: :rebuild_index
  defp command_type_for_struct(%Commands.ExecuteQuery{}), do: :query

  defp command_type_for_struct(%Commands.ExecuteSubscriptionSnapshot{}),
    do: :execute_subscription_snapshot

  defp command_type_for_struct(%Commands.GetRevisionsBatch{}), do: :get_revisions_batch
  defp command_type_for_struct(%Commands.ExplainQuery{}), do: :explain_query
  defp command_type_for_struct(%Commands.ListJobs{}), do: :list_jobs
  defp command_type_for_struct(%Commands.PutJob{}), do: :put_job
  defp command_type_for_struct(%Commands.DeleteJob{}), do: :delete_job
  defp command_type_for_struct(%Commands.CompactRetention{}), do: :compact_retention
  defp command_type_for_struct(%Commands.RetentionStatus{}), do: :retention_status
  defp command_type_for_struct(%Commands.ListPeerPositions{}), do: :list_peer_positions
  defp command_type_for_struct(%Commands.PutPeerPositionCas{}), do: :put_peer_position_cas
  defp command_type_for_struct(%Commands.ReadBoundaryPages{}), do: :read_boundary_pages
  defp command_type_for_struct(%Commands.InstallBoundaryPages{}), do: :install_boundary_pages
  defp command_type_for_struct(%Commands.Close{}), do: :close
  defp command_type_for_struct(_), do: :unknown
end
