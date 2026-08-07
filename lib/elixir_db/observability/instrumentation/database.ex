defmodule ElixirDB.Observability.Instrumentation.Database do
  @moduledoc """
  Emitters for Plan §11 database events:

    * `elixir_db.database.open`        — span + counter
    * `elixir_db.database.command`     — span + histogram
    * `elixir_db.database.overload`    — counter only (not a unit of work)

  Instrumentation lives at the service/owner boundary (the catalog and owner),
  never inside the SQLite adapter.
  """

  alias ElixirDB.Observability.{Meters, Tracer}
  alias ElixirDB.Storage.Commands

  @open_span "elixir_db.database.open"
  @command_span "elixir_db.database.command"

  @doc """
  Wraps an open attempt in the `elixir_db.database.open` span. `uuid` is the
  requested database uuid. The outcome (`:ok` | `:rejected`) is set from the
  result; rejected opens (unavailable/in_use) keep span status UNSET — they are
  expected application outcomes, not failures.
  """
  @spec open(binary(), (-> {:ok, term()} | {:error, ElixirDB.Error.t()})) ::
          {:ok, term()} | {:error, ElixirDB.Error.t()}
  def open(uuid, fun) when is_binary(uuid) and is_function(fun, 0) do
    Tracer.with_span(@open_span, [db_uuid: uuid], fn ->
      result = fun.()
      emit_open_outcome(uuid, result)
      result
    end)
  end

  defp emit_open_outcome(uuid, {:ok, _}) do
    Meters.add(:"elixir_db.database.open.count", db_uuid: uuid, outcome: :ok)
    _ = Tracer.set_attributes(db_uuid: uuid, outcome: :ok)
    :ok
  end

  defp emit_open_outcome(uuid, {:error, %ElixirDB.Error{code: code}}) do
    Meters.add(:"elixir_db.database.open.count", db_uuid: uuid, outcome: :rejected)
    _ = Tracer.set_attributes(db_uuid: uuid, outcome: :rejected, error_code: code)
    :ok
  end

  @doc """
  Wraps a database command in the `elixir_db.database.command` span and records
  `elixir_db.database.command.duration`. `command.type` is derived from the
  command struct. Span status follows the §6.5 policy: only `:internal_error`
  sets ERROR; expected domain errors stay UNSET.
  """
  @spec command(binary(), term(), (-> {:ok, term()} | {:error, ElixirDB.Error.t()})) ::
          {:ok, term()} | {:error, ElixirDB.Error.t()}
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

    Meters.record(:"elixir_db.database.command.duration", duration,
      db_uuid: uuid,
      command_type: command_type,
      outcome: outcome
    )

    _ = Tracer.set_attributes(outcome: outcome)
    :ok
  end

  defp emit_command_result(uuid, command_type, duration, {:error, %ElixirDB.Error{} = error}) do
    Meters.record(:"elixir_db.database.command.duration", duration,
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
    Meters.record(:"elixir_db.database.command.duration", duration,
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
    Meters.add(:"elixir_db.database.overload.count", db_uuid: uuid)
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
    normalized = ElixirDB.Storage.Commands.normalize(command)
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
  defp command_type_for_struct(%Commands.ExplainQuery{}), do: :explain_query
  defp command_type_for_struct(%Commands.ListJobs{}), do: :list_jobs
  defp command_type_for_struct(%Commands.PutJob{}), do: :put_job
  defp command_type_for_struct(%Commands.DeleteJob{}), do: :delete_job
  defp command_type_for_struct(%Commands.Close{}), do: :close
  defp command_type_for_struct(_), do: :unknown
end
