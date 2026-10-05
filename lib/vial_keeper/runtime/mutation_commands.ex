defmodule VialKeeper.Runtime.MutationCommands do
  @moduledoc """
  Runs the sequence-producing write commands for `DatabaseOwner` and
  `WriterSlot`.

  Each command runs with its mutation instrumentation, the derived-database
  write guard and a sequence reservation sized by
  `VialKeeper.Runtime.WriteKeys.sequence_count/1`. `finish/4` then waits until
  the write is visible in the changes feed, so a successful write response
  always implies the feed includes it.
  """

  alias VialKeeper.Commands
  alias VialKeeper.Deadline
  alias VialKeeper.Error
  alias VialKeeper.MapAccess
  alias VialKeeper.Observability.Instrumentation.Mutation
  alias VialKeeper.Runtime.{SequenceLedger, WriteKeys}
  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.Results
  alias VialKeeper.Storage.Services

  @commands [
    Commands.PutDocument,
    Commands.DeleteDocument,
    Commands.ResolveConflict,
    Commands.BulkWrite,
    Commands.ImportRevisionChains,
    Commands.ApplyDerivedSourceBatch,
    Commands.ApplyDerivedRebuildPage,
    Commands.PruneDerivedRebuildStalePage
  ]

  @typedoc "A command result and the highest sequence the command used (0 for none)."
  @type outcome :: {term(), non_neg_integer()}

  @doc "True when `command` is a write command this module executes."
  @spec handles?(struct()) :: boolean()
  def handles?(%module{}), do: module in @commands

  @doc "Runs one write command against `context` for database `uuid`."
  @spec execute(struct(), BackendContext.t(), binary(), Deadline.t()) :: outcome()
  def execute(%Commands.PutDocument{request: request} = command, context, uuid, deadline),
    do:
      local(command, context, uuid, deadline, fn ->
        wrap_put(Services.apply_local_mutation(context, Map.put(request, :operation, :put)))
      end)

  def execute(%Commands.DeleteDocument{request: request} = command, context, uuid, deadline),
    do:
      local(command, context, uuid, deadline, fn ->
        wrap_put(Services.apply_local_mutation(context, Map.put(request, :operation, :delete)))
      end)

  def execute(%Commands.BulkWrite{request: request} = command, context, uuid, deadline),
    do:
      local(command, context, uuid, deadline, fn ->
        Services.apply_bulk_mutation(context, request)
      end)

  def execute(%Commands.ResolveConflict{request: request} = command, context, uuid, deadline),
    do:
      local(command, context, uuid, deadline, fn ->
        Services.resolve_conflict(context, request)
      end)

  def execute(%Commands.ImportRevisionChains{request: request} = command, context, uuid, deadline),
    do:
      guarded(command, context, uuid, deadline, fn ->
        Services.import_revision_chains(context, request)
      end)

  def execute(
        %Commands.ApplyDerivedSourceBatch{request: request} = command,
        context,
        uuid,
        deadline
      ),
      do:
        reserved(command, uuid, deadline, fn ->
          Services.apply_derived_source_batch(context, request)
        end)

  def execute(
        %Commands.ApplyDerivedRebuildPage{request: request} = command,
        context,
        uuid,
        deadline
      ),
      do:
        reserved(command, uuid, deadline, fn ->
          Services.apply_derived_rebuild_page(context, request)
        end)

  def execute(
        %Commands.PruneDerivedRebuildStalePage{request: request} = command,
        context,
        uuid,
        deadline
      ),
      do:
        reserved(command, uuid, deadline, fn ->
          Services.prune_derived_rebuild_stale_page(context, request)
        end)

  @doc """
  Waits until a successful write is visible in the changes feed, then
  returns its result.

  It waits for the highest sequence the write used and the highest sequence
  its result reports: a replayed write reports the sequence of an earlier
  write, which has committed but may not be visible yet. The wait is measured
  as the mutation's `:change_notifier` phase, because the ledger publishes
  the change when the write becomes visible.
  """
  @spec finish(outcome(), struct(), binary(), Deadline.t()) :: term()
  def finish({{:ok, value} = result, max_used}, command, uuid, deadline) do
    case max(max_used, result_sequence(value)) do
      0 ->
        result

      sequence ->
        await = fn -> SequenceLedger.await_visible(uuid, sequence, deadline) end

        _ =
          case operation(command) do
            operation when operation in [nil, :import] -> await.()
            operation -> Mutation.phase(operation, :change_notifier, await)
          end

        result
    end
  end

  def finish({result, _max_used}, _command, _uuid, _deadline), do: result

  defp result_sequence(values) when is_list(values),
    do: Enum.reduce(values, 0, &max(result_sequence(&1), &2))

  defp result_sequence(%{sequence: sequence}) when is_integer(sequence), do: sequence
  defp result_sequence(%{last_sequence: sequence}) when is_integer(sequence), do: sequence
  defp result_sequence(_value), do: 0

  @doc "Returns the mutation instrumentation operation of a command, if any."
  @spec operation(struct()) :: :put | :delete | :resolve | :bulk_write | :import | nil
  def operation(%Commands.PutDocument{}), do: :put
  def operation(%Commands.DeleteDocument{}), do: :delete
  def operation(%Commands.ResolveConflict{}), do: :resolve
  def operation(%Commands.BulkWrite{}), do: :bulk_write
  def operation(%Commands.ImportRevisionChains{}), do: :import
  def operation(_command), do: nil

  defp local(command, context, uuid, deadline, fun),
    do:
      Mutation.with_operation(operation(command), fn ->
        guarded(command, context, uuid, deadline, fun)
      end)

  defp guarded(command, %BackendContext{identity: identity}, uuid, deadline, fun) do
    case MapAccess.get(identity, :database_kind, :ordinary) do
      :derived ->
        {{:error,
          Error.derived_database_read_only(
            "derived databases accept writes only from their materializer"
          )}, 0}

      _writable ->
        reserved(command, uuid, deadline, fun)
    end
  end

  defp reserved(command, uuid, deadline, fun),
    do: SequenceLedger.with_reservation(uuid, WriteKeys.sequence_count(command), deadline, fun)

  defp wrap_put({:ok, map}) when is_map(map), do: {:ok, Results.put_document(map)}
  defp wrap_put(other), do: other
end
