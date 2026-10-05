defmodule VialKeeper.Runtime.CommandIO do
  @moduledoc """
  Closed read/concurrent-write/write/exclusive classification for owner
  command envelopes.

  This is the IO class used to choose a snapshot reader, a writer slot, or
  the writer owner. `:concurrent_write` commands (document writes) may run in
  parallel in the writer pool; `:write` commands run serially on the owner
  behind a writer-pool barrier; `:exclusive` commands also pause readers.
  It is distinct from admission service class (`foreground`, `subscription`, and
  so on), which only describes scheduling origin.
  """

  alias VialKeeper.Commands

  @type class :: :read | :concurrent_write | :write | :exclusive

  @read MapSet.new([
          Commands.Identity,
          Commands.GetDocument,
          Commands.GetRevision,
          Commands.ReadChanges,
          Commands.DiffRevisions,
          Commands.GetRevisionChains,
          Commands.GetCheckpoint,
          Commands.GetLocalRecord,
          Commands.ListIndexes,
          Commands.ExecuteQuery,
          Commands.ExecuteSubscriptionSnapshot,
          Commands.GetRevisionsBatch,
          Commands.ExplainQuery,
          Commands.ListJobs,
          Commands.RetentionStatus,
          Commands.ListPeerPositions,
          Commands.ReadBoundaryPages,
          Commands.HasLocalOriginChanges,
          Commands.ResolveAttachmentTicket,
          Commands.ResolveBlobMetadata,
          Commands.ListViews,
          Commands.ViewState,
          Commands.QueryView,
          Commands.ReadWinningDocumentsPage,
          Commands.GetDerivedView,
          Commands.ListDerivedSources
        ])

  @concurrent_write MapSet.new([
                      Commands.PutDocument,
                      Commands.CreateDocument,
                      Commands.DeleteDocument,
                      Commands.ResolveConflict,
                      Commands.BulkWrite
                    ])

  @write MapSet.new([
           Commands.UpdateConfig,
           Commands.ImportRevisionChains,
           Commands.PutLocalRecord,
           Commands.PutCheckpoint,
           Commands.CreateIndex,
           Commands.DeleteIndex,
           Commands.PutJob,
           Commands.DeleteJob,
           Commands.PutPeerPositionCas,
           Commands.InstallBoundaryPages,
           Commands.ClearPendingLocalCausal,
           Commands.ProtectPendingBlob,
           Commands.ProtectPendingBlobs,
           Commands.RemovePendingBlobProtection,
           Commands.CreateView,
           Commands.DeleteView,
           Commands.ApplyViewBatch,
           Commands.BeginViewRebuild,
           Commands.AppendViewRebuildPage,
           Commands.FinishViewRebuild,
           Commands.SetDerivedEnabled,
           Commands.SetDerivedSourceError,
           Commands.ApplyDerivedSourceBatch,
           Commands.BeginDerivedSourceRebuild,
           Commands.ApplyDerivedRebuildPage,
           Commands.PruneDerivedRebuildStalePage,
           Commands.FinishDerivedSourceRebuild
         ])

  @exclusive MapSet.new([
               Commands.IntegrityCheck,
               Commands.RebuildIndex,
               Commands.CompactRetention,
               Commands.CleanupExpiredPendingBlobs,
               Commands.ListLiveAttachmentDigests,
               Commands.Close
             ])

  @classes @read
           |> Map.new(&{&1, :read})
           |> Map.merge(Map.new(@concurrent_write, &{&1, :concurrent_write}))
           |> Map.merge(Map.new(@write, &{&1, :write}))
           |> Map.merge(Map.new(@exclusive, &{&1, :exclusive}))

  @spec classes() :: %{module() => class()}
  def classes, do: @classes

  @spec classify(struct()) :: class()
  def classify(%module{}) do
    cond do
      module in @read -> :read
      module in @concurrent_write -> :concurrent_write
      module in @write -> :write
      module in @exclusive -> :exclusive
      true -> raise ArgumentError, "unclassified command #{inspect(module)}"
    end
  end
end
