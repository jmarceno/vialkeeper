defmodule ElixirDB.Runtime.CommandIO do
  @moduledoc """
  Closed read/write/exclusive classification for owner command envelopes.

  This is the IO class used to choose the writer owner versus a snapshot reader.
  It is distinct from admission service class (`foreground`, `subscription`, and
  so on), which only describes scheduling origin.
  """

  alias ElixirDB.Commands

  @type class :: :read | :write | :exclusive

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

  @write MapSet.new([
           Commands.UpdateConfig,
           Commands.PutDocument,
           Commands.CreateDocument,
           Commands.DeleteDocument,
           Commands.ResolveConflict,
           Commands.BulkWrite,
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

  @spec classes() :: %{module() => class()}
  def classes do
    %{}
    |> put_class(@read, :read)
    |> put_class(@write, :write)
    |> put_class(@exclusive, :exclusive)
  end

  @spec classify(struct()) :: class()
  def classify(%module{}) do
    cond do
      module in @read -> :read
      module in @write -> :write
      module in @exclusive -> :exclusive
      true -> raise ArgumentError, "unclassified command #{inspect(module)}"
    end
  end

  defp put_class(acc, modules, class) do
    Enum.reduce(modules, acc, fn module, map -> Map.put(map, module, class) end)
  end
end
