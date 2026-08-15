defmodule VialKeeper.Storage.Memory.Inspection do
  @moduledoc """
  Memory inspection port for integrity snapshots and capability probes.

  Builds normalized logical snapshots from the in-memory store so shared
  `VialKeeper.Integrity.Rules` can run without SQL. Physical probes report the
  memory engine only.
  """
  @behaviour VialKeeper.Storage.Ports.Inspection

  alias VialKeeper.Attachments.Orchestration
  alias VialKeeper.Integrity.Rules
  alias VialKeeper.JSON.StrictDecoder
  alias VialKeeper.MapAccess
  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.Memory.{Context, Peers, Store}
  alias VialKeeper.Storage.Ports.Errors
  alias VialKeeper.Storage.Services.Integrity

  @impl true
  def integrity_check(%BackendContext{} = context, options \\ %{}) when is_map(options) do
    Integrity.check(context, options)
  end

  @impl true
  def load_integrity_snapshot(%BackendContext{} = context, _options \\ %{}) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(build_snapshot(Store.get(adapter.store)))
    end
  end

  @impl true
  def physical_integrity_check(%BackendContext{} = _context, _options \\ %{}) do
    {:ok, %{backend_details: %{engine: "memory"}}}
  end

  @impl true
  def capabilities_report, do: %{engine: "memory"}

  @impl true
  def validate_capabilities!, do: :ok

  defp build_snapshot(state) do
    with {:ok, peers} <- Peers.decode(state.peers),
         {:ok, changes} <- decode_changes(state.changes) do
      revisions = flatten_revisions(state)
      revision_attachments = Rules.flatten_revision_attachments(revisions)

      {:ok,
       %{
         meta: snapshot_meta(state.identity),
         boundaries: state.boundaries,
         peers: peers,
         maintenance_counter: state.maintenance_counter,
         documents: snapshot_documents(state),
         revisions: revisions,
         changes: changes,
         pending_blobs: snapshot_pending_blobs(state.pending_blobs),
         checkpoints: snapshot_checkpoints(state.checkpoints),
         revision_attachments: revision_attachments
       }}
    end
  end

  defp snapshot_meta(identity) do
    %{
      database_uuid: Map.fetch!(identity, :database_uuid),
      history_epoch: Map.fetch!(identity, :history_epoch),
      current_sequence: Map.get(identity, :current_sequence, 0),
      retention_floor_sequence: Map.get(identity, :retention_floor_sequence, 0),
      compaction_epoch: Map.get(identity, :compaction_epoch, 0),
      retention_boundary_digest: Map.get(identity, :retention_boundary_digest)
    }
  end

  defp snapshot_documents(state) do
    Enum.map(state.documents, fn {document_id, doc} ->
      %{
        document_id: document_id,
        winning_revision: doc.winning_revision,
        winning_deleted: doc.winning_deleted,
        update_sequence: doc.update_sequence,
        body: doc.body
      }
    end)
  end

  defp flatten_revisions(state) do
    Enum.flat_map(state.revisions, fn {document_id, by_rev} ->
      Enum.map(by_rev, fn {_id, %{revision: revision, is_leaf: is_leaf}} ->
        %{
          document_id: document_id,
          revision_id: revision.revision_id,
          generation: revision.generation,
          parent: revision.parent_revision,
          history_id: revision.history_id,
          digest: revision.digest,
          deleted: revision.deleted,
          body: revision.body,
          attachments: revision.attachments || %{},
          is_leaf: is_leaf
        }
      end)
    end)
  end

  defp decode_changes(changes) do
    Enum.reduce_while(changes, {:ok, []}, fn change, {:ok, acc} ->
      case StrictDecoder.decode(change.leaf_set_json) do
        {:ok, leaves} when is_list(leaves) ->
          {:cont,
           {:ok,
            [
              %{
                sequence: change.sequence,
                document_id: change.document_id,
                winning_revision: change.winning_revision,
                leaf_revisions: leaves
              }
              | acc
            ]}}

        _ ->
          {:halt, {:error, VialKeeper.Error.integrity_violation("change JSON and term differ")}}
      end
    end)
    |> case do
      {:ok, list} -> {:ok, Enum.reverse(list)}
      error -> error
    end
  end

  defp snapshot_pending_blobs(pending) when is_map(pending) do
    Enum.map(pending, fn {digest, meta} ->
      Orchestration.pending_snapshot(
        digest,
        MapAccess.get(meta, :logical_size),
        MapAccess.get(meta, :expires_at),
        MapAccess.get(meta, :updated_at)
      )
    end)
  end

  defp snapshot_checkpoints(checkpoints) when is_map(checkpoints) do
    Enum.map(checkpoints, fn {key, %{version: version, value: value}} ->
      %{key: key, version: version, value: value}
    end)
  end
end
