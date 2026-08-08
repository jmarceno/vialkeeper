defmodule ElixirDB.Commands do
  @moduledoc """
  Typed command envelopes used at the database-owner boundary.

  Public catalog/document callers may still pass tagged `{:command, ...}` tuples;
  `normalize/1` converts those into these structs before `DatabaseOwner` dispatches.
  """

  defmodule Identity do
    @moduledoc false
    defstruct request: %{}
  end

  defmodule UpdateConfig do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule IntegrityCheck do
    @moduledoc false
    defstruct request: %{}
  end

  defmodule GetDocument do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule GetRevision do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule PutDocument do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule CreateDocument do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule DeleteDocument do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule ResolveConflict do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule BulkWrite do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule ReadChanges do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule DiffRevisions do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule GetRevisionChains do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule ImportRevisionChains do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule GetCheckpoint do
    @moduledoc false
    @enforce_keys [:replication_id]
    defstruct [:replication_id]
  end

  defmodule PutLocalRecord do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule GetLocalRecord do
    @moduledoc false
    @enforce_keys [:namespace, :key]
    defstruct [:namespace, :key]
  end

  defmodule PutCheckpoint do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule ListIndexes do
    @moduledoc false
    defstruct request: %{}
  end

  defmodule CreateIndex do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule DeleteIndex do
    @moduledoc false
    @enforce_keys [:index_id]
    defstruct [:index_id]
  end

  defmodule RebuildIndex do
    @moduledoc false
    @enforce_keys [:index_id]
    defstruct [:index_id]
  end

  defmodule ExecuteQuery do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule ExplainQuery do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule ListJobs do
    @moduledoc false
    defstruct request: %{}
  end

  defmodule PutJob do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule DeleteJob do
    @moduledoc false
    @enforce_keys [:job_id]
    defstruct [:job_id]
  end

  defmodule CompactRetention do
    @moduledoc false
    defstruct request: %{}
  end

  defmodule RetentionStatus do
    @moduledoc false
    defstruct request: %{}
  end

  defmodule ListPeerPositions do
    @moduledoc false
    defstruct request: %{}
  end

  defmodule PutPeerPositionCas do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule ReadBoundaryPages do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule InstallBoundaryPages do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule HasLocalOriginChanges do
    @moduledoc false
    defstruct [:peer_database_uuid]
  end

  defmodule ClearPendingLocalCausal do
    @moduledoc false
    defstruct [:peer_database_uuid]
  end

  defmodule Close do
    @moduledoc false
    defstruct []
  end

  defmodule ResolveAttachmentTicket do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule ResolveBlobMetadata do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule ProtectPendingBlob do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule RemovePendingBlobProtection do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule ListLiveAttachmentDigests do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  defmodule CleanupExpiredPendingBlobs do
    @moduledoc false
    @enforce_keys [:request]
    defstruct [:request]
  end

  @doc "Converts tagged owner tuples and already-normalized structs into command structs."
  @spec normalize(term()) :: struct() | term()
  def normalize(%_{} = command), do: command

  def normalize({:command, :identity, request}), do: %Identity{request: request || %{}}
  def normalize({:command, :update_config, request}), do: %UpdateConfig{request: request}
  def normalize({:command, :integrity_check, request}), do: %IntegrityCheck{request: request || %{}}
  def normalize({:command, :get_document, request}), do: %GetDocument{request: request}
  def normalize({:command, :get_revision, request}), do: %GetRevision{request: request}
  def normalize({:command, :put, request}), do: %PutDocument{request: request}
  def normalize({:command, :delete, request}), do: %DeleteDocument{request: request}
  def normalize({:command, :bulk_write, request}), do: %BulkWrite{request: request}
  def normalize({:command, :resolve, request}), do: %ResolveConflict{request: request}
  def normalize({:command, :read_changes, request}), do: %ReadChanges{request: request}
  def normalize({:command, :diff_revisions, request}), do: %DiffRevisions{request: request}

  def normalize({:command, :get_revision_chains, request}),
    do: %GetRevisionChains{request: request}

  def normalize({:command, :import_revision_chains, request}),
    do: %ImportRevisionChains{request: request}

  def normalize({:command, :get_local_record, "checkpoints", replication_id}),
    do: %GetCheckpoint{replication_id: replication_id}

  def normalize({:command, :get_local_record, namespace, key}),
    do: %GetLocalRecord{namespace: namespace, key: key}

  def normalize({:command, :put_local_record, %{namespace: "checkpoints"} = request}),
    do: %PutCheckpoint{request: request}

  def normalize({:command, :put_local_record, %{"namespace" => "checkpoints"} = request}),
    do: %PutCheckpoint{request: request}

  def normalize({:command, :put_local_record, request}), do: %PutLocalRecord{request: request}
  def normalize({:command, :list_indexes, request}), do: %ListIndexes{request: request || %{}}
  def normalize({:command, :create_index, request}), do: %CreateIndex{request: request}
  def normalize({:command, :delete_index, index_id}), do: %DeleteIndex{index_id: index_id}
  def normalize({:command, :rebuild_index, index_id}), do: %RebuildIndex{index_id: index_id}
  def normalize({:command, :query, request}), do: %ExecuteQuery{request: request}
  def normalize({:command, :explain_query, request}), do: %ExplainQuery{request: request}
  def normalize({:command, :list_jobs, request}), do: %ListJobs{request: request || %{}}
  def normalize({:command, :put_job, request}), do: %PutJob{request: request}
  def normalize({:command, :delete_job, job_id}), do: %DeleteJob{job_id: job_id}

  def normalize({:command, :compact_retention, request}),
    do: %CompactRetention{request: request || %{}}

  def normalize({:command, :compact, request}), do: %CompactRetention{request: request || %{}}

  def normalize({:command, :retention_status, request}),
    do: %RetentionStatus{request: request || %{}}

  def normalize({:command, :get_retention_state, request}),
    do: %RetentionStatus{request: request || %{}}

  def normalize({:command, :list_peer_positions, request}),
    do: %ListPeerPositions{request: request || %{}}

  def normalize({:command, :put_peer_position_cas, request}),
    do: %PutPeerPositionCas{request: request}

  def normalize({:command, :read_boundary_pages, request}),
    do: %ReadBoundaryPages{request: request}

  def normalize({:command, :install_boundary_pages, request}),
    do: %InstallBoundaryPages{request: request}

  def normalize({:command, :has_local_origin_changes}), do: %HasLocalOriginChanges{}

  def normalize({:command, :has_local_origin_changes, peer_database_uuid}),
    do: %HasLocalOriginChanges{peer_database_uuid: peer_database_uuid}

  def normalize({:command, :clear_pending_local_causal}), do: %ClearPendingLocalCausal{}

  def normalize({:command, :clear_pending_local_causal, peer_database_uuid}),
    do: %ClearPendingLocalCausal{peer_database_uuid: peer_database_uuid}

  def normalize({:command, :resolve_attachment_ticket, request}),
    do: %ResolveAttachmentTicket{request: request}

  def normalize({:command, :resolve_blob_metadata, request}),
    do: %ResolveBlobMetadata{request: request}

  def normalize({:command, :protect_pending_blob, request}),
    do: %ProtectPendingBlob{request: request}

  def normalize({:command, :remove_pending_blob_protection, request}),
    do: %RemovePendingBlobProtection{request: request}

  def normalize({:command, :list_live_attachment_digests, request}),
    do: %ListLiveAttachmentDigests{request: request}

  def normalize({:command, :cleanup_expired_pending_blobs, request}),
    do: %CleanupExpiredPendingBlobs{request: request}

  def normalize({:command, :close}), do: %Close{}
  def normalize(other), do: other
end
