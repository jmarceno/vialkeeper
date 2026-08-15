defmodule VialKeeper.Replication.Endpoint do
  @moduledoc "Storage-neutral replication endpoint."

  alias VialKeeper.Replication.BlobRepresentationStream

  @callback identity(term()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback read_changes(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback has_local_origin_changes?(term()) ::
              {:ok, boolean()} | {:error, VialKeeper.Error.t()}
  @callback has_local_origin_changes?(term(), binary() | nil) ::
              {:ok, boolean()} | {:error, VialKeeper.Error.t()}
  @callback clear_pending_local_causal(term()) :: {:ok, :cleared} | {:error, VialKeeper.Error.t()}
  @callback clear_pending_local_causal(term(), binary() | nil) ::
              {:ok, :cleared} | {:error, VialKeeper.Error.t()}
  @callback diff_revisions(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback get_revision_chains(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback import_revision_chains(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback confirm_durable_commit(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback get_checkpoint(term(), binary()) :: {:ok, map() | nil} | {:error, VialKeeper.Error.t()}
  @callback get_shadow_checkpoint(term(), binary()) ::
              {:ok, map() | nil} | {:error, VialKeeper.Error.t()}
  @callback get_local_record(term(), binary(), binary()) ::
              {:ok, map() | nil} | {:error, VialKeeper.Error.t()}
  @callback put_checkpoint(term(), binary(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback put_shadow_checkpoint(term(), binary(), map()) ::
              {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback read_boundary_pages(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback install_boundary_pages(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback put_peer_position(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback list_peer_positions(term()) :: {:ok, list()} | {:error, VialKeeper.Error.t()}
  @callback diff_blobs(term(), [binary()]) :: {:ok, [binary()]} | {:error, VialKeeper.Error.t()}
  @callback open_blob_representation(term(), binary()) ::
              {:ok, BlobRepresentationStream.t()} | {:error, VialKeeper.Error.t()}
  @callback put_blob_representation(term(), BlobRepresentationStream.t()) ::
              :ok | {:error, VialKeeper.Error.t()}

  @optional_callbacks get_shadow_checkpoint: 2, put_shadow_checkpoint: 3
end
