defmodule VialKeeper.Storage.Adapter do
  @moduledoc "Engine-neutral persistence boundary."

  @callback create(binary(), map()) :: {:ok, term()} | {:error, VialKeeper.Error.t()}
  @callback open(binary(), map()) :: {:ok, term()} | {:error, VialKeeper.Error.t()}
  @callback close(term()) :: :ok | {:error, VialKeeper.Error.t()}
  @callback identity(term()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback update_config(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback integrity_check(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback get_document(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback get_revision(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback read_changes(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback has_local_origin_changes?(term()) ::
              {:ok, boolean()} | {:error, VialKeeper.Error.t()}
  @callback has_local_origin_changes?(term(), binary() | nil) ::
              {:ok, boolean()} | {:error, VialKeeper.Error.t()}
  @callback clear_pending_local_causal(term()) :: {:ok, :cleared} | {:error, VialKeeper.Error.t()}
  @callback clear_pending_local_causal(term(), binary() | nil) ::
              {:ok, :cleared} | {:error, VialKeeper.Error.t()}
  @callback get_local_record(term(), binary(), binary()) ::
              {:ok, map() | nil} | {:error, VialKeeper.Error.t()}
  @callback put_local_record_cas(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback retention_state(term()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback list_peer_positions(term()) :: {:ok, list()} | {:error, VialKeeper.Error.t()}
  @callback put_peer_position_cas(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback read_boundary_pages(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback install_boundary_pages(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback compact_retention(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback list_replication_jobs(term()) :: {:ok, list()} | {:error, VialKeeper.Error.t()}
  @callback put_replication_job(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback delete_replication_job(term(), binary()) :: :ok | {:error, VialKeeper.Error.t()}
  @callback create_index(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback delete_index(term(), binary()) :: :ok | {:error, VialKeeper.Error.t()}
  @callback rebuild_index(term(), binary()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback list_indexes(term()) :: {:ok, list()} | {:error, VialKeeper.Error.t()}
  @callback execute_query(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback execute_subscription_snapshot(term(), map()) ::
              {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback get_revisions_batch(term(), [map()]) ::
              {:ok, [map()]} | {:error, VialKeeper.Error.t()}
  @callback explain_query(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback resolve_attachment_ticket(term(), map()) ::
              {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback resolve_blob_metadata(term(), map()) ::
              {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback protect_pending_blob(term(), map()) ::
              {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback remove_pending_blob_protection(term(), map()) ::
              {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback list_live_attachment_digests(term(), map()) ::
              {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback cleanup_expired_pending_blobs(term(), map()) ::
              {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback list_views(term()) :: {:ok, list()} | {:error, VialKeeper.Error.t()}
  @callback create_view(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback delete_view(term(), binary()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback view_state(term(), binary()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback apply_view_batch(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback begin_view_rebuild(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback append_view_rebuild_page(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback finish_view_rebuild(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback query_view(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback read_winning_documents_page(term(), map()) ::
              {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback get_derived_view(term()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback set_derived_enabled(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback list_derived_sources(term()) :: {:ok, list()} | {:error, VialKeeper.Error.t()}
  @callback set_derived_source_error(term(), map()) ::
              {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback apply_derived_source_batch(term(), map()) ::
              {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback begin_derived_source_rebuild(term(), map()) ::
              {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback apply_derived_rebuild_page(term(), map()) ::
              {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback prune_derived_rebuild_stale_page(term(), map()) ::
              {:ok, map()} | {:error, VialKeeper.Error.t()}
  @callback finish_derived_source_rebuild(term(), map()) ::
              {:ok, map()} | {:error, VialKeeper.Error.t()}
end
