defmodule VialKeeper.HTTP.Schemas do
  @moduledoc """
  Central allow-lists for Version 1 JSON request objects (`API-009` / F5).

  Routes pass these through `Request.call/3` → `BodyReader` so unknown top-level
  fields are rejected at a single HTTP boundary.
  """

  @document_get ["id", "revision", "include_conflicts"]
  @document_put ["id", "if_revision", "body", "attachments"]
  @document_delete ["id", "if_revision"]

  @attachment_get ["id", "revision", "name"]
  @attachment_upload []

  @document_resolve [
    "id",
    "document_id",
    "expected_live_revisions",
    "chosen_parent_revision",
    "body",
    "delete_all",
    "attachments"
  ]

  @database_create ["path", "config"]
  @database_register ["path"]
  @index_create ["name", "type", "fields"]

  @query [
    "selector",
    "fields",
    "sort",
    "limit",
    "bookmark",
    "index",
    "search",
    "include_docs",
    "include_conflicts"
  ]

  @changes ["since", "limit", "wait_ms"]
  @changes_stream ["since", "limit", "heartbeat_ms"]
  @query_stream ["query", "heartbeat_ms"]
  @federation_query ["databases", "query"]
  @federation_saved_query_execute ["name", "limit", "bookmark"]

  @replication_job [
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
    "batch_documents"
  ]

  @wire_changes ["since", "limit", "wait_ms"]
  @wire_diff ["documents", "source_database_uuid"]
  @wire_get_chains ["documents", "bootstrap", "cursor", "page_cursor", "limit"]
  @wire_put_chains [
    "chains",
    "purged_boundaries",
    "source_database_uuid",
    "profile",
    "shadow_database_uuid",
    "shadow_generation",
    "operation_id",
    "source_watermark"
  ]
  @wire_checkpoint [
    "expected_checkpoint_version",
    "version",
    "checkpoint_version",
    "replication_id",
    "session_id",
    "source_sequence",
    "history",
    "source_history_epoch",
    "source_compaction_epoch",
    "safe_source_sequence",
    "installed_source_compaction_epoch"
  ]
  @wire_boundaries ["source_history_epoch", "compaction_epoch", "page_cursor", "cursor", "limit"]
  @wire_boundary_install [
    "source_database_uuid",
    "source_history_epoch",
    "compaction_epoch",
    "boundary_digest",
    "next_page",
    "boundaries",
    "install_id",
    "replace"
  ]
  @wire_blob_diff ["digests"]
  @compact_retention []
  @view_create ["name", "selector", "key", "value", "reducer"]
  @view_query [
    "consistency",
    "key",
    "start_key",
    "end_key",
    "inclusive_end",
    "group_level",
    "limit",
    "bookmark"
  ]
  @view_rebuild []
  @materialized_view_create [
    "version",
    "name",
    "sources",
    "map",
    "reduce",
    "group_level",
    "options",
    "enabled"
  ]
  @materialized_view_action []

  def allowed(:document_get), do: @document_get
  def allowed(:document_put), do: @document_put
  def allowed(:document_delete), do: @document_delete
  def allowed(:attachment_get), do: @attachment_get
  def allowed(:attachment_upload), do: @attachment_upload
  def allowed(:document_resolve), do: @document_resolve
  def allowed(:database_create), do: @database_create
  def allowed(:database_register), do: @database_register
  def allowed(:index_create), do: @index_create
  def allowed(:query), do: @query
  def allowed(:changes), do: @changes
  def allowed(:changes_stream), do: @changes_stream
  def allowed(:query_stream), do: @query_stream
  def allowed(:federation_query), do: @federation_query
  def allowed(:federation_saved_query_execute), do: @federation_saved_query_execute
  def allowed(:replication_job), do: @replication_job
  def allowed(:wire_changes), do: @wire_changes
  def allowed(:wire_diff), do: @wire_diff
  def allowed(:wire_get_chains), do: @wire_get_chains
  def allowed(:wire_put_chains), do: @wire_put_chains
  def allowed(:wire_checkpoint), do: @wire_checkpoint
  def allowed(:wire_boundaries), do: @wire_boundaries
  def allowed(:wire_boundary_install), do: @wire_boundary_install
  def allowed(:wire_blob_diff), do: @wire_blob_diff
  def allowed(:compact_retention), do: @compact_retention
  def allowed(:view_create), do: @view_create
  def allowed(:view_query), do: @view_query
  def allowed(:view_rebuild), do: @view_rebuild
  def allowed(:materialized_view_create), do: @materialized_view_create
  def allowed(:materialized_view_action), do: @materialized_view_action

  def opts(schema, message) when is_atom(schema) and is_binary(message) do
    [allowed_fields: allowed(schema), unknown_message: message]
  end
end
