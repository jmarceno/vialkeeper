defmodule ElixirDB.HTTP.Schemas do
  @moduledoc """
  Central allow-lists for Version 1 JSON request objects (`API-009` / F5).

  Routes pass these through `Request.call/3` → `BodyReader` so unknown top-level
  fields are rejected at a single HTTP boundary.
  """

  @document_get ["id", "revision", "include_conflicts"]
  @document_put ["id", "if_revision", "body"]
  @document_delete ["id", "if_revision"]

  @document_resolve [
    "id",
    "document_id",
    "expected_live_revisions",
    "chosen_parent_revision",
    "body",
    "delete_all"
  ]

  @database_create ["path", "config"]
  @database_register ["path"]
  @index_create ["name", "type", "fields", "tokenization"]

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

  @replication_job [
    "persist",
    "mode",
    "direction",
    "endpoint",
    "enabled",
    "batch",
    "retry",
    "wait_ms"
  ]

  @wire_changes ["since", "limit", "wait_ms"]
  @wire_diff ["documents"]
  @wire_get_chains ["documents"]
  @wire_put_chains ["chains"]
  @wire_checkpoint [
    "expected_checkpoint_version",
    "version",
    "checkpoint_version",
    "replication_id",
    "session_id",
    "source_sequence",
    "history"
  ]

  def allowed(:document_get), do: @document_get
  def allowed(:document_put), do: @document_put
  def allowed(:document_delete), do: @document_delete
  def allowed(:document_resolve), do: @document_resolve
  def allowed(:database_create), do: @database_create
  def allowed(:database_register), do: @database_register
  def allowed(:index_create), do: @index_create
  def allowed(:query), do: @query
  def allowed(:changes), do: @changes
  def allowed(:changes_stream), do: @changes_stream
  def allowed(:replication_job), do: @replication_job
  def allowed(:wire_changes), do: @wire_changes
  def allowed(:wire_diff), do: @wire_diff
  def allowed(:wire_get_chains), do: @wire_get_chains
  def allowed(:wire_put_chains), do: @wire_put_chains
  def allowed(:wire_checkpoint), do: @wire_checkpoint

  def opts(schema, message) when is_atom(schema) and is_binary(message) do
    [allowed_fields: allowed(schema), unknown_message: message]
  end
end
