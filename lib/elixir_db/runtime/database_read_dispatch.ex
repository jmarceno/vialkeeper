defmodule ElixirDB.Runtime.DatabaseReadDispatch do
  @moduledoc """
  Executes classified read commands against an opaque backend context.

  The writer owner and snapshot reader workers share this dispatcher so read
  clauses are not duplicated. Mutation, compaction, and close stay on the owner.
  """

  alias ElixirDB.Commands
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Results
  alias ElixirDB.Storage.Services

  @spec run(BackendContext.t(), struct()) :: term()
  def run(%BackendContext{} = context, %Commands.Identity{}), do: Services.identity(context)

  def run(%BackendContext{} = context, %Commands.GetDocument{request: request}),
    do: wrap_get(Services.get_document(context, request))

  def run(%BackendContext{} = context, %Commands.GetRevision{request: request}),
    do: wrap_get(Services.get_revision(context, request))

  def run(%BackendContext{} = context, %Commands.ReadChanges{request: request}),
    do: wrap_changes(Services.read_changes(context, request))

  def run(%BackendContext{} = context, %Commands.DiffRevisions{request: request}),
    do: Services.diff_revisions(context, request)

  def run(%BackendContext{} = context, %Commands.GetRevisionChains{request: request}),
    do: Services.get_revision_chains(context, request)

  def run(%BackendContext{} = context, %Commands.GetLocalRecord{namespace: namespace, key: key}),
    do: Services.get_local_record(context, namespace, key)

  def run(%BackendContext{} = context, %Commands.GetCheckpoint{replication_id: id}),
    do: Services.get_local_record(context, "checkpoints", id)

  def run(%BackendContext{} = context, %Commands.ListIndexes{}), do: Services.list_indexes(context)

  def run(%BackendContext{} = context, %Commands.ExecuteQuery{request: request}),
    do: Services.execute_public_query(context, request)

  def run(%BackendContext{} = context, %Commands.ExecuteSubscriptionSnapshot{request: request}),
    do: Services.execute_public_subscription_snapshot(context, request)

  def run(%BackendContext{} = context, %Commands.GetRevisionsBatch{requests: requests}),
    do: Services.get_revisions_batch(context, requests)

  def run(%BackendContext{} = context, %Commands.ExplainQuery{request: request}),
    do: Services.explain_public_query(context, request)

  def run(%BackendContext{} = context, %Commands.ListJobs{}),
    do: Services.list_replication_jobs(context)

  def run(%BackendContext{} = context, %Commands.RetentionStatus{}),
    do: Services.retention_state(context)

  def run(%BackendContext{} = context, %Commands.ListPeerPositions{}),
    do: Services.list_peer_positions(context)

  def run(%BackendContext{} = context, %Commands.ReadBoundaryPages{request: request}),
    do: Services.read_boundary_pages(context, request)

  def run(%BackendContext{} = context, %Commands.HasLocalOriginChanges{peer_database_uuid: peer}),
    do: Services.has_local_origin_changes?(context, peer)

  def run(%BackendContext{} = context, %Commands.ResolveAttachmentTicket{request: request}),
    do: Services.resolve_attachment_ticket(context, request)

  def run(%BackendContext{} = context, %Commands.ResolveBlobMetadata{request: request}),
    do: Services.resolve_blob_metadata(context, request)

  def run(%BackendContext{} = context, %Commands.ListViews{}), do: Services.list_views(context)

  def run(%BackendContext{} = context, %Commands.ViewState{view_id: view_id}),
    do: Services.view_state(context, view_id)

  def run(%BackendContext{} = context, %Commands.QueryView{request: request}),
    do: Services.query_view(context, request)

  def run(%BackendContext{} = context, %Commands.ReadWinningDocumentsPage{request: request}),
    do: Services.read_winning_documents_page(context, request)

  def run(%BackendContext{} = context, %Commands.GetDerivedView{}),
    do: Services.get_derived_view(context)

  def run(%BackendContext{} = context, %Commands.ListDerivedSources{}),
    do: Services.list_derived_sources(context)

  def run(%BackendContext{}, command) do
    {:error,
     ElixirDB.Error.invalid_request("unknown database command", %{command: inspect(command)})}
  end

  defp wrap_get({:ok, map}) when is_map(map), do: {:ok, Results.get_document(map)}
  defp wrap_get(other), do: other

  defp wrap_changes({:ok, map}) when is_map(map), do: {:ok, Results.read_changes(map)}
  defp wrap_changes(other), do: other
end
