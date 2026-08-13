defmodule ElixirDB.Replication.LocalEndpoint do
  @moduledoc "Replication endpoint backed by a local database."

  @behaviour ElixirDB.Replication.Endpoint
  alias ElixirDB.Attachments
  alias ElixirDB.MapAccess
  alias ElixirDB.Runtime.{CommandContext, DatabaseCatalog}
  alias ElixirDB.Storage.LocalRecordRequest

  defstruct [
    :database_uuid,
    :source_database_uuid,
    :generation,
    :operation_id,
    shadow?: false
  ]

  @type t :: %__MODULE__{
          database_uuid: binary(),
          shadow?: boolean(),
          source_database_uuid: binary() | nil,
          generation: non_neg_integer() | nil,
          operation_id: binary() | nil
        }

  def new(uuid, opts \\ []) when is_binary(uuid) and is_list(opts) do
    {:ok,
     %__MODULE__{
       database_uuid: uuid,
       shadow?: Keyword.get(opts, :shadow, false),
       source_database_uuid: Keyword.get(opts, :source_database_uuid),
       generation: Keyword.get(opts, :generation),
       operation_id: Keyword.get(opts, :operation_id)
     }}
  end

  defp command(%__MODULE__{shadow?: true} = endpoint, command, timeout) do
    DatabaseCatalog.command_with_context(
      endpoint.database_uuid,
      shadow_context(endpoint),
      command,
      timeout
    )
  end

  defp command(%__MODULE__{database_uuid: uuid}, command, timeout) do
    DatabaseCatalog.command_as(uuid, :replication, command, timeout)
  end

  defp shadow_context(%__MODULE__{} = endpoint) do
    CommandContext.shadow_replication(
      source_database_uuid: endpoint.source_database_uuid,
      shadow_database_uuid: endpoint.database_uuid,
      generation: endpoint.generation,
      operation_id: endpoint.operation_id
    )
  end

  defp timeout, do: ElixirDB.Config.request_timeout_ms()

  @impl true
  def identity(%__MODULE__{} = endpoint),
    do: command(endpoint, {:command, :identity, %{}}, timeout())

  @impl true
  def has_local_origin_changes?(%__MODULE__{} = endpoint),
    do: command(endpoint, {:command, :has_local_origin_changes}, timeout())

  @impl true
  def has_local_origin_changes?(%__MODULE__{} = endpoint, peer_database_uuid),
    do: command(endpoint, {:command, :has_local_origin_changes, peer_database_uuid}, timeout())

  @impl true
  def clear_pending_local_causal(%__MODULE__{} = endpoint),
    do: command(endpoint, {:command, :clear_pending_local_causal}, timeout())

  @impl true
  def clear_pending_local_causal(%__MODULE__{} = endpoint, peer_database_uuid),
    do: command(endpoint, {:command, :clear_pending_local_causal, peer_database_uuid}, timeout())

  @impl true
  def read_changes(%__MODULE__{database_uuid: uuid} = endpoint, request),
    do:
      if(MapAccess.get(request, :wait_ms, 0) > 0,
        do: ElixirDB.Changes.wait(uuid, request, admission_class: :replication),
        else: command(endpoint, {:command, :read_changes, request}, timeout())
      )

  @impl true
  def diff_revisions(%__MODULE__{} = endpoint, request),
    do: command(endpoint, {:command, :diff_revisions, request}, timeout())

  @impl true
  def get_revision_chains(%__MODULE__{} = endpoint, request),
    do: command(endpoint, {:command, :get_revision_chains, request}, timeout())

  @impl true
  def import_revision_chains(%__MODULE__{} = endpoint, request),
    do: command(endpoint, {:command, :import_revision_chains, request}, timeout())

  @impl true
  def confirm_durable_commit(%__MODULE__{} = endpoint, _request) do
    with {:ok, identity} <- command(endpoint, {:command, :identity, %{}}, timeout()) do
      {:ok,
       %{
         "confirmed" => true,
         "current_sequence" => MapAccess.get(identity, :current_sequence, 0)
       }}
    end
  end

  @impl true
  def get_checkpoint(%__MODULE__{} = endpoint, replication_id),
    do: command(endpoint, {:command, :get_local_record, "checkpoints", replication_id}, timeout())

  @impl true
  def get_shadow_checkpoint(%__MODULE__{} = endpoint, replication_id),
    do:
      command(
        endpoint,
        {:command, :get_local_record, "shadow_checkpoints", replication_id},
        timeout()
      )

  @impl true
  def get_local_record(%__MODULE__{} = endpoint, namespace, key),
    do: command(endpoint, {:command, :get_local_record, namespace, key}, timeout())

  @impl true
  def put_checkpoint(%__MODULE__{} = endpoint, replication_id, checkpoint),
    do:
      command(
        endpoint,
        {:command, :put_local_record,
         LocalRecordRequest.new(
           "checkpoints",
           replication_id,
           MapAccess.get(checkpoint, :expected_checkpoint_version, 0),
           Map.delete(checkpoint, "expected_checkpoint_version")
           |> Map.delete(:expected_checkpoint_version)
         )},
        timeout()
      )

  @impl true
  def put_shadow_checkpoint(%__MODULE__{} = endpoint, replication_id, checkpoint),
    do:
      command(
        endpoint,
        {:command, :put_local_record,
         LocalRecordRequest.new(
           "shadow_checkpoints",
           replication_id,
           MapAccess.get(checkpoint, :expected_checkpoint_version, 0),
           Map.delete(checkpoint, "expected_checkpoint_version")
           |> Map.delete(:expected_checkpoint_version)
         )},
        timeout()
      )

  @impl true
  def read_boundary_pages(%__MODULE__{} = endpoint, request),
    do: command(endpoint, {:command, :read_boundary_pages, request}, timeout())

  @impl true
  def install_boundary_pages(%__MODULE__{} = endpoint, request),
    do: command(endpoint, {:command, :install_boundary_pages, request}, timeout())

  @impl true
  def put_peer_position(%__MODULE__{} = endpoint, request),
    do: command(endpoint, {:command, :put_peer_position_cas, request}, timeout())

  @impl true
  def list_peer_positions(%__MODULE__{} = endpoint),
    do: command(endpoint, {:command, :list_peer_positions, %{}}, timeout())

  @impl true
  def diff_blobs(%__MODULE__{database_uuid: uuid}, digests),
    do: Attachments.diff_blobs(uuid, digests, admission_class: :replication)

  @impl true
  def open_blob_representation(%__MODULE__{database_uuid: uuid}, digest),
    do: Attachments.open_blob_representation(uuid, digest, admission_class: :replication)

  @impl true
  def put_blob_representation(%__MODULE__{database_uuid: uuid}, stream),
    do: Attachments.put_blob_representation(uuid, stream, admission_class: :replication)
end
