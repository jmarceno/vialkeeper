defmodule ElixirDB.Replication.LocalEndpoint do
  @moduledoc "Replication endpoint backed by a local database."

  @behaviour ElixirDB.Replication.Endpoint
  alias ElixirDB.Attachments
  alias ElixirDB.MapAccess
  alias ElixirDB.Runtime.{CommandContext, DatabaseCatalog}
  alias ElixirDB.Storage.LocalRecordRequest

  defstruct [:database_uuid, shadow?: false]
  @type t :: %__MODULE__{database_uuid: binary(), shadow?: boolean()}
  def new(uuid, opts \\ []) when is_binary(uuid) and is_list(opts),
    do: {:ok, %__MODULE__{database_uuid: uuid, shadow?: Keyword.get(opts, :shadow, false)}}

  defp command(%__MODULE__{database_uuid: uuid, shadow?: true}, command, timeout) do
    DatabaseCatalog.command_with_context(
      uuid,
      CommandContext.shadow_replication(shadow_database_uuid: uuid),
      command,
      timeout
    )
  end

  defp command(%__MODULE__{database_uuid: uuid}, command, timeout) do
    DatabaseCatalog.command_as(uuid, :replication, command, timeout)
  end

  @impl true
  def identity(%__MODULE__{} = endpoint),
    do: command(endpoint, {:command, :identity, %{}}, 30_000)

  @impl true
  def has_local_origin_changes?(%__MODULE__{} = endpoint),
    do: command(endpoint, {:command, :has_local_origin_changes}, 30_000)

  @impl true
  def has_local_origin_changes?(%__MODULE__{} = endpoint, peer_database_uuid),
    do: command(endpoint, {:command, :has_local_origin_changes, peer_database_uuid}, 30_000)

  @impl true
  def clear_pending_local_causal(%__MODULE__{} = endpoint),
    do: command(endpoint, {:command, :clear_pending_local_causal}, 30_000)

  @impl true
  def clear_pending_local_causal(%__MODULE__{} = endpoint, peer_database_uuid),
    do: command(endpoint, {:command, :clear_pending_local_causal, peer_database_uuid}, 30_000)

  @impl true
  def read_changes(%__MODULE__{database_uuid: uuid} = endpoint, request),
    do:
      if(MapAccess.get(request, :wait_ms, 0) > 0,
        do: ElixirDB.Changes.wait(uuid, request, admission_class: :replication),
        else: command(endpoint, {:command, :read_changes, request}, 30_000)
      )

  @impl true
  def diff_revisions(%__MODULE__{} = endpoint, request),
    do: command(endpoint, {:command, :diff_revisions, request}, 30_000)

  @impl true
  def get_revision_chains(%__MODULE__{} = endpoint, request),
    do: command(endpoint, {:command, :get_revision_chains, request}, 30_000)

  @impl true
  def import_revision_chains(%__MODULE__{} = endpoint, request),
    do: command(endpoint, {:command, :import_revision_chains, request}, 30_000)

  @impl true
  def confirm_durable_commit(%__MODULE__{} = endpoint, _request) do
    # Import commits with synchronous=EXTRA before returning; confirm the owner is live.
    with {:ok, identity} <- command(endpoint, {:command, :identity, %{}}, 30_000) do
      {:ok,
       %{
         "confirmed" => true,
         "current_sequence" => MapAccess.get(identity, :current_sequence, 0)
       }}
    end
  end

  @impl true
  def get_checkpoint(%__MODULE__{} = endpoint, replication_id),
    do: command(endpoint, {:command, :get_local_record, "checkpoints", replication_id}, 30_000)

  @impl true
  def get_shadow_checkpoint(%__MODULE__{} = endpoint, replication_id),
    do:
      command(endpoint, {:command, :get_local_record, "shadow_checkpoints", replication_id}, 30_000)

  @impl true
  def get_local_record(%__MODULE__{} = endpoint, namespace, key),
    do: command(endpoint, {:command, :get_local_record, namespace, key}, 30_000)

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
        30_000
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
        30_000
      )

  @impl true
  def read_boundary_pages(%__MODULE__{} = endpoint, request),
    do: command(endpoint, {:command, :read_boundary_pages, request}, 30_000)

  @impl true
  def install_boundary_pages(%__MODULE__{} = endpoint, request),
    do: command(endpoint, {:command, :install_boundary_pages, request}, 30_000)

  @impl true
  def put_peer_position(%__MODULE__{} = endpoint, request),
    do: command(endpoint, {:command, :put_peer_position_cas, request}, 30_000)

  @impl true
  def list_peer_positions(%__MODULE__{} = endpoint),
    do: command(endpoint, {:command, :list_peer_positions, %{}}, 30_000)

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
