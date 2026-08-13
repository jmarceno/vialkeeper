defmodule ElixirDB.Replication.LocalEndpoint do
  @moduledoc "Replication endpoint backed by a local database."

  @behaviour ElixirDB.Replication.Endpoint
  alias ElixirDB.Attachments
  alias ElixirDB.MapAccess
  alias ElixirDB.Runtime.DatabaseCatalog

  defstruct [:database_uuid]
  @type t :: %__MODULE__{database_uuid: binary()}
  def new(uuid), do: {:ok, %__MODULE__{database_uuid: uuid}}

  defp command(uuid, command, timeout \\ 30_000) do
    DatabaseCatalog.command_as(uuid, :replication, command, timeout)
  end

  @impl true
  def identity(%__MODULE__{database_uuid: uuid}),
    do: command(uuid, {:command, :identity, %{}})

  @impl true
  def has_local_origin_changes?(%__MODULE__{database_uuid: uuid}),
    do: command(uuid, {:command, :has_local_origin_changes})

  @impl true
  def has_local_origin_changes?(%__MODULE__{database_uuid: uuid}, peer_database_uuid),
    do: command(uuid, {:command, :has_local_origin_changes, peer_database_uuid})

  @impl true
  def clear_pending_local_causal(%__MODULE__{database_uuid: uuid}),
    do: command(uuid, {:command, :clear_pending_local_causal})

  @impl true
  def clear_pending_local_causal(%__MODULE__{database_uuid: uuid}, peer_database_uuid),
    do: command(uuid, {:command, :clear_pending_local_causal, peer_database_uuid})

  @impl true
  def read_changes(%__MODULE__{database_uuid: uuid}, request),
    do:
      if(MapAccess.get(request, :wait_ms, 0) > 0,
        do: ElixirDB.Changes.wait(uuid, request, admission_class: :replication),
        else: command(uuid, {:command, :read_changes, request})
      )

  @impl true
  def diff_revisions(%__MODULE__{database_uuid: uuid}, request),
    do: command(uuid, {:command, :diff_revisions, request})

  @impl true
  def get_revision_chains(%__MODULE__{database_uuid: uuid}, request),
    do: command(uuid, {:command, :get_revision_chains, request})

  @impl true
  def import_revision_chains(%__MODULE__{database_uuid: uuid}, request),
    do: command(uuid, {:command, :import_revision_chains, request})

  @impl true
  def confirm_durable_commit(%__MODULE__{database_uuid: uuid}, _request) do
    # Import commits with synchronous=EXTRA before returning; confirm the owner is live.
    with {:ok, identity} <- command(uuid, {:command, :identity, %{}}) do
      {:ok,
       %{
         "confirmed" => true,
         "current_sequence" => MapAccess.get(identity, :current_sequence, 0)
       }}
    end
  end

  @impl true
  def get_checkpoint(%__MODULE__{database_uuid: uuid}, replication_id),
    do: command(uuid, {:command, :get_local_record, "checkpoints", replication_id})

  @impl true
  def get_shadow_checkpoint(%__MODULE__{database_uuid: uuid}, replication_id),
    do: command(uuid, {:command, :get_local_record, "shadow_checkpoints", replication_id})

  @impl true
  def get_local_record(%__MODULE__{database_uuid: uuid}, namespace, key),
    do: command(uuid, {:command, :get_local_record, namespace, key})

  @impl true
  def put_checkpoint(%__MODULE__{database_uuid: uuid}, replication_id, checkpoint),
    do:
      command(
        uuid,
        {:command, :put_local_record,
         %{
           namespace: "checkpoints",
           key: replication_id,
           expected_version: MapAccess.get(checkpoint, :expected_checkpoint_version, 0),
           value:
             Map.delete(checkpoint, "expected_checkpoint_version")
             |> Map.delete(:expected_checkpoint_version)
         }}
      )

  @impl true
  def put_shadow_checkpoint(%__MODULE__{database_uuid: uuid}, replication_id, checkpoint),
    do:
      command(
        uuid,
        {:command, :put_local_record,
         %{
           namespace: "shadow_checkpoints",
           key: replication_id,
           expected_version: MapAccess.get(checkpoint, :expected_checkpoint_version, 0),
           value:
             Map.delete(checkpoint, "expected_checkpoint_version")
             |> Map.delete(:expected_checkpoint_version)
         }}
      )

  @impl true
  def read_boundary_pages(%__MODULE__{database_uuid: uuid}, request),
    do: command(uuid, {:command, :read_boundary_pages, request})

  @impl true
  def install_boundary_pages(%__MODULE__{database_uuid: uuid}, request),
    do: command(uuid, {:command, :install_boundary_pages, request})

  @impl true
  def put_peer_position(%__MODULE__{database_uuid: uuid}, request),
    do: command(uuid, {:command, :put_peer_position_cas, request})

  @impl true
  def list_peer_positions(%__MODULE__{database_uuid: uuid}),
    do: command(uuid, {:command, :list_peer_positions, %{}})

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
