defmodule ElixirDB.Replication.LocalEndpoint do
  @behaviour ElixirDB.Replication.Endpoint
  alias ElixirDB.Runtime.DatabaseCatalog

  defstruct [:database_uuid]
  def new(uuid), do: {:ok, %__MODULE__{database_uuid: uuid}}

  @impl true
  def identity(%__MODULE__{database_uuid: uuid}),
    do: DatabaseCatalog.command(uuid, {:command, :identity, %{}})

  @impl true
  def read_changes(%__MODULE__{database_uuid: uuid}, request),
    do:
      if((request[:wait_ms] || request["wait_ms"] || 0) > 0,
        do: ElixirDB.Changes.wait(uuid, request),
        else: DatabaseCatalog.command(uuid, {:command, :read_changes, request})
      )

  @impl true
  def diff_revisions(%__MODULE__{database_uuid: uuid}, request),
    do: DatabaseCatalog.command(uuid, {:command, :diff_revisions, request})

  @impl true
  def get_revision_chains(%__MODULE__{database_uuid: uuid}, request),
    do: DatabaseCatalog.command(uuid, {:command, :get_revision_chains, request})

  @impl true
  def import_revision_chains(%__MODULE__{database_uuid: uuid}, request),
    do: DatabaseCatalog.command(uuid, {:command, :import_revision_chains, request})

  @impl true
  def confirm_durable_commit(%__MODULE__{database_uuid: uuid}, _request) do
    # Import commits with synchronous=EXTRA before returning; confirm the owner is live.
    with {:ok, identity} <- DatabaseCatalog.command(uuid, {:command, :identity, %{}}) do
      {:ok,
       %{
         "confirmed" => true,
         "current_sequence" => identity[:current_sequence] || identity["current_sequence"] || 0
       }}
    end
  end

  @impl true
  def get_checkpoint(%__MODULE__{database_uuid: uuid}, replication_id),
    do: DatabaseCatalog.command(uuid, {:command, :get_local_record, "checkpoints", replication_id})

  @impl true
  def put_checkpoint(%__MODULE__{database_uuid: uuid}, replication_id, checkpoint),
    do:
      DatabaseCatalog.command(
        uuid,
        {:command, :put_local_record,
         %{
           namespace: "checkpoints",
           key: replication_id,
           expected_version:
             checkpoint["expected_checkpoint_version"] || checkpoint[:expected_checkpoint_version] ||
               0,
           value:
             Map.delete(checkpoint, "expected_checkpoint_version")
             |> Map.delete(:expected_checkpoint_version)
         }}
      )
end
