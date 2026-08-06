defmodule ElixirDB.Replication.RemoteEndpoint do
  @behaviour ElixirDB.Replication.Endpoint
  alias ElixirDB.Replication.RemoteTransport

  defstruct [:base_url, :database_uuid]

  def new(attrs) when is_map(attrs) do
    normalized = Map.new(attrs, fn {key, value} -> {to_string(key), value} end)

    case ElixirDB.Domain.ReplicationEndpoint.new(Map.put(normalized, "kind", "remote")) do
      {:ok, endpoint} ->
        {:ok, %__MODULE__{base_url: endpoint.base_url, database_uuid: endpoint.database_uuid}}

      {:error, error} ->
        {:error, error}
    end
  end

  def new(_), do: {:error, ElixirDB.Error.invalid_request("invalid remote endpoint")}

  @impl true
  def identity(endpoint),
    do: call(endpoint, :get, "/v1/databases/#{endpoint.database_uuid}/replication/identity")

  @impl true
  def read_changes(endpoint, request),
    do:
      call(endpoint, :post, "/v1/databases/#{endpoint.database_uuid}/replication/changes", request)

  @impl true
  def diff_revisions(endpoint, request),
    do:
      call(
        endpoint,
        :post,
        "/v1/databases/#{endpoint.database_uuid}/replication/revisions/diff",
        request
      )

  @impl true
  def get_revision_chains(endpoint, request),
    do:
      call(
        endpoint,
        :post,
        "/v1/databases/#{endpoint.database_uuid}/replication/revisions/get",
        request
      )

  @impl true
  def import_revision_chains(endpoint, request),
    do:
      call(
        endpoint,
        :post,
        "/v1/databases/#{endpoint.database_uuid}/replication/revisions/put",
        request
      )

  @impl true
  def confirm_durable_commit(endpoint, _request) do
    # Remote import responses already imply durable commit; confirm with a live identity round-trip.
    with {:ok, identity} <- identity(endpoint) do
      {:ok,
       %{
         "confirmed" => true,
         "current_sequence" => identity["current_sequence"] || identity[:current_sequence] || 0
       }}
    end
  end

  @impl true
  def get_checkpoint(endpoint, replication_id),
    do:
      call(
        endpoint,
        :get,
        "/v1/databases/#{endpoint.database_uuid}/replication/checkpoints/#{replication_id}"
      )

  @impl true
  def put_checkpoint(endpoint, replication_id, checkpoint),
    do:
      call(
        endpoint,
        :put,
        "/v1/databases/#{endpoint.database_uuid}/replication/checkpoints/#{replication_id}",
        checkpoint
      )

  defp call(endpoint, method, path, body \\ nil) do
    with {:ok, response} <- RemoteTransport.request(endpoint.base_url, method, path, body) do
      case response do
        %{"data" => data} ->
          {:ok, data}

        data when is_map(data) ->
          {:ok, data}

        _ ->
          {:error,
           ElixirDB.Error.database_unavailable("remote endpoint returned an invalid response")}
      end
    end
  end
end
