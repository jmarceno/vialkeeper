defmodule ElixirDB.Replication.RemoteEndpoint do
  @moduledoc "Replication endpoint backed by a remote HTTP server."

  @behaviour ElixirDB.Replication.Endpoint
  alias ElixirDB.Attachments.Manifest
  alias ElixirDB.Domain.ReplicationEndpoint
  alias ElixirDB.MapAccess
  alias ElixirDB.Replication.BlobRepresentationStream
  alias ElixirDB.Replication.RemoteTransport

  defstruct [:base_url, :database_uuid, :auth_token]

  def new(attrs) when is_map(attrs) do
    normalized = Map.new(attrs, fn {key, value} -> {to_string(key), value} end)

    case ReplicationEndpoint.new(Map.put(normalized, "kind", "remote")) do
      {:ok, endpoint} ->
        {:ok,
         %__MODULE__{
           base_url: endpoint.base_url,
           database_uuid: endpoint.database_uuid,
           auth_token: endpoint.auth_token
         }}

      {:error, error} ->
        {:error, error}
    end
  end

  def new(_), do: {:error, ElixirDB.Error.invalid_request("invalid remote endpoint")}

  @impl true
  def identity(endpoint),
    do: call(endpoint, :get, "/v1/databases/#{endpoint.database_uuid}/replication/identity")

  @impl true
  def has_local_origin_changes?(endpoint) do
    has_local_origin_changes?(endpoint, nil)
  end

  @impl true
  def has_local_origin_changes?(endpoint, peer_database_uuid) do
    path = local_origin_path(endpoint, peer_database_uuid)

    case call(endpoint, :get, path) do
      {:ok, %{"has_local_origin_changes" => value}} when is_boolean(value) ->
        {:ok, value}

      {:ok, %{has_local_origin_changes: value}} when is_boolean(value) ->
        {:ok, value}

      other ->
        other
    end
  end

  @impl true
  def clear_pending_local_causal(endpoint), do: clear_pending_local_causal(endpoint, nil)

  @impl true
  def clear_pending_local_causal(endpoint, peer_database_uuid) do
    body =
      if is_binary(peer_database_uuid),
        do: %{"peer_database_uuid" => peer_database_uuid},
        else: %{}

    case call(
           endpoint,
           :post,
           "/v1/databases/#{endpoint.database_uuid}/replication/local-origin/clear",
           body
         ) do
      {:ok, _} -> {:ok, :cleared}
      {:error, _} = error -> error
    end
  end

  defp local_origin_path(endpoint, nil),
    do: "/v1/databases/#{endpoint.database_uuid}/replication/local-origin"

  defp local_origin_path(endpoint, peer_database_uuid),
    do:
      "/v1/databases/#{endpoint.database_uuid}/replication/local-origin?peer_database_uuid=#{URI.encode_www_form(peer_database_uuid)}"

  @impl true
  def read_changes(endpoint, request),
    do:
      call(
        endpoint,
        :post,
        "/v1/databases/#{endpoint.database_uuid}/replication/changes",
        changes_request(request)
      )

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
         "current_sequence" => MapAccess.get(identity, :current_sequence, 0)
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
  def get_shadow_checkpoint(endpoint, replication_id),
    do:
      call(
        endpoint,
        :get,
        "/v1/databases/#{endpoint.database_uuid}/replication/local-records/shadow_checkpoints/#{URI.encode_www_form(replication_id)}"
      )

  @impl true
  def get_local_record(endpoint, namespace, key) do
    path =
      if namespace == "peer_ledger" do
        "/v1/databases/#{endpoint.database_uuid}/replication/peers/#{key}"
      else
        "/v1/databases/#{endpoint.database_uuid}/replication/local-records/#{URI.encode_www_form(namespace)}/#{URI.encode_www_form(key)}"
      end

    call(endpoint, :get, path)
  end

  @impl true
  def put_checkpoint(endpoint, replication_id, checkpoint),
    do:
      call(
        endpoint,
        :put,
        "/v1/databases/#{endpoint.database_uuid}/replication/checkpoints/#{replication_id}",
        checkpoint
      )

  @impl true
  def put_shadow_checkpoint(endpoint, replication_id, checkpoint),
    do:
      call(
        endpoint,
        :put,
        "/v1/databases/#{endpoint.database_uuid}/replication/local-records/shadow_checkpoints/#{URI.encode_www_form(replication_id)}",
        checkpoint
      )

  @impl true
  def read_boundary_pages(endpoint, request),
    do:
      call(
        endpoint,
        :post,
        "/v1/databases/#{endpoint.database_uuid}/replication/boundaries",
        request
      )

  @impl true
  def install_boundary_pages(endpoint, request),
    do:
      call(
        endpoint,
        :post,
        "/v1/databases/#{endpoint.database_uuid}/replication/boundaries/install",
        request
      )

  @impl true
  def put_peer_position(endpoint, request) do
    peer_uuid =
      MapAccess.get(request, :peer_database_uuid) ||
        get_in(request, [:value, :peer_database_uuid]) ||
        get_in(request, ["value", "peer_database_uuid"])

    call(
      endpoint,
      :put,
      "/v1/databases/#{endpoint.database_uuid}/replication/peers/#{peer_uuid}",
      request
    )
  end

  @impl true
  def list_peer_positions(endpoint),
    do: call(endpoint, :get, "/v1/databases/#{endpoint.database_uuid}/replication/peers")

  @impl true
  def diff_blobs(endpoint, digests) when is_list(digests) do
    case call(
           endpoint,
           :post,
           "/v1/databases/#{endpoint.database_uuid}/replication/blobs/diff",
           %{"digests" => digests}
         ) do
      {:ok, missing} when is_list(missing) ->
        {:ok, missing}

      {:ok, %{"missing_digests" => missing}} when is_list(missing) ->
        {:ok, missing}

      {:ok, %{missing_digests: missing}} when is_list(missing) ->
        {:ok, missing}

      {:ok, _} ->
        {:error,
         ElixirDB.Error.database_unavailable("remote endpoint returned an invalid blob diff")}

      {:error, _} = error ->
        error
    end
  end

  def diff_blobs(_endpoint, _digests),
    do: {:error, ElixirDB.Error.invalid_request("blob digests must be a list")}

  @impl true
  def open_blob_representation(endpoint, digest) do
    with {:ok, digest} <- Manifest.validate_digest(digest),
         path = "/v1/databases/#{endpoint.database_uuid}/replication/blobs/#{digest}",
         {:ok, descriptor, body} <-
           RemoteTransport.open_stream(endpoint.base_url, path, digest, endpoint.auth_token) do
      BlobRepresentationStream.new(Map.put(descriptor, :body, body))
    end
  end

  @impl true
  def put_blob_representation(endpoint, %BlobRepresentationStream{} = stream) do
    path = "/v1/databases/#{endpoint.database_uuid}/replication/blobs/#{stream.logical_digest}"
    RemoteTransport.put_stream(endpoint.base_url, path, stream, endpoint.auth_token)
  end

  defp call(endpoint, method, path, body \\ nil) do
    with {:ok, response} <-
           RemoteTransport.request(endpoint.base_url, method, path, body, endpoint.auth_token) do
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

  defp changes_request(%ElixirDB.Changes.Request{since: since, limit: limit, wait_ms: wait_ms}),
    do: %{"since" => since, "limit" => limit, "wait_ms" => wait_ms}

  defp changes_request(request), do: request
end
