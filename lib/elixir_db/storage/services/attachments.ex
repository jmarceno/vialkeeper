defmodule ElixirDB.Storage.Services.Attachments do
  @moduledoc """
  Shared attachment-metadata orchestration over storage ports.

  Owns request shaping, pending TTL renewal, live-digest union/paging, expiry
  cleanup, and reachability checks. Byte storage remains an independently
  configured attachment store; backends only supply metadata facts through
  `:attachment_metadata`.
  """

  alias ElixirDB.Attachments.{MetadataRequest, Orchestration}
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Access
  alias ElixirDB.Storage.Transaction

  @doc "Resolves an attachment stream ticket from document/revision/name."
  @spec resolve_attachment_ticket(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def resolve_attachment_ticket(%BackendContext{} = context, request) when is_map(request) do
    Access.port(context, :attachment_metadata).resolve_attachment_ticket(context, request)
  end

  @doc "Returns authoritative logical size for a reachable blob digest."
  @spec resolve_blob_metadata(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def resolve_blob_metadata(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, digest} <- MetadataRequest.request_digest(request),
         {:ok, logical_size} <-
           Access.port(context, :attachment_metadata).lookup_reachable_size(context, digest) do
      {:ok, %{digest: digest, logical_size: logical_size, length: logical_size}}
    end
  end

  def resolve_blob_metadata(_context, _request),
    do: {:error, ElixirDB.Error.invalid_request("blob metadata request must be an object")}

  @doc "Inserts or renews pending-blob protection."
  @spec protect_pending_blob(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def protect_pending_blob(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, digest} <- MetadataRequest.request_digest(request),
         {:ok, logical_size} <- MetadataRequest.request_logical_size(request) do
      now = DateTime.utc_now() |> DateTime.truncate(:second)
      row = Orchestration.pending_row(digest, logical_size, now)

      Transaction.run(context, fn tx ->
        Access.port(tx, :attachment_metadata).put_pending_blob(tx, row)
      end)
    end
  end

  def protect_pending_blob(_context, _request),
    do:
      {:error, ElixirDB.Error.invalid_request("pending blob protection request must be an object")}

  @doc "Removes pending protection for one digest or a digest list."
  @spec remove_pending_blob_protection(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def remove_pending_blob_protection(%BackendContext{} = context, request) when is_map(request) do
    digests = MetadataRequest.pending_digests_from_request(request)

    with :ok <- MetadataRequest.validate_digest_list(digests) do
      delete_pending_tx(context, digests)
    end
  end

  def remove_pending_blob_protection(_context, _request),
    do:
      {:error,
       ElixirDB.Error.invalid_request("remove pending blob protection request must be an object")}

  @doc "Pages the union of retained and unexpired pending digests."
  @spec list_live_attachment_digests(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def list_live_attachment_digests(%BackendContext{} = context, request) when is_map(request) do
    after_digest = ElixirDB.MapAccess.get(request, :after_digest)
    limit = ElixirDB.MapAccess.get(request, :limit)
    now = DateTime.utc_now() |> DateTime.truncate(:second)
    port = Access.port(context, :attachment_metadata)

    with {:ok, retained} <- port.list_retained_digests(context),
         {:ok, pending} <- port.list_pending_blobs(context) do
      retained
      |> Orchestration.union_live_digests(pending, now)
      |> Orchestration.page_digests(after_digest, limit)
    end
  end

  def list_live_attachment_digests(_context, _request),
    do: {:error, ElixirDB.Error.invalid_request("live attachment digest request must be an object")}

  @doc "Deletes expired pending-blob protection rows."
  @spec cleanup_expired_pending_blobs(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def cleanup_expired_pending_blobs(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, now} <- MetadataRequest.cleanup_now(request) do
      Transaction.run(context, fn tx -> cleanup_expired_tx(tx, now) end)
    end
  end

  def cleanup_expired_pending_blobs(context, _request),
    do: cleanup_expired_pending_blobs(context, %{})

  @doc "Ensures every digest in a manifest is reachable via pending or retained rows."
  @spec ensure_manifest_reachable(BackendContext.t(), map()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def ensure_manifest_reachable(%BackendContext{} = context, manifest) when is_map(manifest) do
    port = Access.port(context, :attachment_metadata)

    Orchestration.ensure_reachable(manifest, fn digest ->
      port.lookup_reachable_size(context, digest)
    end)
  end

  @doc "Removes pending protection for digests newly referenced by a revision manifest."
  @spec clear_pending_for_manifest(BackendContext.t(), map()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def clear_pending_for_manifest(%BackendContext{} = context, manifest) when is_map(manifest) do
    digests = Orchestration.manifest_digests(manifest)
    Access.port(context, :attachment_metadata).delete_pending_digests(context, digests)
  end

  @doc "Verifies physical attachment digests before import."
  @spec verify_physical_digests(BackendContext.t(), [{binary(), non_neg_integer()}]) ::
          :ok | {:error, ElixirDB.Error.t()}
  def verify_physical_digests(%BackendContext{} = context, digests) when is_list(digests) do
    Access.port(context, :attachment_metadata).verify_physical_digests(context, digests)
  end

  defp delete_pending_tx(context, digests) do
    Transaction.run(context, fn tx ->
      case Access.port(tx, :attachment_metadata).delete_pending_digests(tx, digests) do
        :ok -> {:ok, %{removed: length(digests), digests: digests}}
        {:error, _} = error -> error
      end
    end)
  end

  defp cleanup_expired_tx(tx, now) do
    port = Access.port(tx, :attachment_metadata)

    with {:ok, pending} <- port.list_pending_blobs(tx),
         digests = Orchestration.expired_pending_digests(pending, now),
         :ok <- port.delete_pending_digests(tx, digests) do
      {:ok, %{removed: length(digests)}}
    end
  end
end
