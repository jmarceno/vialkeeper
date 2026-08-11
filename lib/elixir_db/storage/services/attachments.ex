defmodule ElixirDB.Storage.Services.Attachments do
  @moduledoc """
  Shared attachment-metadata orchestration over storage ports.

  Owns request shaping for tickets, pending protection, live-digest paging, and
  cleanup. Byte storage remains an independently configured attachment store;
  backends only supply metadata reachability facts through
  `:attachment_metadata`.
  """

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
    Access.port(context, :attachment_metadata).resolve_blob_metadata(context, request)
  end

  @doc "Inserts or renews pending-blob protection."
  @spec protect_pending_blob(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def protect_pending_blob(%BackendContext{} = context, request) when is_map(request) do
    Transaction.run(context, fn tx ->
      Access.port(tx, :attachment_metadata).protect_pending_blob(tx, request)
    end)
  end

  @doc "Removes pending protection for one digest or a digest list."
  @spec remove_pending_blob_protection(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def remove_pending_blob_protection(%BackendContext{} = context, request) when is_map(request) do
    Transaction.run(context, fn tx ->
      Access.port(tx, :attachment_metadata).remove_pending_blob_protection(tx, request)
    end)
  end

  @doc "Pages the union of retained and unexpired pending digests."
  @spec list_live_attachment_digests(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def list_live_attachment_digests(%BackendContext{} = context, request) when is_map(request) do
    Access.port(context, :attachment_metadata).list_live_attachment_digests(context, request)
  end

  @doc "Deletes expired pending-blob protection rows."
  @spec cleanup_expired_pending_blobs(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def cleanup_expired_pending_blobs(%BackendContext{} = context, request) when is_map(request) do
    Transaction.run(context, fn tx ->
      Access.port(tx, :attachment_metadata).cleanup_expired_pending_blobs(tx, request)
    end)
  end
end
