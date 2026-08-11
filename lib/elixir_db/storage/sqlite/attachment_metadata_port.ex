defmodule ElixirDB.Storage.SQLite.AttachmentMetadataPort do
  @moduledoc """
  SQLite attachment-metadata fact port.
  """
  @behaviour ElixirDB.Storage.Ports.AttachmentMetadata

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.SQLite.{Adapter, Context}

  @impl true
  def resolve_attachment_ticket(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Adapter.resolve_attachment_ticket(adapter, request))
  end

  @impl true
  def resolve_blob_metadata(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Adapter.resolve_blob_metadata(adapter, request))
  end

  @impl true
  def protect_pending_blob(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Adapter.protect_pending_blob(adapter, request))
  end

  @impl true
  def remove_pending_blob_protection(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Adapter.remove_pending_blob_protection(adapter, request))
  end

  @impl true
  def list_live_attachment_digests(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Adapter.list_live_attachment_digests(adapter, request))
  end

  @impl true
  def cleanup_expired_pending_blobs(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Adapter.cleanup_expired_pending_blobs(adapter, request))
  end
end
