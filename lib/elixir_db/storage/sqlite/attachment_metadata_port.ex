defmodule ElixirDB.Storage.SQLite.AttachmentMetadataPort do
  @moduledoc """
  SQLite attachment-metadata fact port.
  """
  @behaviour ElixirDB.Storage.Ports.AttachmentMetadata

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.RequestValidation
  alias ElixirDB.Storage.SQLite.{Adapter, Attachments, Context}

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

  @impl true
  def ensure_manifest_reachable(%BackendContext{} = context, manifest) when is_map(manifest) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(Attachments.ensure_reachable(adapter.conn, manifest))
    end
  end

  @impl true
  def clear_pending_for_manifest(%BackendContext{} = context, manifest) when is_map(manifest) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(Attachments.remove_pending_for_manifest(adapter.conn, manifest))
    end
  end

  @impl true
  def verify_physical_digests(%BackendContext{} = context, digests) when is_list(digests) do
    with {:ok, adapter} <- Context.unwrap(context) do
      case adapter do
        %{storage_mode: :memory} ->
          :ok

        _ ->
          verify_digests_on_disk(adapter, digests)
      end
    end
  end

  defp verify_digests_on_disk(adapter, digests) do
    root = digest_root(adapter)

    if is_nil(root) do
      :ok
    else
      verify_each_digest(root, digests)
    end
  end

  defp digest_root(%{storage_mode: :memory}), do: nil

  defp digest_root(%{path: path}) when is_binary(path),
    do: Path.dirname(Path.expand(path))

  defp digest_root(_), do: nil

  defp verify_each_digest(root, digests) do
    Enum.reduce_while(digests, :ok, fn {digest, logical_size}, :ok ->
      case RequestValidation.verify_blob(root, digest, logical_size) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end
end
