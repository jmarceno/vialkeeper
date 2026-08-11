defmodule ElixirDB.Storage.SQLite.AttachmentMetadataPort do
  @moduledoc """
  SQLite attachment-metadata fact port.

  Calls `ElixirDB.Storage.SQLite.Attachments` SQL helpers for pending rows and
  retained digests. Live-digest union, pending TTL, and reachability policy are
  owned by `ElixirDB.Storage.Services.Attachments`.
  """
  @behaviour ElixirDB.Storage.Ports.AttachmentMetadata

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.RequestValidation
  alias ElixirDB.Storage.SQLite.{Attachments, Context}

  @impl true
  def resolve_attachment_ticket(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Attachments.resolve_attachment_ticket(adapter, request))
  end

  @impl true
  def lookup_reachable_size(%BackendContext{} = context, digest) when is_binary(digest) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Attachments.lookup_reachable_size(adapter.conn, digest))
  end

  @impl true
  def put_pending_blob(%BackendContext{} = context, row) when is_map(row) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Attachments.put_pending_blob(adapter.conn, row))
  end

  @impl true
  def delete_pending_digests(%BackendContext{} = context, digests) when is_list(digests) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Attachments.delete_pending_digests(adapter.conn, digests))
  end

  @impl true
  def list_retained_digests(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Attachments.list_retained_digests(adapter.conn))
  end

  @impl true
  def list_pending_blobs(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context),
         do: Errors.wrap(Attachments.list_pending_blobs(adapter.conn))
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
