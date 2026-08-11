defmodule ElixirDB.Storage.Memory.AttachmentMetadata do
  @moduledoc "Memory attachment-metadata port for mutation and import guards."
  @behaviour ElixirDB.Storage.Ports.AttachmentMetadata

  alias ElixirDB.MapAccess
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Memory.{Context, Store}
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.RequestValidation

  @impl true
  def resolve_attachment_ticket(%BackendContext{}, _request),
    do:
      {:error, ElixirDB.Error.invalid_request("memory backend attachment tickets are unsupported")}

  @impl true
  def resolve_blob_metadata(%BackendContext{} = context, request) when is_map(request) do
    digest = MapAccess.get(request, :digest)

    with {:ok, adapter} <- Context.unwrap(context) do
      case Map.get(Store.get(adapter.store).pending_blobs, digest) do
        nil ->
          {:error, ElixirDB.Error.attachment_blob_not_found("blob metadata not found")}

        meta ->
          {:ok, meta}
      end
    end
  end

  @impl true
  def protect_pending_blob(%BackendContext{} = context, request) when is_map(request) do
    digest = MapAccess.get(request, :digest)
    logical_size = MapAccess.get(request, :logical_size)

    with {:ok, adapter} <- Context.unwrap(context),
         true <- is_binary(digest),
         true <- is_integer(logical_size) and logical_size >= 0 do
      Store.update(adapter.store, fn state ->
        pending =
          Map.put(state.pending_blobs, digest, %{digest: digest, logical_size: logical_size})

        {:ok, %{state | pending_blobs: pending}, %{digest: digest, logical_size: logical_size}}
      end)
    else
      false -> {:error, ElixirDB.Error.invalid_request("pending blob fields are invalid")}
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def remove_pending_blob_protection(%BackendContext{} = context, request) when is_map(request) do
    digest = MapAccess.get(request, :digest)

    with {:ok, adapter} <- Context.unwrap(context) do
      Store.update(adapter.store, fn state ->
        {:ok, %{state | pending_blobs: Map.delete(state.pending_blobs, digest)}, %{digest: digest}}
      end)
    end
  end

  @impl true
  def list_live_attachment_digests(%BackendContext{}, _request),
    do: {:ok, %{digests: [], continuation_cursor: nil}}

  @impl true
  def cleanup_expired_pending_blobs(%BackendContext{}, _request), do: {:ok, %{removed: 0}}

  @impl true
  def ensure_manifest_reachable(%BackendContext{} = context, manifest) when is_map(manifest) do
    with {:ok, adapter} <- Context.unwrap(context) do
      check_manifest_reachable(Store.get(adapter.store), manifest)
    end
  end

  @impl true
  def clear_pending_for_manifest(%BackendContext{} = context, manifest) when is_map(manifest) do
    with {:ok, adapter} <- Context.unwrap(context) do
      digests =
        manifest
        |> Map.values()
        |> Enum.map(&(MapAccess.get(&1, :digest) || MapAccess.get(&1, "digest")))
        |> Enum.filter(&is_binary/1)

      adapter.store
      |> Store.update(fn state ->
        {:ok, %{state | pending_blobs: Map.drop(state.pending_blobs, digests)}, :ok}
      end)
      |> normalize_ok()
    end
  end

  @impl true
  def verify_physical_digests(%BackendContext{} = context, digests) when is_list(digests) do
    with {:ok, adapter} <- Context.unwrap(context) do
      verify_digests(Store.get(adapter.store), BackendContext.bundle_root(context), digests)
    end
  end

  defp check_manifest_reachable(state, manifest) do
    Enum.reduce_while(manifest, :ok, fn {_name, entry}, :ok ->
      digest = MapAccess.get(entry, :digest) || MapAccess.get(entry, "digest")

      cond do
        not is_binary(digest) ->
          {:halt, {:error, ElixirDB.Error.invalid_request("attachment digest is required")}}

        Map.has_key?(state.pending_blobs, digest) ->
          {:cont, :ok}

        digest_in_revisions?(state, digest) ->
          {:cont, :ok}

        true ->
          {:halt,
           {:error,
            ElixirDB.Error.attachment_blob_not_found("attachment blob is not reachable", %{
              digest: digest
            })}}
      end
    end)
  end

  defp verify_digests(state, root, digests) do
    Enum.reduce_while(digests, :ok, fn {digest, logical_size}, :ok ->
      verify_one_digest(state, root, digest, logical_size)
    end)
  end

  defp verify_one_digest(state, root, digest, logical_size) do
    cond do
      Map.has_key?(state.pending_blobs, digest) ->
        {:cont, :ok}

      digest_in_revisions?(state, digest) ->
        {:cont, :ok}

      is_binary(root) and File.dir?(root) ->
        case RequestValidation.verify_blob(root, digest, logical_size) do
          :ok -> {:cont, :ok}
          {:error, _} = error -> {:halt, error}
        end

      true ->
        {:halt,
         {:error,
          ElixirDB.Error.attachment_blob_not_found(
            "imported revision references a missing attachment blob",
            %{digest: digest}
          )}}
    end
  end

  defp digest_in_revisions?(state, digest) do
    Enum.any?(state.revisions, fn {_doc, by_rev} ->
      Enum.any?(by_rev, fn {_id, %{revision: revision}} ->
        attachment_digest_match?(revision.attachments || %{}, digest)
      end)
    end)
  end

  defp attachment_digest_match?(attachments, digest) do
    Enum.any?(attachments, fn {_name, entry} ->
      (MapAccess.get(entry, :digest) || MapAccess.get(entry, "digest")) == digest
    end)
  end

  defp normalize_ok({:ok, :ok}), do: :ok
  defp normalize_ok({:error, reason}), do: {:error, Errors.normalize(reason)}
end
