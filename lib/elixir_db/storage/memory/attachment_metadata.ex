defmodule ElixirDB.Storage.Memory.AttachmentMetadata do
  @moduledoc """
  Memory attachment-metadata fact port.

  Tracks pending protection and revision attachment reachability in the
  in-memory store. Live-digest union, pending TTL, and reachability policy are
  owned by `ElixirDB.Storage.Services.Attachments`.
  """
  @behaviour ElixirDB.Storage.Ports.AttachmentMetadata

  alias ElixirDB.Attachments.{Manifest, MetadataRequest, Orchestration, Ticket}
  alias ElixirDB.MapAccess
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Memory.{Context, Store}
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.RequestValidation

  @impl true
  def resolve_attachment_ticket(%BackendContext{} = context, request) when is_map(request) do
    document_id = MapAccess.get(request, :document_id) || MapAccess.get(request, :id)
    revision_id = MapAccess.get(request, :revision_id) || MapAccess.get(request, :revision)
    name = MapAccess.get(request, :attachment_name) || MapAccess.get(request, :name)

    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, name} <- Manifest.validate_name(name),
         {:ok, doc} <- require_document(Store.get(adapter.store), document_id),
         {:ok, resolved_revision} <- MetadataRequest.resolve_revision_id(doc, revision_id),
         {:ok, entry} <-
           load_attachment_entry(Store.get(adapter.store), document_id, resolved_revision, name) do
      identity = Store.identity(adapter.store)
      bundle_root = BackendContext.bundle_root(context) || adapter.root

      Ticket.build(
        identity.database_uuid,
        bundle_root,
        MapAccess.get(entry, :digest),
        MapAccess.get(entry, :length) || MapAccess.get(entry, :logical_size),
        MapAccess.get(entry, :content_type),
        document_id,
        resolved_revision,
        name
      )
    end
  end

  def resolve_attachment_ticket(_context, _request),
    do: {:error, ElixirDB.Error.invalid_request("attachment ticket request must be an object")}

  @impl true
  def lookup_reachable_size(%BackendContext{} = context, digest) when is_binary(digest) do
    with {:ok, adapter} <- Context.unwrap(context) do
      size_for_digest(Store.get(adapter.store), digest)
    end
  end

  @impl true
  def put_pending_blob(%BackendContext{} = context, row) when is_map(row) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, digest} <- require_digest(row),
         {:ok, logical_size} <- require_logical_size(row) do
      meta = %{
        digest: digest,
        logical_size: logical_size,
        length: logical_size,
        expires_at: MapAccess.get(row, :expires_at),
        updated_at: MapAccess.get(row, :updated_at)
      }

      update_pending(adapter.store, fn pending ->
        {Map.put(pending, digest, meta), meta}
      end)
    end
  end

  @impl true
  def delete_pending_digests(%BackendContext{} = context, digests) when is_list(digests) do
    with {:ok, adapter} <- Context.unwrap(context),
         :ok <- MetadataRequest.validate_digest_list(digests) do
      drop_pending(adapter.store, digests)
    end
  end

  @impl true
  def list_retained_digests(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      {:ok, retained_digests(Store.get(adapter.store))}
    end
  end

  @impl true
  def list_pending_blobs(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      pending =
        Store.get(adapter.store).pending_blobs
        |> Enum.map(fn {digest, meta} ->
          %{
            digest: digest,
            logical_size: MapAccess.get(meta, :logical_size),
            expires_at: MapAccess.get(meta, :expires_at),
            updated_at: MapAccess.get(meta, :updated_at)
          }
        end)

      {:ok, pending}
    end
  end

  @impl true
  def verify_physical_digests(%BackendContext{} = context, digests) when is_list(digests) do
    with {:ok, adapter} <- Context.unwrap(context) do
      verify_digests(Store.get(adapter.store), BackendContext.bundle_root(context), digests)
    end
  end

  defp drop_pending(store, digests) do
    case update_pending(store, fn pending -> {Map.drop(pending, digests), :ok} end) do
      {:ok, :ok} -> :ok
      {:error, _} = error -> error
    end
  end

  defp require_digest(row) do
    case MapAccess.get(row, :digest) do
      digest when is_binary(digest) -> Manifest.validate_digest(digest)
      _ -> {:error, ElixirDB.Error.invalid_request("pending blob digest is required")}
    end
  end

  defp require_logical_size(row) do
    size = MapAccess.get(row, :logical_size) || MapAccess.get(row, :length)

    if is_integer(size) and size >= 0,
      do: {:ok, size},
      else: {:error, ElixirDB.Error.invalid_request("logical_size must be a non-negative integer")}
  end

  defp update_pending(store, fun) when is_function(fun, 1) do
    Store.update(store, fn state ->
      {pending, result} = fun.(state.pending_blobs)
      {:ok, %{state | pending_blobs: pending}, result}
    end)
  end

  defp require_document(state, document_id) when is_binary(document_id) and document_id != "" do
    case Store.find_document(state, document_id) do
      nil -> {:error, ElixirDB.Error.document_not_found("document does not exist")}
      doc -> {:ok, doc}
    end
  end

  defp require_document(_state, _document_id),
    do: {:error, ElixirDB.Error.invalid_request("document id must be a non-empty UTF-8 string")}

  defp load_attachment_entry(state, document_id, revision_id, name) do
    case Store.find_revision(state, document_id, revision_id) do
      {:ok, revision} ->
        attachments = revision.attachments || %{}

        case Map.get(attachments, name) || Map.get(attachments, to_string(name)) do
          nil ->
            {:error, ElixirDB.Error.attachment_not_found("attachment not found")}

          entry ->
            {:ok, entry}
        end

      {:error, %ElixirDB.Error{code: :revision_not_found} = error} ->
        {:error, error}

      {:error, error} ->
        {:error, Errors.normalize(error)}
    end
  end

  defp size_for_digest(state, digest) do
    now = DateTime.utc_now() |> DateTime.truncate(:second)

    case Map.get(state.pending_blobs, digest) do
      meta when is_map(meta) ->
        if Orchestration.pending_unexpired?(meta, now) do
          {:ok, MapAccess.get(meta, :logical_size)}
        else
          lookup_revision_digest_size(state, digest)
        end

      _ ->
        lookup_revision_digest_size(state, digest)
    end
  end

  defp lookup_revision_digest_size(state, digest) do
    case find_revision_digest(state, digest) do
      {:ok, size} ->
        {:ok, size}

      :error ->
        {:error, ElixirDB.Error.attachment_blob_not_found("attachment blob not found")}
    end
  end

  defp find_revision_digest(state, digest) do
    Enum.find_value(state.revisions, :error, &find_digest_in_document(&1, digest))
  end

  defp find_digest_in_document({_doc, by_rev}, digest) do
    Enum.find_value(by_rev, &find_digest_in_revision(&1, digest))
  end

  defp find_digest_in_revision({_id, %{revision: revision}}, digest) do
    (revision.attachments || %{})
    |> Enum.find_value(&digest_size_if_match(&1, digest))
  end

  defp digest_size_if_match({_name, entry}, digest) do
    if (MapAccess.get(entry, :digest) || MapAccess.get(entry, "digest")) == digest do
      {:ok, MapAccess.get(entry, :length) || MapAccess.get(entry, :logical_size)}
    end
  end

  defp retained_digests(state) do
    Enum.flat_map(state.revisions, fn {_doc, by_rev} ->
      Enum.flat_map(by_rev, fn {_id, %{revision: revision}} ->
        (revision.attachments || %{})
        |> Map.values()
        |> Enum.map(&(MapAccess.get(&1, :digest) || MapAccess.get(&1, "digest")))
        |> Enum.filter(&is_binary/1)
      end)
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
end
