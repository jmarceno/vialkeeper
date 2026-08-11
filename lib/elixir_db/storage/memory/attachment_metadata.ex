defmodule ElixirDB.Storage.Memory.AttachmentMetadata do
  @moduledoc """
  Memory attachment-metadata port for pending protection, live digests, and
  attachment tickets.

  Byte storage remains on the filesystem under the bundle root; this port only
  tracks reachability metadata in the in-memory store.
  """
  @behaviour ElixirDB.Storage.Ports.AttachmentMetadata

  alias ElixirDB.Attachments.{Manifest, MetadataRequest, Ticket}
  alias ElixirDB.MapAccess
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Memory.{Context, Store}
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.RequestValidation

  @pending_ttl_seconds 86_400
  @live_digest_page_size 256

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
  def resolve_blob_metadata(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, digest} <- MetadataRequest.request_digest(request),
         {:ok, logical_size} <- lookup_reachable_size(Store.get(adapter.store), digest) do
      {:ok, %{digest: digest, logical_size: logical_size, length: logical_size}}
    end
  end

  def resolve_blob_metadata(_context, _request),
    do: {:error, ElixirDB.Error.invalid_request("blob metadata request must be an object")}

  @impl true
  def protect_pending_blob(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, digest} <- MetadataRequest.request_digest(request),
         {:ok, logical_size} <- MetadataRequest.request_logical_size(request) do
      now = DateTime.utc_now() |> DateTime.truncate(:second)
      expires_at = DateTime.add(now, @pending_ttl_seconds, :second)
      now_iso = DateTime.to_iso8601(now)
      expires_iso = DateTime.to_iso8601(expires_at)

      meta = %{
        digest: digest,
        logical_size: logical_size,
        length: logical_size,
        expires_at: expires_iso,
        updated_at: now_iso
      }

      update_pending(adapter.store, fn pending ->
        {Map.put(pending, digest, meta), meta}
      end)
    end
  end

  def protect_pending_blob(_context, _request),
    do:
      {:error, ElixirDB.Error.invalid_request("pending blob protection request must be an object")}

  @impl true
  def remove_pending_blob_protection(%BackendContext{} = context, request) when is_map(request) do
    digests = MetadataRequest.pending_digests_from_request(request)

    with {:ok, adapter} <- Context.unwrap(context),
         :ok <- MetadataRequest.validate_digest_list(digests) do
      update_pending(adapter.store, fn pending ->
        {Map.drop(pending, digests), %{removed: length(digests), digests: digests}}
      end)
    end
  end

  def remove_pending_blob_protection(_context, _request),
    do:
      {:error,
       ElixirDB.Error.invalid_request("remove pending blob protection request must be an object")}

  @impl true
  def list_live_attachment_digests(%BackendContext{} = context, request) when is_map(request) do
    after_digest = MapAccess.get(request, :after_digest)
    limit = MetadataRequest.page_limit(MapAccess.get(request, :limit), @live_digest_page_size)
    now = DateTime.utc_now() |> DateTime.truncate(:second)

    with {:ok, adapter} <- Context.unwrap(context),
         :ok <- MetadataRequest.validate_after_digest(after_digest) do
      digests =
        Store.get(adapter.store)
        |> live_digests(now)
        |> Enum.sort()
        |> drop_after(after_digest)

      page = Enum.take(digests, limit)

      next_after =
        if length(digests) > limit do
          List.last(page)
        else
          nil
        end

      {:ok, %{digests: page, next_after_digest: next_after}}
    end
  end

  def list_live_attachment_digests(_context, _request),
    do: {:error, ElixirDB.Error.invalid_request("live attachment digest request must be an object")}

  @impl true
  def cleanup_expired_pending_blobs(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, now} <- MetadataRequest.cleanup_now(request) do
      update_pending(adapter.store, &split_expired_pending(&1, now))
    end
  end

  def cleanup_expired_pending_blobs(context, _request),
    do: cleanup_expired_pending_blobs(context, %{})

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
      |> update_pending(fn pending -> {Map.drop(pending, digests), :ok} end)
      |> normalize_ok()
    end
  end

  @impl true
  def verify_physical_digests(%BackendContext{} = context, digests) when is_list(digests) do
    with {:ok, adapter} <- Context.unwrap(context) do
      verify_digests(Store.get(adapter.store), BackendContext.bundle_root(context), digests)
    end
  end

  defp split_expired_pending(pending, now) do
    {kept, removed} = Enum.split_with(pending, &pending_entry_unexpired?(&1, now))
    {Map.new(kept), %{removed: length(removed)}}
  end

  defp pending_entry_unexpired?({_digest, meta}, now), do: pending_unexpired?(meta, now)

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

  defp lookup_reachable_size(state, digest) do
    now = DateTime.utc_now() |> DateTime.truncate(:second)

    case Map.get(state.pending_blobs, digest) do
      meta when is_map(meta) ->
        if pending_unexpired?(meta, now) do
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

  defp live_digests(state, now) do
    retained =
      Enum.flat_map(state.revisions, fn {_doc, by_rev} ->
        Enum.flat_map(by_rev, fn {_id, %{revision: revision}} ->
          (revision.attachments || %{})
          |> Map.values()
          |> Enum.map(&(MapAccess.get(&1, :digest) || MapAccess.get(&1, "digest")))
          |> Enum.filter(&is_binary/1)
        end)
      end)

    pending =
      state.pending_blobs
      |> Enum.filter(fn {_digest, meta} -> pending_unexpired?(meta, now) end)
      |> Enum.map(fn {digest, _} -> digest end)

    Enum.uniq(retained ++ pending)
  end

  defp pending_unexpired?(meta, now) do
    case MapAccess.get(meta, :expires_at) do
      expires when is_binary(expires) ->
        case DateTime.from_iso8601(expires) do
          {:ok, expires_dt, _} -> DateTime.compare(expires_dt, now) == :gt
          _ -> false
        end

      _ ->
        false
    end
  end

  defp drop_after(digests, nil), do: digests

  defp drop_after(digests, after_digest) when is_binary(after_digest) do
    Enum.drop_while(digests, &(&1 <= after_digest))
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
