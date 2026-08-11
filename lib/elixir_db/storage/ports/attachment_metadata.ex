defmodule ElixirDB.Storage.Ports.AttachmentMetadata do
  @moduledoc """
  Attachment metadata port: manifests, pending protection, and reachability.

  Byte storage remains an independently configured attachment store. This port
  never returns engine paths or connection handles.
  """

  alias ElixirDB.Storage.BackendContext

  @type result(ok) :: {:ok, ok} | {:error, ElixirDB.Error.t()}

  @callback resolve_attachment_ticket(BackendContext.t(), map()) :: result(map())
  @callback resolve_blob_metadata(BackendContext.t(), map()) :: result(map())
  @callback protect_pending_blob(BackendContext.t(), map()) :: result(map())
  @callback remove_pending_blob_protection(BackendContext.t(), map()) :: result(map())
  @callback list_live_attachment_digests(BackendContext.t(), map()) :: result(map())
  @callback cleanup_expired_pending_blobs(BackendContext.t(), map()) :: result(map())
end
