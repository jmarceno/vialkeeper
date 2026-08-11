defmodule ElixirDB.Storage.Ports.AttachmentMetadata do
  @moduledoc """
  Attachment metadata fact port: manifests, pending rows, and reachability.

  Byte storage remains an independently configured attachment store. This port
  never returns engine paths or connection handles. Shared live-digest union,
  pending TTL, and reachability orchestration live in
  `ElixirDB.Storage.Services.Attachments`.
  """

  alias ElixirDB.Storage.BackendContext

  @type result(ok) :: {:ok, ok} | {:error, ElixirDB.Error.t()}

  @callback resolve_attachment_ticket(BackendContext.t(), map()) :: result(map())
  @callback lookup_reachable_size(BackendContext.t(), binary()) :: result(non_neg_integer())
  @callback put_pending_blob(BackendContext.t(), map()) :: result(map())
  @callback delete_pending_digests(BackendContext.t(), [binary()]) ::
              :ok | {:error, ElixirDB.Error.t()}
  @callback list_retained_digests(BackendContext.t()) :: result([binary()])
  @callback list_pending_blobs(BackendContext.t()) :: result([map()])
  @callback verify_physical_digests(BackendContext.t(), [{binary(), non_neg_integer()}]) ::
              :ok | {:error, ElixirDB.Error.t()}
end
