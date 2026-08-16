defmodule VialKeeper.Observability.Instrumentation.AttachmentStore do
  @moduledoc """
  Low-cardinality phase measurements for physical attachment writes.

  Phase metadata is a closed atom vocabulary and never includes paths, digests,
  attachment names, content types, or payload data.
  """

  @phases [
    :begin,
    :logical_hash,
    :compression_probe,
    :payload_write,
    :compression_finalize,
    :digest_finalize,
    :trailer_write,
    :file_sync,
    :file_close,
    :cas_install
  ]

  @typedoc "A bounded physical attachment-write phase."
  @type phase ::
          :begin
          | :logical_hash
          | :compression_probe
          | :payload_write
          | :compression_finalize
          | :digest_finalize
          | :trailer_write
          | :file_sync
          | :file_close
          | :cas_install

  use VialKeeper.Observability.Instrumentation.TimedPhase,
    metric: :"vial_keeper.attachment.store.phase.duration",
    event: [:vial_keeper, :attachment, :store, :phase]
end
