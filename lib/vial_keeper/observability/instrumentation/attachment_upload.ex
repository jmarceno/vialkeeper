defmodule VialKeeper.Observability.Instrumentation.AttachmentUpload do
  @moduledoc """
  Low-cardinality phase measurements for attachment upload orchestration.

  Phase metadata is a closed atom vocabulary and contains no database UUID,
  path, digest, attachment name, content type, or payload data.
  """

  @phases [
    :open_check,
    :writable_check,
    :bundle_lookup,
    :coordinator_wait,
    :physical_store,
    :pending_protection
  ]

  @typedoc "A bounded attachment-upload orchestration phase."
  @type phase ::
          :open_check
          | :writable_check
          | :bundle_lookup
          | :coordinator_wait
          | :physical_store
          | :pending_protection

  use VialKeeper.Observability.Instrumentation.TimedPhase,
    metric: :"vial_keeper.attachment.upload.phase.duration",
    event: [:vial_keeper, :attachment, :upload, :phase]
end
