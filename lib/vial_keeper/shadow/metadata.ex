defmodule VialKeeper.Shadow.Metadata do
  @moduledoc "Shared immutable metadata shape for a managed shadow database."

  @type t :: %{
          source_database_uuid: binary(),
          shadow_database_uuid: binary(),
          generation: non_neg_integer(),
          operation_id: binary(),
          attachment_store_type: binary(),
          attachment_location: binary(),
          specification_digest: binary() | nil,
          created_at: binary()
        }

  @spec new(
          binary(),
          binary(),
          non_neg_integer(),
          binary(),
          binary(),
          binary(),
          binary() | nil,
          binary()
        ) :: t()
  def new(
        source_database_uuid,
        shadow_database_uuid,
        generation,
        operation_id,
        attachment_store_type,
        attachment_location,
        specification_digest,
        created_at
      ) do
    %{
      source_database_uuid: source_database_uuid,
      shadow_database_uuid: shadow_database_uuid,
      generation: generation,
      operation_id: operation_id,
      attachment_store_type: attachment_store_type,
      attachment_location: attachment_location,
      specification_digest: specification_digest,
      created_at: created_at
    }
  end
end
