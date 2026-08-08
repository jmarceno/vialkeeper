defmodule ElixirDB.Attachments.Ticket do
  @moduledoc "Immutable validated attachment stream ticket."

  alias ElixirDB.Attachments.Manifest

  @enforce_keys [
    :database_uuid,
    :bundle_path,
    :blob_digest,
    :logical_size,
    :content_type,
    :document_id,
    :revision_id,
    :attachment_name
  ]
  defstruct [
    :database_uuid,
    :bundle_path,
    :blob_digest,
    :logical_size,
    :content_type,
    :document_id,
    :revision_id,
    :attachment_name
  ]

  @type t :: %__MODULE__{
          database_uuid: binary(),
          bundle_path: binary(),
          blob_digest: binary(),
          logical_size: non_neg_integer(),
          content_type: binary(),
          document_id: binary(),
          revision_id: binary(),
          attachment_name: binary()
        }

  @spec build(
          binary(),
          binary(),
          binary(),
          non_neg_integer(),
          binary(),
          binary(),
          binary(),
          binary()
        ) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def build(
        database_uuid,
        bundle_path,
        blob_digest,
        logical_size,
        content_type,
        document_id,
        revision_id,
        attachment_name
      ) do
    with {:ok, digest} <- Manifest.validate_digest(blob_digest),
         {:ok, content_type} <- Manifest.validate_content_type(content_type),
         {:ok, name} <- Manifest.validate_name(attachment_name),
         true <- is_binary(database_uuid),
         true <- is_binary(bundle_path),
         true <- is_binary(document_id),
         true <- is_binary(revision_id),
         true <- is_integer(logical_size) and logical_size >= 0 do
      {:ok,
       %__MODULE__{
         database_uuid: database_uuid,
         bundle_path: Path.expand(bundle_path),
         blob_digest: digest,
         logical_size: logical_size,
         content_type: content_type,
         document_id: document_id,
         revision_id: revision_id,
         attachment_name: name
       }}
    else
      false -> {:error, ElixirDB.Error.invalid_request("invalid attachment ticket fields")}
      {:error, _} = error -> error
    end
  end

  @spec new(map()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def new(attrs) when is_map(attrs) do
    required = [
      :database_uuid,
      :bundle_path,
      :blob_digest,
      :logical_size,
      :content_type,
      :document_id,
      :revision_id,
      :attachment_name
    ]

    if Enum.all?(required, &Map.has_key?(attrs, &1)) do
      build(
        attrs.database_uuid,
        attrs.bundle_path,
        attrs.blob_digest,
        attrs.logical_size,
        attrs.content_type,
        attrs.document_id,
        attrs.revision_id,
        attrs.attachment_name
      )
    else
      {:error, ElixirDB.Error.invalid_request("invalid attachment ticket fields")}
    end
  end

  def new(_), do: {:error, ElixirDB.Error.invalid_request("invalid attachment ticket fields")}
end
