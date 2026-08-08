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
      with {:ok, digest} <- Manifest.validate_digest(attrs.blob_digest),
           {:ok, content_type} <- Manifest.validate_content_type(attrs.content_type),
           {:ok, name} <- Manifest.validate_name(attrs.attachment_name),
           true <- is_binary(attrs.database_uuid),
           true <- is_binary(attrs.bundle_path),
           true <- is_binary(attrs.document_id),
           true <- is_binary(attrs.revision_id),
           true <- is_integer(attrs.logical_size) and attrs.logical_size >= 0 do
        {:ok,
         struct(__MODULE__, %{
           database_uuid: attrs.database_uuid,
           bundle_path: Path.expand(attrs.bundle_path),
           blob_digest: digest,
           logical_size: attrs.logical_size,
           content_type: content_type,
           document_id: attrs.document_id,
           revision_id: attrs.revision_id,
           attachment_name: name
         })}
      else
        false -> {:error, ElixirDB.Error.invalid_request("invalid attachment ticket fields")}
        {:error, _} = error -> error
      end
    else
      {:error, ElixirDB.Error.invalid_request("invalid attachment ticket fields")}
    end
  end

  def new(_), do: {:error, ElixirDB.Error.invalid_request("invalid attachment ticket fields")}
end
