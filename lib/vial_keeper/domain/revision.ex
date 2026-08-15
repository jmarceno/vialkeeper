defmodule VialKeeper.Domain.Revision do
  @moduledoc "Validated immutable document revision state."

  alias VialKeeper.Attachments.Manifest

  @enforce_keys [
    :document_id,
    :history_id,
    :revision_id,
    :generation,
    :parent_revision,
    :deleted,
    :body,
    :attachments
  ]
  defstruct [
    :document_id,
    :history_id,
    :revision_id,
    :generation,
    :parent_revision,
    :digest,
    :deleted,
    :body,
    :attachments,
    :insertion_sequence
  ]

  @type t :: %__MODULE__{
          document_id: binary() | nil,
          history_id: binary(),
          revision_id: binary(),
          generation: pos_integer(),
          parent_revision: binary() | nil,
          digest: binary() | nil,
          deleted: boolean(),
          body: map() | nil,
          attachments: Manifest.t(),
          insertion_sequence: non_neg_integer() | nil
        }

  def new(attrs) when is_map(attrs) do
    with :ok <- validate_attrs(attrs),
         {:ok, generation} <- generation(attrs),
         :ok <- validate_body(attrs),
         {:ok, attachments} <- validate_attachments(attrs) do
      {:ok,
       struct(
         __MODULE__,
         Map.put(attrs, :generation, generation) |> Map.put(:attachments, attachments)
       )}
    end
  end

  defp validate_attrs(attrs) do
    required = [:document_id, :history_id, :revision_id, :deleted]

    if Enum.all?(required, &Map.has_key?(attrs, &1)) and is_binary(attrs.document_id) and
         is_binary(attrs.history_id) and attrs.history_id != "" and is_binary(attrs.revision_id) and
         is_boolean(attrs.deleted),
       do: :ok,
       else: {:error, VialKeeper.Error.invalid_request("invalid revision fields")}
  end

  defp generation(%{generation: generation}) when is_integer(generation) and generation > 0,
    do: {:ok, generation}

  defp generation(_),
    do: {:error, VialKeeper.Error.invalid_request("revision generation must be positive")}

  defp validate_body(%{deleted: true, body: nil}), do: :ok
  defp validate_body(%{deleted: false, body: body}) when is_map(body), do: :ok

  defp validate_body(_),
    do: {:error, VialKeeper.Error.invalid_request("revision body does not match deletion state")}

  defp validate_attachments(%{deleted: true, attachments: attachments}) do
    case attachments do
      nil -> Manifest.normalize(%{})
      other -> Manifest.normalize(other)
    end
    |> case do
      {:ok, empty} when map_size(empty) == 0 ->
        {:ok, empty}

      {:ok, _} ->
        {:error,
         VialKeeper.Error.invalid_request(
           "tombstone revisions must have an empty attachment manifest"
         )}

      {:error, _} = error ->
        error
    end
  end

  defp validate_attachments(%{attachments: attachments})
       when is_map(attachments) or is_nil(attachments) do
    Manifest.normalize(attachments || %{})
  end

  defp validate_attachments(_),
    do: {:error, VialKeeper.Error.invalid_request("revision attachments must be a map")}

  @doc false
  def assemble(fields) when is_list(fields) do
    struct!(__MODULE__, fields)
  end
end
